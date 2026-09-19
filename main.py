import asyncio
import hmac
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, time as dtime, timedelta
from aiohttp import web
from astrbot.api.all import *
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.config.astrbot_config import AstrBotConfig

logger = logging.getLogger("astrbot")

_HM_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _is_valid_hm(text: str) -> bool:
    """校验 "HH:MM" 时间格式，避免把非法时段写进配置。"""
    return bool(_HM_RE.match(str(text or "").strip()))

ENDPOINT_PATH = "/api/sensor/event"
AUTH_HEADERS = ["X-Sensor-Token", "X-Auth-Token"]
MAX_BODY_BYTES = 1024 * 100  # 100KB

# 应用关闭类事件：只做时长结算与静默感知，永远不返回拦截指令
CLOSED_EVENT_TYPES = {"app_closed", "app_close", "closed", "close", "app_closed_event"}

TOOL_INSTRUCTIONS = """
【手机事件感知器 (Event Sensor) 工具规范】
- set_sensor_config: 修改事件感知配置（互动时间段、触发关键词、告别豁免词、冷却时间、未回复判定阈值等），立即生效无需重载。
- get_sensor_config: 查询当前的事件感知配置与实时激活状态。
- get_recent_device_events: 查询用户最近的手机上报事件/App打开与关闭记录，含每次连续使用时长（分钟）与今日分应用用量汇总，可用于推测用户最近在干什么、刷了多久手机等。
"""


@register(
    "astrbot_plugin_event_sensor",
    "mmq",
    "手机事件感知与即时唤醒插件 - 接收手机端自动化事件上报（打开/关闭App），即时唤醒角色对话并统计使用时长",
    "1.8.0",
)
class EventSensorPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        if config is None:
            config = getattr(context, "config", {})
        self.config = config
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._last_event: AstrMessageEvent | None = None
        self._last_trigger_time: float = 0.0
        self._last_user_msg_time: float = 0.0
        self._last_bot_msg_time: float = 0.0
        self._cq_bot = None
        self._bot_qq_id = str(self.config.get("bot_qq_id", "")).strip()

        # 上下文/关键词触发的临时抓包状态
        self._keyword_catch_active: bool = False
        self._keyword_trigger_time: float = 0.0
        self._keyword_trigger_text: str = ""
        self._matched_keyword: str = ""

        # 对话自然结束/告别豁免标记
        self._dialogue_ended_by_farewell: bool = False
        self._farewell_reason: str = ""

        # 本地历史事件记录路径
        self._history_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "events_history.json")

        # v1.8.0：应用使用时长统计（记录每个应用本轮的打开时间点，纯内存）
        self._app_open_times: dict[str, float] = {}
        # v1.8.0：护眼关怀注入冷却（避免同一次深夜刷手机被反复念叨）
        self._last_care_time: float = 0.0

    def _record_event_to_history(
        self,
        app_name: str,
        raw_data: dict,
        status: str = "received",
        duration_seconds: int | None = None,
        duration_minutes: float | None = None,
    ) -> None:
        """记录手机上报事件到本地历史文件。

        保留窗口由 history_keep_days 决定（默认 1，即只留当天），
        另有 history_max_records 作为条数保底，防止高频上报把文件撑爆。

        v1.8.0：关闭事件会额外带上本次连续使用时长（秒 / 分钟）。
        """
        try:
            history = self._read_history()
            entry = {
                "timestamp": int(time.time()),
                "time_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "app_name": app_name,
                "event_type": str(raw_data.get("event") or "blocked_app_opened").strip().lower(),
                "status": status,
            }
            if duration_seconds is not None:
                entry["duration_seconds"] = int(duration_seconds)
                entry["duration_minutes"] = round(float(duration_minutes or 0.0), 1)
            history.append(entry)
            self._write_history(self._prune_history(history))
        except Exception:
            logger.exception("[event_sensor] 记录历史事件失败")

    def _read_history(self) -> list[dict]:
        """读取本地历史事件文件；文件缺失或损坏时当作空历史。"""
        if not os.path.exists(self._history_file):
            return []
        try:
            with open(self._history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
            return history if isinstance(history, list) else []
        except Exception:
            return []

    def _write_history(self, history: list[dict]) -> None:
        """原子写入：先落 .tmp 再替换，避免中途失败把历史文件写坏。"""
        tmp = f"{self._history_file}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._history_file)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _history_keep_days(self) -> int:
        """历史事件保留天数，默认 1（只留当天）。"""
        try:
            days = int(self._get_cfg("history_keep_days", 1))
        except (TypeError, ValueError):
            return 1
        return days if days >= 1 else 1

    def _history_max_records(self) -> int:
        """历史事件条数保底上限，默认 500。"""
        try:
            n = int(self._get_cfg("history_max_records", 500))
        except (TypeError, ValueError):
            return 500
        return n if n >= 1 else 500

    def _prune_history(self, history: list) -> list[dict]:
        """裁掉保留窗口之外的记录，只留最近 keep_days 天。

        基准取服务器当前时间（每条记录的 time_str 就是接收时刻），
        时间戳缺失或格式不对的脏记录直接丢弃。
        """
        cutoff = (datetime.now() - timedelta(days=self._history_keep_days() - 1)).date()
        kept: list[dict] = []
        for item in history:
            if not isinstance(item, dict):
                continue
            try:
                day = datetime.strptime(str(item.get("time_str", ""))[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            if day >= cutoff:
                kept.append(item)
        return kept[-self._history_max_records():]

    def _prune_history_file(self) -> None:
        """启动时清一次历史，过期记录直接裁掉，日常不用手动管。"""
        try:
            if not os.path.exists(self._history_file):
                return
            history = self._read_history()
            kept = self._prune_history(history)
            if len(kept) == len(history):
                return
            self._write_history(kept)
            logger.info(
                f"[event_sensor] 历史事件已按天清理：{len(history)} -> {len(kept)} 条"
            )
        except Exception:
            logger.exception("[event_sensor] 清理历史事件失败")

    def _get_recent_history(self, limit: int = 10) -> list[dict]:
        """获取最近的历史事件记录"""
        return self._read_history()[-limit:]

    # ==================== v1.8.0 使用时长统计 ====================

    def _duration_bounds(self) -> tuple[int, float]:
        """本次使用的有效时长区间（秒）：下限过滤误触，上限过滤跨天脏数据。"""
        try:
            min_sec = int(self._get_cfg("duration_min_seconds", 10))
        except (TypeError, ValueError):
            min_sec = 10
        try:
            max_sec = float(self._get_cfg("duration_max_hours", 12)) * 3600.0
        except (TypeError, ValueError):
            max_sec = 12 * 3600.0
        if min_sec <= 0:
            min_sec = 10
        if max_sec <= 0:
            max_sec = 12 * 3600.0
        return min_sec, max_sec

    def _note_app_opened(self, app_name: str, now: float | None = None) -> None:
        """打开事件：登记本轮使用的起始时间。

        同一个应用可能连续上报多次打开事件，这里只保留最早的一次；
        只有距上次记录已经超过有效时长上限时，才当作新的一轮使用重新计时。
        """
        if not app_name:
            return
        now = time.time() if now is None else now
        _, max_sec = self._duration_bounds()
        prev = self._app_open_times.get(app_name)
        if prev is None or (now - prev) > max_sec:
            self._app_open_times[app_name] = now

    def _consume_app_closed(self, app_name: str, now: float | None = None) -> int | None:
        """关闭事件：结算本轮使用时长（秒）。

        结算后立即清除打开标记，避免重复计算；跨度过短（误触）或过长（跨天脏数据）
        一律返回 None，表示本轮无效、不写时长。
        """
        if not app_name:
            return None
        now = time.time() if now is None else now
        prev = self._app_open_times.pop(app_name, None)
        if prev is None:
            return None
        duration = now - prev
        min_sec, max_sec = self._duration_bounds()
        if duration < min_sec or duration > max_sec:
            return None
        return int(duration)

    def _summarize_today_usage(self) -> dict:
        """按应用汇总「今天已结算」的使用时长（分钟），用于回答她今天刷了多久。"""
        today = datetime.now().strftime("%Y-%m-%d")
        by_app: dict[str, float] = {}
        for item in self._read_history():
            if not isinstance(item, dict):
                continue
            if not str(item.get("time_str", "")).startswith(today):
                continue
            try:
                minutes = float(item.get("duration_minutes"))
            except (TypeError, ValueError):
                continue
            name = str(item.get("app_name") or "未知应用")
            by_app[name] = round(by_app.get(name, 0.0) + minutes, 1)
        ordered = dict(sorted(by_app.items(), key=lambda kv: kv[1], reverse=True))
        return {
            "date": today,
            "total_minutes": round(sum(by_app.values()), 1),
            "by_app": ordered,
        }

    # ==================== v1.8.0 护眼关怀（静默感知，非抓包） ====================

    def _maybe_trigger_closed_care(self, app_name: str, duration_min: float) -> bool:
        """退出应用后，判断是否值得温和提一句（长时使用或深夜退出）。

        返回是否真的注入了关怀。注意：这只是自然提醒，不会设置 locked / triggered，
        更不会让手机端执行任何跳转拦截。
        """
        if not bool(self._get_cfg("enable_closed_care", True)):
            return False

        try:
            threshold = float(self._get_cfg("care_duration_threshold_minutes", 45))
        except (TypeError, ValueError):
            threshold = 45.0

        is_long = threshold > 0 and duration_min >= threshold
        is_late_night = self._in_time_range(
            self._get_cfg("care_late_night_start", "00:00"),
            self._get_cfg("care_late_night_end", "06:00"),
        )
        if not is_long and not is_late_night:
            return False

        # 默认只在常规互动时间段内插话，避免角色「睡着」时被后台事件吵醒
        if bool(self._get_cfg("care_require_active_window", True)) and not self._in_time_range(
            self._get_cfg("active_start_time", "08:00"),
            self._get_cfg("active_end_time", "23:30"),
        ):
            return False

        try:
            cooldown_sec = float(self._get_cfg("care_cooldown_minutes", 60)) * 60.0
        except (TypeError, ValueError):
            cooldown_sec = 3600.0

        now = time.time()
        if cooldown_sec > 0 and (now - self._last_care_time) < cooldown_sec:
            logger.info("[event_sensor] 护眼关怀仍在冷却期，本次仅在后台记账")
            return False
        self._last_care_time = now

        asyncio.create_task(
            self._trigger_closed_care(app_name, duration_min, is_late_night)
        )
        return True

    async def _trigger_closed_care(
        self, app_name: str, duration_min: float, is_late_night: bool,
    ) -> None:
        """注入一句温和的护眼关怀：只说人不拦人，不触发任何强制跳转。"""
        minutes = int(round(duration_min))
        late_hint = "（而且是深夜）" if is_late_night else ""
        default_prompt = (
            f"【系统静默感知事件（应用使用时长）】对方刚刚退出了「{app_name}」{late_hint}，"
            f"这一轮连续停留了约 {minutes} 分钟。这是后台隐私信息，仅供你参考，用来判断她现在的状态。"
            f"你可以选择不提、继续做你原本正在做的事；如果觉得时机自然，也可以随口关心一句"
            f"（比如眼睛累不累、该歇歇了、明天还要早起），但不要说教、不要盘问、不要显得在监控她。"
            f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
        )
        try:
            total_minutes = self._summarize_today_usage().get("by_app", {}).get(app_name, minutes)
        except Exception:
            logger.exception("[event_sensor] 汇总今日使用时长失败，护眼关怀退回单次时长")
            total_minutes = minutes

        prompt = self._safe_format(
            self._get_cfg("prompt_app_closed_care", ""),
            default_prompt,
            app_name=app_name,
            minutes=minutes,
            duration_minutes=minutes,
            total_minutes=total_minutes,
            time_str=datetime.now().strftime("%H:%M"),
        )
        await self._trigger_event_wakeup(app_name, {"event": "app_closed"}, fixed_prompt=prompt)

    def _safe_format(self, template: str, default: str, **kwargs) -> str:
        """安全渲染提示词模板：模板为空用默认文案，占位符写错也不至于让任务崩掉。"""
        tpl = str(template or "").strip()
        if not tpl:
            return default
        try:
            return tpl.format(**kwargs)
        except (KeyError, IndexError, ValueError):
            logger.warning("[event_sensor] 提示词模板占位符异常，已回退默认文案")
            return default

    def _in_time_range(self, start_str: str, end_str: str) -> bool:
        """判断当前时间是否落在 [start, end] 区间内（自动处理跨夜），解析失败视为全天。"""
        start_str = str(start_str or "").strip()
        end_str = str(end_str or "").strip()
        if not start_str or not end_str:
            return True
        try:
            sh, sm = map(int, start_str.split(":"))
            eh, em = map(int, end_str.split(":"))
            t_start = dtime(sh, sm)
            t_end = dtime(eh, em)
        except Exception:
            return True

        now_t = datetime.now().time()
        if t_start <= t_end:
            return t_start <= now_t <= t_end
        return now_t >= t_start or now_t <= t_end

    def _get_cfg(self, key: str, default: any = None) -> any:
        val = self.config.get(key)
        if val is None:
            cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_config.json")
            if os.path.exists(cfg_path):
                try:
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        val = data.get(key)
                except Exception:
                    pass
        return val if val is not None else default

    def _save_cfg_key(self, key: str, val: any) -> None:
        self.config[key] = val
        try:
            self.config.save_config()
        except Exception:
            pass
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_config.json")
        try:
            data = {}
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            data[key] = val
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _get_keywords(self) -> list[str]:
        raw = str(self._get_cfg("trigger_keywords", "老公晚安,晚安,睡觉了,去睡了,睡觉觉,做梦去啦")).strip()
        if not raw:
            return []
        parts = re.split(r"[,，\s]+", raw)
        return [p.strip() for p in parts if p.strip()]

    def _get_farewell_keywords(self) -> list[str]:
        raw = str(self._get_cfg("dialogue_end_keywords", "去玩会儿手机,去玩手机,玩会手机,先去忙了,去忙了,去洗澡,去吃饭,出门了,先下了,拜拜")).strip()
        if not raw:
            return []
        parts = re.split(r"[,，\s]+", raw)
        return [p.strip() for p in parts if p.strip()]

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req):
        try:
            if hasattr(req, "system_prompt") and req.system_prompt is not None:
                req.system_prompt = (req.system_prompt or "") + "\n" + TOOL_INSTRUCTIONS.strip()
            else:
                setattr(
                    req, "system_prompt",
                    (getattr(req, "system_prompt", "") or "") + "\n" + TOOL_INSTRUCTIONS.strip(),
                )
        except Exception:
            pass

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message_record(self, event: AstrMessageEvent):
        if hasattr(event, "bot") and event.bot:
            self._cq_bot = event.bot
        try:
            self_id = str(event.get_self_id() or "")
            if self_id and self_id != "None":
                self._bot_qq_id = self_id
        except Exception:
            pass

        sender_id = str(event.get_sender_id() or "")
        self_id_str = str(event.get_self_id() or "")

        now = time.time()
        if sender_id and self_id_str and sender_id == self_id_str:
            self._last_bot_msg_time = now
            return

        if not sender_id:
            return

        text = str(event.message_str or "").strip()
        sender_name = str(getattr(event, "sender", None) and getattr(event.sender, "nickname", "") or "")
        if (
            sender_name == "wakeup"
            or sender_name == "event_sensor"
            or text.startswith("【系统")
            or text.startswith("【系统实时感知事件")
            or text.startswith("「")
            or text.startswith("设定的休眠时间已到")
            or getattr(event, "role", "") == "system"
        ):
            return

        self._last_user_msg_time = now
        self._last_event = event
        umo = getattr(event, "unified_msg_origin", None)
        if umo:
            self._target_umo = umo

        if text:
            self._dialogue_ended_by_farewell = False
            self._farewell_reason = ""

            farewells = self._get_farewell_keywords()
            for fw in farewells:
                if fw in text:
                    self._dialogue_ended_by_farewell = True
                    self._farewell_reason = fw
                    logger.info(f"[event_sensor] ☕ 捕捉到告别/暂离词「{fw}」，已标记正常结束对话（豁免未回复抓包）| 原文: {text}")
                    break

            keywords = self._get_keywords()
            for kw in keywords:
                if kw in text:
                    self._keyword_catch_active = True
                    self._keyword_trigger_time = now
                    self._keyword_trigger_text = text
                    self._matched_keyword = kw
                    logger.info(f"[event_sensor] 🎯 捕捉到关键词「{kw}」，临时抓包模式已激活！原文: {text}")
                    break
        else:
            self._keyword_catch_active = False

    @filter.on_decorating_result()
    async def on_decorating_result_record(self, event: AstrMessageEvent):
        self._last_bot_msg_time = time.time()

    async def initialize(self) -> None:
        self._ensure_auth_token()
        self._prune_history_file()
        await self._start_server()

    async def terminate(self) -> None:
        await self._stop_server()

    def _get_active_token(self) -> str:
        return str(self._get_cfg("auth_token", "") or "").strip()

    def _ensure_auth_token(self) -> None:
        token = self._get_active_token()
        if token:
            self.config["auth_token"] = token
            return
        new_token = secrets.token_urlsafe(24)
        self._save_cfg_key("auth_token", new_token)
        logger.warning(
            "[event_sensor] 未配置 auth_token，已自动生成并写入配置。"
            "请在 WebUI 插件配置中查看 auth_token 并填入手机端。"
        )

    async def _start_server(self) -> None:
        await self._stop_server()
        try:
            port = int(self._get_cfg("listen_port", 8788))
        except (TypeError, ValueError):
            port = 8788

        runner = None
        try:
            app = web.Application(client_max_size=MAX_BODY_BYTES)
            app.router.add_post(ENDPOINT_PATH, self._handle_event_report)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host="0.0.0.0", port=port)
            await site.start()
            self._app, self._runner, self._site = app, runner, site
            logger.info(
                f"[event_sensor] 事件感知接收端已启动：监听 0.0.0.0:{port}{ENDPOINT_PATH}（POST）"
            )
        except Exception:
            logger.exception(f"[event_sensor] 启动失败（端口 {port} 可能被占用）")
            if runner is not None:
                try:
                    await runner.cleanup()
                except Exception:
                    pass

    async def _stop_server(self) -> None:
        site, runner, app = self._site, self._runner, self._app
        self._site, self._runner, self._app = None, None, None
        if site is not None:
            try:
                await site.stop()
            except Exception:
                pass
        if runner is not None:
            try:
                await runner.cleanup()
            except Exception:
                pass

    def _is_in_active_time(self) -> bool:
        """是否处于常规互动时间段；顺带把仅在时间段内有意义的临时抓包状态重置掉。"""
        in_range = self._in_time_range(
            self._get_cfg("active_start_time", "08:00"),
            self._get_cfg("active_end_time", "23:30"),
        )

        if in_range and self._keyword_catch_active:
            self._keyword_catch_active = False

        return in_range

    async def _handle_event_report(self, request: web.Request) -> web.Response:
        try:
            token = self._get_active_token()
            provided = ""
            for h in AUTH_HEADERS:
                if h in request.headers:
                    provided = request.headers[h]
                    break

            if not token:
                return web.json_response({"ok": False, "error": "server not configured"}, status=401)
            if not provided or not hmac.compare_digest(provided, token):
                return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

            raw = await request.read()
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return web.json_response({"ok": False, "error": "bad json"}, status=400)

            logger.info(f"[event_sensor] 收到手机事件上报: {data}")
            app_name = str(data.get("app_name") or data.get("event") or "某个应用").strip()
            event_type = str(data.get("event") or "").strip().lower()
            now = time.time()

            # v1.8.0：应用关闭事件独立分流——只结算时长/静默感知，绝不拦截
            if event_type in CLOSED_EVENT_TYPES:
                return self._handle_app_closed(app_name, data, now)

            # 打开事件：先登记本轮使用起点（不受时间段限制），再走原有抓包判定
            self._note_app_opened(app_name, now)

            # 记录到本地事件历史文件
            self._record_event_to_history(app_name, data, status="received")

            # 1. 常规互动时间段
            in_active_time = self._is_in_active_time()

            # 2. 关键词临时激活抓包（晚安/睡觉）：最多持续 5.5 小时或到早上互动开始
            is_keyword_active = False
            keyword_diff_min = 0
            if self._keyword_catch_active:
                diff = now - self._keyword_trigger_time
                if 30 <= diff <= 5.5 * 3600:
                    is_keyword_active = True
                    keyword_diff_min = int(diff // 60)
                    # 抓包触发成功后立即重置装睡状态，避免后续连续刷屏
                    self._keyword_catch_active = False
                else:
                    self._keyword_catch_active = False

            # 3. 超时未回复抓包（前提：没有触发告别/自然结束豁免）
            enable_unreplied = bool(self._get_cfg("enable_unreplied_trigger", True))
            unreplied_thresh = float(self._get_cfg("unreplied_threshold_minutes", 30)) * 60.0

            is_unreplied_catch = False
            unreplied_duration_min = 0
            if enable_unreplied and not self._dialogue_ended_by_farewell:
                # 判定条件：以用户上次发言时间为基准（类似 wakeup），只要用户发完消息后超过阈值未再次回复
                if self._last_user_msg_time > 0:
                    diff = now - self._last_user_msg_time
                    if diff >= unreplied_thresh:
                        is_unreplied_catch = True
                        unreplied_duration_min = int(diff // 60)
            elif self._dialogue_ended_by_farewell:
                logger.info(f"[event_sensor] 处于告别/暂离豁免期（原因: {self._farewell_reason}），跳过未回复抓包判定")

            logger.info(
                f"[event_sensor] 判定状态: in_active={in_active_time}, is_kw_catch={is_keyword_active}({keyword_diff_min}m), "
                f"is_unreplied={is_unreplied_catch}({unreplied_duration_min}m), farewell_exempt={self._dialogue_ended_by_farewell}"
            )

            # 判定放行：在常规时间段 OR 触发了关键词临时抓包 OR 满足超时未回复抓包
            if not in_active_time and not is_keyword_active and not is_unreplied_catch:
                logger.info(f"[event_sensor] 当前非互动时间段且未满足特定场景抓包条件，静默忽略: {app_name}")
                return web.json_response({"ok": True, "status": "ignored_outside_active_time", "locked": 0, "triggered": False})

            # 4. 检查冷却时间 (转换为秒判断)
            cooldown_min = self._get_cfg("cooldown_minutes", None)
            if cooldown_min is None:
                cooldown_sec = int(self._get_cfg("cooldown_seconds", 0))
            else:
                cooldown_sec = int(cooldown_min) * 60

            if cooldown_sec > 0 and (now - self._last_trigger_time < cooldown_sec):
                logger.info(f"[event_sensor] 仍在冷却期（{int(now - self._last_trigger_time)}s < {cooldown_sec}s），静默忽略: {app_name}")
                return web.json_response({"ok": True, "status": "ignored_in_cooldown", "locked": 0, "triggered": False})

            self._last_trigger_time = now

            # 触发唤醒/注入
            asyncio.create_task(
                self._trigger_event_wakeup(
                    app_name,
                    data,
                    is_unreplied=is_unreplied_catch,
                    unreplied_minutes=unreplied_duration_min,
                    is_keyword_catch=is_keyword_active,
                    keyword_minutes=keyword_diff_min,
                    keyword_text=self._keyword_trigger_text,
                    matched_keyword=self._matched_keyword,
                )
            )
            force_redirect = bool(self._get_cfg("enable_force_redirect", True))
            locked_val = 1 if force_redirect else 0
            return web.json_response({
                "ok": True,
                "status": "triggered",
                "locked": locked_val,
                "force_redirect": force_redirect,
                "triggered": True,
                "received": data
            })
        except web.HTTPException:
            raise
        except Exception:
            logger.exception("[event_sensor] 处理事件上报异常")
            return web.json_response({"ok": False, "error": "internal error"}, status=500)

    def _handle_app_closed(self, app_name: str, raw_data: dict, now: float) -> web.Response:
        """处理应用关闭事件。

        关闭一律返回 locked=0 / triggered=False：用户主动退出应用不该被弹回，
        否则会形成「关掉软件立刻被拽回聊天软件」的死循环。这里只做两件事：
        结算本轮使用时长、必要时补一句静默的护眼关怀。
        """
        if not bool(self._get_cfg("enable_closed_event", True)):
            return web.json_response({
                "ok": True,
                "status": "closed_ignored",
                "locked": 0,
                "triggered": False,
                "received": raw_data,
            })

        duration_sec = self._consume_app_closed(app_name, now)
        duration_min = round(duration_sec / 60.0, 1) if duration_sec else 0.0
        self._record_event_to_history(
            app_name,
            raw_data,
            status="closed",
            duration_seconds=duration_sec,
            duration_minutes=duration_min,
        )

        care = False
        if duration_sec is not None:
            care = self._maybe_trigger_closed_care(app_name, duration_min)
            logger.info(
                f"[event_sensor] ⏱️ 已结算 {app_name} 本轮使用时长 {duration_min} 分钟（关怀注入={care}）"
            )
        else:
            logger.info(
                f"[event_sensor] {app_name} 关闭事件：无有效打开记录或时长超出有效区间，仅记录不结算"
            )

        return web.json_response({
            "ok": True,
            "status": "closed",
            "locked": 0,
            "triggered": False,
            "duration_seconds": duration_sec or 0,
            "duration_minutes": duration_min,
            "care_triggered": care,
            "received": raw_data,
        })

    async def _trigger_event_wakeup(
        self,
        app_name: str,
        raw_data: dict,
        is_unreplied: bool = False,
        unreplied_minutes: int = 0,
        is_keyword_catch: bool = False,
        keyword_minutes: int = 0,
        keyword_text: str = "",
        matched_keyword: str = "",
        fixed_prompt: str = "",
    ) -> None:
        umo = None
        bot_self_id = str(self._bot_qq_id or "")
        cq_bot = self._cq_bot

        if self._last_event:
            umo = self._last_event.unified_msg_origin
            if not cq_bot and hasattr(self._last_event, "bot"):
                cq_bot = self._last_event.bot

        if not cq_bot:
            try:
                plat_mgr = getattr(self.context, "_platform_manager", None) or getattr(self.context, "platform_manager", None)
                if plat_mgr:
                    for p in getattr(plat_mgr, "platforms", []):
                        if hasattr(p, "bot") and p.bot:
                            cq_bot = p.bot
                            break
            except Exception:
                pass

        if not umo:
            logger.warning("[event_sensor] 尚未收到任何用户消息，无法确定会话目标")
            return

        parts = umo.rsplit(":", 2)
        if len(parts) < 3:
            return
        session_id = parts[2]
        msg_type_str = parts[1]
        is_group = "Group" in msg_type_str

        if fixed_prompt:
            prompt = fixed_prompt
        elif is_keyword_catch:
            prompt = self._safe_format(
                self._get_cfg("prompt_keyword_catch", ""),
                (
                    f"【系统实时感知事件（关键词触发提前抓包/装睡抓包）】"
                    f"对方在 {keyword_minutes} 分钟前发了「{keyword_text}」（触发了关键词「{matched_keyword}」），"
                    f"此时系统检测到对方并没有入睡或离开，而是在手机上打开了「{app_name}」。"
                    f"请结合当前对话上下文与你的角色人设性格，自然地抓包、调侃或逗弄对方（比如：抓到刚才说过晚安却在偷偷刷手机）。"
                    f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
                ),
                minutes=keyword_minutes,
                text=keyword_text,
                keyword=matched_keyword,
                app_name=app_name,
            )
        elif is_unreplied:
            prompt = self._safe_format(
                self._get_cfg("prompt_unreplied_catch", ""),
                (
                    f"【系统实时感知事件（已读不回/超时未回复抓包）】你发完上一条消息后，对方已有超过 {unreplied_minutes} 分钟没有回复你，"
                    f"但此时检测到对方在手机上打开了「{app_name}」。"
                    f"请结合当前对话上下文与你的角色人设性格，自然地抓包、吃醋、调侃或逗弄对方（比如质问怎么有空刷手机却不理你）。"
                    f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
                ),
                minutes=unreplied_minutes,
                app_name=app_name,
            )
        else:
            prompt = (
                f"【系统静默感知事件】检测到对方在手机上打开了「{app_name}」。"
                f"这是后台隐私信息，仅供你参考，用于判断她现在的状态。"
                f"你可以选择不提，继续做你原本正在做的事；"
                f"如果觉得时机合适，也可以用非常自然、随口一提的方式带过，不要盘问、不要说教、不要显得在监控她。"
                f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
            )
        try:
            from aiocqhttp import Event as CQEvent
        except ImportError:
            logger.warning("[event_sensor] 未找到 aiocqhttp，跳过伪造注入")
            return

        if is_group:
            if "_" in session_id:
                uid, gid = session_id.rsplit("_", 1)
            else:
                return
            payload = {
                "post_type": "message",
                "message_type": "group",
                "sub_type": "normal",
                "message_id": int(time.time()) % 2147483647,
                "group_id": int(gid),
                "user_id": int(uid),
                "message": [{"type": "text", "data": {"text": prompt}}],
                "raw_message": prompt,
                "font": 0,
                "sender": {"user_id": int(uid), "nickname": "event_sensor", "card": ""},
                "time": int(time.time()),
                "self_id": int(bot_self_id),
            }
        else:
            payload = {
                "post_type": "message",
                "message_type": "private",
                "sub_type": "friend",
                "message_id": int(time.time()) % 2147483647,
                "user_id": int(session_id),
                "message": [{"type": "text", "data": {"text": prompt}}],
                "raw_message": prompt,
                "font": 0,
                "sender": {"user_id": int(session_id), "nickname": "event_sensor", "sex": "unknown", "age": 0},
                "time": int(time.time()),
                "self_id": int(bot_self_id),
            }

        fake_event = CQEvent.from_payload(payload)
        if not fake_event:
            return

        if cq_bot:
            handler = getattr(cq_bot, "_handle_event", None) or getattr(cq_bot, "handle_event", None)
            if handler:
                await handler(fake_event)
                logger.info(f"[event_sensor] 🎯 已成功注入事件感知消息 | app={app_name} | umo={umo} | kw={is_keyword_catch}")
            else:
                logger.warning("[event_sensor] cq_bot 没有可用 handle_event 方法")
        else:
            logger.warning("[event_sensor] 未获取到 cq_bot 实例，无法注入")

    # ==================== Commands & LLM Tools ====================

    @filter.command("抓包关键词")
    async def cmd_show_keywords(self, event: AstrMessageEvent):
        """查看当前配置的抓包关键词与豁免词"""
        t_kws = self._get_keywords()
        f_kws = self._get_farewell_keywords()
        t_str = "、".join(t_kws) if t_kws else "无"
        f_str = "、".join(f_kws) if f_kws else "无"
        st = self._get_cfg("active_start_time", "08:00")
        et = self._get_cfg("active_end_time", "23:30")
        thresh = self._get_cfg("unreplied_threshold_minutes", 30)
        cooldown = self._get_cfg("cooldown_minutes", 0)
        closed_on = bool(self._get_cfg("enable_closed_event", True))
        care_on = bool(self._get_cfg("enable_closed_care", True))
        care_cd = self._get_cfg("care_cooldown_minutes", 60)
        care_thresh = self._get_cfg("care_duration_threshold_minutes", 45)
        night_start = self._get_cfg("care_late_night_start", "00:00")
        night_end = self._get_cfg("care_late_night_end", "06:00")

        msg = (
            f"📱【手机事件感知器·关键词与配置】\n\n"
            f"🌙 抓包触发词（激活装睡抓包）：\n{t_str}\n\n"
            f"☕ 告别豁免词（豁免已读不回）：\n{f_str}\n\n"
            f"⏰ 互动时间段：{st} ~ {et}\n"
            f"⌛ 未回复判定时长：{thresh} 分钟\n"
            f"🧊 防刷冷却时长：{cooldown} 分钟\n"
            f"⏱️ 关闭事件时长统计：{'开启' if closed_on else '关闭'}\n"
            f"👁️ 护眼关怀：{'开启' if care_on else '关闭'}"
            f"（长时阈值 {care_thresh} 分钟｜冷却 {care_cd} 分钟｜深夜 {night_start} ~ {night_end}）"
        )
        yield event.plain_result(msg)

    @filter.llm_tool(name="set_sensor_config")
    async def tool_set_config(
        self,
        event: AstrMessageEvent,
        active_start_time: str = "",
        active_end_time: str = "",
        trigger_keywords: str = "",
        dialogue_end_keywords: str = "",
        cooldown_minutes: int = -1,
        enable_unreplied_trigger: str = "",
        enable_force_redirect: str = "",
        unreplied_threshold_minutes: int = -1,
        deactivate_keyword_catch: str = "",
        enable_closed_care: str = "",
        care_duration_threshold_minutes: int = -1,
        care_cooldown_minutes: int = -1,
        care_late_night_start: str = "",
        care_late_night_end: str = "",
        care_require_active_window: str = "",
    ):
        """修改手机事件感知器（Event Sensor）的配置，修改后立即热生效，无需重载插件。

        Args:
            active_start_time(string): 互动开始时间，格式 "HH:MM"。不改传空。
            active_end_time(string): 互动结束时间，格式 "HH:MM"。不改传空。
            trigger_keywords(string): 提前激活抓包关键词（例如 "老公晚安,晚安,睡觉了"）。不改传空。
            dialogue_end_keywords(string): 告别/暂离豁免关键词（例如 "去玩手机,先去忙了,去吃饭"）。不改传空。
            cooldown_minutes(number): 抓包防刷冷却时间（分钟），0 表示无冷却。不改传 -1。
            enable_unreplied_trigger(string): 是否开启超时未回复抓包，"true" 或 "false"。不改传空。
            enable_force_redirect(string): 是否开启抓包强行切回/跳转QQ，"true" 或 "false"。不改传空。
            unreplied_threshold_minutes(number): 超时未回复判定时长（分钟）。不改传 -1。
            deactivate_keyword_catch(string): 是否立即关闭当前已被激活的临时装睡/晚安抓包状态，"true" 或 "false"。不改传空。
            enable_closed_care(string): 是否开启「退出应用后的护眼关怀」（长时使用/深夜退出的温和提醒），"true" 或 "false"。不改传空。
            care_duration_threshold_minutes(number): 单次使用多久算长（触发护眼关怀，分钟）。不改传 -1。
            care_cooldown_minutes(number): 护眼关怀两条提醒之间的最短间隔（分钟），0 表示不冷却。不改传 -1。
            care_late_night_start(string): 深夜时段起点，格式 "HH:MM"（例如 "00:00"）。不改传空。
            care_late_night_end(string): 深夜时段终点，格式 "HH:MM"（例如 "06:00"）。不改传空。
            care_require_active_window(string): 是否只允许在互动时间段内发送护眼关怀，"true" 或 "false"。不改传空。

        Returns:
            操作结果字典。
        """
        changes = []
        if deactivate_keyword_catch.strip().lower() in ("true", "1", "yes", "on", "关闭", "退出"):
            self._keyword_catch_active = False
            self._keyword_trigger_text = ""
            self._matched_keyword = ""
            changes.append("当前临时装睡抓包状态已手动重置关闭")
        if active_start_time.strip():
            st = active_start_time.strip()
            self._save_cfg_key("active_start_time", st)
            changes.append(f"互动开始时间 -> {st}")

        if active_end_time.strip():
            et = active_end_time.strip()
            self._save_cfg_key("active_end_time", et)
            changes.append(f"互动结束时间 -> {et}")

        if trigger_keywords.strip():
            kw = trigger_keywords.strip()
            self._save_cfg_key("trigger_keywords", kw)
            changes.append(f"触发关键词 -> {kw}")

        if dialogue_end_keywords.strip():
            dk = dialogue_end_keywords.strip()
            self._save_cfg_key("dialogue_end_keywords", dk)
            changes.append(f"告别豁免词 -> {dk}")

        if cooldown_minutes >= 0:
            self._save_cfg_key("cooldown_minutes", int(cooldown_minutes))
            changes.append(f"冷却时长 -> {cooldown_minutes}分钟")

        if enable_unreplied_trigger.strip():
            val = enable_unreplied_trigger.strip().lower() in ("true", "1", "yes", "on", "开启")
            self._save_cfg_key("enable_unreplied_trigger", val)
            changes.append(f"未回复抓包 -> {'开启' if val else '关闭'}")

        if enable_force_redirect.strip():
            val = enable_force_redirect.strip().lower() in ("true", "1", "yes", "on", "开启")
            self._save_cfg_key("enable_force_redirect", val)
            changes.append(f"抓包强行切回/跳转 -> {'开启' if val else '关闭'}")

        if unreplied_threshold_minutes > 0:
            self._save_cfg_key("unreplied_threshold_minutes", int(unreplied_threshold_minutes))
            changes.append(f"未回复判定时长 -> {unreplied_threshold_minutes}分钟")

        if enable_closed_care.strip():
            val = enable_closed_care.strip().lower() in ("true", "1", "yes", "on", "开启")
            self._save_cfg_key("enable_closed_care", val)
            changes.append(f"退出应用护眼关怀 -> {'开启' if val else '关闭'}")

        if care_duration_threshold_minutes > 0:
            self._save_cfg_key("care_duration_threshold_minutes", int(care_duration_threshold_minutes))
            changes.append(f"长时使用判定 -> {care_duration_threshold_minutes}分钟")

        if care_cooldown_minutes >= 0:
            self._save_cfg_key("care_cooldown_minutes", int(care_cooldown_minutes))
            changes.append(f"护眼关怀冷却 -> {care_cooldown_minutes}分钟")

        if care_late_night_start.strip():
            ns = care_late_night_start.strip()
            if _is_valid_hm(ns):
                self._save_cfg_key("care_late_night_start", ns)
                changes.append(f"深夜时段起点 -> {ns}")
            else:
                changes.append(f"深夜时段起点 {ns} 格式非法（应为 HH:MM），已忽略")

        if care_late_night_end.strip():
            ne = care_late_night_end.strip()
            if _is_valid_hm(ne):
                self._save_cfg_key("care_late_night_end", ne)
                changes.append(f"深夜时段终点 -> {ne}")
            else:
                changes.append(f"深夜时段终点 {ne} 格式非法（应为 HH:MM），已忽略")

        if care_require_active_window.strip():
            val = care_require_active_window.strip().lower() in ("true", "1", "yes", "on", "开启")
            self._save_cfg_key("care_require_active_window", val)
            changes.append(f"护眼关怀仅在互动时间段内 -> {'开启' if val else '关闭'}")

        if not changes:
            return {"ok": False, "msg": "未传入任何需要修改的配置项"}

        return {
            "ok": True,
            "message": f"事件感知配置已更新并立即生效：{', '.join(changes)}",
            "current_config": {
                "active_start_time": self._get_cfg("active_start_time", "08:00"),
                "active_end_time": self._get_cfg("active_end_time", "23:30"),
                "trigger_keywords": self._get_cfg("trigger_keywords", "老公晚安,晚安,睡觉了,去睡了,睡觉觉,做梦去啦"),
                "dialogue_end_keywords": self._get_cfg("dialogue_end_keywords", "去玩会儿手机,去玩手机,玩会手机,先去忙了,去忙了,去洗澡,去吃饭,出门了,先下了,拜拜"),
                "cooldown_minutes": self._get_cfg("cooldown_minutes", 0),
                "enable_unreplied_trigger": self._get_cfg("enable_unreplied_trigger", True),
                "unreplied_threshold_minutes": self._get_cfg("unreplied_threshold_minutes", 30),
            }
        }

    @filter.llm_tool(name="get_sensor_config")
    async def tool_get_config(self, event: AstrMessageEvent):
        """查询手机事件感知器（Event Sensor）当前的配置与运行状态。"""
        return {
            "ok": True,
            "config": {
                "active_start_time": self._get_cfg("active_start_time", "08:00"),
                "active_end_time": self._get_cfg("active_end_time", "23:30"),
                "trigger_keywords": self._get_cfg("trigger_keywords", "老公晚安,晚安,睡觉了,去睡了,睡觉觉,做梦去啦"),
                "dialogue_end_keywords": self._get_cfg("dialogue_end_keywords", "去玩会儿手机,去玩手机,玩会手机,先去忙了,去忙了,去洗澡,去吃饭,出门了,先下了,拜拜"),
                "cooldown_minutes": self._get_cfg("cooldown_minutes", 0),
                "enable_unreplied_trigger": self._get_cfg("enable_unreplied_trigger", True),
                "unreplied_threshold_minutes": self._get_cfg("unreplied_threshold_minutes", 30),
                "keyword_catch_active": self._keyword_catch_active,
                "dialogue_ended_by_farewell": self._dialogue_ended_by_farewell,
                "farewell_reason": self._farewell_reason or "无",
                "enable_closed_event": self._get_cfg("enable_closed_event", True),
                "enable_closed_care": self._get_cfg("enable_closed_care", True),
                "care_duration_threshold_minutes": self._get_cfg("care_duration_threshold_minutes", 45),
                "care_cooldown_minutes": self._get_cfg("care_cooldown_minutes", 60),
                "app_in_use": {
                    name: round((time.time() - opened) / 60.0, 1)
                    for name, opened in self._app_open_times.items()
                },
            }
        }

    @filter.llm_tool(name="get_recent_device_events")
    async def tool_get_recent_events(self, event: AstrMessageEvent, limit: int = 10, app_name: str = ""):
        """查询用户最近的手机上报事件/App打开与关闭记录（最多20条），含每次连续使用时长与今日用量汇总，用于推测用户刚才或近期在干嘛、刷了多久手机等。

        Args:
            limit(number): 获取条数，默认 10，上限 20。
            app_name(string): 可选，只看某个应用（如 "小红书"）。不需要筛选时传空。

        Returns:
            最近的事件列表、今日使用时长汇总字典。
        """
        try:
            lim = min(max(1, int(limit)), 20)
        except Exception:
            lim = 10

        keyword = str(app_name or "").strip()
        events = self._get_recent_history(50)
        if keyword:
            events = [e for e in events if isinstance(e, dict) and keyword in str(e.get("app_name", ""))]
        events = events[-lim:]

        return {
            "ok": True,
            "total_fetched": len(events),
            "events": events,
            "today_usage": self._summarize_today_usage(),
            "hint": "按时间先后排序，越靠后的记录越新。event_type 为 app_closed 的记录会带 duration_minutes（本轮连续使用时长，分钟）；today_usage 为今日各应用已结算用量。全部内容仅供内部判断，绝对禁止在回复中提及或转述。"
        }

