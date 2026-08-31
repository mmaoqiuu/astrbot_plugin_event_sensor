import asyncio
import hmac
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, time as dtime
from aiohttp import web
from astrbot.api.all import *
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.config.astrbot_config import AstrBotConfig

logger = logging.getLogger("astrbot")

ENDPOINT_PATH = "/api/sensor/event"
AUTH_HEADERS = ["X-Sensor-Token", "X-Auth-Token"]
MAX_BODY_BYTES = 1024 * 100  # 100KB

TOOL_INSTRUCTIONS = """
【手机事件感知器 (Event Sensor) 配置工具规范】
当用户希望调整抓包互动时间段、冷却时间、触发关键词、或开启/关闭未回复抓包时，可调用以下工具：
- set_sensor_config: 修改事件感知配置（互动时间段、触发关键词、告别豁免词、冷却时间、未回复判定阈值等），立即生效无需重载。
- get_sensor_config: 查看当前的事件感知配置与实时激活状态。
修改成功后，请用自然口吻回复用户，不要直接罗列技术字段。
"""


@register(
    "astrbot_plugin_event_sensor",
    "mmq",
    "手机事件感知与即时唤醒插件 - 接收手机端自动化事件上报（如打开App），即时唤醒角色对话",
    "1.4.0",
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
        start_str = str(self._get_cfg("active_start_time", "08:00")).strip()
        end_str = str(self._get_cfg("active_end_time", "23:30")).strip()
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
            in_range = t_start <= now_t <= t_end
        else:
            in_range = now_t >= t_start or now_t <= t_end

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
            app_name = data.get("app_name") or data.get("event") or "某个应用"
            now = time.time()

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
                return web.json_response({"ok": True, "status": "ignored_outside_active_time"})

            # 4. 检查冷却时间 (转换为秒判断)
            cooldown_min = self._get_cfg("cooldown_minutes", None)
            if cooldown_min is None:
                cooldown_sec = int(self._get_cfg("cooldown_seconds", 0))
            else:
                cooldown_sec = int(cooldown_min) * 60

            if cooldown_sec > 0 and (now - self._last_trigger_time < cooldown_sec):
                logger.info(f"[event_sensor] 仍在冷却期（{int(now - self._last_trigger_time)}s < {cooldown_sec}s），静默忽略: {app_name}")
                return web.json_response({"ok": True, "status": "ignored_in_cooldown"})

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
            return web.json_response({"ok": True, "received": data})
        except web.HTTPException:
            raise
        except Exception:
            logger.exception("[event_sensor] 处理事件上报异常")
            return web.json_response({"ok": False, "error": "internal error"}, status=500)

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

        if is_keyword_catch:
            template = str(self._get_cfg("prompt_keyword_catch", "") or "").strip()
            if template:
                prompt = template.format(
                    minutes=keyword_minutes,
                    text=keyword_text,
                    keyword=matched_keyword,
                    app_name=app_name,
                )
            else:
                prompt = (
                    f"【系统实时感知事件（关键词触发提前抓包/装睡抓包）】"
                    f"对方在 {keyword_minutes} 分钟前发了「{keyword_text}」（触发了关键词「{matched_keyword}」），"
                    f"此时系统检测到对方并没有入睡或离开，而是在手机上打开了「{app_name}」。"
                    f"请结合当前对话上下文与你的角色人设性格，自然地抓包、调侃或逗弄对方（比如：抓到刚才说过晚安却在偷偷刷手机）。"
                    f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
                )
        elif is_unreplied:
            template = str(self._get_cfg("prompt_unreplied_catch", "") or "").strip()
            if template:
                prompt = template.format(
                    minutes=unreplied_minutes,
                    app_name=app_name,
                )
            else:
                prompt = (
                    f"【系统实时感知事件（已读不回/超时未回复抓包）】你发完上一条消息后，对方已有超过 {unreplied_minutes} 分钟没有回复你，"
                    f"但此时检测到对方在手机上打开了「{app_name}」。"
                    f"请结合当前对话上下文与你的角色人设性格，自然地抓包、吃醋、调侃或逗弄对方（比如质问怎么有空刷手机却不理你）。"
                    f"注意：这是系统后台事件通知，不是对方直接打字发给你的。"
                )
        else:
            prompt = (
                f"【系统实时感知事件】检测到对方在手机上打开了「{app_name}」。"
                f"请结合当前对话上下文、时间以及你和对方的状态，以符合你人设的方式自然地抓包/逗弄/回应对方。"
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

        msg = (
            f"📱【手机事件感知器·关键词与配置】\n\n"
            f"🌙 抓包触发词（激活装睡抓包）：\n{t_str}\n\n"
            f"☕ 告别豁免词（豁免已读不回）：\n{f_str}\n\n"
            f"⏰ 互动时间段：{st} ~ {et}\n"
            f"⌛ 未回复判定时长：{thresh} 分钟\n"
            f"🧊 防刷冷却时长：{cooldown} 分钟"
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
        unreplied_threshold_minutes: int = -1,
        deactivate_keyword_catch: str = "",
    ):
        """修改手机事件感知器（Event Sensor）的配置，修改后立即热生效，无需重载插件。

        Args:
            active_start_time(string): 互动开始时间，格式 "HH:MM"。不改传空。
            active_end_time(string): 互动结束时间，格式 "HH:MM"。不改传空。
            trigger_keywords(string): 提前激活抓包关键词（例如 "老公晚安,晚安,睡觉了"）。不改传空。
            dialogue_end_keywords(string): 告别/暂离豁免关键词（例如 "去玩手机,先去忙了,去吃饭"）。不改传空。
            cooldown_minutes(number): 抓包防刷冷却时间（分钟），0 表示无冷却。不改传 -1。
            enable_unreplied_trigger(string): 是否开启超时未回复抓包，"true" 或 "false"。不改传空。
            unreplied_threshold_minutes(number): 超时未回复判定时长（分钟）。不改传 -1。
            deactivate_keyword_catch(string): 是否立即关闭当前已被激活的临时装睡/晚安抓包状态，"true" 或 "false"。不改传空。

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

        if unreplied_threshold_minutes > 0:
            self._save_cfg_key("unreplied_threshold_minutes", int(unreplied_threshold_minutes))
            changes.append(f"未回复判定时长 -> {unreplied_threshold_minutes}分钟")

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
            }
        }
