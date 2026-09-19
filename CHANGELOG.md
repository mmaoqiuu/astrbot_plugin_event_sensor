# 更新日志

## v1.8.0
* 新增：
  - 应用关闭事件分流：识别 `app_closed` / `close` 上报，恒定返回 `locked: 0`、`triggered: false`，从机制上杜绝「关掉软件立刻被拽回聊天软件」的死循环。
  - 使用时长统计：打开事件登记起点（同应用重复上报只保留最早，超过上限自动重新计时），关闭事件结算时长；只有超过下限、且不超过上限的记录才计入。
  - 今日分应用用量汇总：记录中新增 `duration_seconds` / `duration_minutes`，可按应用查询今日使用时长。
  - 护眼关怀（静默感知、非抓包）：长时使用或深夜退出时注入一句温和提醒，带冷却时间，可开关，且可限制只在常规互动时间段内插话，绝不触发拦截。
  - `get_recent_device_events` 工具升级：支持按应用名筛选，并返回今日用量汇总。
  - `set_sensor_config` 工具新增可热改参数：`enable_closed_care`、`care_duration_threshold_minutes`、`care_cooldown_minutes`、`care_late_night_start`、`care_late_night_end`、`care_require_active_window`。
  - 配置面板新增 10 项：`enable_closed_event`、`duration_min_seconds`、`duration_max_hours`、`enable_closed_care`、`care_duration_threshold_minutes`、`care_cooldown_minutes`、`care_late_night_start`、`care_late_night_end`、`care_require_active_window`、`prompt_app_closed_care`。
  - `抓包关键词` 指令的输出里补充了关闭事件开关、护眼关怀长时阈值 / 冷却 / 深夜时段的当前值，方便一眼核对。
* 修改：
  - 提示词模板统一走安全渲染：模板为空时用默认文案，占位符写错时回退默认文案并记日志，不再让对话任务崩掉。
  - `prompt_app_closed_care` 支持占位符 `{app_name}`、`{duration_minutes}`、`{minutes}`、`{total_minutes}`、`{time_str}`。
  - 时间区间判断、历史裁剪等重复逻辑合并复用，减少分支分叉。
  - `@register` 描述与 `metadata.yaml` 同步升级到 v1.8.0，并补齐 `short_desc` 字段。
* 修复：
  - 修复上一版改动中 `抓包关键词` 指令返回文案的 f-string 拼接断裂（行尾换行转义被写成真实换行导致插件无法加载）。
  - 非法时间格式（非 `HH:MM`）现在会被拒绝并提示，不会再写脏配置。
* 原因：
  - 关闭应用属于「人主动离开」的信号，把它当成抓包事件会在逻辑上自相矛盾（关掉 App 反而被弹回 App）；同时关闭事件本身就是唯一能拿到「用了多久」的时间点，顺势接入时长统计与护眼关怀，感知能力更完整也更温柔。

## v1.7.0
* 新增：
  - 配置项 `history_keep_days`：历史事件保留天数，默认 `1`，即只留当天。
  - 配置项 `history_max_records`：条数保底上限，默认 `500`，防止高频上报把文件撑爆。
  - 启动时自动清一次历史（`_prune_history_file`），日常不用手动管。
* 修改：
  - 历史保留策略由「最近 100 条」改为「按天保留」，不再出现跨天旧数据残留。
  - 历史文件改为原子写入（先写 `.tmp` 再替换），避免写入中途失败损坏文件。
  - `metadata.yaml`（v1.6.0）与 `@register`（1.5.0）版本号不一致，统一为 v1.7.0。
* 原因：
  - 「最近 100 条」在高频上报时只覆盖几个小时、低频时又横跨好几天，旧数据永远不落底；改为按天保留后行为可预期，也符合隐私最小化。
