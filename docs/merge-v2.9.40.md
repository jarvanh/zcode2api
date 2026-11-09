# v2.9.40 合并语义说明（上游 e038650）

合并策略：采用上游「渠道梯 + 通道键 + 跨提供商降级链」新框架，fork 增强逐项挂回。

## 上游吸收
- 渠道梯 `_channel_ladder`：zai_plan（OAuth@标准端点）→ zai（JWT@zcode-plan）→ zai_fallback（Key）
- `build_request(upstream_key=...)` 通道键模型
- 跨提供商降级链 `_provider_chain`（PROVIDER_FALLBACK_ORDER）
- provider 命名空间：`zai/GLM-5.3` 前缀（/v1/models、_detect_provider、_normalize_body）
- 1310 额度耗尽：EXHAUSTED + 解析重置时间（沿用 fork 池级 402 口径）
- plan_std 401/403 下沉下一渠道不判死账号

## fork 增强保留（挂到新扩展点）
- plan_lane_available 门禁（lane 级风控路由）→ build_request 自动路由
- force_fallback 翻转 → _jump_to_fallback 渠道下沉
- 排队预算/请求死线/BUSY_HOLD/startGate/风控波次两账号确认 → _dispatch per-provider 循环内
- 429 换号优先 + 扣号、BUSY_WAITS 重问、1113 模型级冷却、rate_break 熔断 → _try_account 各分支
- edge.py IP 级静默、endpoint_routing、CAPTCHA_ATTACH_PLAN → 对应通道判定
- 监控词表 plan/apikey 二元（前端徽章/reqlog 聚合不变）
- _recovery 定性用入站 req_provider（防降级链泄漏链尾）
- bigmodel 成本闸保留（默认关 + 降级链过滤；测试基线打开）

## 顺修
- `_quota_windows_live` expires_at str/float 混型 TypeError（ISO8601 兜底归一）
- `_parse_exhaust_reset` 在 `from datetime import datetime` 遮蔽下的 AttributeError
- `_reject_model_not_allowed` 剥 `provider/` 前缀再判定（防前缀请求误拒）

验证：单元 ~1008 + 集成 225 全绿。
