# v2.9.41 合并语义说明（上游 ff39dbc）

合并策略：采用上游「拆 plan_std 假通道 + Magpie 3.17.2 提示词对齐 + 正文版本锁」，
fork 增强逐项挂回新扩展点。

## 上游吸收
- df42a2d 移除 plan_std 假通道：OAuth access_token 实测打不了标准 Anthropic 端点
  （401），通道二元化回 zai（zcode-plan JWT）/ zai_fallback（API Key）；
  oauth_token 降级为信息留存（后台 has_oauth_token 标记），401/403 直接判死
- ebbc73e 全量对齐 ZCode 3.17.2（Magpie packages/zcode 0.1.57）：
  body_transform 重写为 dress() 同构（严格三块 + 逐字节版本锁文本 +
  poweredBy 按模型注入 + sessionGuidance 按 Skill 工具注入）；identity 头换
  build_identity_headers（带 X-Device-Mid 与 attribution blob 同源；
  无 anthropic-beta、无 x-api-key——vlelyanin 实证两项已随 3.17.2 移除）
- 25fbbd2 版本同步不得越过提示词正文：PROMPT_ALIGNED_VERSION 正文锁，
  set_active_version 钳制 + version_tracker 只记录不顶版（prompt_port_required）
- a6fcbc0 chore 2.7.22；README 双语重构不采纳（fork 功能集与上游不同）

## fork 增强保留（挂到新扩展点）
- plan_lane_available 通道级风控门禁 → zai/apikey 二元路由：plan 腿冷却且有
  Key 回退时直接 zai_fallback（原 zai_plan 分支随通道拆除重挂）
- endpoint_routing.apply 端点映射跟随（fail-open）→ zai 通道
  （plan_style 现仅剩 zai，语义不变）
- CAPTCHA_ATTACH_PLAN / 排队闸 / 请求死线 / BUSY_HOLD / startGate /
  风控波次两账号确认 / 429 换号优先 / 1113 模型级冷却 / rate_break 熔断 /
  edge.py IP 级静默 / bigmodel 成本闸：全部在 _dispatch/_try_account 原位
- 监控词表 plan/apikey 二元不变（_lane_for 通道键入参收窄）

## 随拆除与改造
- fork per-account 环境注入退役：上游按 dress() 固定环境段（Magpie「未知
  place」形态，platform/OS Version 来自身份常量），与版本锁配套；
  test_body_transform_system / zcode_sections_3172.json / capture_sections.py 删除
- freshness 哨兵探测对象切到 zcode_system.json _meta（补 _meta 记录移植出处）
  + PROMPT_ALIGNED_VERSION 一致性检查；历史 sidecar_* 字段名保留
  （前端 accounts.html 与 admin API 消费方不变）

## 合并校准（1d8c954）
- tests/mock_upstream/server.py 恢复 fork 测试协议（合并时误取上游侧丢失
  87 行：edge_block_page / concurrency_3008 / claim_known_plan_ids /
  billing_balance_plans / touch_plans / oauth_userinfo mock 等）
- test_freshness fixture 改 patch sections_meta（调用时求值）
- test_regression_baseline 头断言对齐 3.17.2 新形状

验证：全量 1230 例 0 失败（基线 1187 + 上游新增 43）。
