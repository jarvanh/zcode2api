# zcode2api (zcode-hub)

**ZCode 账号运营 + 双协议网关一体机**：把池内 ZCode Coding Plan / Start Plan / API Key 账号额度，统一转换为标准的
**Anthropic Messages API**（`/v1/messages`）与 **OpenAI Chat Completions API**（`/v1/chat/completions`），向 Claude Code、Cline、Codex CLI、Chatbox、NextChat 等各类 Agent 与客户端提供高可用 API 服务。

自带一套完整的账号运营控制台：多账号池轮询、高可信成套桌面指纹仿真、官方首启安装序仿真、单账号并发控制、429/5xx/3012 阶梯容灾、请求全景监控、限时套餐自动与手动领取、OAuth 免密登录入池，以及免浏览器的阿里云无痕验证求解——开箱即用，自托管部署。

---

## 核心能力

- **双协议兼容网关**：原生支持 `POST /v1/messages`（Anthropic Messages 协议，支持流式 SSE）与 `POST /v1/chat/completions`（OpenAI 格式双向翻译，支持流式与非流式），并提供 `GET /v1/models` 模型列表。
- **高可信桌面指纹池（一号一台）**：入池自动生成加权桌面成套 SKU（Mac Sequoia/Tahoe、Win11 主流分辨率等），每账号独立全新 UUIDv4 `device_mid`，杜绝宿主机云 Linux 内核共享导致的账号集群关联风险。
- **官方首启安装序仿真**：入池与指纹轮换自动执行 `client/configs` 与激活事件上报（`app_launch` / `app_daily_active`），持久化 `install_id` 与 `installed_at`。
- **反代与 CDN 基础设施头深度清洗**：自动剔除 `x-forwarded-*`、`forwarded`、`cf-*`、`cdn-loop`、`x-real-ip`、`via`、`cookie`、`referer`、`origin`、`accept-language` 等网络中间件头部，杜绝部署网络拓扑泄露与客户端伪装冲突。
- **多账号负载均衡与会话亲和性**：
  - **新请求负载均衡**：新来的请求自动进行动态负载均衡，优先分配给当前在飞负载最低的可用账号（Least-Inflight）；负载相同时通过轮询指针平局交替分流。
  - **会话上下文亲和性 (Session Affinity)**：支持显式 Header（`x-session-id`, `session-id`, `x-conversation-id`, `conversation-id` 等）及多轮对话消息指纹；同一上下文的多轮请求粘性路由到同一账号，最大化复用上游 Prompt Cache。
  - **单账号并发硬上限（恒为 2）与满载溢出**：单账号同时处理上限写死为 2（env/后台设置一律无效）。若会话绑定的账号已达并发上限（2/2）或进入冷却/禁用，请求无缝溢出至其他可用账号并更新绑定，绝不阻塞用户。
- **阶梯式故障转移与容灾策略**：
  - **429 智能退避重试**：优先根据上游 `Retry-After` 原地等待重试（等待期间释放并发槽位，防止阻塞）；重试耗尽无损换号且**不冷却账号**；Plan 通道 429 耗尽且同账号有 API Key 时自动切 `api.z.ai` 回退通道。
  - **5xx 熔断冷却**：重试 3 次后进入 300s 冷却，自动切下一个可用账号。
  - **3012/405 真风控隔离**：识别 "unusual activity" 立即标记 `DISABLED` 禁用账号保护资产，停止自动化空转，支持后台人工确认后一键启用恢复。
  - **401/403 鉴权失效标记**：非验证码的凭证失效直接标 `INVALID`。
- **风控观测与韧性套件（v2.8.0）**：system 文本新鲜度哨兵（官方升版即告警，防 405/3012 文本校验波次复发）；失败请求诊断留存（响应体 + 请求形状指纹，3 天）；同内容请求去重（失败冷却重放 + 非流式在途合并，防重试风暴烧池）；验证码波次助推（暂停期预热抬升，C 档全量带头默认关）；会话亲和观测（observe 档，攒数据定夺是否转发稳定 session id）。
- **全景请求监控与安全防护**：
  - 请求监控面板（`/admin/monitoring`）：实时展示在飞请求、历史请求耗时、状态码、模型、账号及失败原因；行点开看详情（客户端 IP/UA、是否探测、Plan/Key 通道、出口 direct/proxy/account、多账号尝试梯子、排队与 429/5xx/验证码等待拆解、上游业务错误码、cf-ray 追踪 ID、stop_reason）；统计卡额外给出 P95 首字/总耗时、输出 tok/s、重试与排队累计；历史视图新增渠道/客户端分组与成功-失败堆叠日曲线。
  - **实时并发面板**：网关在途 / 上游占槽 / 排队深度 / 每账号 n÷limit 四口径同屏（四者刻意不相等 —— 429 与铸码等待会先释放槽位，所以波次期典型形态是「在途 8 · 占槽 0 · 排队 5」）；峰值事件驱动精确采样、分钟曲线落盘进历史（重启不清零长时段）；附进程体征行（RSS、solver 与 Chromium 后代进程数、僵尸数、令牌池水位、事件循环延迟）。
  - **号池健康页签**：逐号近 1 小时 P50/P95 首字、429/5xx/风控/额度计数与 strike 徽标，答「这个号现在还能不能用」（与按天趋势视图分工不同）。
  - **缓存窗口与请求形状**：会话复见按 5 分钟 idle 窗口拆「窗口内 / 窗口外」两档（窗口外上游 prompt 缓存必然重建，是 `cache_read` 上不去的解释变量，不是缓存坏了）；`max_tokens` 截断率对照 P95 请求形状（消息数 / 预算），直接指向「该提预算还是该缩上下文」。
  - **额度节奏 + 告警历史**：回血时刻取运营事实（北京 23:00 日窗 / 已领待生效套餐，与网关共用 `app/quota_windows.py` 口径）；耗尽 ETA 只在快照新鲜且账本一致时给出——线上 `exhausted` 却 `remaining>0` 属两套账矛盾，宁可标「不可靠」也不造假精度。告警台账按北京日记录已发送规则集合，面板给出 7 天曲线、规则 TOP 与静默时长。
  - **阈值值守**：`/admin/api/monitoring/watch` 只出数不做判断，主机侧 cron 脚本按阈值（风控 surge、上游 5xx、排队深度、RSS、僵尸/Chromium 堆积、loop 延迟、号池全灭）发 Telegram 告警；去重键取「触发规则集合」而非全文，避免长故障期刷屏；token 不入仓。
  - 后台暴力破解防护：客户端 IP 连续登录失败达上限自动锁定 5 分钟。
  - 设置密钥脱敏回显：GET 设置接口密钥自动打码，保障安全性。
- **限时套餐领取（双轨机制）**：
  - 自动领取：入池即自动领取全部可领套餐，并有可配置的周期自动领取轮（默认 600 秒，后台设置页可改，0 = 关闭），持续盯当期活动赠送。
  - 领取与使用解耦：领取/查额度只看 JWT 是否失效（INVALID）；**停用、风控禁用、冷却中的账号照样参与领取**，不再因为「号停着 / 被 B 链风控」漏领投放。
  - 领取语义对齐官方 3.11.2：成功回执携带上游 `server_time` 与套餐窗口；1005「名额用完」附带名额恢复时间 `next_at`，领取轮按其退避（等待期不重试领取、不耗验证码，`next_at` 过后自动恢复）。
  - 浏览器过码：支持在前端调起阿里云滑块验证码进行手动领取，应对极严风控环境。
- **OAuth CLI 免密登录**：
  - 提供海外邮箱与 Google 登录直达引导卡片，解决上游 `chat.z.ai/auth` 隐藏第三方登录按钮问题。
  - 严格遵循官方 CLI 的干净请求头规范，JWT 授权入池秒级完成，API Key 兑换异步后台回填。
  - 跟进官方新协议：采用 init 响应服务端下发的 `poll_token` 轮询（未下发时回落自造，兼容旧协议）；poll 4xx 承载的 `code=3004` 明确映射为会话过期。

## 支持的账号类型

| Provider | 模式 | 说明 |
|----------|------|------|
| `zai` | `jwt` | Coding Plan 额度（Plan 通道，需无痕验证码），自动续领活动套餐 |
| `zai` | `apiKey` | Z.AI API Key（回退通道，免验证码） |
| `bigmodel` | `apiKey` | 智谱开放平台（Anthropic 兼容端点） |

> 一个 Z.AI 账号可同时持有 JWT 与 API Key：OAuth 登录后会一并入池，JWT 额度告罄或 429 耗尽时自动无缝走 API Key 回退通道。

## 快速开始

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env            # 按需修改密钥、端口等
.venv/bin/python cli.py serve   # 启动网关 + 后台 UI（默认 http://127.0.0.1:3000）
```

- 后台管理：`http://127.0.0.1:3000/admin/login`（初始账号见 `.env` 的 `ZCODE_ADMIN_KEY`；未配置时首启随机生成并打印在启动日志）
- 对话端点：
  - Anthropic 协议：`POST http://127.0.0.1:3000/v1/messages`（兼容 Claude Code、Cline）
  - OpenAI 协议：`POST http://127.0.0.1:3000/v1/chat/completions`（兼容 OpenAI 客户端）
  - 模型列表：`GET http://127.0.0.1:3000/v1/models`
  - 探活端点：`GET http://127.0.0.1:3000/meta`

> 使用 Z.AI **JWT 模式**需要求解验证码：happy-dom 路线需 bun，真浏览器路线需
> Node + 系统 Chromium。首次先执行：`cd captcha_node && npm install`。

## 快速上手一个账号

```bash
# 方式一：OAuth 登录（免密，自动入池 + 自动兑换回退 Key）
.venv/bin/python cli.py login zai

# 方式二：手动加入现成凭证（JWT 三段点分 或 API Key）
.venv/bin/python cli.py add-account zai 名称 <jwt|key>
```

## 命令行

```bash
python cli.py serve [--port 3000]    启动网关 + 后台 UI
python cli.py login zai              通过 OAuth 登录 Z.AI 并自动入池
python cli.py add-account <zai|bigmodel> <name> <jwt|key>   添加账号
python cli.py accounts [zai|bigmodel]  查看账号列表
python cli.py remove-account <provider> <id|name>  删除账号
python cli.py quota                  查看各账号实时额度
python cli.py status                 查看配置概览
python cli.py set-admin-key <key>    设置后台密码
python cli.py export [file]          导出全部账号
python cli.py import <file>          导入账号
```

## 后台 UI

| 页面 | 说明 |
|------|------|
| `/admin/login` | 后台登录（Bearer 密钥鉴权，连续失败 IP 锁定防护，凭证加密存于浏览器 localStorage）|
| `/admin/accounts` | 账号池：新增/导入/导出、启用禁用、指纹换发、**实时额度与状态监控**、套餐领取（自动/手动）|
| `/admin/settings` | 后台密码、网关 API Key（GET 脱敏掩码）、额度刷新间隔、单账号并发上限（写死 2，只读展示） |
| `/admin/monitoring` | 请求监控中心：实时耗时/状态码/上游模型/使用账号/失败原因；逐请求详情可展开（来源、通道与出口、尝试梯子、等待拆解、业务错误码、上游追踪 ID）；**实时并发面板**（在途/上游占槽/排队深度/每账号 n÷limit 四口径 + 峰值 + 分钟曲线）与分布条（上游码直方图、stop_reason 截断率、去重挡下量、断开率、净上游耗时） |

账号池页实时展示每个账号的**状态（正常 / 额度用完 / 限流 / 异常 / 禁用）**、各模型剩余额度、
调用与失败次数；并提供「手动刷新额度」与「批量领取套餐」入口。

## 多账号轮询与故障转移

- 在账号池粘贴 Coding Plan JWT（3 段点分）或 API Key，每行一个即可加入轮询。
- 网关每次请求选择下一个「可用」账号，跳过用完 / 限流 / 异常 / 禁用的账号。
- **单账号并发限制**：所选账号在飞并发达上限时智能跳过，不排队、不计入 `MAX_ACCOUNT_ATTEMPTS` 计数。
- **额度用完**（余额为 0 / 上游 402 / 错误体含 quota、余额不足等关键词）→ 标记 `exhausted`，换下一个账号。
- **上游 429** → **不冷却账号**，按 Retry-After（封顶）原地等待重试；等待期间释放该账号并发槽。耗尽后换号，账号保持可用。Plan 通道 429 耗尽且同账号有 API Key 时切回退通道。
- **上游 5xx** → 重试 3 次后标记 `cooling` 冷却 300 秒，自动换下一个账号。
- **真风控（3012 / 405 unusual activity）** → 标记 `disabled` 并熔断停用，防止账号进一步受损，需在后台手动恢复。
- **鉴权失败 401/403（非验证码）** → 标记 `invalid`，需重新登录。
- **验证码过期（403 / code 3007）** → 原地刷新验证码对同一账号重试（最多 3 次）。
- 达到请求上限仍无可用账号 → 返回 `503 no_available_account`。

账号状态机：

```
            ┌─────────┐   额度用完(定期自动再探)    ┌──────────┐
  登录 ────▶│ ACTIVE  │◀─────────────────────────│ EXHAUSTED│
            │         │   5xx 重试耗尽(冷却 N 秒) └──────────┘
            │         │◀───────┐      ┌─────────┐
            │         │        ├──────│ COOLING │
            │         │        └─────▶└─────────┘
            │         │ 401/403(非验证码)
            │         │──────────────────────▶ INVALID（凭证失效，重新授权）
            │         │ 3012/405(真风控)
            │         │──────────────────────▶ DISABLED（风控保护，后台手动恢复）
            └─────────┘
```

> 成功响应可清除 `COOLING` / `EXHAUSTED`；额度恢复时后台监控也会自动回 `ACTIVE`。

## 无痕验证（免浏览器）

JWT 账号调用上游时需携带阿里云无痕验证参数（请求头 `X-Aliyun-Captcha-Verify-Param`）。

存在**两条求解路线**，由配置切换（详见环境变量表）：

- **happy-dom 模拟（默认）**：`captcha_node/solver-bun.ts` + vendored `captcha-happy.ts`，
  Bun 运行，**不启动真实浏览器**，纯 JS 模拟 DOM + 伪造指纹骗过风控。
- **真浏览器**：`captcha_node/solver_pw.js`，Node + puppeteer-core 拉真 Chromium，
  官方 SDK 在真环境里自行通过。较重（数百 MB / 枚），且需系统存在 Chromium。

两条路线子进程协议一致（stdout 打印 `VERIFY_PARAM=…` + 退出码 0），故可互相切换：
显式指定 `ZCODE_CAPTCHA_SOLVER_JS` 优先，否则按 `ZCODE_CAPTCHA_SOLVER` 推导，默认真浏览器。

- `app/captcha.py` 以子进程方式调用，内置预热池（目标 1–2 枚、单枚 TTL 95s）、
  池空竞速（3 路并行，死线 25s）、宽限补货（10s）、失败风暴检测与 certifyId 去重。
- 铸码台账：`app/captcha_ledger.py` 按路线记录每次求解，落盘
  `data/captchalog-<日期>.jsonl`（保留 30 天），供 `GET /admin/api/captcha/ledger`
  统计各路线成功率与平均耗时。
- 配置与会话缓存兜底：`client/configs` 拉取失败时回落内置默认参数。

## 鉴权

- **后台鉴权**：所有 `/admin/api/*` 需 `Authorization: Bearer <后台密码>`（也支持 `?app_key=`），内置 IP 失败节流保护。
- **网关鉴权（可选）**：在「设置」配置「网关 API Key」后，`/v1/messages` 与 `/v1/chat/completions` 须携带
  `Authorization: Bearer <key>` 或 `x-api-key: <key>`；留空则不校验。

## 思考档（GLM-5.3 家族）

- GLM-5.3 / GLM-5.3-Flash 是恒思考模型，默认带 thinking；ZCode 目录默认档位为 **max**。
- OpenAI 兼容入口（`/v1/chat/completions`、`/v1/responses`）：未传 `reasoning_effort` 时按 **max** 注入
  （thinking 32K 预算 + `output_config.effort=max`），预算加在 `max_tokens` 之外并按模型输出上限截顶。
- Anthropic 原生入口（`/v1/messages`）：未显式传 `thinking` 时默认 **max**（32K 预算，`max_tokens` 加预算再截顶）；
  客户端显式传 `thinking`（enabled / disabled）时原样尊重。`ZCODE_THINKING_NATIVE_DEFAULT=0` 可恢复纯透传。
- 客户端只取 `text` 块时注意：小 `max_tokens` 会被思考吃满，出现 HTTP 200 但正文为空；关闭默认注入或
  `ZCODE_THINKING_COMPAT` 时建议 `max_tokens` ≥ 1024。

## 环境变量

`cp .env.example .env` 后按需修改（.env 永不入库）：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ZCODE_PORT` | 3000 | 服务端口 |
| `ZCODE_HOST` | 127.0.0.1 | 监听地址（默认仅回环；空网关 Key 时非回环监听拒绝启动） |
| `ZCODE_ADMIN_KEY` | 空 | 后台密码初始值（之后写库，以库为准）；留空 = 首启随机生成并打印，旧版默认 `zcode` 会被随机替换 |
| `ZCODE_GATEWAY_KEY` | 空 | 网关 API Key；留空 = fail-closed（`/v1` 一律 403，非回环监听还会拒绝启动） |
| `ZCODE_GATEWAY_ALLOW_NO_KEY` | 0 | 仅本地开发/测试：显式 `1` 才允许未配置网关 Key 时匿名访问 |
| `ZCODE_CORS_ORIGINS` | 空 | 前后端分离（2.7.34）：允许跨源调用本 API 的前端源，逗号分隔；空 = 仅同源 |
| `ZCODE_BILLING_EGRESS` | direct | billing/claim 链路出口：`direct` 或 `proxy`（机房小 ASN 被风控拒时切家宽代理；token 与请求必须同出口） |
| `ZCODE_TRUST_PROXY_IP` | 0 | 仅显式 `1` 且来源为受信代理时信任 `cf-connecting-ip` / `x-real-ip`；直连忽略转发头 |
| `ZCODE_TRUSTED_PROXIES` | 127.0.0.0/8,::1 | 受信反向代理来源（逗号分隔 IP/CIDR） |
| `ZCODE_BIGMODEL_CHANNEL` | 0 | bigmodel Key 回退通道（`bigmodel/` 前缀 / `x-provider: bigmodel` 头）；默认关闭，`1` 开启（后台「设置」可改，以库为准）|
| `ZCODE_DATA_DIR` | data | 数据目录（SQLite 存放处）|
| `ZCODE_QUOTA_REFRESH_INTERVAL` | 60 | 后台刷新额度间隔（秒），0 关闭 |
| `ZCODE_COOLING_SECONDS` | 300 | 限流冷却时长（秒）|
| `ZCODE_CLAIM_ROUND_INTERVAL` | 600 | 套餐自动领取轮间隔（秒），0 关闭（运行时可在后台设置改，以库为准）|
| `ZCODE_THINKING_COMPAT` | 1 | 思考预算契约（GLM-5.3 家族恒思考，默认 max 档）：注入 thinking+output_config，max_tokens += 预算再截顶，thinking 开启时剔除采样参数；0 = 直传 |
| `ZCODE_THINKING_NATIVE_DEFAULT` | 1 | 原生 `/v1/messages` 未显式传 thinking 时默认注入 max 档（32K 预算，max_tokens += 预算）；0 = 纯透传 |
| `ZCODE_ENDPOINT_ROUTING` | 1 | 端点映射跟随：周期拉 agent/configs 的 proxyEndpoint.mapping 表按上游指令改写推理端点（fail-open，不匹配原样放行）；0 = 关闭 |
| `ZCODE_SESSION_OBSERVE` | 1 | 会话亲和观测档：reqlog 落会话键/复见标记与 cache_read，不改行为；`GET /admin/api/session-affinity` 看对比 |
| `ZCODE_SESSION_AFFINITY` | 1 | 会话亲和 enforce 档：同会话转发稳定 x-session-id（六件 attribution 形状不变），提上游 prompt cache 命中；0 = 关闭 |
| `ZCODE_NODE_PATH` | node | 求解器可执行文件（bun 或 node） |
| `ZCODE_CAPTCHA_SOLVER_JS` | 空 | 显式指定求解器脚本（相对路径按项目根解析）；**优先级最高**，如 `captcha_node/solver-bun.ts` |
| `ZCODE_CAPTCHA_SOLVER` | pw | 未显式指定时的推导：`legacy` → `solver.js`，`pw` → `solver_pw.js` |
| `ZCODE_CAPTCHA_RETRIES` | 4 | 单次求解失败重试次数 |
| `ZCODE_CAPTCHA_TIMEOUT` | 240 | 单次求解超时（秒，需容得下 Chromium 启动 + 进程内自旋重试） |
| `ZCODE_CHROMIUM_PATH` | /usr/local/bin/chromium | 真浏览器路线用的 Chromium（不存在时 solver 内部按常见路径探测） |
| `ZAI_UPSTREAM_URL` / `ZAI_FALLBACK_URL` / `BIGMODEL_UPSTREAM_URL` | — | 上游端点覆盖 |

## 开发与测试

```bash
.venv/bin/python -m pytest            # 全量测试（Mock 上游，无真实网络）
.venv/bin/ruff check app tests
.venv/bin/mypy app/constants.py
```

测试全部走 **Mock 上游**：`tests/mock_upstream/` 模拟 Z.AI 被依赖的全部端点，
支持 `x-mock-scenario` 故障注入（具体见 `docs/testing`），不依赖真实网络，可离线回归。

## 文档

完整文档见 [`docs/README.md`](docs/README.md)：

- **开发**：架构 · 数据格式 · API 规范 · 上游协议 · 开发指南 · 路线图
- **测试**：测试策略 · 单元用例 · 互通向量 · 集成 E2E · 验收标准

## 技术栈

- Python 3.11+ · FastAPI · Uvicorn · httpx
- SQLite（账号 / 设置持久化，WAL 模式）
- Node.js + jsdom（免浏览器求解阿里云无痕验证）

## 许可证

本项目采用 [AGPL-3.0](LICENSE) 许可证。

## 免责声明

本仓库仅供学习、研究、个人实验与内部验证使用，不提供任何形式的商业授权、适用性保证或结果保证。

作者不因使用、修改、分发、部署或依赖本项目产生的任何直接或间接损失、账号封禁、数据丢失、
法律风险或第三方索赔负责。请勿将本项目用于违反服务条款、协议、法律或平台规则的场景；商业前请自行确认许可证与相关协议。