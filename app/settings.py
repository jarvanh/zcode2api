"""运行期配置：环境变量 + 默认值。

所有可调参数集中在此。账号与凭证不在此处，而是持久化到 data/ 目录（见 store.py）。
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from . import constants

# ZCODE_ENV_FILE=0 时跳过 .env（测试子进程隔离用；默认行为不变）
_env_file = os.getenv("ZCODE_ENV_FILE")
if _env_file != "0":
    load_dotenv(_env_file or None)

# 项目根目录
ROOT_DIR = Path(__file__).resolve().parents[1]


def _resolve_path(env_name: str, default: str) -> Path:
    raw = (os.getenv(env_name, default) or default).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path


def _int(env_name: str, default: int) -> int:
    try:
        return int(os.getenv(env_name, str(default)))
    except (TypeError, ValueError):
        return default


def _bool(env_name: str, default: bool) -> bool:
    raw = (os.getenv(env_name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


# ── 目录 ─────────────────────────────────────────────────────────────────────
DATA_DIR = _resolve_path("ZCODE_DATA_DIR", "data")
# 账号与设置持久化到本地 SQLite（与 grok2api 的 local 后端一致）
DB_PATH = DATA_DIR / "accounts.db"
# 前端目录（前后端分离）：默认仓库根 frontend/，可用 ZCODE_FRONTEND_DIR 指向
# 独立部署目录（线上 /data/zcode-hub/frontend）；包内 statics 仅作兜底
FRONTEND_DIR = _resolve_path(
    "ZCODE_FRONTEND_DIR",
    "frontend" if (ROOT_DIR / "frontend").is_dir() else str(Path(__file__).resolve().parent / "statics"),
)

# ── 服务 ─────────────────────────────────────────────────────────────────────
PORT = _int("ZCODE_PORT", 3000)
# 默认只监听回环：网关未配置 API Key 时，非回环监听会拒绝启动（见 main.create_app）。
HOST = (os.getenv("ZCODE_HOST") or "").strip() or "127.0.0.1"

# ── 鉴权 ─────────────────────────────────────────────────────────────────────
# 后台管理密码初始值，首次启动写入 data/accounts.db，之后以数据库（meta 表）为准。
# 不提供硬编码默认口令：未配置环境变量时由 Store 首启随机生成并落库。
DEFAULT_ADMIN_KEY = (os.getenv("ZCODE_ADMIN_KEY") or "").strip()
# 网关访问密钥默认值（README 的 ZCODE_GATEWAY_KEY）：数据库 meta 表未设置时生效。
GATEWAY_KEY = os.getenv("ZCODE_GATEWAY_KEY", "")
# bigmodel Key 回退通道（bigmodel/ 前缀 / x-provider: bigmodel 头）默认关闭：
# 该通道欠费时上游 429 且网关原地重试，请求会挂起约 2 分钟才报错，默认不暴露。
# 可用环境变量 ZCODE_BIGMODEL_CHANNEL=1 或管理后台「设置」开启。
BIGMODEL_CHANNEL_ENABLED = os.getenv("ZCODE_BIGMODEL_CHANNEL", "0").strip().lower() in ("1", "true", "yes", "on")

# ── 验证码 ───────────────────────────────────────────────────────────────────
# 预解 token 池。真浏览器求解重（单枚 10–40s、Chromium 数百 MB），池收敛为 1/2
#（TTL 95s → 稳态约 95s 解一枚，Chromium 占空比 ~10–40%）；回滚 legacy 可调大。
CAPTCHA_POOL_MIN = _int("CAPTCHA_POOL_MIN", 1)        # 目标库存（低于则补）
CAPTCHA_POOL_MAX = _int("CAPTCHA_POOL_MAX", 2)        # 池上限
CAPTCHA_TOKEN_TTL = _int("CAPTCHA_TOKEN_TTL", 95_000) # 单枚 token 最大可用时长（ms；上游实际 ~2min）
CAPTCHA_CONFIG_CACHE_TTL = _int("CAPTCHA_CONFIG_CACHE_TTL", 600_000)  # ms
CAPTCHA_EMPTY_TAKE_RACE = _int("CAPTCHA_EMPTY_TAKE_RACE", 3)  # 池空竞速并行路数
CAPTCHA_RACE_DEADLINE = _int("CAPTCHA_RACE_DEADLINE", 25)     # 竞速总死线（秒）
CAPTCHA_TAKE_GRACE = _int("CAPTCHA_TAKE_GRACE", 10)           # 竞速无果后等后台补货宽限（秒）
# 铸码失败风暴检测（对齐 zapi mint-storm 信号语义；执行端为全账号同步冷却，
# zapi 的 Telegram IP 重置是作者私有环境设施，开源代码未接线）：
# 5 分钟内 >= CAPTCHA_STORM_THRESHOLD 次铸码失败且 3 分钟内零成功 → 触发，
# 全部 active 账号冷却 CAPTCHA_STORM_COOL 秒（到期自动恢复），冷却期间停止
# 主动铸码（不对被盯上的出口 IP 施压）；CAPTCHA_STORM_DEDUPE 去重窗口。
CAPTCHA_STORM_THRESHOLD = _int("CAPTCHA_STORM_THRESHOLD", 8)
CAPTCHA_STORM_COOL = _int("CAPTCHA_STORM_COOL", 900)         # 15 分钟
CAPTCHA_STORM_DEDUPE = _int("CAPTCHA_STORM_DEDUPE", 720)     # 12 分钟去重

# 闲置停解（对齐 zapi deep-idle 语义）：零流量时段后台停止铸码，不对上游
# 产无谓的求解流量（真浏览器一枚 10–40s + 数百 MB Chromium，闲时空转代价高）。
# 无取用超过 CAPTCHA_IDLE_AFTER 秒 → 后台补货停（池内余量自然过期蒸发）；
# 任意 get_verify_param 取用（含池空竞速）立即恢复。0 = 不停解。
CAPTCHA_IDLE_AFTER = _int("CAPTCHA_IDLE_AFTER", 600)

# 验证码求解（真浏览器：puppeteer-core + 系统 Chromium 跑阿里云官方无痕 SDK，
# 对齐 zcode-switch captcha.js）。happy-dom 路线（solver.js / solver-bun.ts）
# 2026-09 起曾被风控「unusual activity」全拒，故默认仍走 pw 路线；
# ZCODE_CAPTCHA_SOLVER=legacy 回退 solver.js。
#
# ZCODE_CAPTCHA_SOLVER_JS 可显式指定求解器脚本（相对路径按项目根解析），
# 优先于 mode 推导的默认值 —— 上游 Bun + happy-dom 架构（solver-bun.ts）
# 由此接入；留空则回落 mode 默认值。
NODE_PATH = os.getenv("ZCODE_NODE_PATH", "node")
CAPTCHA_SOLVER_DIR = ROOT_DIR / "captcha_node"
CAPTCHA_SOLVER_MODE = (os.getenv("ZCODE_CAPTCHA_SOLVER", "pw").strip().lower() or "pw")
if (os.getenv("ZCODE_CAPTCHA_SOLVER_JS") or "").strip():
    CAPTCHA_SOLVER_JS = _resolve_path("ZCODE_CAPTCHA_SOLVER_JS", "")
else:
    CAPTCHA_SOLVER_JS = CAPTCHA_SOLVER_DIR / (
        "solver.js" if CAPTCHA_SOLVER_MODE == "legacy" else "solver_pw.js"
    )
# Chromium 可执行文件（solver_pw.js 用；env 可覆盖，缺省按常见路径探测）
CHROMIUM_PATH = os.getenv("ZCODE_CHROMIUM_PATH", "/usr/local/bin/chromium")
CAPTCHA_SOLVE_RETRIES = _int("ZCODE_CAPTCHA_RETRIES", 4)
# 每次求解超时（秒）：真浏览器含 launch（内存压力下可 30s+）+ SDK 加载 + 无痕验证，
# 且 solver 进程内自旋重试 3 次（约 40s×3），须容得下
CAPTCHA_SOLVE_TIMEOUT = _int("ZCODE_CAPTCHA_TIMEOUT", 240)
# 同时运行的求解器进程上限（含预热与热路径现解）。每次求解是一个真 Chromium
# （数百 MB + 单核），池空突发时若无上限会按并发请求数同时拉起 N 个浏览器，
# 在 1–2GB 小机上直接把内存打满（2.7.33）。超出的求解排队，而不是并行。
CAPTCHA_MAX_CONCURRENT_SOLVES = max(1, _int("ZCODE_CAPTCHA_MAX_SOLVES", 2))
# 预热哪个出口的 token 池：空 = 跟随 messages 上游的实际出口（推荐）。
# 可强制 "proxy" / "direct"；非预热出口按需现解（claim 一小时一轮，够用）。
CAPTCHA_POOL_EGRESS = (os.getenv("ZCODE_CAPTCHA_POOL_EGRESS", "").strip().lower() or "")

# ── 用量监控 ─────────────────────────────────────────────────────────────────
# 后台自动刷新账号额度的间隔（秒）。0 表示关闭后台轮询，仅按需刷新。
# 默认 1800（2.6.5 整改）：zcode-switch 参照系下额度查询是用户开界面才触发
# （人节奏，日均几十次）；60s 轮询 ≈ 每号每天 4300+ billing 请求，是风控
# 「unusual activity」的主信号源（vault「billing 连续查询易触发拦截」落地）。
QUOTA_REFRESH_INTERVAL = _int("ZCODE_QUOTA_REFRESH_INTERVAL", 1800)
# 成功对话后计费刷新的最小间隔（秒）：billing/* 连续查询易触发上游拦截，
# 每条消息都刷是流量放大器，与 monitor 轮询共享 last_checked_at 去抖。
BILLING_REFRESH_MIN_INTERVAL = _int("ZCODE_BILLING_REFRESH_MIN_INTERVAL", 60)
# ── 上游错误重试 / 冷却（参数可设定）─────────────────────────────────────────
# 429 频控：账号不冷却，原地等待后重试，耗尽后换下一个账号（账号保持可用）
RETRY_429_TIMES = _int("ZCODE_RETRY_429_TIMES", 5)       # 429 重试次数
RETRY_429_WAIT = _int("ZCODE_RETRY_429_WAIT", 60)        # 429 重试等待秒数（上游 Retry-After 优先）
RETRY_429_WAIT_MAX = _int("ZCODE_RETRY_429_WAIT_MAX", 120)  # Retry-After 采信上限（防吊死客户端）
# 429 频控「换号优先」：池内还有其他可服务账号时，429 不再原地静默等待
# RETRY_429_WAIT×N —— 实测 60s×2 轮即 120s，会撞穿 CDN 边缘 ~100s 硬超时
#（HTTP 524），且等待期间零字节发给客户端。改成立刻换号（workbuddy-gateway
# 同型做法）。仅当池内无其他账号可换（全池限流）时才回退到原地等待兜底。
RETRY_429_FAILOVER_FIRST = _bool("ZCODE_RETRY_429_FAILOVER_FIRST", True)
# 429 频控扣号（magpie plugins#503）：换号后把该号扣住到上游 Retry-After 期满，
# 无 Retry-After 时用默认秒数；上限防 CDN 边缘超时。0 = 关闭（退回旧行为）。
RETRY_429_HOLD_DEFAULT = _int("ZCODE_RETRY_429_HOLD_DEFAULT", 30)
RETRY_429_HOLD_MAX = _int("ZCODE_RETRY_429_HOLD_MAX", 60)
# 429 自动熔断：重试梯连续耗尽 RATE_BREAK_THRESHOLD 轮即熔断冷却（指数退避：
# RATE_COOL_BASE 起、每次翻倍、RATE_COOL_MAX 封顶）。冷却到期自动回轮询，
# 任何成功请求清零计数 —— 「自动禁用 + 自动启用」闭环，无需人工介入。
RATE_BREAK_THRESHOLD = _int("ZCODE_RATE_BREAK_THRESHOLD", 2)
RATE_COOL_BASE = _int("ZCODE_RATE_COOL_BASE", 900)       # 首次熔断 15 分钟
RATE_COOL_MAX = _int("ZCODE_RATE_COOL_MAX", 7200)        # 封顶 2 小时
# 小池短档：可服务账号 <= RATE_SMALL_POOL 时熔断改用短冷却（60s 起步、
# 600s 封顶）——最后一个账号被熔断不应意味着服务中断 15 分钟级。
RATE_SMALL_POOL = _int("ZCODE_RATE_SMALL_POOL", 4)
RATE_COOL_SMALL_BASE = _int("ZCODE_RATE_COOL_SMALL_BASE", 60)
RATE_COOL_SMALL_MAX = _int("ZCODE_RATE_COOL_SMALL_MAX", 600)
# 请求级总死线：单请求"调度+梯等待"的总耗时上限（0 = 不限）。只约束换号
# 循环（_dispatch），流式一旦建立即脱离死线，多长的流都不会被截断。
# 实测被 429 梯拖住的请求 p50=300s/p90=600s/max=780s（全是梯等待叠加，
# 正常请求 p50=5s/max=64s），600s 放行 1-2 轮完整梯、在极端叠加前止步。
REQUEST_DEADLINE = _int("ZCODE_REQUEST_DEADLINE", 600)
# ── 限界排队（选不到可用账号时）────────────────────────────────────────────
# 账号池全被标记 EXHAUSTED / COOLING 时，旧行为是立刻 503。但上游额度是
# 分钟级滚动恢复的（2026-10-06 实测故障窗 4 分钟：账号耗尽 → 后台 60s 探测
# 发现额度恢复 → 回轮询），秒回 503 会把这类自愈型故障全部变成用户可见报错。
# 排队让请求等一等，把分钟级抖动吃掉；限界是因为「当天额度真耗尽」等也无用，
# 那时排队只是把报错推迟，不如早点告诉客户端。
# 0 = 关闭（维持秒回 503 的旧行为）。实际生效值再与 REQUEST_DEADLINE 取小。
QUEUE_WAIT = _int("ZCODE_QUEUE_WAIT", 240)  # 排队总时长上限（秒，流式）
# 非流式请求的排队预算：无法 early flush（提前发头会破坏本地聚合协议），
# 排队静默直接计入 TTFB —— 必须压在 CDN 边缘 ~100s 硬超时内快速失败
#（60s + 梯内开销留余量），超时给明确 503 而非 CF 524 页。实际生效值
# 再与 QUEUE_WAIT 取小。
QUEUE_WAIT_SYNC = _int("ZCODE_QUEUE_WAIT_SYNC", 60)
QUEUE_POLL = _int("ZCODE_QUEUE_POLL", 5)    # 排队期间重新选号的间隔（秒）
# 排队触发的主动额度探测最小间隔（秒）：并发排队时会同时涌进多个请求，
# 不节流会让 billing 查询放大成风控信号（见上方 BILLING 风控说明）。
QUEUE_PROBE_MIN_INTERVAL = _int("ZCODE_QUEUE_PROBE_MIN_INTERVAL", 30)
# ── Early Flush「响应头先行」（治 CDN 边缘 524）────────────────────────────
# 背景：客户端经 Cloudflare Tunnel 访问时，CDN 边缘对「已建连但迟迟拿不到
# 响应头」的请求有 ~100s 硬超时（HTTP 524）。调度排队、429/504 重试梯与
# 上游首字延迟叠加会把 TTFB 推过 100s：请求明明活着，却被边缘掐断（同型
# 问题 workbuddy-gateway earlyflush.go 已实证并修复，2026-09-28）。
# 方案：流式请求给宽限期——期内完成保持真实状态码语义（clirelay 可按码
# 重试）；耗尽仍未拿到上游响应，提前下发 200+SSE 响应头，CDN「等待响应
# 头」计时随之停止。此后上游失败降级为 SSE error 事件而非伪造正常结束。
# 仅流式路径启用（非流式本地聚合完整 JSON，提前发头会破坏协议）。0 = 禁用。
EARLY_FLUSH_GRACE = _int("ZCODE_EARLY_FLUSH_GRACE", 30)
# 提前发头后、等待调度结果期间的心跳间隔（SSE 注释行）：既冲刷缓冲，
# 也防流式 idle 被中间层掐断。<=0 时回退 15s。
EARLY_FLUSH_BEAT = _int("ZCODE_EARLY_FLUSH_BEAT", 15)
# 429/code 3009「model concurrency limit exceeded」是并发争用而非频控：上游没有
# Retry-After，等 60s 纯属浪费（槽位早空了）。这类回短退避，配 jitter 打散重试。
RETRY_429_CONCURRENCY_WAIT = _int("ZCODE_RETRY_429_CONCURRENCY_WAIT", 3)
# 纯无号/排队超时类 503 的 Retry-After（秒）：给客户端一个短退避提示，
# 避免上游网关把 503 当「节点坏了」立刻重试造成二次风暴。0/负值按 1s 处理。
RETRY_503_AFTER = _int("ZCODE_RETRY_503_AFTER", 5)
# ── BUSY_HOLD（magpie plugins#503 同款全池刹车）─────────────────────────
# 429 body 3008/3009/3010 = 并发槽「没轮到」（官方客户端读作「系统繁忙」：
# 1s/2s 重问两次后放弃）。同一请求 1s 重问仍被拒 = 持续饱和；60s 窗口内
# 累计 BUSY_N 次即全池静默 BUSY_HOLD 秒 —— 赶在阿里云 WAF 因地址高频
# 把整个 IP 405 封 1–2 小时之前主动刹车（magpie 两天日志实测：重问吸收
# 了 ~90% 的 3008，没人察觉地址饱和才被连坐封禁）。0 值关闭对应层。
BUSY_N = _int("ZCODE_BUSY_N", 5)
BUSY_WINDOW = _int("ZCODE_BUSY_WINDOW", 60)
BUSY_HOLD = _int("ZCODE_BUSY_HOLD", 180)
# 持续饱和时的原地重问间隔（秒，官方客户端节奏）：3008/3009/3010 专用，
# 不占 429 频控重试梯，不扣号。
BUSY_WAITS = (1, 2)
# startGate 本地排队闸（v2.9.32）：3008 用尽官方重问后，该账号本地闸门
# 时长（秒）。闸门内新请求只排队、零上游压力；到期放一个进去试探。
# 实测上游槽位释放中位 64.7s，默认 20s 平衡「早恢复」与「不空等」；
# 仍 3008 会续闸（上限 QUEUE_WAIT 240s / 同步 60s 由请求预算兜底）。
BUSY_GATE_WAIT = _int("ZCODE_BUSY_GATE_WAIT", 20)
# 5xx 等一般错误：重试，耗尽后账号冷却 COOLING_SECONDS 并换下一个账号
RETRY_5XX_TIMES = _int("ZCODE_RETRY_5XX_TIMES", 3)       # 5xx 重试次数
RETRY_5XX_WAIT = _int("ZCODE_RETRY_5XX_WAIT", 5)         # 5xx 重试等待秒数
# 限流（cooling）冷却时长（秒）——仅 5xx 重试耗尽使用
COOLING_SECONDS = _int("ZCODE_COOLING_SECONDS", 300)
# 连接失败（网络层）独立退避（2026-10-30，与上游风控分开计数）：请求未达上游、
# 零风控代价，故起步短、快速恢复；×2 递进仅针对持续失联的坏节点（cap 封顶），
# 不再共用 COOLING_SECONDS 的 300s 整号一刀切。
NET_COOLING_BASE = _int("ZCODE_NET_COOLING_BASE", 60)
NET_COOLING_CAP = _int("ZCODE_NET_COOLING_CAP", 600)
# 1113「无余额/无资源包」按模型冷却：同一账号可能只缺某个模型的包，
# 不能整号禁用；冷却到期后由下一次请求探测，成功才恢复（指数退避封顶）。
MODEL_NO_QUOTA_COOLDOWN_BASE = _int("ZCODE_MODEL_NO_QUOTA_COOLDOWN_BASE", 1800)  # 30m
MODEL_NO_QUOTA_COOLDOWN_MAX = _int("ZCODE_MODEL_NO_QUOTA_COOLDOWN_MAX", 21600)   # 6h
# 领包回血探测轮（2026-10-28）：定时探测「模型级冷却中」的账号模型，包到账即
# 消冷却恢复产能（不定期到账下，等下一次真实请求才探测会白摆数小时产能）。
# 0 = 关闭；默认 600s。探测成功只清该账号该模型冷却，失败不打扰冷却退避。
MODEL_PROBE_INTERVAL = _int("ZCODE_MODEL_PROBE_INTERVAL", 600)
# 风控（3012/405「unusual activity」）指数退避冷却：实测为频道级瞬时频控
#（同号同刻 billing 正常、数小时自愈），冷却自动恢复；累计 RISK_BAN_STRIKES
# 次才升级为禁用（人工恢复）。冷却期零上游流量（is_cooling 全通道门禁）。
RISK_COOLDOWN_BASE = _int("ZCODE_RISK_COOLDOWN_BASE", 900)    # 首次冷却秒数
RISK_COOLDOWN_MAX = _int("ZCODE_RISK_COOLDOWN_MAX", 86400)    # 冷却上限（24h）
RISK_BAN_STRIKES = _int("ZCODE_RISK_BAN_STRIKES", 4)          # 窗口内累计命中达到即禁用
RISK_STRIKE_DECAY_SECONDS = _int("ZCODE_RISK_STRIKE_DECAY_SECONDS", 12 * 3600)
# strikes 指数半衰（2026-10-30）：计分随时间连续消退——lane 30min 减半、
# net 15min 减半，不再「满额挂满 12h 硬窗口后一次性清零」。硬窗口
# （RISK_STRIKE_DECAY_SECONDS）保留作兜底。0 = 关闭半衰（回退旧行为）。
RISK_STRIKE_HALF_LIFE = _int("ZCODE_RISK_STRIKE_HALF_LIFE", 1800)
NET_STRIKE_HALF_LIFE = _int("ZCODE_NET_STRIKE_HALF_LIFE", 900)
# 距上次风控命中超过该窗口则 strikes 重新起算。2026-10-29 lane 整改由 7d → 12h：
# #716 形态是小时级封锁，7 天窗口意味着一个坏夜晚污染整周 strikes；窗口按
# lane 独立计时（models.risk_lane_penalty），整号计数仅存审计展示
# ── 通道级（lane-scoped）风控处罚（2026-10-29 整改，总开关）────────────────
# 依据：zai-org/feedback #716 实证 3012 是「通道级」封锁（同账号同机器付费
# Coding Plan 腿 0 拦截、只有 zcode-plan grant 腿被拦；billing 全程正常），
# 自家代码也以 RISK_WAVE_* 按 provider 判波次 —— 检测知道是通道级，旧处罚却
# 按整号执行：封顶即 DISABLED，连带杀死健康回退腿、自动领包轮与额度刷新。
# 开启后风控只冷却真正被打的那条腿，整号 DISABLED 回归「人工停用」单一语义。
# 0/关 = 维持旧整号曲线（灰度回退用）。
RISK_LANE_SCOPED = _bool("ZCODE_RISK_LANE_SCOPED", True)
# 2026-10-30 冷却曲线整体下调（900/7200/21600 → 180/1800/3600）：探测轮试探
# 不烧 strike（实测「保持冷却，不追加惩罚」），短名义冷却 + 免费探测 = 自动
# 调参——上游还在封锁就继续冷却、解封了 ≤3min 回池，不再动辄 15min/2h/6h。
# 二次下调（用户定调）：一切冷却最长 10 分钟——180×2^n 在第 3 档即触顶 600s，
# 长冷亦同；与 net 连接失败退避的 600s 上限对齐，全链路无超 10 分钟的冷却。
RISK_LANE_COOLDOWN = _int("ZCODE_RISK_LANE_COOLDOWN", 180)          # 首次 lane 冷却秒数
RISK_LANE_COOLDOWN_MAX = _int("ZCODE_RISK_LANE_COOLDOWN_MAX", 600)  # 阶梯上限（10min）
RISK_LANE_BAN_STRIKES = _int("ZCODE_RISK_LANE_BAN_STRIKES", 4)      # 该 lane 累计达即长冷
RISK_LANE_BAN_COOLDOWN = _int("ZCODE_RISK_LANE_BAN_COOLDOWN", 600)  # 长冷 10min（到期自愈）
# strikes 衰减窗口沿用 RISK_STRIKE_DECAY_SECONDS，但按 lane 独立计时（旧实现按
# 整号：只要打击持续到来，旧 strike 永不衰减，这正是跨天累积成禁用的机制）。
# plan-lane 回血探测轮（复用 MODEL_PROBE_INTERVAL 节流周期）：冷却尾段主动发
# 一个最小请求探 plan 通道是否已解封，成功即清该 lane；旧 DISABLED 零探测、
# 永久等人工。
RISK_LANE_PROBE = _bool("ZCODE_RISK_LANE_PROBE", True)
# 风控禁用自动换发指纹的频控（修身份抖动循环：禁用→换机→启用（不清 strikes）
# →一发即再封→再换机，每周期烧一套设备身份，上游看到同号频繁换机）。lane 模式
# 下本就极少走到换机；整号模式下同账号该间隔内只换一次，超限只冷却不换机。
RISK_ROTATE_MIN_INTERVAL = _int("ZCODE_RISK_ROTATE_MIN_INTERVAL", 86400)
RISK_AUTO_ROTATE = _bool("ZCODE_RISK_AUTO_ROTATE", True)
# 2026-10-25 防风控放大器：上游 405/3012 波次是「频道级」封锁（官方客户端同拦，
# 社区 zai-org/feedback #716/#1185 实证家宽直连也拦），波次窗口内换号重试必然
# 同样 405，只会把池内账号挨个烧 strike（08:41 实测：单请求触发 3 号 strikes
# 4→5 全禁用）。判定制：单账号命中只冷却自身、请求内仍可换号/回退（保留
# 健康号 failover）；窗口内第 2 个不同账号也命中 → 必然频道级波次，全池短窗
# 暂停（新请求入口快速 503、不排队不试探，暂停指数延长），波次首请求最多
# 烧 2 次 strike。请求内 API Key 回退不受影响（api.z.ai 通道与被拦的 plan
# 通道无关）；Plan 通道真实成功流量解除暂停（Key 回退成功不算）。
RISK_FAIL_FAST = _bool("ZCODE_RISK_FAIL_FAST", True)       # 防放大器总开关
# 波次暂停同步下调（180/3600 → 90/600，2026-10-30）：暂停窗口内全池快速 503，
# 是 openclaw 感知「持续报错」的直接来源；探测轮会兜住解封检测。
# 上限与 lane 冷却 10min 封顶对齐——任何机制都不把池子压住超过 10 分钟。
RISK_WAVE_PAUSE = _int("ZCODE_RISK_WAVE_PAUSE", 90)        # 波次暂停基础秒数（0 = 关闭）
RISK_WAVE_PAUSE_MAX = _int("ZCODE_RISK_WAVE_PAUSE_MAX", 600)  # 暂停指数延长上限

# ── 进程级上游闸门（只作用于 zcode-plan 渠道，见 app/gate.py）────────────────
GATE_RATE_PER_MIN = _int("ZCODE_GATE_RATE_PER_MIN", 0)
GATE_MAX_INFLIGHT = _int("ZCODE_GATE_MAX_INFLIGHT", 0)
GATE_EDGE_HOLD_SECONDS = _int("ZCODE_GATE_EDGE_HOLD_SECONDS", 180)
GATE_BUSY_THRESHOLD = _int("ZCODE_GATE_BUSY_THRESHOLD", 5)
GATE_BUSY_WINDOW_SECONDS = _int("ZCODE_GATE_BUSY_WINDOW_SECONDS", 60)
GATE_BUSY_HOLD_SECONDS = _int("ZCODE_GATE_BUSY_HOLD_SECONDS", 180)
GATE_POOL_HOLD_MAX_SECONDS = _int("ZCODE_GATE_POOL_HOLD_MAX", 3600)
# 证据链补查的进程级节流（2.7.31 review 整改）：真空期上游 balances 为空时
# （2026-11-12 线上实锤：22:49 refresh 跑了但 verified_at 没动——quota_map
# 空 → 证据点不打 → 账号永远 stale），若无节流每条请求都会对全池 stale 号
# spawn billing 实查，60s 去抖挡不住「每分钟一轮 × 整个真空期」的持续轰炸
# （fetch_quota 一次 = 3 条 GET，是 monitor 1800s 节奏的 60 倍，vault 明载
# billing 连续查询易触发拦截，403 还会把号标 INVALID）。首探立即、后续每
# 号最多按此间隔重探；balances 一旦列出窗口，一轮实查即闭合证据链
GATE_POOL_PROBE_MIN_SECONDS = _int("ZCODE_GATE_POOL_PROBE_MIN", 300)
# 429/code 1302/1303「真限流」：账号休息该时长（秒），请求下沉下一渠道/换号
RATE_LIMIT_COOLDOWN = _int("ZCODE_RATE_LIMIT_COOLDOWN", 60)
# Key 通道 1113「无资源包」后的选号冷却（秒）
KEY_NO_BALANCE_COOLDOWN = _int("ZCODE_KEY_NO_BALANCE_COOLDOWN", 3600)
# Plan 通道模型级额度耗尽（1005 exceed quota limit）后的选号冷却（秒）
PLAN_MODEL_EXHAUST_COOLDOWN = _int("ZCODE_PLAN_MODEL_EXHAUST_COOLDOWN", 1800)
# 风控升级禁用时自动换发设备指纹（新 SKU + 新 device_mid）并后台补跑安装序
# ——「风控后换设备重生」语义自动化，账号重新启用时即全新身份
RISK_AUTO_ROTATE = _bool("ZCODE_RISK_AUTO_ROTATE", True)
# ── IP 级封锁静默（2026-11-05，对齐 magpie zcode 0.1.43 / plugins#649）──────
# 405 + HTML 拦截页（errors.aliyun.com / <html>）= 本机出口 IP 被阿里云
# ESA/WAF 拦，不是账号风控。命中后**该出口**静默这么久：不发上游、
# 不记 strike、不改账号状态；该出口的请求本地回同款 405 让下游切 provider。
# 3 分钟与 magpie 一致（短到换网络/代理的用户等得起，长到防火墙计数降下来）。
EDGE_HOLD_SECONDS = _int("ZCODE_EDGE_HOLD_SECONDS", 180)
# ── mihomo 节点自动轮换（出口 405 → 换组内节点，见 app/proxy_rotate.py）────
# 每个账号出口是本地 mihomo listener（端口 1:1），组内 Fallback 只对连接
# 失败切换，HTTP 405 不触发。网关在 405 且**未携带 captcha token** 时主动
# 调 controller 换节点（token 绑定出口 IP，切换后复用会被判 3012）。
PROXY_ROTATE_ENABLED = _bool("ZCODE_PROXY_ROTATE_ENABLED", True)
PROXY_CONTROLLER_URL = (os.getenv("ZCODE_PROXY_CONTROLLER_URL", "http://127.0.0.1:9090").strip().rstrip("/")
                        or "http://127.0.0.1:9090")
PROXY_ROTATE_COOLDOWN = _int("ZCODE_PROXY_ROTATE_COOLDOWN", 90)      # 每账号两次轮换最小间隔（秒）
PROXY_ROTATE_TIMEOUT = _int("ZCODE_PROXY_ROTATE_TIMEOUT", 3)          # controller API 超时（秒）
PROXY_ROTATE_BLOCK_TTL = _int("ZCODE_PROXY_ROTATE_BLOCK_TTL", 3600)   # 节点黑名单记忆（秒）
# 同时投入的账号数上限（0 = 不限）。magpie 实测：同一出口 3 个号并发会被
# 405 拒，2 个不会（plugins#649 引用 01huadalang 的 Discord 反馈）。
# 控制台可改（meta 表即时生效）；超出的号留作备用，在用的号冷却/耗尽时顶上。
MAX_ACTIVE_ACCOUNTS = _int("ZCODE_MAX_ACTIVE_ACCOUNTS", 2)
# 单账号并发上限（env 默认值）。控制台可改（meta 表即时生效，与
# MAX_ACTIVE_ACCOUNTS 同模式）。2026-11-04 曾按线上实测写死 2；2026-11-08
# 恢复可改 —— v2.9.32 的 startGate 排队闸已把「并发满 → 敲上游 → 503」
# 的放大链吃掉，上调并发不再有 3008 风暴放大风险，交还运营调节。
# 2.9.48 采纳上游 2.7.31 口径：上游 ZCode 单账号并发写死 3，默认值随之上调；
# fork 仍保留 env / meta 覆盖（运营可调，见 store.account_concurrency）。
ACCOUNT_CONCURRENCY = _int("ZCODE_ACCOUNT_CONCURRENCY", 3)
# 单账号最小请求间隔（毫秒）：上游按账号限制突发并发，account_concurrency=1
# 时两个请求只要同拍到达仍可能撞 3008。给每个账号加 150ms 的最小间隔，
# 让突发在本地自然排队，而不是同时压上游。0 = 关闭。
ACCOUNT_MIN_INTERVAL_MS = _int("ZCODE_ACCOUNT_MIN_INTERVAL_MS", 150)
# 套餐自动领取轮间隔（秒）：周期对全部可打 billing 的 JWT 账号轮一遍
# preview + 领取（有可领套餐才补激活上报，见 claim.auto_claim_all_plans）。
# 默认 3600（2.6.5 整改）：zcode-switch 参照系下 preview/领取只在用户点界面
# 时触发；10 分钟轮次 + 每轮 app_launch 上报是标记不消退的帮凶。0 = 关闭轮次，
# 仅入池/手动触发。运行期可在后台设置改（meta 表即时生效）
CLAIM_ROUND_INTERVAL = _int("ZCODE_CLAIM_ROUND_INTERVAL", 3600)
# 领取窗口（北京时间小时，逗号分隔；空/"*" = 全天）。上游套餐按天投放，
# 用户侧实证每天 19:00 放量——窗口外轮询只会空转 preview。
CLAIM_ROUND_HOURS = os.getenv("ZCODE_CLAIM_ROUND_HOURS", "19")

# 领取轮账号错峰上限（秒）：轮内相邻账号间 0～该值的随机延迟，打散「同轮
# 全池同拍打上游 billing」的请求簇（行为聚类是风控信号之一；zcode-proxy
# 的「账号间防风控延迟」同思路）。0 = 关闭（默认，保持旧行为）。运行期
# 可在后台设置改（meta 表即时生效，下一轮生效）。
CLAIM_ROUND_STAGGER = _int("ZCODE_CLAIM_ROUND_STAGGER", 0)

# ── 上游端点 ─────────────────────────────────────────────────────────────────
# 上游端点：默认值统一收口在 constants.py，环境变量仅作覆盖
UPSTREAM = {
    "zai": os.getenv("ZAI_UPSTREAM_URL", constants.MESSAGES_URLS["zai"]),
    "zai_fallback": os.getenv("ZAI_FALLBACK_URL", constants.MESSAGES_URLS["zai_fallback"]),
    "bigmodel": os.getenv("BIGMODEL_UPSTREAM_URL", constants.MESSAGES_URLS["bigmodel"]),
}

# 跨 provider 降级顺序（Magpie 模式）：当前 provider 池无可用号时按此顺序下沉。
# 首个为默认路由 provider；裸模型名与 zai/ 前缀均落在 providers[0]。
PROVIDER_FALLBACK_ORDER = tuple(
    os.getenv("ZCODE_PROVIDER_FALLBACK_ORDER", "zai,bigmodel").replace(" ", "").split(",")
) if os.getenv("ZCODE_PROVIDER_FALLBACK_ORDER") else ("zai", "bigmodel")

# ZCode 计费 / 额度查询端点
ZCODE_BILLING_BASE = constants.BILLING_BASE
# 官方更新清单端点（测试时指向 Mock 上游）
ZCODE_MANIFEST_URL = os.getenv("ZCODE_MANIFEST_URL", constants.ELECTRON_MANIFEST_URL)
# 版本探测的发布通道（逗号分隔，默认稳定 1 + 预览 3）：3.18.x 先在预览/server
# 轨道发，只看 channel=1 会让「官方最新」永远停在旧版（2026-11-11 实证：
# channel=1 → 3.17.2，channel=3 → 3.18.1）。取各通道最高版本；能否顶版仍由
# PROMPT_ALIGNED_VERSION 正文锁决定（文本未移植的新版只记录不顶版）。
ZCODE_MANIFEST_CHANNELS = os.getenv("ZCODE_MANIFEST_CHANNELS", "1,3")
# 官方版本自动探测间隔（秒，默认 12 小时）
VERSION_CHECK_INTERVAL = _int("ZCODE_VERSION_CHECK_INTERVAL", 43200)
# 是否自动将客户端伪装对齐至官方最新版本
AUTO_SYNC_CLIENT_VERSION = _bool("ZCODE_AUTO_SYNC_CLIENT_VERSION", True)
# messages 通道强制直连（2026-11-04 线上实证：env 代理出口 GCP IP 被上游持续
# 标记——messages 走代理 17h+ 零成功（3012 unusual activity），而同账号
# billing/claim/telemetry 直连全部正常。切直连统一全链路出口，消除
# 「CN billing + US messages」双 IP 异常信号；captcha 签发出口由 gateway
# 的同一 egress tag 驱动自动对齐（app/egress.py「token 出口=请求出口」）。
MESSAGES_FORCE_DIRECT = _bool("ZCODE_MESSAGES_FORCE_DIRECT", False)
# 网关密钥未配置时是否放行（默认拒：未配置 = 公开代理，任何人可白嫖账号池）。
# 仅本地开发/测试可显式打开；生产请务必在后台设置 Gateway API Key（2.7.33）。
GATEWAY_ALLOW_NO_KEY = _bool("ZCODE_GATEWAY_ALLOW_NO_KEY", False)
# 前后端分离跨域白名单（2.7.34）：前端部署到独立源（GitHub Pages/CF Pages/独立
# nginx）时填该源，逗号分隔；默认空 = 仅同源（不输出 CORS 头）。鉴权是纯
# Bearer 头（无 cookie），跨域无 CSRF 面。
CORS_ORIGINS = os.getenv("ZCODE_CORS_ORIGINS", "")
# billing/claim 链路出口（2026-11-13 tebi 线上实证）："direct"（历史缺省，
# GCP 类大厂 ASN 直连可用）或 "proxy"（机房小 ASN 直连 IP 被 billing 风控
# 拒——preview 空投放/429、验证码 token 3007；走家宽代理 3/3 号立即可领）。
# 覆盖四处：_billing_request / report_activation（trust_env）、claim 验证码
# egress tag——三者必须同出口，token 与请求 IP 不一致会被判 unusual activity。
BILLING_EGRESS = (os.getenv("ZCODE_BILLING_EGRESS", "direct").strip().lower() or "direct")
if BILLING_EGRESS not in ("direct", "proxy"):
    BILLING_EGRESS = "direct"
# 营销触达 / 活动投放查询端点（测试时指向 Mock 上游）
ZCODE_MARKETING_TOUCH_URL = os.getenv("ZCODE_MARKETING_TOUCH_URL", constants.MARKETING_TOUCH_URL)
# 激活事件上报（测试时指向 Mock 上游）
ZCODE_EVENT_REPORT_URL = os.getenv("ZCODE_EVENT_REPORT_URL", constants.EVENT_REPORT_URL)
# 营销横幅 feed（测试时指向 Mock 上游）
ZCODE_MARKETING_TOUCH_URL = os.getenv("ZCODE_MARKETING_TOUCH_URL",
                                      constants.MARKETING_TOUCH_URL)

# OAuth 与兑换链 origin（测试时指向 Mock 上游）
OAUTH_API_BASE = os.getenv("ZCODE_OAUTH_API_BASE", constants.ZCODE_ORIGIN + "/api/v1")
ZAI_EXCHANGE_ORIGIN = os.getenv("ZCODE_EXCHANGE_ORIGIN", constants.ZAI_API_ORIGIN)

USER_AGENT = os.getenv("UPSTREAM_USER_AGENT", constants.USER_AGENT)
APP_VERSION = "2.9.53"

# system 提示词真源新鲜度（2026-10-31 WAF 文本校验波次防复发，app/freshness.py）：
# 真源 _meta.captured_at 距今超过该天数即判「过时」，每日巡检落 meta 表
# 供 admin API（GET /admin/api/sections/freshness）与 healthcheck 读取；
# 风控波次确认时若已过时，日志给「高度疑似措辞触发」提示（gateway 波次点）。
SECTIONS_STALE_DAYS = _int("ZCODE_SECTIONS_STALE_DAYS", 21)

# ── 失败请求诊断留存（app/diaglog.py，2026-10-31 405 排障经验）──────────────
# 上游 4xx/5xx/业务错误时按天落双侧现场（响应体截断 + 请求形状指纹，不含对话
# 正文），admin API GET /admin/api/diaglog 查询。0 依赖热路径，落盘失败只留痕。
DIAGLOG_ENABLED = _bool("ZCODE_DIAGLOG_ENABLED", True)
DIAGLOG_DAYS = _int("ZCODE_DIAGLOG_DAYS", 3)          # 保留天数（按天清理）
DIAGLOG_MAX_BODY = _int("ZCODE_DIAGLOG_MAX_BODY", 2048)  # 响应体截断字节

# ── 同内容请求去重（app/dedup.py，防 openclaw 重试风暴烧池）─────────────────
# 按「客户端内容指纹」两级：TTL 内失败重放（零调度零 strike）+ 非流式在途
# 合并（leader 失败重放/成功放行）。流式不合并不缓存（真实结局在生成器里）。
DEDUP_ENABLED = _bool("ZCODE_DEDUP_ENABLED", True)
DEDUP_TTL = _int("ZCODE_DEDUP_TTL", 30)                # 失败缓存冷却秒数
DEDUP_FOLLOWER_WAIT = _int("ZCODE_DEDUP_FOLLOWER_WAIT", 25)  # follower 等 leader 上限
DEDUP_MAX = _int("ZCODE_DEDUP_MAX", 1024)              # 内存 LRU 上限

# ── 验证码波次助推 + 全量带头（2026-11-01，app/captcha.py ④）────────────────
# B 档：波次暂停确认时预热目标临时抬升 WAVE_BOOST 枚、闲置停解压制（到期自动
# 回落）——暂停到期前后的试探/恢复期验证码消耗最集中；
# C 档：plan 通道每请求附验证码头（zcode-api 实弹证明「承重结构」，当前上游
# 未强制，默认关）。开启时建议同步调高 CAPTCHA_POOL_MIN/MAX（求解 10-40s/枚）。
CAPTCHA_WAVE_BOOST = _int("ZCODE_CAPTCHA_WAVE_BOOST", 2)
CAPTCHA_ATTACH_PLAN = _bool("ZCODE_CAPTCHA_ATTACH_PLAN", False)

# ── 会话亲和观测（app/session_ctx.py，observe 档）───────────────────────────
# 每请求在 reqlog 行落会话键与是否复见、响应侧落 cache_read——为「是否值得做
# 会话亲和（转发稳定 session id 换上游 prompt cache 命中）」攒数据。纯观测，
# 绝不改变请求行为；summary 走 admin API GET /admin/api/session-affinity。
SESSION_OBSERVE = _bool("ZCODE_SESSION_OBSERVE", True)

# ── 会话亲和 enforce 档（app/session_ctx.py）───────────────────────
# 开启后 JWT plan 通道转发稳定 x-session-id（每会话键一枚持久 UUID，替换
# build_trace_headers 里的逐请求随机值），换上游 prompt cache 命中；六件
# attribution 头形状不变，仅 session-id 生命周期与官方对齐。
# 2026-11-01 主人拍板默认开：observe 数据（repeat_ratio=0）尚未支持，但改动
# 仅一枚头的生命周期、风险低，实效以 admin API session-affinity 持续观察。
SESSION_AFFINITY = _bool("ZCODE_SESSION_AFFINITY", True)

# ── 思考档处理（app/reasoning.py）──────────────────────────────────────────
# OpenAI 入站的思考档兼容：GLM-5.3 家族恒思考（关不掉）且思考与正文共用
# max_tokens——小额度请求会被思考吃光出「200 但正文为空」。开启后按官方
# bundle 口径注入 thinking+output_config，max_tokens += 预算再按模型上限截
# 顶。0 = 整体旁路（恢复直传形态）。
THINKING_COMPAT = _bool("ZCODE_THINKING_COMPAT", True)
# 原生 /v1/messages 默认思考档：GLM-5.3 家族未显式传 thinking 时注入 max
# 档（thinking 32K + output_config.effort=max，预算加在 max_tokens 之外）。
# 1 = 开启（默认，GLM-5.3-Flash 默认 max 思考）；0 = 原生透传。
THINKING_NATIVE_DEFAULT = _bool("ZCODE_THINKING_NATIVE_DEFAULT", True)
# 未显式传思考档时的默认档位（low/high/max）。默认 max 保持不变；
# 需要按部署权衡质量与吞吐时可改 low/high，非法值回退 max。
THINKING_DEFAULT_EFFORT = os.getenv("ZCODE_THINKING_DEFAULT_EFFORT", "max").strip().lower() or "max"

# ── 端点映射跟随（app/endpoint_routing.py）────────────────────────────
# 周期拉 agent/configs 的 proxyEndpoint.mapping 表，按上游指令改写推理端点
# （上游正把 coding-plan 流量往 zcode.z.ai ultra 端点搬）。fail-open：拉不到
# 或表里没有 → 原 URL 原样放行。0 = 关闭（不拉表不改写）。
ENDPOINT_ROUTING = _bool("ZCODE_ENDPOINT_ROUTING", True)
