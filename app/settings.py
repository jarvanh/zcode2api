"""运行期配置：环境变量 + 默认值。

所有可调参数集中在此。账号与凭证不在此处，而是持久化到 data/ 目录（见 store.py）。
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

from . import constants

load_dotenv()

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
HOST = os.getenv("ZCODE_HOST", "0.0.0.0")

# ── 鉴权 ─────────────────────────────────────────────────────────────────────
# 后台管理密码默认值，首次启动写入 data/accounts.db，之后以数据库（meta 表）为准。
DEFAULT_ADMIN_KEY = os.getenv("ZCODE_ADMIN_KEY", "zcode")
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
QUEUE_WAIT = _int("ZCODE_QUEUE_WAIT", 240)  # 排队总时长上限（秒）
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
# 5xx 等一般错误：重试，耗尽后账号冷却 COOLING_SECONDS 并换下一个账号
RETRY_5XX_TIMES = _int("ZCODE_RETRY_5XX_TIMES", 3)       # 5xx 重试次数
RETRY_5XX_WAIT = _int("ZCODE_RETRY_5XX_WAIT", 5)         # 5xx 重试等待秒数
# 限流（cooling）冷却时长（秒）——仅 5xx 重试耗尽 / 连接失败使用
COOLING_SECONDS = _int("ZCODE_COOLING_SECONDS", 300)
# 风控（3012/405「unusual activity」）指数退避冷却：实测为频道级瞬时频控
#（同号同刻 billing 正常、数小时自愈），冷却自动恢复；累计 RISK_BAN_STRIKES
# 次才升级为禁用（人工恢复）。冷却期零上游流量（is_cooling 全通道门禁）。
RISK_COOLDOWN_BASE = _int("ZCODE_RISK_COOLDOWN_BASE", 900)    # 首次冷却秒数
RISK_COOLDOWN_MAX = _int("ZCODE_RISK_COOLDOWN_MAX", 86400)    # 冷却上限（24h）
RISK_BAN_STRIKES = _int("ZCODE_RISK_BAN_STRIKES", 4)          # 窗口内累计命中达到即禁用
RISK_STRIKE_DECAY_SECONDS = _int("ZCODE_RISK_STRIKE_DECAY_SECONDS", 7 * 86400)
# 距上次风控命中超过该窗口则 strikes 重新起算：跨月偶发命中不累积成禁用
RISK_AUTO_ROTATE = _bool("ZCODE_RISK_AUTO_ROTATE", True)
# 风控升级禁用时自动换发设备指纹（新 SKU + 新 device_mid）并后台补跑安装序
# ——「风控后换设备重生」语义自动化，账号重新启用时即全新身份
# 单账号并发上限（0 = 不限）。默认 2；运行期可在后台设置改（meta 表即时生效）
ACCOUNT_CONCURRENCY = _int("ZCODE_ACCOUNT_CONCURRENCY", 2)
# 套餐自动领取轮间隔（秒）：周期对全部可打 billing 的 JWT 账号轮一遍
# preview + 领取（有可领套餐才补激活上报，见 claim.auto_claim_all_plans）。
# 默认 3600（2.6.5 整改）：zcode-switch 参照系下 preview/领取只在用户点界面
# 时触发；10 分钟轮次 + 每轮 app_launch 上报是标记不消退的帮凶。0 = 关闭轮次，
# 仅入池/手动触发。运行期可在后台设置改（meta 表即时生效）
CLAIM_ROUND_INTERVAL = _int("ZCODE_CLAIM_ROUND_INTERVAL", 3600)

# ── 上游端点 ─────────────────────────────────────────────────────────────────
# 上游端点：默认值统一收口在 constants.py，环境变量仅作覆盖
UPSTREAM = {
    "zai": os.getenv("ZAI_UPSTREAM_URL", constants.MESSAGES_URLS["zai"]),
    "zai_fallback": os.getenv("ZAI_FALLBACK_URL", constants.MESSAGES_URLS["zai_fallback"]),
    "bigmodel": os.getenv("BIGMODEL_UPSTREAM_URL", constants.MESSAGES_URLS["bigmodel"]),
}

# ZCode 计费 / 额度查询端点
ZCODE_BILLING_BASE = constants.BILLING_BASE
# 激活事件上报（测试时指向 Mock 上游）
ZCODE_EVENT_REPORT_URL = os.getenv("ZCODE_EVENT_REPORT_URL", constants.EVENT_REPORT_URL)

# OAuth 与兑换链 origin（测试时指向 Mock 上游）
OAUTH_API_BASE = os.getenv("ZCODE_OAUTH_API_BASE", constants.ZCODE_ORIGIN + "/api/v1")
ZAI_EXCHANGE_ORIGIN = os.getenv("ZCODE_EXCHANGE_ORIGIN", constants.ZAI_API_ORIGIN)

USER_AGENT = os.getenv("UPSTREAM_USER_AGENT", constants.USER_AGENT)
APP_VERSION = "2.6.8"

_FRONTEND_VERSION_FILE = FRONTEND_DIR / "version"


def frontend_version() -> str:
    """前端版本号（frontend/version 文件，每次读取 → 前端独立发版即生效）。

    文件缺失/为空时回退 APP_VERSION，保证本地开发与旧部署不破。
    """
    try:
        v = _FRONTEND_VERSION_FILE.read_text("utf-8").strip()
        return v or APP_VERSION
    except OSError:
        return APP_VERSION
