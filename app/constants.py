"""上游常量收口 —— 所有与 zcode/zai/bigmodel 上游相关的 URL、模型名、判定关键字集中在此。

约定（docs/development/06-dev-guide.md）：
- 模块代码禁止再硬编码上游 URL / 模型名 / 关键字，一律 import 本模块。
- 改动任何常量必须先在 tests/unit/test_constants.py 补断言，防止无意漂移。
- 值均来自源码实证，出处标注见 docs/development/05-upstream-protocols.md。
"""

from __future__ import annotations

import os
import re

# ── 上游 origin ──────────────────────────────────────────────────────────────
# Plan 通道（JWT + 验证码）：zcode.z.ai 的 coding-plan 代理端点
ZCODE_ORIGIN = "https://zcode.z.ai"
# API Key 通道（回退 / bigmodel 同形）：api.z.ai 的 anthropic 兼容端点
ZAI_API_ORIGIN = "https://api.z.ai"
# bigmodel（智谱开放平台）anthropic 兼容端点
BIGMODEL_ORIGIN = "https://open.bigmodel.cn"

# ── messages 端点（settings.UPSTREAM 的默认值来源）────────────────────────────
MESSAGES_PATHS = {
    # zai + jwt（Plan 通道，需 X-Aliyun-Captcha-Verify-Param）
    "zai": "/api/v1/zcode-plan/anthropic/v1/messages",
    # zai + apiKey 回退通道（免验证码）
    "zai_fallback": "/api/anthropic/v1/messages",
    "bigmodel": "/api/anthropic/v1/messages",
}
MESSAGES_URLS = {
    "zai": ZCODE_ORIGIN + MESSAGES_PATHS["zai"],
    "zai_fallback": ZAI_API_ORIGIN + MESSAGES_PATHS["zai_fallback"],
    "bigmodel": BIGMODEL_ORIGIN + MESSAGES_PATHS["bigmodel"],
}

# ── 计费 / 额度端点（quota.py / 未来 claim.py）────────────────────────────────
BILLING_BASE = f"{ZCODE_ORIGIN}/api/v1/zcode-plan"
BILLING_CURRENT_PATH = "/billing/current"
BILLING_BALANCE_PATH = "/billing/balance"
USAGE_PATH = "/usage"
# WAF 风险点：billing/* 连续查询易触发拦截，轮询必须错峰（docs 05 §风险控制）

# Coding 平面额度：api.z.ai 自有端点（与 billing 族是两套独立账本）。
# 鉴权用 `authorization: <api_key>`（裸值，无 Bearer，也不是 JWT）。
# 2026-11-02 实测：体验套餐号返回 {"code":500,"msg":"当前用户不存在coding plan"}。
CODING_LIMIT_URL = f"{ZAI_API_ORIGIN}/api/monitor/usage/quota/limit"

# ── OAuth ────────────────────────────────────────────────────────────────────
OAUTH_CLI_INIT_PATH = "/api/v1/oauth/cli/init"
OAUTH_CLI_POLL_PATH = "/api/v1/oauth/cli/poll"   # + /{flow_id}
# chat.z.ai 域（非 zcode.z.ai）：OAuth access_token 换用户身份。只认 poll 返回的
# data.zai.access_token；池内存量 zcode JWT / apiKey 打它一律 401（2026-09-30 实测
# ——uid 与真实账号用户名的对应关系只在授权瞬间可取，错过即不可补查）
OAUTH_USERINFO_URL = "https://chat.z.ai/api/oauth/userinfo"

# ── 客户端版本（单一真相源；3.18.1 对齐 ZCode 官方构建当前版）──
# 客户端 claim 头实测缺版本/平台头 → 上游 3007；client/configs 带 platform 参数 → 3001。
# 出处：ZCode 3.18.1 官方 AppImage（electron 预览清单 channel=3，2026-11-11 构建）
# 内 resources/glm/zcode.cjs 与 3.18.0 源码（zai-org/ZCode@b95ebaffea）逐段比对：
# cliPrefix/身份+Harness/dynamicBehavior/sessionGuidance/contextManagement 全部同文，
# 仅版本号不同。前值 3.18.0（2026-11-12 跟进）、3.17.2（2026-11-09）、3.17.1（11-03）。
# 注：桌面端 electron 稳定清单（channel=1）2026-11-11 仍报 3.17.2，3.18.x 先在
# 预览/server 轨道落地（本机 ~/.zcode/server/current.json appVersion=3.18.0）。
# zcode-switch 在 Windows 上经 `zcode_app_version()` 读注册表本机真实版去打
# billing；hub 跑 Linux 无注册表，静态基线钉此处，运行期由 version_tracker
# 轮询官方 manifest 接口自动更新活跃版本。
# ⚠️ 11-03 起 zcode.z.ai start-plan 对 body 提示词锁版本：版本头与 body 文本
# 必须同版（prompt.mjs 注释：新版本头 + 旧版本文本 → 405/3012 "unusual activity"）。
# 发现 preview 为空、1004（领取不落条件）或 405/3012 时优先核对此常量是否落后官方。

# 提示词对齐版本（正文锁）：zcode_system.json 的文本取自该版本客户端。
# 活跃版本永远不得越过此值——头比正文新就是 405/3012，比"落后一点"更致命。
# 自动同步 / 手动设置统一由 set_active_version 钳制；真正升级 ZCode 版本时，
# 先移植新版提示词文本进 zcode_system.json，再同步抬高此常量。
# 3.18.0 变更：Desktop 段（# ZCode Desktop Context）移出 system 块2，改由环境
# 公告走 meta_user；块2 只剩身份+Harness（identity.ts 文本与 3.17.2 逐行相同）。
# 3.18.1：文本与 3.18.0 完全一致（官方 AppImage 逐段比对），仅版本号差异。
PROMPT_ALIGNED_VERSION = "3.18.1"
DEFAULT_CLIENT_APP_VERSION = PROMPT_ALIGNED_VERSION
CLIENT_APP_VERSION = DEFAULT_CLIENT_APP_VERSION
CLIENT_PLATFORM = "darwin-arm64"  # asar TH() = process.platform-arch，服务端固定伪装
# bundle 实证：安装/遥测序仍走 /api/v1/client/configs?app_version=…；
# mitm 另见 /api/v1/agent/configs（运行时配置通道，无参数），两者并存勿混。
CLIENT_CONFIGS_URL = f"{ZCODE_ORIGIN}/api/v1/client/configs"
CLIENT_CONFIGS_QUERY = f"app_version={CLIENT_APP_VERSION}"

# ── 官方更新清单端点（2026-11-03 官方桌面端逆向实证）────────────────────────────
# 官方客户端检查更新直连端点，返回 YAML 格式的最新版本、发布时间与原生 Markdown 更新日志
ELECTRON_MANIFEST_PATH = "/api/v1/releases/electron/manifest"
ELECTRON_MANIFEST_URL = f"{ZCODE_ORIGIN}{ELECTRON_MANIFEST_PATH}?platform=darwin-aarch64&channel=1"

# ── 营销触达 / 活动投放端点（2026-11-03 官方客户端逆向实证）───────────────────────
# 官方活动套餐（如 zcode-v3-start-plan-MMDD）经由此服务（marketing-touch）下发，
# 常规 billing/preview 端点保持为空。返回 deliveries[].banner/popup 中包含
# action.type="claim_zcode_plan" 及 args.zcode_plan。
MARKETING_TOUCH_PATH = "/api/v1/marketing/touch"
MARKETING_TOUCH_URL = f"{ZCODE_ORIGIN}{MARKETING_TOUCH_PATH}"

# ── billing 族版本 / 激活上报（zcode-switch v1.5.4 实证，2026-09-06 移植）─────
# billing 族（preview/claim/balance/current/usage/configs/event）用官方桌面端
# 现行版（对齐 zcode-switch Windows 路径读到的本机真实版；hub 钉常量，见上）
BILLING_APP_VERSION = DEFAULT_CLIENT_APP_VERSION  # 须与 CLIENT_APP_VERSION 同步升（半更新=2026-10-31 组合病因，freshness 告警项；version_tracker.set_active_version 已联动双写）
BILLING_TITLE = "Z Code@electron"        # bundle：sourceTitle 缺省即 electron（CLI surface 才覆盖 cli）
BILLING_RELEASE_CHANNEL = "production"   # bundle DF()：恒 production（ZCODE_ENV=test 才 test）；
                                         # 旧值 stable 系 3.11.2 时代 zcode-switch 静态对齐，已过时

# 官方客户端每日活跃事件：POST /api/v1/event/report（不在 zcode-plan 下、无
# Authorization）。zcode-switch claim_refresh 在 preview 前上报 app_launch +
# app_daily_active —— 疑似活动套餐投放的资格信号（官方客户端可见活动而纯
# billing 轮询号 preview 为空的差异点）。
EVENT_REPORT_URL = f"{ZCODE_ORIGIN}/api/v1/event/report"
# 营销横幅 feed（官方活动第二投放列表）：App 活动横幅不走 billing/preview，
# 从这里读投放，按钮 claim_zcode_plan action 直接带 plan_id（magpie #503 /
# cpa-multi-plugins touch.go 实证；WEEKEND BUILD 挂横幅期间 preview/current
# 均看不到它——2026-11-02 我们的「空窗」形态同源）。3.17.2 bundle 对拍：
# GET ?seq=<每进程递增>，头 = JWT + X-Device-Mid + X-Client-Language +
# X-ZCode-App-Version。
MARKETING_TOUCH_URL = f"{ZCODE_ORIGIN}/api/v1/marketing/touch"
ACTIVATION_ELEMENTS = ("app_launch", "app_daily_active")
ACTIVATION_SCREEN_RESOLUTION = "2560x1440"

# ── 验证码默认配置（client/configs 拉取失败时的兜底；region 实测线上为 cn）────
# skip_model_request 缺省 True（2026-11-04 线上实锤）：真实配置是
# skip_model_request=true（官方客户端模型请求不带码）；拉取失败时若误按
# 「带码」处理，会给 messages 挂上官方客户端不会带的验证码 token —— 上游直接
# 3012 unusual activity，且每号 4 次自动升级禁用。fail-safe 方向 = 官方形态
# （不带码）；上游若真挑战（3007/响应头/文案），gateway 已有补码重试兜底。
CAPTCHA_DEFAULTS = {
    "enabled": True,
    "prefix": "no8xfe",
    "region": "cn",
    "sceneId": "11xygtvd",
    "skip_model_request": True,
}

# ── 模型名 ───────────────────────────────────────────────────────────────────
# Z.AI 上游模型名大小写敏感；客户端传小写别名时映射到官方名（gateway.MODEL_NAME_MAP）
#
# 2026-10-30 实测：套餐只放行 GLM-5.3 系，GLM-5.2 / GLM-5-Turbo / GLM-5.1 / GLM-4.7
# 一律 400 code 3006「model not allowed」——它们是**已退役别名**，不是拼写别名。
# 老客户端（Claude Code 配置、第三方 SDK 默认 GLM-5.2）会固定带这些名字，若原样
# 透传，客户端拿到的是上游 3006 而网关没有任何可用模型，等于「用一个合规请求换一个
# 必然失败的响应」。因此退役名统一改写到当前主力型号（GLM-5.3），保留大小写别名。
RETIRED_MODEL_ALIASES = {
    "GLM-5.2": "GLM-5.3",
    "GLM-5-Turbo": "GLM-5.3",
    "GLM-5.1": "GLM-5.3",
    "GLM-4.7": "GLM-5.3",
}
MODEL_NAME_MAP = {
    "glm-5.3-flash": "GLM-5.3-Flash",
    "glm-5.3": "GLM-5.3",
    "glm-5.2": "GLM-5.3",
    "glm-5-turbo": "GLM-5.3",
    "glm-turbo": "GLM-5.3",
    "glm-5.1": "GLM-5.3",
    "glm-4.7": "GLM-5.3",
}
# bigmodel 资源包模型：仅 OpenAI 兼容端点（/api/paas/v4/chat/completions）可用；
# Anthropic 兼容端点受 Coding Plan 门禁（1309），不能走 _dispatch 的 Anthropic 通道。
BIGMODEL_OPENAI_MODELS = ("glm-4.5-air", "glm-4.6v", "glm-4.7")
# /v1/models 回退名单（capabilities 并集取不到时使用）
AVAILABLE_MODELS = ["GLM-5.3-Flash", "GLM-5.3"]
# provider 命名空间模型目录（Magpie 模式：`zai/GLM-5.3` 前缀供外部 Agent 选 provider；
# 裸名仍向后兼容，默认路由 zai）
# provider 命名空间模型目录（Magpie 模式，上游 e038650）：`zai/GLM-5.3` 前缀
# 供外部 Agent 选 provider；裸名仍向后兼容，默认路由 zai。仅作 /v1/models
# 池空时的展示回退；实际允许的模型由账号 entitlements 动态判定。
MODEL_CATALOG_NAMESPACED = [
    "zai/GLM-5.3-Flash",
    "zai/GLM-5.3",
    "bigmodel/GLM-5.3-Flash",
    "bigmodel/GLM-5.3",
]

# 展示规格目录（context/max_output 同步自 zcode-api src/provider/models.ts，
# 规格源自 ZCode 3.11.2 桌面端目录）。仅作 /v1/models 展示补充；实际允许
# 的模型由账号 entitlements 的 capabilities 动态判定（套餐外上游 3006）。
MODEL_CATALOG = [
    {"id": "glm-4.5-air", "context_window": 131_072, "max_output_tokens": 98_304},
    {"id": "glm-4.6", "context_window": 200_000, "max_output_tokens": 131_072},
    {"id": "glm-4.6v", "context_window": 131_072, "max_output_tokens": 32_768},
    {"id": "glm-4.7", "context_window": 200_000, "max_output_tokens": 131_072},
    {"id": "glm-5", "context_window": 200_000, "max_output_tokens": 64_000},
    {"id": "glm-5-turbo", "context_window": 200_000, "max_output_tokens": 64_000},
    {"id": "glm-5v-turbo", "context_window": 200_000, "max_output_tokens": 131_072},
    {"id": "glm-5.1", "context_window": 200_000, "max_output_tokens": 64_000},
    {"id": "glm-5.2", "context_window": 1_000_000, "max_output_tokens": 128_000},
    {"id": "glm-5.3", "context_window": 1_000_000, "max_output_tokens": 128_000},
    {"id": "glm-5.3-flash", "context_window": 1_000_000, "max_output_tokens": 128_000},
]

# 上游 max_tokens 合法范围（2026-09-06 实测：超限报 400 code 1210
# 「max_tokens参数非法：限制数值范围[1,131072]」，客户端（如 auto-compact 续传）
# 可能带更大的值，网关统一钳制）
MAX_TOKENS_LIMIT = 131072

# ── 请求头 ───────────────────────────────────────────────────────────────────
ANTHROPIC_VERSION = "2023-06-01"
USER_AGENT = f"ZCode/{CLIENT_APP_VERSION}"
# Start Plan messages 的 UA 后缀（官方 anthropic SDK 形状，billing 族不带）：
# Magpie packages/zcode/index.mjs startHeaders UA = "ZCode/{ver} ai-sdk/anthropic/3.0.81"
ANTHROPIC_SDK_UA = "ai-sdk/anthropic/3.0.81"
X_ZCODE_APP_VERSION = CLIENT_APP_VERSION
X_ZCODE_AGENT = "glm"


def parse_semver(v: str) -> tuple[int, ...]:
    """提取版本数字用于大小比较，如 '3.17.2' -> (3, 17, 2)。"""
    nums = re.findall(r"\d+", str(v or ""))
    return tuple(int(n) for n in nums) if nums else (0,)


def is_newer_version(candidate: str, baseline: str) -> bool:
    """candidate 是否比 baseline 更新（纯数字分量比较）。"""
    c = parse_semver(candidate)
    b = parse_semver(baseline)
    max_len = max(len(c), len(b))
    c_pad = c + (0,) * (max_len - len(c))
    b_pad = b + (0,) * (max_len - len(b))
    return c_pad > b_pad


def set_active_version(new_version: str) -> str:
    """动态更新客户端与 billing 伪装版本号（当自动探测发现新版本时）。

    规范化：剥离首尾空白与前导 v/V——官方 CFBundleShortVersionString 与
    X-ZCode-App-Version 头均为纯数字形态，"v3.17.2" 会使 UA 变成
    "ZCode/v3.17.2" 产生指纹漂移。

    正文锁：活跃版本永不越过 PROMPT_ALIGNED_VERSION（zcode_system.json 文本
    所属版本）。上游按「版本头 + system 正文」同版校验，头新于正文即
    405/3012，故越过时钳制到锁版本，返回实际生效版本。升级客户端必须先移植
    prompt.mjs 文本并抬高 PROMPT_ALIGNED_VERSION，本函数不提供绕过。
    """
    global CLIENT_APP_VERSION, BILLING_APP_VERSION, USER_AGENT, X_ZCODE_APP_VERSION, CLIENT_CONFIGS_QUERY
    v = str(new_version or "").strip().lstrip("vV").strip()
    if not v:
        return CLIENT_APP_VERSION
    if is_newer_version(v, PROMPT_ALIGNED_VERSION):
        v = PROMPT_ALIGNED_VERSION
    CLIENT_APP_VERSION = v
    BILLING_APP_VERSION = v
    USER_AGENT = f"ZCode/{v}"
    X_ZCODE_APP_VERSION = v
    CLIENT_CONFIGS_QUERY = f"app_version={v}"
    try:
        from . import settings

        if not os.getenv("UPSTREAM_USER_AGENT"):
            settings.USER_AGENT = USER_AGENT
    except (ImportError, AttributeError):
        pass
    return CLIENT_APP_VERSION

# 无尾斜杠：与 zcode-switch（billing 实证形态）及本服务 billing 头统一，
# 消除 messages/billing 通道间 Referer 形状抖动
HTTP_REFERER = "https://zcode.z.ai"
CAPTCHA_HEADER = "X-Aliyun-Captcha-Verify-Param"
# region 头（zapi captcha.ts REGION_HEADER；与 PARAM 成对下发，缺失易 3007）
CAPTCHA_REGION_HEADER = "X-Aliyun-Captcha-Verify-Region"

# ── 身份头仿真（对齐 zapi identity.ts 的 pio；顺序、取值逐字段镜像官方客户端）──
# 官方客户端 companion 头集合，让代理在指纹层与官方 ZCode 桌面端不可区分。
# 以下 IDENTITY_* 仅是无账号回退路径的进程级身份（identity.py 无 account 分支、
# install.py 进程级安装序、body_transform 无账号 Environment），配全局 device_mid；
# 账号请求一律走 app/fingerprint.py 的每账号 DeviceProfile（accounts.data.fingerprint，
# 成套 SKU + 独立 device_mid），两者勿混。进程级固定 darwin-arm64 是有意设计。
IDENTITY_TITLE = "Z Code@electron"      # X-Title = "Z Code@{sourceTitle}"；桌面端账号走 electron
IDENTITY_RELEASE_CHANNEL = "production"  # 同 BILLING_RELEASE_CHANNEL（bundle DF() 实证）
IDENTITY_CLIENT_LANGUAGE = "zh-CN"
IDENTITY_CLIENT_TIMEZONE = "Asia/Shanghai"
# X-Os-Category：darwin→macos / win32→windows / 其它→linux
IDENTITY_OS_CATEGORY = "macos"
# X-Os-Version：os.release() 语义；darwin 25.x 对应 macOS 26（本机实测 25.6.0）。
# 固定伪装值。
IDENTITY_OS_VERSION = "25.6.0"

# ── 上游被拒信号 → 账号动作（gateway / quota 判定共用）────────────────────────
EXHAUST_HTTP_STATUSES = (402,)
# 额度耗尽信号。2026-10-31 复盘：原实现是**裸子串**匹配 "quota"，任何含该词的
# 文案（含风控/验证码页）都会被判成「额度用完」而把号踢成 EXHAUSTED——比漏判更糟。
# 改为 ① 业务码（Plan 通道 {"code":1005,"msg":"exceed quota limit"} 实测形态）
#      ② 定量短语（完整词 + 明确的无额度语义），不再用裸词。
EXHAUST_BUSINESS_CODES = (1005, 1113, 1310)
# 1113「无余额/无资源包」单列：通道感知处置要用（Plan=套餐耗尽、Key=该 Key 无资源包）
NO_BALANCE_BUSINESS_CODE = 1113
# 429 承载的「真限流」业务码（Z.ai 频控）：与并发争用相反，原地重试只会加重
RATE_LIMIT_429_CODES = frozenset({1302, 1303})   # 1005 exceed quota limit / 1113 无余额 / 1310 周/月使用上限
EXHAUST_KEYWORDS = (
    "exceed quota", "quota limit", "quota exceeded", "quota exhausted",
    "insufficient balance", "insufficient quota", "no balance",
    "额度已用完", "额度用完", "额度不足", "余额不足", "余额已用完", "无余额",
    "使用上限", "weekly limit", "monthly limit", "rate limit exceeded",
)
# 验证码挑战：HTTP 403 + 文案，或 HTTP 400 + body {"code":3007}（docs 05 §被拒信号表）
# 注意：403 不再无条件判 invalid —— 需先排除验证码挑战（captcha/verify 文案或
# challenge 头），否则一次人机校验续期就把账号错杀成 INVALID（对齐 zapi
# classifyAccountFailure：403 + captcha 文案 → 非账号失败）。
CAPTCHA_BODY_MARKERS = ('"code":3007', '"code": 3007')

# ── 风控信号（2026-09-05 3012 事件实证）────────────────────────────────────────
# 3012「unusual activity」= 上游风控（HTTP 405 承载），不在官方公开错误码表；
# 高频请求触发，官方政策定性为临时限制（限流/冻结，3 次以上违规才封号）。
# 处置：账号指数退避冷却 + 暂停验证码池预热（池预热本身即上游流量，会加剧风控）。
RISK_CONTROL_HTTP_STATUSES = (405,)
RISK_BUSINESS_CODE = 3012
# 429 承载的「模型并发已满」（2026-11-02 实测 {"code":3009,"msg":"model
# concurrency limit exceeded"}）：并发争用是瞬态，短退避重试，不等 Retry-After。
# ⚠️ 判定必须解析业务码，不能用裸子串——响应体里的 logid 是 20 位随机数字，
# 裸子串 "3009" 会撞车（与 EXHAUST 裸子串教训同款，见上方 EXHAUST_KEYWORDS 注）。
CONCURRENCY_BUSINESS_CODE = 3009
RISK_CONTROL_MARKERS = (
    '"code":3012', '"code": 3012',
    "unusual activity",
)

# ── IP 级拦截页（阿里云 ESA/WAF，2026-11-05 对齐 magpie plugins#649）──────────
# 与上面的 3012 是**两种东西**，只是共用 HTTP 405 状态码：
#   拦截页 = 出口 IP 被网络防火墙拦（换账号无用，继续敲会延长封锁）
#   3012  = Z.ai 应用层风控（算账号的，走 lane 冷却）
# 判别只看响应体：正常响应都是 JSON/SSE，拦截页则是 HTML 错误页。
EDGE_PAGE_MARKERS = ("errors.aliyun.com",)
EDGE_PAGE_HTML_HINTS = ("<!doctype", "<html")
# ── 边缘封禁（IP 级，与账号级 3012 区分）─────────────────────────────────────
# 405 + HTML/「address blocked」= 出口 IP 被边缘整体封禁：换账号无用，必须
# 全进程暂停 plan 渠道（app/gate.py）。裸 405 / JSON 3012 才归账号风控。
EDGE_BLOCK_MARKERS = (
    "address was blocked",
    "network's address",
    "blocked by the edge",
)

