"""上游请求构建。

负责根据账号凭证选择端点、组装请求头、应用 body 变换。
实际发送与流式透传在 routes/gateway.py。
"""

from __future__ import annotations

import json
import uuid

from . import body_transform, constants, endpoint_routing, settings
from .captcha import captcha_manager
from .identity import build_llm_identity_headers, build_trace_headers
from .models import Account

# 透传客户端 header 时需要剔除的字段
_DROP_HEADERS = {
    "host",
    "content-length",
    "content-type",
    "x-api-key",
    "authorization",
    "user-agent",
    "http-referer",
    "referer",
    "origin",
    "cookie",
    "accept",
    "accept-language",
    "accept-encoding",
    "connection",
    "true-client-ip",
    "x-original-forwarded-for",
    # 协议头固定由本服务生成（application/json / anthropic-version），
    # 客户端自定义值（如 multipart boundary、异常版本号）会破坏上游请求形状
    "anthropic-version",
    "anthropic-beta",
    # 身份/追踪头由本服务仿真生成，禁止客户端透传覆盖（指纹一致性）
    "x-device-mid",
    "x-request-id",
    "x-zcode-trace-id",
    "x-zcode-session-type",
    "x-query-id",
    "x-session-id",
    "x-title",
    "x-platform",
    "x-release-channel",
    "x-client-language",
    "x-client-timezone",
    "x-os-category",
    "x-os-version",
}

# 前缀剔除：本服务仿真的头族 + 客户端 SDK 特征头族。
# x-stainless-* 是 Anthropic SDK 自动附加的运行环境指纹（lang/runtime/package 版本），
# 值来自真实调用客户端而非官方 ZCode 桌面端 —— 与 ZCode/3.10.2 的 UA 组成矛盾信号，
# 且 zapi（Node fetch 直发、无 stainless 头）长期被上游正常接受，剔除后同为已验证形状。
# x-forwarded-*/forwarded/x-real-ip/via/cf-*/cdn-loop 是反代与 CDN 隧道注入的
# 基础设施头（2026-09 review）：官方客户端永不携带，透传泄露部署拓扑并构成指纹矛盾。
_DROP_HEADER_PREFIXES = ("x-zcode", "x-stainless", "x-forwarded", "forwarded",
                         "x-real-ip", "via", "cf-", "cdn-loop")


def build_request(
    account: Account,
    body: dict,
    verify_param: str | None,
    incoming_headers: dict | None = None,
    verify_region: str | None = None,
    force_fallback: bool = False,
    force_plan: bool = False,
) -> tuple[str, dict, bytes]:
    """返回 (目标 URL, 请求头, 序列化后的请求体)。

    body 变换（cache_control / metadata.user_id）在此统一应用：变换幂等，
    网关验证码重试时用同一 body 重建请求，重复调用安全。

    force_fallback：强制走 API Key 回退通道（Plan 通道瞬态失败如 429 时，
    账号 status 保持 ACTIVE，不能靠状态推导路由，须显式指定）。

    force_plan：反向例外，**仅供 plan-lane 探测轮使用**。它要测的正是「这条
    被冷却的腿通了没」，必须顶过自己的 lane 门禁把请求发到 Plan 通道；真实
    流量禁止传该标志（否则「冷却期零上游流量」不变量会被热路径踩穿）。
    """
    provider = account.provider

    use_plan = provider == "zai" and account.uses_plan_channel() and not force_fallback
    if provider == "zai" and use_plan and not force_plan:
        # 通道级风控：Plan 腿正在冷却且有可用回退 Key 时，本请求直接路由到
        # Key 通道（否则会把封锁拆开重踩，且选号能选中≠Plan 腿可打）。
        use_plan = account.plan_lane_available(
            body.get("model") if isinstance(body.get("model"), str) else ""
        )

    if provider == "zai":
        if use_plan:
            target_url = settings.UPSTREAM["zai"]
            auth = {"Authorization": f"Bearer {account.jwt_token}"}
        elif account.api_key:
            target_url = settings.UPSTREAM["zai_fallback"]
            auth = {"x-api-key": account.api_key}
        else:
            raise RuntimeError("账号缺少有效凭证")
    elif provider == "bigmodel":
        target_url = settings.UPSTREAM["bigmodel"]
        if not account.api_key:
            raise RuntimeError("BigModel 账号缺少 API Key")
        auth = {"x-api-key": account.api_key}
    else:
        raise RuntimeError(f"未知提供商: {provider}")

    if provider == "zai" and use_plan:
        # JWT 通道：LLM 身份头（g6n：无 Device-Mid、UA 带 SDK 后缀）+ 追踪头。
        # 会话粘性（2.7.x 并入）：同首轮文本 → 同 session_id，官方会话内稳定，
        # 上游 prompt cache 依赖它。必须在 transform_body 之前算——日期轮会插到
        # messages[0] 改变首轮内容。device_mid/session_id 同时喂 attribution blob，
        # 与 x-session-id 同值（不一致会构成指纹矛盾）。
        from .fingerprint import profile_for

        profile = profile_for(account)
        session_id = body_transform.conversation_session_id(body, account.id) or str(uuid.uuid4())
        user_id = body_transform.jwt_user_id(account.jwt_token)
        model = body.get("model") if isinstance(body.get("model"), str) else None
        body = body_transform.transform_body(
            body, user_id, model,
            account=account,
            device_mid=profile.device_mid,
            session_id=session_id,
        )
        # 端点映射跟随（fail-open）：按上游 agent/configs 快照改写 URL；
        # 标记条目带 X-ZCode-Authorization（本侧有 JWT 才附）
        target_url, extra_headers = endpoint_routing.apply(target_url, account.jwt_token)
        headers = {
            "content-type": "application/json",
            **auth,
            "anthropic-version": constants.ANTHROPIC_VERSION,
            # start-plan 网关的 harness 要求：mid-conversation system 能力位，
            # 官方客户端 LLM 调用恒带（vlelyanin / TriDefender 双源一致）。
            "anthropic-beta": "mid-conversation-system-2026-04-07",
            **build_llm_identity_headers(account),
            **build_trace_headers(session_id),
            **extra_headers,
        }
        # 官方 anthropic SDK 同时下发 x-api-key（与 Bearer 同值）；
        # 缺失会被 start-plan 网关视为非客户端流量（vlelyanin 实证）。
        if account.jwt_token:
            headers["x-api-key"] = account.jwt_token
    else:
        # API Key 通道（回退 / bigmodel）：保持原有最小头集
        headers = {
            "content-type": "application/json",
            **auth,
            "anthropic-version": constants.ANTHROPIC_VERSION,
            "User-Agent": settings.USER_AGENT,
            "X-ZCode-App-Version": constants.X_ZCODE_APP_VERSION,
            "X-ZCode-Agent": constants.X_ZCODE_AGENT,
            "HTTP-Referer": constants.HTTP_REFERER,
        }
    if verify_param:
        headers[constants.CAPTCHA_HEADER] = verify_param
    if settings.CAPTCHA_ATTACH_PLAN and not verify_param and use_plan:
        # C 档（默认关）：plan 通道每请求附一枚池内验证码 token（zcode-api 实弹：
        # 验证码头是 start-plane 承重结构，裸发会被 ESA 边缘间歇拒绝）。仅在
        # 池内直取（try_take_now 绝不现解），池空 fail-open 按无码形态发出。
        token = captcha_manager.try_take_now()
        if token:
            headers[constants.CAPTCHA_HEADER] = token[0]
            if token[1]:
                headers[constants.CAPTCHA_REGION_HEADER] = token[1]
    if verify_region:
        headers[constants.CAPTCHA_REGION_HEADER] = verify_region

    for key, value in (incoming_headers or {}).items():
        lower = key.lower()
        if lower in _DROP_HEADERS or lower.startswith(_DROP_HEADER_PREFIXES):
            continue
        headers[key] = value

    return target_url, headers, json.dumps(body, ensure_ascii=False).encode("utf-8")
