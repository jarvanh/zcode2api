"""上游身份头仿真 —— 对齐 zapi identity.ts 镜像的官方 ZCode 客户端 `pio` 头集合。

官方客户端对上游发的每个请求都携带这组 companion 头（指纹层），缺失或形状
不对都会提高 WAF 关注度。此处逐字段、按序复刻：

    HTTP-Referer, User-Agent, X-ZCode-App-Version, X-Title, X-ZCode-Agent,
    X-Platform, X-Release-Channel, X-Client-Language, X-Client-Timezone,
    X-Os-Category, X-Os-Version, X-Device-Mid

有账号时 X-Device-Mid / 平台 / 语言 / 时区取自该号 DeviceProfile（一号一台
生成的桌面设备）。无账号时回退全局伪装常量 + quota.device_mid()。

另含追踪头（zapi upstream.ts buildTraceHeaders）：JWT 通道即 zapi 的
start-plan，只发 x-request-id / x-zcode-session-type / x-zcode-trace-id
三个头（start-plan 不发 x-query-id / x-session-id，误发触发 3012）。
每请求重新生成。

平台/语言/时区/设备 ID 取自账号指纹（fingerprint.profile_for）：每账号独立
设备档案，消灭「多账号同一设备」的关联信号。account 为 None（CLI 自检等）
时回退全局伪装常量 + 本机 device_mid。
"""

from __future__ import annotations

import os
import re
import uuid

from . import constants, settings
from .quota import device_mid

# 打印可见 ASCII 门（ZCode bundle fio 助手）；任何头值不含此形态即丢弃该头
_ASCII_PRINTABLE = re.compile(r"^[\x20-\x7e]+$")


def _clean(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    v = value.strip()
    return v if v and _ASCII_PRINTABLE.match(v) else None


def _os_category(sys_platform: str) -> str:
    # 兼容两种形态：platform.system() 产出 darwin/windows/linux，伪装平台用 win32
    if sys_platform in ("darwin", "macos"):
        return "macos"
    if sys_platform in ("win32", "windows"):
        return "windows"
    return "linux"


def build_identity_headers(account=None) -> dict[str, str]:
    """构建完整身份头（保持 pio 的字段顺序；条件性缺失语义同样镜像）。

    有 account 时按其指纹档案出值（每账号独立设备形态，见 fingerprint.py）；
    无 account 时回退全局伪装常量（constants.CLIENT_PLATFORM 等，与 zapi
    identity.ts 同策略：服务端部署在 Linux 时 platform.* 会暴露云服务器特征，
    与官方桌面端形状不符）。env 覆盖仅用于无 account 路径的指纹实验。
    """
    app_version = _clean(constants.CLIENT_APP_VERSION)
    if account is not None:
        from .fingerprint import profile_for

        profile = profile_for(account)
        plat, arch = profile.platform, profile.arch
        release = profile.os_version
        channel: str | None = constants.BILLING_RELEASE_CHANNEL
        language, timezone = profile.language, profile.timezone
        device_mid_val = profile.device_mid
    else:
        _plat_arch = constants.CLIENT_PLATFORM.split("-")  # "darwin-arm64"
        plat = _clean(os.getenv("ZCODE_IDENTITY_PLATFORM", _plat_arch[0])) or "darwin"
        arch = _clean(os.getenv("ZCODE_IDENTITY_ARCH", _plat_arch[1] if len(_plat_arch) > 1 else "arm64")) or "arm64"
        release = _clean(os.getenv("ZCODE_IDENTITY_RELEASE", constants.IDENTITY_OS_VERSION))
        channel = _clean(os.getenv("ZCODE_IDENTITY_RELEASE_CHANNEL", constants.IDENTITY_RELEASE_CHANNEL))
        language = _clean(os.getenv("ZCODE_IDENTITY_CLIENT_LANGUAGE", constants.IDENTITY_CLIENT_LANGUAGE))
        timezone = _clean(os.getenv("ZCODE_IDENTITY_CLIENT_TIMEZONE", constants.IDENTITY_CLIENT_TIMEZONE))
        device_mid_val = _clean(os.getenv("ZCODE_IDENTITY_DEVICE_MID", device_mid()))

    headers: dict[str, str] = {
        "HTTP-Referer": constants.HTTP_REFERER,
        "User-Agent": settings.USER_AGENT,
    }
    if app_version:
        headers["X-ZCode-App-Version"] = app_version
    headers["X-Title"] = (
        constants.BILLING_TITLE if account is not None else constants.IDENTITY_TITLE
    )
    headers["X-ZCode-Agent"] = constants.X_ZCODE_AGENT
    headers["X-Platform"] = f"{plat}-{arch}"
    if channel:
        headers["X-Release-Channel"] = channel
    if language:
        headers["X-Client-Language"] = language
    if timezone:
        headers["X-Client-Timezone"] = timezone
    if plat:
        headers["X-Os-Category"] = _os_category(plat)
    if release:
        headers["X-Os-Version"] = release
    if device_mid_val:
        headers["X-Device-Mid"] = device_mid_val
    return headers


# anthropic SDK 身份合并进 UA（bundle Y2：`ai-sdk/anthropic/3.0.81`，3.16.3
# bundle gjs 常量实测提取；真实 LLM 请求的 UA 形状）
ANTHROPIC_SDK_UA = "ai-sdk/anthropic/3.0.81"


def build_llm_identity_headers(account=None) -> dict[str, str]:
    """LLM 请求头（bundle 3.12.3 g6n 源头构造器，与控制面 TV 头集的差异）：

    - 无 X-Device-Mid（LLM 请求从不带）；
    - X-Os-Category 始终发（CSs 绕过 printable 门）；
    - 语言/时区始终发，缺失时回退 "unknown"（真实客户端形状）；
    - X-ZCode-Agent 内联第 8 位（3.12.3 起不再由 x4i 包装器追加在末尾）。
    字段顺序镜像 g6n：HTTP-Referer, User-Agent, [X-ZCode-App-Version],
    X-Title, X-Client-Language, X-Client-Timezone, X-ZCode-Agent,
    [X-Platform], X-Os-Category, [X-Os-Version]。
    """
    base = build_identity_headers(account)
    llm: dict[str, str] = {
        "HTTP-Referer": base["HTTP-Referer"],
        "User-Agent": f"{base['User-Agent']} {ANTHROPIC_SDK_UA}",
    }
    if "X-ZCode-App-Version" in base:
        llm["X-ZCode-App-Version"] = base["X-ZCode-App-Version"]
    llm["X-Title"] = base["X-Title"]
    llm["X-Client-Language"] = base.get("X-Client-Language", "unknown")
    llm["X-Client-Timezone"] = base.get("X-Client-Timezone", "unknown")
    llm["X-ZCode-Agent"] = base["X-ZCode-Agent"]
    if "X-Platform" in base:
        llm["X-Platform"] = base["X-Platform"]
    if "X-Release-Channel" in base:
        llm["X-Release-Channel"] = base["X-Release-Channel"]
    # X-Os-Category 始终发（bundle CSs 绕过 printable 门）
    plat = base.get("X-Platform", "").split("-")[0]
    llm["X-Os-Category"] = base.get("X-Os-Category") or _os_category(plat or "darwin")
    if "X-Os-Version" in base:
        llm["X-Os-Version"] = base["X-Os-Version"]
    # 无 X-Device-Mid：LLM 请求从不携带（bundle g6n 实证）
    return llm


def build_trace_headers(plan: str = "start-plan") -> dict[str, str]:
    """追踪头：每请求全新 UUID（对齐 zapi upstream.ts buildTraceHeaders）。

    通道差异（关键，误发会触发上游 3012 "unusual activity"）：
      - start-plan（JWT 通道，cred.jwt 存在）：只发 x-request-id /
        x-zcode-session-type / x-zcode-trace-id 三个头。**不发**
        x-query-id / x-session-id —— 官方客户端 start-plan 请求不带这两个。
      - coding-plan（API Key 通道）：额外发 x-query-id / x-session-id。

    本服务 JWT 通道即 zapi 的 start-plan，故默认 plan="start-plan"。
    """
    headers = {
        "x-request-id": str(uuid.uuid4()),
        "x-zcode-session-type": "main",
        "x-zcode-trace-id": str(uuid.uuid4()),
    }
    if plan != "start-plan":
        headers["x-query-id"] = str(uuid.uuid4())
        headers["x-session-id"] = str(uuid.uuid4())
    return headers
