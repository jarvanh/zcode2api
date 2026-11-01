"""端点映射跟随（app/endpoint_routing.py，2026-11-01 移植自 TriDefender/zcode-api）。

对齐官方 ZCode 3.7+ ProviderEndpointRoutingService（参考实现
endpoint-routing.ts）：官方客户端周期性 GET {origin}/api/v1/agent/configs，
按响应 data.proxyEndpoint.mapping 的 from → to 表改写 provider 请求地址。
2026-08-19 起服务端把 coding-plan 的 Anthropic 端点映射到
zcode.z.ai/api/v1/ultra[-zai]/... —— 上游正在把推理流量从开放平台往
zcode.z.ai 搬。跟随同一张表，上游迁移时不需要等发版。

fail-open 铁律：拉取/解析失败保留旧快照（或无快照），请求一律原 URL 放行；
改写是「上游说了才做」的优化，永远不能变成网关自造的故障。热路径 apply()
纯同步查表：无网络、无重试、绝不抛。

表形状与校验（与参考实现逐条对齐，违例整快照拒绝、退回旧快照）：
  - from/to 必须是无 query/hash/凭据的纯 https URL；key = 规范化请求 URL
    （scheme/host 小写、缺省端口 443、path 去尾斜杠）；
  - 表上限 256 条（防上游被玩坏撑爆内存）；from 冲突整快照拒绝；
  - 条目可带 zcodeAuthorizationHeader 标记（3.16.2）：字段值必须是头名自身
    （大小写不敏感）且 to 必须在配置 origin 的官方 ultra 路径下，否则整快照
    拒绝；请求侧 fail-open——标记命中但本侧无 JWT 时照发、只是不带该头。
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from . import constants, identity, logs, settings

SUCCESS_TTL = 300.0        # 成功快照有效期（参考实现 5 分钟）
FAILURE_COOLDOWN = 30.0    # 失败冷却（参考实现 30s）
REQUEST_TIMEOUT = 3.0      # 单次拉取超时（参考实现 3s）
MAX_MAPPING_ENTRIES = 256

_CONFIG_PATH = "/api/v1/agent/configs"
_AUTH_HEADER_NAME = "X-ZCode-Authorization"
_AUTH_HEADER_MARKER = "x-zcode-authorization"
_ALLOWED_AUTH_PATHS = ("/api/v1/ultra", "/api/v1/ultra-zai")

_lock = threading.Lock()
_snapshot: dict | None = None  # {"expires_at": float, "mapping": dict[str, _Target]}
_retry_after = 0.0
_task: asyncio.Task | None = None


@dataclass(frozen=True)
class _Target:
    url: str
    include_zcode_auth: bool


def _norm_path(path: str) -> str:
    if path == "/":
        return "/"
    return path.rstrip("/") or "/"


def _routing_key(url: str) -> str | None:
    """规范化查表键；非 https / 解析失败返回 None（按不匹配处理，原样放行）。"""
    try:
        parts = urlsplit(url)
        if parts.scheme.lower() != "https" or not parts.hostname:
            return None
        port = parts.port or 443
        return f"https://{parts.hostname.lower()}:{port}{_norm_path(parts.path)}"
    except ValueError:
        return None


def _parse_plain_url(value: object) -> str:
    """from/to 校验：纯 https URL，无 query/fragment/凭据；返回规范化 href。"""
    if not isinstance(value, str):
        raise ValueError("mapping url must be a string")
    parts = urlsplit(value.strip())
    if parts.scheme.lower() != "https" or not parts.hostname:
        raise ValueError("mapping url must be https")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("mapping url must be a plain https URL")
    port = parts.port or 443
    return f"https://{parts.hostname.lower()}:{port}{_norm_path(parts.path)}"


def _is_allowed_auth_target(parts: urlsplit, endpoint_origin: str) -> bool:
    """3.16.2 isAllowedIdentityTarget：origin 必须等于配置 origin，路径必须是官方 ultra 路径（含子路径）。"""
    origin = f"https://{parts.hostname.lower()}:{parts.port or 443}"
    if origin != endpoint_origin:
        return False
    path = _norm_path(parts.path)
    return any(path == p or path.startswith(f"{p}/") for p in _ALLOWED_AUTH_PATHS)


def _build_mapping(data: object) -> dict[str, _Target]:
    """data.proxyEndpoint.mapping → 查表 dict；任何违例抛 ValueError（整快照拒绝）。"""
    if not isinstance(data, dict):
        raise ValueError("agent_configs data missing")
    proxy = data.get("proxyEndpoint")
    entries = proxy.get("mapping") if isinstance(proxy, dict) else None
    if entries is None:
        return {}
    if not isinstance(entries, list):
        raise ValueError("mapping must be a list")
    if len(entries) > MAX_MAPPING_ENTRIES:
        raise ValueError("too many mappings")
    endpoint_origin = f"https://{urlsplit(constants.ZCODE_ORIGIN).hostname}:443"
    mapping: dict[str, _Target] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("mapping entry must be an object")
        from_url = _parse_plain_url(entry.get("from"))
        to_parts = urlsplit(str(_parse_plain_url(entry.get("to"))))
        raw_marker = entry.get("zcodeAuthorizationHeader")
        if raw_marker is not None and not isinstance(raw_marker, str):
            raise ValueError("invalid zcodeAuthorizationHeader")
        marker = (raw_marker or "").strip()
        include_auth = bool(marker)
        if include_auth and (
            marker.lower() != _AUTH_HEADER_MARKER
            or not _is_allowed_auth_target(to_parts, endpoint_origin)
        ):
            raise ValueError("unsafe zcode authorization target")
        if from_url in mapping:
            raise ValueError("duplicate from")
        mapping[from_url] = _Target(url=to_parts.geturl(), include_zcode_auth=include_auth)
    return mapping


def refresh() -> int:
    """拉一次 agent/configs 并换快照。成功返回条目数；失败抛异常（调用方记冷却）。"""
    headers = {**identity.build_identity_headers(None), "Accept": "application/json"}
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=False) as client:
        resp = client.get(f"{constants.ZCODE_ORIGIN}{_CONFIG_PATH}", headers=headers)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(f"agent_configs_http_{resp.status_code}")
    envelope = resp.json()
    if not isinstance(envelope, dict) or envelope.get("code") != 0:
        raise RuntimeError("agent_configs_nonzero_code")
    mapping = _build_mapping(envelope.get("data"))
    global _snapshot
    with _lock:
        _snapshot = {"expires_at": time.monotonic() + SUCCESS_TTL, "mapping": mapping}
    return len(mapping)


def _refresh_guarded() -> bool:
    """带失败冷却的拉取；返回是否成功。全局异常兜底，绝不向上抛。"""
    global _retry_after
    now = time.monotonic()
    if _retry_after > now:
        return False
    try:
        n = refresh()
        logs.info("endpoint_routing", f"快照更新：{n} 条映射")
        return True
    except Exception as err:  # noqa: BLE001 - fail-open
        _retry_after = time.monotonic() + FAILURE_COOLDOWN
        logs.info("endpoint_routing", f"拉取失败（{FAILURE_COOLDOWN:.0f}s 后重试）：{err}")
        return False


async def _loop() -> None:
    while True:
        ok = await asyncio.to_thread(_refresh_guarded)
        await asyncio.sleep(SUCCESS_TTL if ok else FAILURE_COOLDOWN)


def start() -> None:
    """启动后台跟随轮（main.lifespan 调用；不阻塞启动，首拍前请求原样放行）。"""
    global _task
    if settings.ENDPOINT_ROUTING and (_task is None or _task.done()):
        _task = asyncio.create_task(_loop())


async def stop() -> None:
    global _task
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
    _task = None


def apply(url: str, zcode_jwt: str | None = None) -> tuple[str, dict[str, str]]:
    """热路径查表改写。返回 (最终 URL, 附加头)；不匹配/关闭/任何异常 → (原 URL, {})。

    原请求 query 保留拼到改写后地址（参考实现口径：to 上的 query 丢弃）。
    """
    if not settings.ENDPOINT_ROUTING:
        return url, {}
    try:
        snap = _snapshot
        if not snap or snap["expires_at"] <= time.monotonic():
            return url, {}
        key = _routing_key(url)
        if key is None:
            return url, {}
        target = snap["mapping"].get(key)
        if target is None:
            return url, {}
        extra: dict[str, str] = {}
        if target.include_zcode_auth and zcode_jwt:
            extra[_AUTH_HEADER_NAME] = f"Bearer {zcode_jwt}"
        query = urlsplit(url).query
        new_url = f"{target.url}?{query}" if query else target.url
        return new_url, extra
    except Exception:  # noqa: BLE001 - fail-open
        return url, {}
