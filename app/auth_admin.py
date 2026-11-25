"""鉴权依赖：后台管理密钥 + 可选的网关 API Key。"""

from __future__ import annotations

import hmac
import ipaddress
import os
import time

from fastapi import Header, HTTPException, Query, Request, status

from . import settings
from .store import store

ADMIN_FAIL_LIMIT = 8
ADMIN_LOCK_SECONDS = 300
# 失败记录容量上限：每伪造 IP 一条记录的字典不能无界增长
#（对公网暴露的实例，攻击者可用海量假 IP 撑爆内存）
_MAX_FAIL_TRACKED = 4096

# 是否信任反代注入的客户端 IP 头（cf-connecting-ip / x-real-ip）。
# fork：ZCODE_TRUST_PROXY_IP=1 + 受信网段；上游：TRUST_PROXY_HEADERS=1 显式开关。
TRUST_PROXY_IP = (os.getenv("ZCODE_TRUST_PROXY_IP") or "0").strip() == "1"
DEFAULT_TRUSTED_PROXIES = "127.0.0.0/8,::1"


def _parse_trusted_proxies(raw: str) -> tuple:
    networks = []
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            networks.append(ipaddress.ip_network(part, strict=False))
        except ValueError:
            continue
    return tuple(networks)


TRUSTED_PROXY_NETWORKS = _parse_trusted_proxies(
    os.getenv("ZCODE_TRUSTED_PROXIES", DEFAULT_TRUSTED_PROXIES)
)

# ip -> {count, locked_until, last}；容量封顶（上游 2026-11-21 P1）。
_failures: dict[str, dict] = {}
_FAILURES_MAX = 1024


def reset_failures() -> None:
    """测试夹具：清空登录失败计数。"""
    _failures.clear()


def _is_trusted_proxy(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
    return any(addr in net for net in TRUSTED_PROXY_NETWORKS)


def _client_ip(request: Request) -> str:
    """客户端身份（锁定键）。三条采信路径，其余用 socket 对端地址：
    1) fork：TRUST_PROXY_IP=1 且来源属于受信网段；
    2) upstream：settings.TRUST_PROXY_HEADERS=1；
    3) upstream 回环豁免：HOST=127.0.0.1 且来源 127.0.0.1（本机隧道）。
    头值必须为合法 IP，畸形忽略退回真实来源。"""
    peer = request.client.host if (request.client and request.client.host) else ""

    def _from_headers() -> str | None:
        for header in ("cf-connecting-ip", "x-real-ip"):
            val = (request.headers.get(header) or "").strip()
            if not val:
                continue
            candidate = val.split(",")[0].strip()
            try:
                ipaddress.ip_address(candidate)
            except ValueError:
                continue
            return candidate
        return None

    if TRUST_PROXY_IP and peer and _is_trusted_proxy(peer):
        got = _from_headers()
        if got:
            return got
    if settings.TRUST_PROXY_HEADERS:
        got = _from_headers()
        if got:
            return got
    if peer == "127.0.0.1" and settings.HOST == "127.0.0.1":
        got = _from_headers()
        if got:
            return got
    return peer or "unknown"


def _gc_failures(now: float) -> None:
    """过期清扫（fork 2026-09-29 配套）：超容量按 last 淘汰一半。"""
    stale = [
        ip for ip, rec in _failures.items()
        if float(rec.get("locked_until") or 0) < now
        and now - float(rec.get("last") or 0) > ADMIN_LOCK_SECONDS
    ]
    for ip in stale:
        _failures.pop(ip, None)
    if len(_failures) > _MAX_FAIL_TRACKED:
        ordered = sorted(_failures.items(), key=lambda kv: float(kv[1].get("last") or 0))
        for ip, _ in ordered[: len(ordered) // 2]:
            _failures.pop(ip, None)


def _is_locked(ip: str) -> bool:
    rec = _failures.get(ip)
    if not rec:
        return False
    until = float(rec.get("locked_until") or 0)
    if until and time.time() < until:
        return True
    if until and time.time() >= until:
        _failures.pop(ip, None)
    return False


def _record_failure(ip: str, probed: bool = False) -> None:
    """probed=True 只计数不锁（fork 2026-09-29 修复）；容量封顶（上游 P1）。"""
    now = time.time()
    _gc_failures(now)
    cap = min(_MAX_FAIL_TRACKED, _FAILURES_MAX)
    if len(_failures) >= cap and ip not in _failures:
        for _ in range(len(_failures) - cap + 1):
            try:
                _failures.pop(next(iter(_failures)))
            except StopIteration:
                break
    rec = _failures.setdefault(ip, {"count": 0, "locked_until": 0.0, "last": now})
    rec["last"] = now
    rec["count"] = int(rec.get("count") or 0) + 1
    if probed:
        return
    if rec["count"] >= ADMIN_FAIL_LIMIT:
        rec["locked_until"] = now + ADMIN_LOCK_SECONDS
    rec["count"] = int(rec.get("count") or 0) + 1
    if rec["count"] >= ADMIN_FAIL_LIMIT:
        rec["locked_until"] = now + ADMIN_LOCK_SECONDS


def _extract_bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token


def _key_equal(token: str, key: str) -> bool:
    """常数时间比较（UTF-8 字节）：str 版 compare_digest 遇非 ASCII 会抛
    TypeError（500），等价于把「任意非 ASCII 输入」变成可观测的异常侧信道。"""
    return hmac.compare_digest(token.encode("utf-8"), key.encode("utf-8"))


async def verify_admin_key(
    request: Request,
    authorization: str | None = Header(default=None),
    login: str | None = Query(default=None),
) -> None:
    """校验后台管理密钥。

    只接受 `Authorization: Bearer <key>` 头；查询参数鉴权（`?app_key=`）随上游
    2.7.33 移除——URL 会进访问日志、浏览器历史、Referer，密钥不该出现在 URL 里。

    `login=1` 标记「人正在提交密码」（登录页），失败计入防爆破计数；不带
    则视为会话探测（页面轮询检查本地密钥是否仍有效），失败不计入锁定——
    否则监控页 5 秒一轮的 stale-key 轮询会持续锁死 IP，正确密码也进不来。
    """
    ip = _client_ip(request)
    if _is_locked(ip):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "登录失败次数过多，请稍后再试")

    key = store.admin_key()
    if not key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "未配置后台密钥")

    token = _extract_bearer(authorization)
    probed = login is None or str(login).strip().lower() not in ("1", "true", "yes")
    if token is None:
        _record_failure(ip, probed=probed)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少鉴权凭证")
    if not _key_equal(token, key):
        _record_failure(ip, probed=probed)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "鉴权凭证无效")
    _failures.pop(ip, None)


async def verify_gateway_key(
    request: Request,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
) -> None:
    """校验 /v1/messages 网关访问密钥（未配置则拒绝：fail-closed）。

    未配置密钥时放行等于把上游账号池暴露成公开代理——只要部署后忘了设置
    gateway key，任何人都能用你的额度。唯一逃生口是显式设置
    `ZCODE_GATEWAY_ALLOW_NO_KEY=1`（本地开发/测试）。失败锁定复用 admin
    的 _failures 表按客户端身份分键（上游 2026-11-21 review P3）。
    """
    ip = _client_ip(request)
    if _is_locked(ip):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "失败次数过多，请稍后再试")

    key = store.gateway_key()
    if not key:
        if settings.GATEWAY_ALLOW_NO_KEY:
            return
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "网关密钥未配置：请先在后台设置 Gateway API Key 再使用 /v1 端点",
        )
    token = _extract_bearer(authorization) or x_api_key
    if token is None:
        _record_failure(ip)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少 API Key")
    if not _key_equal(token, key):
        _record_failure(ip)
        raise HTTPException(status.HTTP_403_FORBIDDEN, "API Key 无效")
    _failures.pop(ip, None)
