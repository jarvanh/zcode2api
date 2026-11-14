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
# 默认关闭：直连场景该头可被客户端伪造，每伪造一个 IP 即获得一个全新的
# 失败计数桶，等效绕过登录锁定。仅显式设为 1 且 TCP 来源属于受信代理时
# 才读取转发头；无反代直连部署必须保持 0。
TRUST_PROXY_IP = (os.getenv("ZCODE_TRUST_PROXY_IP") or "0").strip() == "1"
# 受信代理来源（逗号分隔 IP/CIDR）；默认只信任本机回环（cloudflared/nginx
# 与网关同机）。反代在其它地址时用 ZCODE_TRUSTED_PROXIES 显式追加。
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

# ip -> {count, locked_until, last}
_failures: dict[str, dict] = {}


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
    peer = request.client.host if request.client and request.client.host else ""
    if TRUST_PROXY_IP and peer and _is_trusted_proxy(peer):
        for header in ("cf-connecting-ip", "x-real-ip"):
            val = (request.headers.get(header) or "").strip()
            if not val:
                continue
            candidate = val.split(",")[0].strip()
            try:
                ipaddress.ip_address(candidate)
            except ValueError:
                continue  # 伪造/畸形头：忽略，退回真实来源
            return candidate
    return peer or "unknown"


def _gc_failures(now: float) -> None:
    """清理过期失败记录，防无界增长。

    锁定窗口已过且超过一个锁定周期未再失败的记录直接淘汰；仍超容量
    时按 last 淘汰最旧的一半（锁定中的记录保留，避免 GC 放走爆破者）。
    """
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
    """记录一次失败。

    probed=True 表示会话探测（页面轮询 /admin/api/verify 检查本地密钥是否
    仍然有效：密钥过期、后台改密后旧标签页都会打出 401）。这类请求不是人
    在猜密码，却按旧逻辑每次都计数——监控页 5 秒一轮，40 秒即可把 IP 锁满
    300 秒，锁定期内正确密码也被拒（2026-09-29 连续两次实际锁死）。
    探测只计数不锁：锁只保留给真正的登录提交。
    """
    now = time.time()
    _gc_failures(now)
    rec = _failures.setdefault(ip, {"count": 0, "locked_until": 0.0, "last": now})
    rec["last"] = now
    if probed:
        rec["count"] = int(rec.get("count") or 0) + 1
        return
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
    gateway key，任何人都能用你的额度（2.7.33）。唯一逃生口是显式设置
    `ZCODE_GATEWAY_ALLOW_NO_KEY=1`（本地开发/测试）。
    """
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
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少 API Key")
    if not _key_equal(token, key):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "API Key 无效")
