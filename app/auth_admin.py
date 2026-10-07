"""鉴权依赖：后台管理密钥 + 可选的网关 API Key。"""

from __future__ import annotations

import hmac
import os
import time

from fastapi import Header, HTTPException, Query, Request, status

from .store import store

ADMIN_FAIL_LIMIT = 8
ADMIN_LOCK_SECONDS = 300
# 失败记录容量上限：每伪造 IP 一条记录的字典不能无界增长
#（对公网暴露的实例，攻击者可用海量假 IP 撑爆内存）
_MAX_FAIL_TRACKED = 4096

# 是否信任反代注入的客户端 IP 头（cf-connecting-ip / x-real-ip）。
# 部署在 CF/Nginx 反代之后时必须开启（否则所有请求共享代理 IP，一个 IP
# 锁定全站）；无反代直连暴露时置 False —— 直连场景该头可被客户端伪造，
# 每伪造一个 IP 即获得一个全新的失败计数桶，等效绕过登录锁定。
TRUST_PROXY_IP = os.getenv("ZCODE_TRUST_PROXY_IP", "1").strip().lower() not in ("0", "false", "no")

# ip -> {count, locked_until, last}
_failures: dict[str, dict] = {}


def reset_failures() -> None:
    """测试夹具：清空登录失败计数。"""
    _failures.clear()


def _client_ip(request: Request) -> str:
    if TRUST_PROXY_IP:
        for header in ("cf-connecting-ip", "x-real-ip"):
            val = (request.headers.get(header) or "").strip()
            if val:
                return val.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


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


async def verify_admin_key(
    request: Request,
    authorization: str | None = Header(default=None),
    app_key: str | None = Query(default=None),
    login: str | None = Query(default=None),
) -> None:
    """校验后台管理密钥。

    支持 `Authorization: Bearer <key>` 头或 `?app_key=<key>` 查询参数
    （后者用于 EventSource 等无法发送自定义头的场景）。

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

    token = _extract_bearer(authorization) or app_key
    probed = login is None or str(login).strip().lower() not in ("1", "true", "yes")
    if token is None:
        _record_failure(ip, probed=probed)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少鉴权凭证")
    if not hmac.compare_digest(token, key):
        _record_failure(ip, probed=probed)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "鉴权凭证无效")
    _failures.pop(ip, None)


async def verify_gateway_key(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
) -> None:
    """校验 /v1/messages 网关访问密钥（未配置则放行）。"""
    key = store.gateway_key()
    if not key:
        return
    token = _extract_bearer(authorization) or x_api_key
    if token is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少 API Key")
    if not hmac.compare_digest(token, key):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "API Key 无效")
