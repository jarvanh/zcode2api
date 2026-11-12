"""inflight_metrics.py —— 进程级在途请求计数（当前值 + 进程内峰值）

看板「实时/并发」小卡的数据真源。只统计业务请求，观测端点不计入，
避免看板自己的轮询把曲线顶起来。峰值是进程启动以来最大值，重启归零。
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_lock = threading.Lock()
_cur = 0
_peak = 0

# 只统计这些前缀；/meta、/health、静态资源、管理页都不算「AI 请求在途」
_COUNT_PATHS = ("/v1/chat/completions", "/v1/messages", "/chat/completions")


def begin() -> None:
    global _cur, _peak
    with _lock:
        _cur += 1
        if _cur > _peak:
            _peak = _cur


def end() -> None:
    global _cur
    with _lock:
        _cur = max(0, _cur - 1)


def snapshot() -> dict:
    from .store import store
    # 单账号并发上限 × 同时投入账号数 = 网关有效并发上限。
    # 运行时 meta 可覆盖 max_active_accounts，所以从 store 实时读，不写死。
    per_acc = store.account_concurrency()
    active_cap = store.max_active_accounts()
    max_conc = per_acc * active_cap if active_cap > 0 else None
    with _lock:
        return {
            "inflight": _cur,
            "inflight_peak": _peak,
            "max_concurrency": max_conc,
            "max_per_account": per_acc,
            "max_active_accounts": active_cap,
        }


class InflightMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        path = request.url.path
        if request.method != "POST" or not path.startswith(_COUNT_PATHS):
            return await call_next(request)
        begin()
        try:
            return await call_next(request)
        finally:
            end()
