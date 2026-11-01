"""同内容请求去重 —— 防 openclaw 重试风暴烧池（2026-10-31 波次观察）。

波次窗口内，客户端对同一内容的并发重试会各自挑号各打一次上游：既不能提高
成功率（风控按通道拦），还给风控添柴（RISK_FAIL_FAST 的两账号确认制就是被
这种风暴推着烧穿池子的）。本模块按「客户端内容指纹」做两件事：

  1. 冷却期快速失败：一次调度以失败收场（最终响应 >= 400）后，TTL 内同指纹
     请求直接重放该失败响应，完全不进调度——零换号、零 strike、零排队；
  2. 在途合并（仅非流式）：leader 在调度中时，follower 有界等待其结局；
     leader 失败 → 重放失败响应；leader 成功/超时 → follower 正常走自己的
     调度（上游已被证明健康，此时串行重放反而多余）。

指纹 = sha256(model + system + messages 规范化 JSON)：取转换后、transform
前的 body——该形态跨账号一致（transform 的 metadata.user_id 是每账号差异），
正是「同一份客户端内容」的语义。流式不合并也不记失败缓存（SSE 中途失败在
生成器里，守卫层拿不到真实结局），只享受指纹成功路径的免等待。

内存 LRU：上限 ZCODE_DEDUP_MAX，插入时惰性清理过期项。纯进程内，重启即清。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

from . import settings

_inflight: dict[str, _Entry] = {}
_failures: dict[str, tuple[int, bytes, float]] = {}  # fp → (status, body, until_monotonic)


class _Entry:
    __slots__ = ("event", "ok", "status", "payload")

    def __init__(self) -> None:
        self.event = asyncio.Event()
        self.ok: bool | None = None
        self.status: int = 0
        self.payload: bytes = b""


def fingerprint(body: dict) -> str | None:
    """客户端内容指纹；畸形 body 返回 None（去重对畸形输入 no-op）。"""
    try:
        canonical = json.dumps(
            {"model": body.get("model"), "system": body.get("system"),
             "messages": body.get("messages")},
            ensure_ascii=False, sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    except (TypeError, ValueError):
        return None


def _prune(now: float) -> None:
    if len(_failures) > settings.DEDUP_MAX:
        for k in [k for k, v in _failures.items() if v[2] <= now][: len(_failures) - settings.DEDUP_MAX]:
            _failures.pop(k, None)
    if len(_inflight) > settings.DEDUP_MAX:
        for k in [k for k, v in _inflight.items() if v.ok is not None][: len(_inflight) - settings.DEDUP_MAX]:
            _inflight.pop(k, None)


def peek_failure(fp: str) -> tuple[int, bytes] | None:
    """TTL 内的同指纹失败缓存；过期顺带清理。"""
    now = time.monotonic()
    hit = _failures.get(fp)
    if hit is None:
        return None
    status, payload, until = hit
    if until <= now:
        _failures.pop(fp, None)
        return None
    return status, payload


def begin_inflight(fp: str) -> bool:
    """登记在途。True = 本请求是 leader；False = 已有 leader，本请求是 follower。"""
    _prune(time.monotonic())
    if fp in _inflight:
        return False
    _inflight[fp] = _Entry()
    return True


async def wait_inflight(fp: str, timeout: float) -> str:
    """follower 等待 leader 结局。返回 "failed" / "ok" / "timeout"。

    无在途（含 leader 已被 discard/清理）→ "ok" 放行：等待的意义是「不要和
    leader 撞」，没有在途就没有撞的理由。
    """
    entry = _inflight.get(fp)
    if entry is None:
        return "ok"
    try:
        await asyncio.wait_for(entry.event.wait(), timeout=timeout)
    except TimeoutError:
        return "timeout"
    return "failed" if entry.ok is False else "ok"


def failure_of(fp: str) -> tuple[int, bytes]:
    entry = _inflight.get(fp)
    if entry is not None and entry.ok is False:
        return entry.status, entry.payload
    hit = peek_failure(fp)
    return hit if hit else (503, b'{"error":{"message":"upstream failure","type":"upstream_error"}}')


def note_result(fp: str, status: int, payload: bytes, stream: bool) -> None:
    """leader 结局登记：≥400 失败进冷却缓存并唤醒 follower；其余按成功放行。

    流式 leader 的真实结局在生成器里，此处只登记「预流化成功」，失败不进
    冷却缓存（守卫层注释说明了取舍）。
    """
    entry = _inflight.pop(fp, None)
    if entry is not None:
        entry.ok = status < 400
        entry.status = status
        entry.payload = payload
        entry.event.set()
    if status >= 400 and not stream:
        now = time.monotonic()
        _prune(now)
        _failures[fp] = (status, payload, now + settings.DEDUP_TTL)


def discard(fp: str) -> None:
    """异常路径兜底：leader 没能产出结局时清登记，避免 follower 白等满超时。"""
    entry = _inflight.pop(fp, None)
    if entry is not None and entry.ok is None:
        entry.ok = True  # 放行 follower 自己去试
        entry.event.set()


def stats() -> dict:
    return {"inflight": len(_inflight), "failure_cache": len(_failures),
            "ttl": settings.DEDUP_TTL}


def _reset_for_tests() -> None:
    _inflight.clear()
    _failures.clear()
