"""会话亲和观测（observe 档）—— 为「是否值得做会话亲和」攒数据。

背景（2026-11-01 对比 zcode-api session-context.ts 得出）：稳定会话标识可换
上游 prompt cache 命中（长上下文 agent 场景直接省钱），但 start-plan 官方
形态本不带 x-session-id，转发属于「自创节奏」——上不上必须数据说话。

本模块 observe-only，绝不改变任何请求行为：
  - 会话键 = sha256(model + system + 首条 user 消息)[0:16]——无状态前缀哈希，
    同一会话的请求（system 稳定 + 首条消息即会话起点）稳定落在同键；
  - 每请求在 reqlog 行上落 sess（会话键）与 sess_repeat（24h 内非首见）；
  - 响应侧落 cache_read（上游回传的缓存命中 token 数，reqlog.finish_ok）；
  - summary() 聚合近 N 天：首见 vs 复见请求的 cache_read 均值对比——
    复见显著更高才值得做 enforce 档（转发稳定 session id）。

内存注册表仅用于「首见/复见」判定（LRU 4096 / 24h TTL，纯进程内）。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path

from . import settings

_TTL = 24 * 3600
_MAX_KEYS = 4096

_lock = threading.Lock()
_seen: dict[str, float] = {}  # sess_key → last_seen（monotonic）


def session_key(body: dict) -> str | None:
    """无状态会话键；畸形输入返回 None（观测绝不放大请求失败）。"""
    try:
        first_user = ""
        for m in body.get("messages") or []:
            if isinstance(m, dict) and m.get("role") == "user":
                c = m.get("content")
                first_user = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
                break
        canonical = json.dumps(
            {"m": body.get("model"), "s": body.get("system"), "u": first_user},
            ensure_ascii=False, sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    except (TypeError, ValueError):
        return None


def observe(body: dict) -> tuple[str, bool] | None:
    """判定 (sess_key, repeat)。repeat = 该键 24h 内出现过。"""
    key = session_key(body)
    if key is None:
        return None
    now = time.monotonic()
    with _lock:
        prev = _seen.get(key)
        _seen[key] = now
        if len(_seen) > _MAX_KEYS:
            cutoff = now - _TTL
            for k in [k for k, t in _seen.items() if t <= cutoff][: len(_seen) - _MAX_KEYS]:
                _seen.pop(k, None)
    return key, prev is not None and (now - prev) < _TTL


def tag(req_id: str, body: dict) -> None:
    """观测并写入 reqlog 在途条目（best-effort，reqlog 侧幂等容错）。"""
    if not settings.SESSION_OBSERVE:
        return
    try:
        from . import reqlog

        obs = observe(body)
        if obs is not None:
            reqlog.tag_session(req_id, obs[0], obs[1])
    except Exception:  # noqa: BLE001 - 纯观测绝不影响请求
        pass


def reset_for_tests() -> None:
    _seen.clear()


def summary(days: int = 7) -> dict:
    """聚合近 N 天 reqlog：首见 vs 复见请求的 cache_read 均值对比。

    判读：复见的 cache_read 均值显著高于首见（且复见占比高）才值得做
    enforce 档（转发稳定 session id）；差异不大则本观测就是终点——省掉
    一次无意义的自创节奏。纯读盘 best-effort。
    """
    from datetime import datetime, timedelta, timezone


    days = max(1, min(int(days), 30))
    bj = timezone(timedelta(hours=8))
    first_vals: list[int] = []
    repeat_vals: list[int] = []
    requests = 0
    sessions: set[str] = set()
    now_day = datetime.now(bj)
    for i in range(days):
        day = (now_day - timedelta(days=i)).strftime("%Y-%m-%d")
        f = Path(settings.DATA_DIR) / f"reqlog-{day}.jsonl"
        try:
            lines = f.read_text("utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if not r.get("sess"):
                continue
            requests += 1
            sessions.add(r["sess"])
            cr = r.get("cache_read")
            if not isinstance(cr, (int, float)):
                continue
            (repeat_vals if r.get("sess_repeat") else first_vals).append(int(cr))

    def _avg(vals: list[int]) -> float | None:
        return round(sum(vals) / len(vals), 1) if vals else None

    return {
        "days": days,
        "requests": requests,
        "sessions": len(sessions),
        "repeat_ratio": round(len(repeat_vals) / len(first_vals + repeat_vals), 3)
        if (first_vals or repeat_vals) else None,
        "cache_read_first_avg": _avg(first_vals),
        "cache_read_repeat_avg": _avg(repeat_vals),
        "first_samples": len(first_vals),
        "repeat_samples": len(repeat_vals),
    }
