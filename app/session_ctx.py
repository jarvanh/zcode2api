"""会话亲和（observe 观测档 + enforce 转发档）。

背景（2026-11-01 对比 zcode-api session-context.ts 得出）：稳定会话标识可换
上游 prompt cache 命中（长上下文 agent 场景直接省钱），但 start-plan 官方
形态本不带 x-session-id，转发属于「自创节奏」——上不上必须数据说话。

observe 档（SESSION_OBSERVE，默认开）绝不改变任何请求行为：
  - 会话键 = sha256(model + system + 首条 user 消息)[0:16]——无状态前缀哈希，
    同一会话的请求（system 稳定 + 首条消息即会话起点）稳定落在同键；
  - 每请求在 reqlog 行上落 sess（会话键）、sess_repeat（24h 内非首见）与
    sess_window（5min idle 窗口归属：in/out/空）；
  - 响应侧落 cache_read（上游回传的缓存命中 token 数，reqlog.finish_ok）；
  - summary() 聚合近 N 天：首见 vs 复见请求的 cache_read 均值对比——
    复见显著更高才值得做 enforce 档（转发稳定 session id）。

enforce 档（SESSION_AFFINITY，2026-11-01 主人拍板默认开）：会话键 → 稳定 UUID
的进程内映射，JWT plan 通道把 x-session-id 从「逐请求随机」换成「同会话
同值」（六件 attribution 头形状不变，仅该值生命周期与官方对齐）；observe
数据尚未支持（repeat_ratio=0），但改动仅一枚头的生命周期，实效以
admin API session-affinity 的 cache_read 对比持续观察。

内存注册表仅用于「首见/复见」判定与稳定 id 映射（LRU 4096 / 24h TTL，纯进程内）。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from contextvars import ContextVar
from pathlib import Path

from . import settings

_TTL = 24 * 3600
_MAX_KEYS = 4096

# 上游 prompt cache 的 idle 窗口（官方形态：同一会话连续请求间隔 <5min 才复用
# system prompt 缓存）。超过即认定窗口已断，缓存必然重建 —— 这是 cache_read
# 高低的**解释变量**：复见但窗口外，cache_read 上不去属正常，不是缓存坏了。
_IDLE_WINDOW = 5 * 60

_lock = threading.Lock()
_seen: dict[str, float] = {}  # sess_key → last_seen（monotonic）
_stable_ids: dict[str, str] = {}  # sess_key → 稳定 x-session-id（enforce 档）
_MAX_STABLE = 8192

# enforce 档：本请求应携带的稳定 x-session-id（tag() 设置，build_request 消费）。
# contextvar 跨 asyncio 传播：网关 handler 在分发 create_task 前调用 tag()，
# 子任务复制当前上下文时带上该值。
_current: ContextVar[tuple[str, str] | None] = ContextVar("session_affinity", default=None)


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


def observe(body: dict) -> tuple[str, bool, str] | None:
    """判定 (sess_key, repeat, window)。

    repeat = 该键 24h 内出现过（会话归属）；
    window = "in"  —— 复见且间隔 <5min：缓存窗口仍在，system prompt 可复用；
             "out" —— 复见但间隔 ≥5min：窗口已断，本次缓存必然重建；
             ""    —— 首见（无上一请求可比）或流式（调用方不写该字段）。
    流式在本档**不参与窗口判定**：长会话流式两次请求间隔超 5min 是常态，
    判成 out 是假阴性，会污染 sw_in_rate 的解读（见 tag()）。
    """
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
    if prev is None or (now - prev) >= _TTL:
        return key, False, ""
    return key, True, ("in" if (now - prev) < _IDLE_WINDOW else "out")


def _stable_session_id(key: str) -> str:
    """会话键 → 稳定 UUID（进程内持久，同会话每请求同值）。"""
    with _lock:
        sid = _stable_ids.get(key)
        if sid is None:
            sid = str(uuid.uuid4())
            _stable_ids[key] = sid
            if len(_stable_ids) > _MAX_STABLE:
                alive = set(_seen)
                for k in [k for k in _stable_ids if k not in alive][: len(_stable_ids) - _MAX_STABLE]:
                    _stable_ids.pop(k, None)
        return sid


def current_session() -> str | None:
    """enforce 档：当前请求的稳定 x-session-id；未设置/关闭返回 None。"""
    if not settings.SESSION_AFFINITY:
        return None
    entry = _current.get()
    return entry[1] if entry else None


def tag(req_id: str, body: dict, stream: bool = False) -> None:
    """观测 + enforce 装载（best-effort，观测绝不影响请求可用性）。

    observe：写 reqlog 在途条目（SESSION_OBSERVE 门控）；
    enforce：把 (会话键, 稳定 session id) 挂到本请求 context，供
    build_request 经 current_session() 取用（SESSION_AFFINITY 门控）。

    stream=True 时**不写窗口字段**（sess/sess_repeat 照写）：窗口判定要求同
    会话间隔 <5min，而长会话流式两次请求间隔超 5min 是常态，判 out 属于
    假阴性，会把 sw_in_rate 的解释力打掉。宁可留空（面板显示「—」）也不出
    假数。三个入口调用点都显式传 stream（不用 body 默认值推断：
    /v1/messages 的 payload 与转换后 body 分属两个对象，猜错即漏标）。
    """
    if not (settings.SESSION_OBSERVE or settings.SESSION_AFFINITY):
        return
    try:
        obs = observe(body)
        if obs is None:
            return
        if settings.SESSION_AFFINITY:
            _current.set((obs[0], _stable_session_id(obs[0])))
        if settings.SESSION_OBSERVE:
            from . import reqlog

            reqlog.tag_session(req_id, obs[0], obs[1], "" if stream else obs[2])
    except Exception:  # noqa: BLE001 - 纯观测绝不影响请求
        pass


def reset_for_tests() -> None:
    _seen.clear()
    _stable_ids.clear()
    _current.set(None)


def summary(days: int = 7) -> dict:
    """聚合近 N 天 reqlog：首见 / 窗口内复见 / 窗口外复见 三档 cache_read 对比。

    判读：复见的 cache_read 均值显著高于首见（且复见占比高）才值得做
    enforce 档（转发稳定 session id）；差异不大则本观测就是终点——省掉
    一次无意义的自创节奏。纯读盘 best-effort。

    为什么拆三档（2026-11-04）：旧口径把「复见」混成一档，会把窗口外
    （间隔 ≥5min，缓存必然已重建）的请求算进来拉低均值 —— 于是
    「复见 ≈ 首见」既可能是 enforce 真没用，也可能只是节奏太散。三档拆开
    才分得清：看 `cache_read_repeat_in_avg` 对 `cache_read_first_avg`，
    窗口外那一档只用来解释「为什么整体复见不涨」。
    """
    from datetime import datetime, timedelta, timezone


    days = max(1, min(int(days), 30))
    bj = timezone(timedelta(hours=8))
    first_vals: list[int] = []
    repeat_vals: list[int] = []
    repeat_in_vals: list[int] = []
    repeat_out_vals: list[int] = []
    requests = 0
    win_in = win_out = 0
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
            if r.get("sw") == "in":
                win_in += 1
            elif r.get("sw") == "out":
                win_out += 1
            cr = r.get("cache_read")
            if not isinstance(cr, (int, float)):
                continue
            if r.get("sess_repeat"):
                repeat_vals.append(int(cr))
                # 旧数据无 sw 字段：归入「未知」，**不**塞进 in 档 —— 否则
                # 会把窗口内均值往有利方向偏，得出「enforce 有效」的假结论
                if r.get("sw") == "in":
                    repeat_in_vals.append(int(cr))
                elif r.get("sw") == "out":
                    repeat_out_vals.append(int(cr))
            else:
                first_vals.append(int(cr))

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
        # 三档判读口径（新字段向后兼容：旧调用方读 repeat_avg 照旧）
        "cache_read_repeat_in_avg": _avg(repeat_in_vals),
        "cache_read_repeat_out_avg": _avg(repeat_out_vals),
        "window_in": win_in,
        "window_out": win_out,
        "window_in_rate": round(win_in / (win_in + win_out) * 100, 1) if (win_in + win_out) else None,
        "first_samples": len(first_vals),
        "repeat_samples": len(repeat_vals),
        "repeat_in_samples": len(repeat_in_vals),
        "repeat_out_samples": len(repeat_out_vals),
    }
