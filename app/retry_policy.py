"""统一重试决策与退避（参考 claude-code-router retry-policy / new-api DecideRelayRetry）。

本模块只做纯计算，不发请求、不读全局状态；gateway 负责实际的 sleep 与重试。
把「哪些错误可重试 / 等多久」收口到一处，避免各分支各写一套导致行为漂移。
"""

from __future__ import annotations

import random
import time
from datetime import UTC
from email.utils import parsedate_to_datetime

# 可重试的 HTTP 状态：408/425 是请求未完成，429/5xx 是上游瞬态。
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# 明确不可重试：鉴权/参数/额度/风控类，重试只会放大问题。
NON_RETRYABLE_STATUS = frozenset({400, 401, 402, 403, 404, 405, 409, 422})
# 内部错误分类（reqlog/响应 body 的 kind）里可重试的瞬态类别。
RETRYABLE_KINDS = frozenset({
    "network", "timeout", "rate", "busy", "queue_timeout", "channel_cooling",
})


def parse_retry_after(value: object, *, cap: int | float | None = None,
                      now: float | None = None) -> int | None:
    """解析 Retry-After，返回正整数秒；无法解析/非正数返回 None。

    支持两种合法形态：秒数（"30" / "30.0"）和 HTTP-date
    （"Wed, 21 Oct 2015 07:28:00 GMT"）。HTTP-date 按当前时间换算成剩余秒，
    向上取整，避免提前重试。cap > 0 时封顶，防止上游把客户端吊死。
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    try:
        secs = int(float(text))
    except (TypeError, ValueError):
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        base = time.time() if now is None else float(now)
        secs = int(dt.timestamp() - base + 0.999)

    if secs <= 0:
        return None
    if cap is not None and float(cap) > 0:
        return min(secs, int(cap))
    return secs


def retry_delay(attempt: int, *, retry_after: object = None,
                base: float = 1.0, cap: float = 30.0,
                jitter: float = 0.25, rng: random.Random | None = None) -> float:
    """计算第 attempt 次重试前的等待秒数（attempt 从 1 开始）。

    优先采信 Retry-After（1..cap）；没有则指数退避 base * 2^(attempt-1)，
    再乘 ±jitter 的打散量，最后按 cap 封顶。返回 0 表示立即可重试。
    """
    cap = max(0.0, float(cap))
    hinted = parse_retry_after(retry_after, cap=cap)
    if hinted is not None:
        return float(hinted)

    raw = max(0.0, float(base)) * (2 ** max(0, int(attempt) - 1))
    raw = min(raw, cap)
    if raw <= 0:
        return 0.0
    spread = max(0.0, float(jitter))
    if spread:
        source = rng or random
        raw += source.uniform(-raw * spread, raw * spread)
    return max(0.0, raw)


def decide_retry(*, status_code: int | None = None, kind: str | None = None,
                 attempt: int = 1, max_attempts: int = 0,
                 output_started: bool = False,
                 retryable: bool | None = None) -> bool:
    """统一重试决策：输出已开始绝不重试；预算耗尽不重试；显式判定优先。

    status_code/kind 二选一或同时给；retryable 非 None 时作为调用方的最终裁决。
    """
    if output_started:
        return False
    if int(attempt) >= int(max_attempts):
        return False
    if retryable is not None:
        return bool(retryable)
    if kind and str(kind).strip().lower() in RETRYABLE_KINDS:
        return True
    if status_code in NON_RETRYABLE_STATUS:
        return False
    return status_code in RETRYABLE_STATUS
