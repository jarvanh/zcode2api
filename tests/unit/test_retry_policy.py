"""统一重试策略（app/retry_policy.py）单元测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app import retry_policy


def test_parse_retry_after_seconds_and_cap():
    assert retry_policy.parse_retry_after("30") == 30
    assert retry_policy.parse_retry_after("30.9") == 30
    assert retry_policy.parse_retry_after("999", cap=120) == 120
    assert retry_policy.parse_retry_after("0") is None
    assert retry_policy.parse_retry_after("-1") is None
    assert retry_policy.parse_retry_after("") is None
    assert retry_policy.parse_retry_after(None) is None


def test_parse_retry_after_http_date():
    now = datetime(2026, 11, 14, 0, 0, 0, tzinfo=UTC)
    value = (now + timedelta(seconds=17)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert retry_policy.parse_retry_after(value, now=now.timestamp()) == 17
    # 已经过去的日期不采信
    old = (now - timedelta(seconds=5)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert retry_policy.parse_retry_after(old, now=now.timestamp()) is None


def test_retry_delay_prefers_retry_after():
    assert retry_policy.retry_delay(3, retry_after="7", base=1, cap=30) == 7.0
    assert retry_policy.retry_delay(3, retry_after="999", base=1, cap=30) == 30.0


def test_retry_delay_exponential_with_jitter_bounds():
    # attempt 1/2/3 → 1/2/4 秒；jitter 0.25 时落在 ±25% 内
    for attempt, base in ((1, 1.0), (2, 2.0), (3, 4.0)):
        delay = retry_policy.retry_delay(attempt, base=1.0, cap=30.0, jitter=0.25)
        assert base * 0.75 <= delay <= base * 1.25
    # 封顶
    assert retry_policy.retry_delay(10, base=1.0, cap=30.0, jitter=0) == 30.0


def test_decide_retry_output_and_budget_guards():
    assert not retry_policy.decide_retry(status_code=429, attempt=1, max_attempts=3,
                                        output_started=True)
    assert not retry_policy.decide_retry(status_code=429, attempt=3, max_attempts=3)
    assert retry_policy.decide_retry(status_code=429, attempt=1, max_attempts=3)


def test_decide_retry_status_and_kind():
    assert retry_policy.decide_retry(status_code=503, attempt=1, max_attempts=2)
    assert not retry_policy.decide_retry(status_code=400, attempt=1, max_attempts=2)
    assert not retry_policy.decide_retry(status_code=402, attempt=1, max_attempts=2)
    assert retry_policy.decide_retry(kind="network", attempt=1, max_attempts=2)
    assert not retry_policy.decide_retry(kind="auth", attempt=1, max_attempts=2)
    # 显式裁决优先
    assert retry_policy.decide_retry(status_code=400, attempt=1, max_attempts=2,
                                     retryable=True)
    assert not retry_policy.decide_retry(status_code=503, attempt=1, max_attempts=2,
                                         retryable=False)
