"""同内容请求去重（dedup）—— 风暴守卫的指纹与生命周期语义。

锁四件事：指纹跨账号一致（transform 前形态）、leader/follower 登记与唤醒、
失败缓存 TTL 与重放、流式不缓存失败。守卫层（_dispatch_guarded）的接线由
integration 套件的 mock 上游覆盖，这里锁模块语义。
"""

from __future__ import annotations

import asyncio

import pytest

from app import dedup


@pytest.fixture(autouse=True)
def _clean():
    dedup._reset_for_tests()
    yield
    dedup._reset_for_tests()


def _body(user="hi", model="glm-5.3-flash"):
    return {"model": model, "stream": False, "max_tokens": 30,
            "system": [{"type": "text", "text": "sys"}],
            "messages": [{"role": "user", "content": user}]}


def test_fingerprint_stable_across_account_noise():
    """指纹只看 model+system+messages：metadata.user_id 等账号差异不影响。"""
    a = _body()
    b = {**_body(), "metadata": {"user_id": "84041790658495247"}, "max_tokens": 30}
    assert dedup.fingerprint(a) == dedup.fingerprint(b)
    assert dedup.fingerprint(_body(user="different")) != dedup.fingerprint(a)
    assert dedup.fingerprint({"messages": [{"content": {1, 2}}]}) is None  # 不可序列化 no-op


def test_begin_inflight_leader_then_follower():
    fp = dedup.fingerprint(_body())
    assert dedup.begin_inflight(fp) is True   # leader
    assert dedup.begin_inflight(fp) is False  # follower


def test_follower_wakes_on_leader_failure_and_replays():
    fp = dedup.fingerprint(_body())

    async def run():
        dedup.begin_inflight(fp)
        waiter = asyncio.create_task(dedup.wait_inflight(fp, timeout=2))
        await asyncio.sleep(0.01)
        dedup.note_result(fp, 503, b'{"error":"no account"}', stream=False)
        return await waiter, dedup.failure_of(fp)

    outcome, failure = asyncio.run(run())
    assert outcome == "failed"
    assert failure == (503, b'{"error":"no account"}')
    # 失败进冷却缓存：TTL 内 peek 直接重放
    assert dedup.peek_failure(fp) == (503, b'{"error":"no account"}')


def test_follower_wakes_on_leader_success_no_cache():
    fp = dedup.fingerprint(_body())

    async def run():
        dedup.begin_inflight(fp)
        waiter = asyncio.create_task(dedup.wait_inflight(fp, timeout=2))
        await asyncio.sleep(0.01)
        dedup.note_result(fp, 200, b"{}", stream=False)
        return await waiter

    assert asyncio.run(run()) == "ok"
    assert dedup.peek_failure(dedup.fingerprint(_body())) is None  # 成功不进缓存


def test_stream_leader_never_caches_failure():
    fp = dedup.fingerprint(_body())
    dedup.begin_inflight(fp)
    dedup.note_result(fp, 503, b"err", stream=True)  # 流式预流化失败
    assert dedup.peek_failure(fp) is None            # 不进冷却缓存


def test_failure_cache_expires_after_ttl(monkeypatch):
    monkeypatch.setattr(dedup.settings, "DEDUP_TTL", 0)  # 存入即过期
    fp = dedup.fingerprint(_body())
    dedup.begin_inflight(fp)
    dedup.note_result(fp, 503, b"err", stream=False)
    assert dedup.peek_failure(fp) is None


def test_wait_timeout_when_leader_silent():
    fp = dedup.fingerprint(_body())
    dedup.begin_inflight(fp)

    async def wait():
        return await dedup.wait_inflight(fp, timeout=0.05)

    assert asyncio.run(wait()) == "timeout"
    # 超时后 entry 还挂着 —— discard 兜底放行 follower
    dedup.discard(fp)
    assert asyncio.run(wait()) == "ok"
