"""限界排队回归：选不到可用账号时等待，而不是秒回 503。

背景（2026-10-06 故障复盘）：账号被上游判定额度耗尽（业务码 1005）后，
额度是分钟级滚动恢复的 —— 实测故障窗仅 4 分钟，账号被标记 EXHAUSTED 后
由后台探测重新拉回轮询。旧行为秒回 503，把这类「自愈型」故障全部变成
用户可见报错；排队能把它吃掉。之所以限界：当天额度真耗尽时等也无用，
排队只是把报错推迟，不如尽早让客户端自行快速重试。
"""

from __future__ import annotations

import time

import pytest

from app import settings
from app.models import Status
from app.routes import gateway


@pytest.fixture(autouse=True)
def _reset_probe_throttle():
    """探测节流是全局状态，用例间必须重置，否则相互污染。"""
    gateway._queue_probe_last = 0.0
    yield
    gateway._queue_probe_last = 0.0


@pytest.fixture
def probe_calls(monkeypatch):
    """替换主动额度探测，既屏蔽真实上游调用，又记录触发次数。

    探测走 _spawn_bg 后台任务（fire-and-forget），测试若不显式执行协程，
    断言会在任务跑起来前就结束。这里拦下协程、由用例显式 await，
    既验证「触发了」也验证「真的调用了 refresh_accounts」。
    """
    calls: list[int] = []
    pending: list = []

    async def _fake_refresh(accounts):
        calls.append(len(accounts))
        return {"ok": len(accounts), "fail": 0}

    monkeypatch.setattr(gateway, "refresh_accounts", _fake_refresh)
    monkeypatch.setattr(gateway, "_spawn_bg", pending.append)

    async def _drain() -> None:
        while pending:
            await pending.pop(0)

    return {"calls": calls, "drain": _drain}


def _exhaust(fresh_app, name: str = "a"):
    """建一个 JWT 账号并置为额度耗尽（不可被轮询选中）。"""
    acc = fresh_app.add_account("zai", name, f"jwt.token.{name}")
    acc.status = Status.EXHAUSTED
    fresh_app.update_account(acc)
    return acc


class TestQueueDisabledOrMeaningless:
    """不该排队的场景：必须返回 None，走原有 503 路径，不做无谓空等。"""

    async def test_disabled_returns_none(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        monkeypatch.setattr(settings, "QUEUE_WAIT", 0)
        _exhaust(fresh_app)
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0) is None

    async def test_empty_pool_returns_none(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        # 池子压根没账号（区别于「有账号但都不可用」）→ 排队毫无意义
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0) is None

    async def test_budget_already_spent_returns_none(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        _exhaust(fresh_app)
        # queued 已达上限 → 不再追加等待
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 240.0) is None

    async def test_expired_quota_window_not_recoverable(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        """额度窗口已过期 → 不排队，快速 503（2026-10-08 线上实测回归）。

        4 个账号全部 EXHAUSTED 且 expires_at 已过，排队只会把连接吊到
        30s early-flush 宽限期之后，客户端拿到 HTTP 200 + SSE error，
        把「额度耗尽」误判成成功。窗口过期不会自动重置，排队无意义。
        """
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        acc = _exhaust(fresh_app)
        now = time.time()
        acc.quota = {
            "GLM-5.3-Flash": {
                "total": 100000000, "used": 100000000, "remaining": 0,
                "expires_at": now - 3600,  # 窗口已过期
            }
        }
        fresh_app.update_account(acc)

        async def _fake_sleep(sec):  # 若真排队会被调用
            raise AssertionError(f"窗口已过期不应排队，却 sleep({sec})")

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)
        assert await gateway._wait_for_account("r1", "zai", now, 0, 0.0) is None
        assert not gateway._has_recoverable_account("zai", now)

    async def test_live_quota_window_still_recoverable(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        """额度窗口未到期 → 保持原有排队行为（不得误伤自愈型故障）。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        acc = _exhaust(fresh_app)
        now = time.time()
        acc.quota = {
            "GLM-5.3-Flash": {
                "total": 100000000, "used": 100000000, "remaining": 0,
                "expires_at": now + 3600,  # 窗口未到期
            }
        }
        fresh_app.update_account(acc)
        assert gateway._has_recoverable_account("zai", now)

    async def test_missing_quota_treated_as_recoverable(self, fresh_app, monkeypatch, probe_calls):  # noqa: ARG002
        """额度数据缺失（未刷新/上游不提供）→ 按可恢复处理，不拿空数据判死。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        _exhaust(fresh_app)  # 无 quota
        now = time.time()
        assert gateway._has_recoverable_account("zai", now)


class TestQueueWaiting:
    async def test_returns_waited_seconds_when_account_recovers(
        self, fresh_app, monkeypatch, probe_calls,
    ):
        """账号在排队期间恢复 → 返回实际等待秒数，调用方继续调度。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        monkeypatch.setattr(settings, "QUEUE_POLL", 5)
        acc = _exhaust(fresh_app)

        ticks: list[float] = []

        async def _fake_sleep(sec):
            ticks.append(sec)
            if len(ticks) >= 2:  # 第二次轮询前让账号恢复
                live = fresh_app.find("zai", acc.id)
                live.status = Status.ACTIVE
                fresh_app.update_account(live)

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)

        waited = await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0)
        await probe_calls["drain"]()
        assert waited is not None
        assert waited == pytest.approx(10.0)
        assert ticks == [5.0, 5.0]

    async def test_returns_none_when_budget_exhausted(self, fresh_app, monkeypatch, probe_calls):
        """账号始终不恢复 → 等满预算后放弃，由调用方出 503。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 20)
        monkeypatch.setattr(settings, "QUEUE_POLL", 5)
        _exhaust(fresh_app)

        async def _fake_sleep(sec):
            return None

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0) is None
        await probe_calls["drain"]()

    async def test_deadline_caps_remaining_wait(self, fresh_app, monkeypatch, probe_calls):
        """请求级死线优先于排队预算，避免排队把总耗时拖过死线。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        monkeypatch.setattr(settings, "QUEUE_POLL", 5)
        _exhaust(fresh_app)

        sleeps: list[float] = []

        async def _fake_sleep(sec):
            sleeps.append(sec)

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)

        t0 = time.time() - 598  # 已耗时 598s，死线 600s → 只剩约 2s 可等
        await gateway._wait_for_account("r1", "zai", t0, 600, 0.0)
        await probe_calls["drain"]()
        assert len(sleeps) == 1
        assert sleeps[0] == pytest.approx(2.0, abs=0.2)


class TestSyncBudget:
    """非流式请求无法 early flush（提前发头会破坏本地聚合协议），排队静默
    直接计入 TTFB —— 预算必须压在 CDN 边缘 ~100s 硬超时内（QUEUE_WAIT_SYNC
    与 QUEUE_WAIT 取小），超时快速 503 而非 CF 524。"""

    async def test_sync_budget_is_min_of_wait_and_sync_cap(self, fresh_app, monkeypatch):
        """非流式预算 = min(QUEUE_WAIT, QUEUE_WAIT_SYNC)。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        monkeypatch.setattr(settings, "QUEUE_WAIT_SYNC", 60)
        _exhaust(fresh_app)

        async def _fake_sleep(sec):
            return None

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)
        # queued=0，预算应取 60（非 240）→ 60/5 = 12 轮轮询
        budget = float(min(settings.QUEUE_WAIT, settings.QUEUE_WAIT_SYNC))
        # 直接调 _wait_for_account 验证预算生效：耗尽后返回 None
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0, budget=budget) is None

    async def test_sync_budget_zero_disables_sync_queue(self, fresh_app, monkeypatch):
        """QUEUE_WAIT_SYNC=0 时非流式不排队（快速 503）。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        monkeypatch.setattr(settings, "QUEUE_WAIT_SYNC", 0)
        _exhaust(fresh_app)

        async def _fake_sleep(sec):
            return None

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)
        budget = 0.0  # _dispatch 对非流式 budget=0 时不限制，这里直接验 None 分支
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0, budget=budget) is None

    async def test_stream_budget_unchanged(self, fresh_app, monkeypatch):
        """流式请求预算不受 QUEUE_WAIT_SYNC 影响，仍用 QUEUE_WAIT。"""
        monkeypatch.setattr(settings, "QUEUE_WAIT", 240)
        monkeypatch.setattr(settings, "QUEUE_WAIT_SYNC", 60)
        _exhaust(fresh_app)

        async def _fake_sleep(sec):
            return None

        monkeypatch.setattr(gateway, "_sleep", _fake_sleep)
        # 流式走默认预算：调用时不传 budget（等价于 240）→ 等满预算后 None
        assert await gateway._wait_for_account("r1", "zai", time.time(), 0, 0.0) is None


class TestQuotaProbeThrottle:
    async def test_probe_triggered_once_within_throttle_window(
        self, fresh_app, monkeypatch, probe_calls,
    ):
        """并发排队会同时涌进多个请求，探测必须节流 —— billing 连续查询
        是上游风控「unusual activity」的信号源，不能放大。"""
        monkeypatch.setattr(settings, "QUEUE_PROBE_MIN_INTERVAL", 30)
        _exhaust(fresh_app)

        gateway._trigger_quota_probe("r1", "zai")
        gateway._trigger_quota_probe("r2", "zai")
        await probe_calls["drain"]()
        # 两次触发在同一节流窗内 → 只探测一次
        assert len(probe_calls["calls"]) == 1
