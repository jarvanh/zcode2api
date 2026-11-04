"""待生效套餐「到点自动生效」单元测试（2026-11-04 线上实证）。

背景：GLOBAL BUILD 授权 19:xx 已领取、entitlement `effective_at=23:00` 已落库，
但 23:00→23:08 本地仍把 EXHAUSTED 号挡在轮询外（`is_selectable` 直接 False），
只能等 30min 固定轮询打 billing 才翻活。本测试锁死两条不变量：
  · 到点瞬间**零上游流量**即可本地激活；
  · 常规 1800s 节拍不变，冷却 / 停用 / 失效号不被误激活。
"""

from __future__ import annotations

import asyncio

import pytest

from app.models import Account, Status
from app.quota import QuotaMonitor, quota_recoverable

_NOW = 1_793_804_400.0          # 2026-11-04 23:00:00 +08
_DUE = _NOW + 600              # 合成用例的生效时刻
_GRANT = {"period": "one_time", "effective_at": _DUE,
          "grant_units": 300_000_000, "show_name": "GLM-5.3-Flash"}


def _acc(**kw) -> Account:
    base = dict(
        id="acc-1", name="zai-156", provider="zai", mode="jwt",
        jwt_token="a.b.c", status=Status.EXHAUSTED,
        quota={"GLM-5.3-Flash": {
            "total": 300_000_000, "remaining": 213_053_009,
            "expires_at": _NOW - 43_200}},       # 今早 09:00 已过期的旧窗口
        plan={"name": "ZCODE GLOBAL BUILD",
              "entitlements": [dict(_GRANT)]},
    )
    base.update(kw)
    return Account(**base)


class FakeStore:
    """只记录落库次数，不碰真实 DB。"""

    def __init__(self, accounts):
        self.accounts = list(accounts)
        self.updated = 0

    def list_accounts(self, provider=None):
        return list(self.accounts)

    def quota_accounts(self):
        """模拟 monitor 的 allows_billing() 筛选（测试账号均有效 JWT）。"""
        return [a for a in self.accounts if a.mode == "jwt" and a.enabled]

    def quota_refresh_interval(self):
        return 1800

    def update_account(self, acc):
        self.updated += 1
        return True


def _monitor(accounts, monkeypatch):
    fs = FakeStore(accounts)
    m = QuotaMonitor()
    monkeypatch.setattr("app.quota.store", fs)
    return m, fs


class TestQuotaRecoverable:
    """共享谓词：到点前后 / ends_at 失效。"""

    def test_before_due_no_bonus(self):
        _, has_bonus = quota_recoverable(_acc(), {}, _DUE - 2)
        assert has_bonus is False

    def test_at_due_bonus_active(self):
        _, has_bonus = quota_recoverable(_acc(), {}, _DUE)
        assert has_bonus is True

    def test_bonus_expires(self):
        acc = _acc(plan={"entitlements": [dict(_GRANT, ends_at=_DUE + 60)]})
        _, has_bonus = quota_recoverable(acc, {}, _DUE + 120)
        assert has_bonus is False

    def test_compare_with_fetch_quota_same_predicate(self):
        """fetch_quota 与本地激活共用同一谓词（口径不漂移）。"""
        from app.quota import _bonus_active
        acc = _acc()
        assert _bonus_active(acc.plan, _DUE) is True


class TestActivateDue:
    def test_before_due_no_activation(self, monkeypatch):
        acc = _acc()
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE - 2) == 0
        assert acc.status == Status.EXHAUSTED and fs.updated == 0

    def test_at_due_activates_zero_upstream(self, monkeypatch):
        acc = _acc()
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 1
        assert acc.status == Status.ACTIVE
        assert acc.last_error is None and acc.cooling_until is None
        assert acc.is_selectable(_DUE) is True
        assert fs.updated == 1

    def test_cooling_not_activated(self, monkeypatch):
        acc = _acc(status=Status.COOLING, cooling_until=_DUE + 300)
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 0 and fs.updated == 0

    def test_disabled_not_activated(self, monkeypatch):
        acc = _acc(enabled=False)
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 0 and fs.updated == 0

    def test_invalid_not_activated(self, monkeypatch):
        acc = _acc(status=Status.INVALID)
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 0 and fs.updated == 0

    def test_no_entitlement_not_activated(self, monkeypatch):
        acc = _acc(plan={"entitlements": []})
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 0 and fs.updated == 0

    def test_already_active_not_repeated(self, monkeypatch):
        acc = _acc(status=Status.ACTIVE)
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 0 and fs.updated == 0

    def test_millisecond_effective_at(self, monkeypatch):
        acc = _acc(plan={"entitlements": [dict(_GRANT, effective_at=_DUE * 1000)]})
        m, fs = _monitor([acc], monkeypatch)
        assert m._activate_due(_DUE) == 1


class TestDueScheduling:
    """等待节拍：due 早于常规节拍；过点后 pending_plan_start 自动 None。"""

    def test_due_is_earlier_than_regular(self):
        from app.quota_windows import pending_plan_start
        acc = _acc()
        due = pending_plan_start([acc], _NOW)
        assert due == _DUE and due - _NOW < 1800

    def test_due_disappears_after_passing(self):
        from app.quota_windows import pending_plan_start
        acc = _acc()
        assert pending_plan_start([acc], _DUE + 1) is None

    @pytest.mark.asyncio
    async def test_wait_stop_or_wake_returns_on_wake(self, monkeypatch):
        m, _ = _monitor([], monkeypatch)

        async def setter():
            await asyncio.sleep(0)
            m.request_refresh("测试唤醒")

        asyncio.create_task(setter())
        assert await m._wait_stop_or_wake(30) is True
        m._wake.clear()

    @pytest.mark.asyncio
    async def test_request_refresh_is_idempotent(self, monkeypatch):
        m, _ = _monitor([], monkeypatch)
        m.request_refresh("a")
        m.request_refresh("b")
        assert m._wake.is_set() is True


class TestWakePlan:
    """唤醒点决策：due 早于常规节拍才插队。"""

    def test_regular_when_no_pending(self):
        assert QuotaMonitor._wake_plan(_NOW, _NOW + 1800, None) == (_NOW + 1800, "regular")

    def test_due_when_earlier(self):
        assert QuotaMonitor._wake_plan(_NOW, _NOW + 1800, _DUE) == (_DUE, "due")

    def test_regular_when_due_later(self):
        late = _NOW + 7200
        assert QuotaMonitor._wake_plan(_NOW, _NOW + 1800, late) == (_NOW + 1800, "regular")

    def test_pending_gone_after_due(self):
        """过点后 pending_plan_start 自动 None → 天然一次性，不重复插队。"""
        from app.quota_windows import pending_plan_start
        acc = _acc()
        assert pending_plan_start([acc], _NOW) == _DUE
        assert pending_plan_start([acc], _DUE + 1) is None


class TestLoopSequencing:
    """端到端排序：due 插队 → 本地激活 + 确认刷新；常规节拍不被挤掉也不提前。

    用假时钟 + 让出事件循环的假等待（真等待会把测试拖成 30min），
    断言的只是**事件顺序与时刻**，不是真实睡眠。
    """

    class _Clock:
        def __init__(self, t0):
            self.t = float(t0)

        def time(self):
            return self.t

    @pytest.mark.asyncio
    async def test_due_activates_then_regular_continues(self, monkeypatch):
        acc = _acc()
        m, _fs = _monitor([acc], monkeypatch)
        clock = self._Clock(_NOW)
        monkeypatch.setattr("app.quota.time", clock)
        monkeypatch.setattr(QuotaMonitor, "START_DELAY", 0)
        monkeypatch.setattr(QuotaMonitor, "CONFIRM_GRACE", 10)

        seen = []
        real_activate = m._activate_due

        async def _fake_wait(self, timeout):
            clock.t += max(1.0, timeout)
            await asyncio.sleep(0)          # 必须让出：否则整轮循环占死事件循环
            return False

        async def _grace(self, secs):
            clock.t += secs
            await asyncio.sleep(0)
            return False

        async def _regular():
            seen.append(("regular", clock.t))
            await asyncio.sleep(0)

        async def _confirm():
            seen.append(("confirm", clock.t))
            clock.t += 1
            await asyncio.sleep(0)

        def _activate(now):
            seen.append(("activate", now))
            return real_activate(now)

        monkeypatch.setattr(QuotaMonitor, "_wait_stop_or_wake", _fake_wait)
        monkeypatch.setattr(QuotaMonitor, "_sleep_or_stop", _grace)
        monkeypatch.setattr(m, "_regular_refresh", _regular)
        monkeypatch.setattr(m, "_confirm_refresh", _confirm)
        monkeypatch.setattr(m, "_activate_due", _activate)

        task = asyncio.create_task(m._loop())
        for _ in range(2000):
            await asyncio.sleep(0)
            if len([x for x in seen if x[0] == "regular"]) >= 2:
                break
        await m.stop()
        await task

        events = [x[0] for x in seen]
        # ① 启动轮 ② due 插队（激活→确认刷新）③ 常规节拍按原 next_regular_at 继续
        assert events == ["regular", "activate", "confirm", "regular"], events
        assert seen[1][1] == _DUE, seen          # 23:10:00 准点，不被拖到 30min 轮询
        assert seen[2][1] == _DUE + 10, seen     # grace 之后才打 billing
        assert seen[3][1] >= _NOW + 1800, seen   # 常规节拍没被 due 提前/挤掉
        assert acc.status == Status.ACTIVE       # 本地激活真实写回状态
        assert _fs.updated == 1                  # 只落库一次，不重复


class TestConfirmRefresh:
    @pytest.mark.asyncio
    async def test_success_no_retry(self, monkeypatch):
        m, _ = _monitor([_acc()], monkeypatch)
        calls = []

        async def _fake(accounts):
            calls.append(1)
            return {"ok": len(accounts), "fail": 0}

        monkeypatch.setattr("app.quota.refresh_accounts", _fake)
        monkeypatch.setattr(m, "CONFIRM_RETRY_WAIT", 0)
        await m._confirm_refresh()
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_failure_bounded_retries(self, monkeypatch):
        m, _ = _monitor([_acc()], monkeypatch)
        calls = []

        async def _fake(accounts):
            calls.append(1)
            return {"ok": 0, "fail": 1}

        async def _no_wait(self, secs):
            assert secs == m.CONFIRM_RETRY_WAIT
            return False

        monkeypatch.setattr("app.quota.refresh_accounts", _fake)
        monkeypatch.setattr(QuotaMonitor, "_sleep_or_stop", _no_wait)
        await m._confirm_refresh()
        assert len(calls) == m.CONFIRM_RETRIES
