"""preview 空窗兜底与套餐 id 收割单测（2026-10-31 根因修复）。

背景（实证）：三个号的 billing/preview 返回 {"code":0,"msg":"","logid":...}
——连 data 字段都没有——而带明确 plan_id 打 billing/claim 返回 code=0 真实发放。
旧实现把空 preview 一律判「上游无投放套餐」静默跳过，于是主人手动领得到、
网关自动轮永远领不到。配套的两处同源缺陷：_bonus_active 只认 effective_at
（上游实测键名 starts_at），fetch_quota 只从 current 取 plans[]（空窗时被清成
[]，赠送池账号因此被误标 exhausted）。
"""

from __future__ import annotations

import time

import pytest

from app import claim as claim_mod
from app.claim import (
    ClaimError,
    _date_plan_candidates,
    _held_plan_ids,
    _probe_state,
    _probe_unlisted_plans,
    known_plan_ids,
    preview_plans,
    remember_plan_ids,
)
from app.quota import _bonus_active

_NOW = 1_000_000.0


def _acc(store, name: str = "t1"):
    """真实入池账号（jwt 模式 + 设备指纹，同 test_claim_clear_cooling 口径）。"""
    return store.add_account("zai", name, f"a.b.{name}")


class TestBonusActiveKeyNames:
    """上游实测键名是 starts_at；只认 effective_at 会把赠送池账号误标 exhausted。"""

    def test_starts_at_only_is_recognized(self):
        plan = {"entitlements": [{"period": "one_time", "starts_at": _NOW - 100,
                                  "ends_at": _NOW + 100}]}
        assert _bonus_active(plan, _NOW) is True

    def test_effective_at_still_works(self):
        plan = {"entitlements": [{"period": "one_time", "effective_at": _NOW - 100,
                                  "ends_at": _NOW + 100}]}
        assert _bonus_active(plan, _NOW) is True

    def test_future_starts_at_not_active(self):
        plan = {"entitlements": [{"period": "one_time", "starts_at": _NOW + 500}]}
        assert _bonus_active(plan, _NOW) is False

    def test_starts_at_zero_falsy_not_active(self):
        plan = {"entitlements": [{"period": "one_time", "starts_at": 0}]}
        assert _bonus_active(plan, _NOW) is False


class TestPlanIdHarvest:
    def test_remember_merges_and_dedups(self, fresh_app):
        assert known_plan_ids() == []
        remember_plan_ids(["plan-a", "plan-b"])
        remember_plan_ids(["plan-b", "plan-c"])
        assert known_plan_ids() == ["plan-a", "plan-b", "plan-c"]

    def test_remember_ignores_blank(self, fresh_app):
        remember_plan_ids(["", "   ", "x"])
        assert known_plan_ids() == ["x"]

    def test_held_plan_ids(self, fresh_app):
        acc = _acc(fresh_app, "held")
        acc.plans = [{"plan_id": "p1"}, {"plan_id": "  "}, {}, {"plan_id": "p2"}]
        assert _held_plan_ids(acc) == {"p1", "p2"}


def _stub_probe(monkeypatch, code: int = 3007, calls: list[str] | None = None):
    """把无验证码存在性探测桩成指定业务码（默认 3007=存在，待验证码）。"""
    async def fake_probe(account, plan_id):
        if calls is not None:
            calls.append(plan_id)
        return code

    monkeypatch.setattr(claim_mod, "_probe_plan_code", fake_probe)


@pytest.mark.asyncio
class TestProbeUnlistedPlans:
    @pytest.fixture(autouse=True)
    def _isolate_date_candidates(self, monkeypatch):
        """已知 id 用例只测收割候选；日期推导由 TestDatePlanCandidates 单独覆盖。"""
        monkeypatch.setattr(claim_mod, "_date_plan_candidates", lambda now=None: [])

    async def test_claims_harvested_candidate(self, fresh_app, monkeypatch):
        """preview 空 ≠ 无可领：收割到的 id 必须真去打一次 claim。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id, "plan_name": "ZCODE WEEKEND BUILD"}

        remember_plan_ids(["zcode-v3-start-plan-1030-wk"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        out = await _probe_unlisted_plans(_acc(fresh_app, "probe-claim"))

        assert claims == ["zcode-v3-start-plan-1030-wk"]
        assert len(out) == 1
        assert out[0]["ok"] is True and out[0]["probe"] is True
        assert out[0]["plan_name"] == "ZCODE WEEKEND BUILD"

    async def test_skips_already_held_without_spending_budget(
            self, fresh_app, monkeypatch):
        """已持有的 id 必定 1003：不打、也不占用每日预算。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id}

        remember_plan_ids(["held-plan", "new-plan"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        acc = _acc(fresh_app, "probe-held")
        acc.plans = [{"plan_id": "held-plan"}]
        out = await _probe_unlisted_plans(acc)

        assert claims == ["new-plan"], "held 候选不得白烧一次领取请求"
        assert [o["plan_id"] for o in out] == ["new-plan"]

    async def test_respects_skip_plan_ids(self, fresh_app, monkeypatch):
        """1005 名额等待期的 id 与 preview 主路同语义跳过。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id}

        remember_plan_ids(["waiting-plan"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        out = await _probe_unlisted_plans(_acc(fresh_app, "probe-skip"),
                                          {"waiting-plan"})
        assert claims == [] and out == []

    async def test_daily_budget_caps_attempts(self, fresh_app, monkeypatch):
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            raise ClaimError("该套餐已经领取过", code=1003)

        remember_plan_ids(["b1", "b2", "b3"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        monkeypatch.setattr(claim_mod, "_PROBE_DAILY_BUDGET", 2)
        acc = _acc(fresh_app, "probe-budget")

        assert await _probe_unlisted_plans(acc) == []
        assert claims == ["b1", "b2"], "每日最多打 2 个候选"
        claims.clear()
        assert await _probe_unlisted_plans(acc) == []
        assert claims == [], "预算耗尽当日不再打（防烧验证码池/风控分）"

    async def test_forgets_plan_on_probe_1001(self, fresh_app, monkeypatch):
        """1001 = 套餐已下线：探测阶段即剔除，不进入验证码环节。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id}

        remember_plan_ids(["dead-plan"])
        _stub_probe(monkeypatch, 1001)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        assert await _probe_unlisted_plans(_acc(fresh_app, "probe-1001")) == []
        assert known_plan_ids() == []
        assert claims == [], "不存在的 id 不得烧验证码"

    async def test_probe_1003_skips_without_claim(self, fresh_app, monkeypatch):
        """探测已持有（本地 plans 陈旧）→ 不打真实领取。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id}

        remember_plan_ids(["held-upstream"])
        _stub_probe(monkeypatch, 1003)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        assert await _probe_unlisted_plans(_acc(fresh_app, "probe-held-up")) == []
        assert claims == []

    async def test_probe_exception_does_not_escape(self, fresh_app, monkeypatch):
        async def boom(account, plan_id):
            raise RuntimeError("probe exploded")

        remember_plan_ids(["x-plan"])
        monkeypatch.setattr(claim_mod, "_probe_plan_code", boom)
        assert await _probe_unlisted_plans(_acc(fresh_app, "probe-probe-boom")) == []

    async def test_network_exception_does_not_escape(self, fresh_app, monkeypatch):
        async def boom(account, plan_id=None):
            raise RuntimeError("network exploded")

        remember_plan_ids(["x-plan"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", boom)
        assert await _probe_unlisted_plans(_acc(fresh_app, "probe-boom")) == []

    async def test_date_candidate_claimed_without_harvest(
            self, fresh_app, monkeypatch):
        """核心回归：没有任何历史/持有 id 时，日期候选也能触发领取。"""
        claims: list[str] = []

        async def fake_claim(account, plan_id=None):
            claims.append(plan_id)
            return {"plan_id": plan_id, "plan_name": "ZCODE GLOBAL BUILD"}

        monkeypatch.setattr(claim_mod, "_date_plan_candidates",
                            lambda now=None: ["zcode-v3-start-plan-1102"])
        _stub_probe(monkeypatch)
        monkeypatch.setattr(claim_mod, "claim", fake_claim)
        acc = _acc(fresh_app, "probe-date")
        assert known_plan_ids() == []
        out = await _probe_unlisted_plans(acc)
        assert claims == ["zcode-v3-start-plan-1102"]
        assert out and out[0]["probe"] is True


class TestDatePlanCandidates:
    """日期推导候选：不依赖任何账号已持有，解决「每次都要先手动领一个」。"""

    @staticmethod
    def _ts(m, d):
        return time.mktime((2026, m, d, 12, 0, 0, 0, 0, -1))

    def test_weekday_plain_id(self):
        assert _date_plan_candidates(self._ts(11, 2)) == [
            "zcode-v3-start-plan-1102"]

    def test_friday_includes_weekend_variant(self):
        assert _date_plan_candidates(self._ts(10, 30)) == [
            "zcode-v3-start-plan-1030", "zcode-v3-start-plan-1030-wk"]

    def test_sunday_includes_weekend_variant(self):
        assert _date_plan_candidates(self._ts(11, 1)) == [
            "zcode-v3-start-plan-1101", "zcode-v3-start-plan-1101-wk"]


@pytest.mark.asyncio
class TestPreviewPlansSemantics:
    async def _call(self, store, monkeypatch, body: dict) -> list[dict]:
        async def fake_request(account, method, path, **kwargs):
            return body

        monkeypatch.setattr(claim_mod, "_billing_request", fake_request)
        return await preview_plans(_acc(store, "pv"))

    async def test_missing_data_returns_empty_without_remembering(
            self, fresh_app, monkeypatch):
        """空窗形态（code=0 无 data）→ []，且不写入候选（无信息可用）。"""
        out = await self._call(fresh_app, monkeypatch,
                               {"code": 0, "msg": "", "logid": "x"})
        assert out == []
        assert known_plan_ids() == []

    async def test_empty_plans_list_returns_empty(self, fresh_app, monkeypatch):
        out = await self._call(fresh_app, monkeypatch, {"code": 0, "data": {"plans": []}})
        assert out == []

    async def test_listed_plans_sorted_and_harvested(self, fresh_app, monkeypatch):
        body = {"code": 0, "data": {"plans": [
            {"plan_id": "plan-low", "priority": 1},
            {"plan_id": "plan-high", "priority": 99},
        ]}}
        out = await self._call(fresh_app, monkeypatch, body)
        assert [p["plan_id"] for p in out] == ["plan-high", "plan-low"], "优先级降序"
        assert known_plan_ids() == ["plan-high", "plan-low"], "列出过的 id 入库备用"

    async def test_business_error_still_raises(self, fresh_app, monkeypatch):
        with pytest.raises(ClaimError):
            await self._call(fresh_app, monkeypatch, {"code": 1002, "msg": "expired"})
