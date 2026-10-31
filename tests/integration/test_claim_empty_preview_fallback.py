"""preview 空窗兜底 + 套餐 id 收割的端到端集成测试（2026-10-31 根因修复）。

复现真实上游形态（本机实测）：
- billing/preview → {"code":0,"msg":"","logid":...}（**无 data 字段**）
- billing/claim   → 带明确 plan_id 时 code=0 真实发放
- billing/current → 同样可能无 data；而 billing/balance 一直带着已生效 plans[]

旧实现三处全走偏：空 preview 静默跳过（自动轮永远领不到，而主人手动领得到）、
只从 current 取 plans[]（每次刷新清成 []）、_bonus_active 只认 effective_at
（上游实测键名 starts_at，赠送池账号被误标 exhausted）。这里用 Mock 上游钉死
修复后的行为。
"""

from __future__ import annotations

import time

import pytest

from app.claim import auto_claim_all_plans, known_plan_ids
from tests.conftest import seed_account

GOOD_JWT = "h1.eyJzdWIiOiJhIn0.sig"
WEEKEND_PLAN = "zcode-v3-start-plan-1030-wk"


@pytest.fixture
def env(gateway_client, fresh_app, monkeypatch):
    """网关客户端 + Mock + JWT 账号 + claim 验证码桩（同 test_claim.py 口径）。"""
    client, mock = gateway_client
    from app import claim as claim_module

    class _StubCaptcha:
        def __init__(self):
            self.solve_count = 0

        async def get_verify_param(self, port=None):
            self.solve_count += 1
            return "stub-verify-param", "sgp"

        async def fetch_config(self):
            return {"enabled": True, "prefix": "mockpre", "region": "sgp"}

        def invalidate(self):
            pass

    stub = _StubCaptcha()
    monkeypatch.setattr(claim_module, "captcha_manager", stub)
    # session 级 mock：显式复位全部相关场景，防跨用例残留
    mock.state.claim_scenario = None
    mock.state.billing_current_scenario = None
    mock.state.billing_balance_plans = None
    mock.state.billing_balance_windows = None
    acc = seed_account(fresh_app, GOOD_JWT, name="fallback-a")
    return client, mock, stub, acc


def _auth(store) -> dict:
    return {"Authorization": f"Bearer {store.admin_key()}"}


@pytest.mark.integration
class TestPreviewEmptyFallback:
    async def test_empty_preview_still_claims_harvested_plan(self, env, fresh_app):
        """核心回归：preview 无 data 时，仍按收割到的 plan_id 领取成功。"""
        _client, mock, _stub, acc = env
        mock.state.claim_scenario = "claim_no_data"
        fresh_app.set_setting("claim_known_plan_ids", f'["{WEEKEND_PLAN}"]')

        outcomes = await auto_claim_all_plans(acc)

        assert len(outcomes) == 1, "空 preview 不得再静默跳过（旧行为：领不到）"
        assert outcomes[0]["ok"] is True
        assert outcomes[0]["plan_id"] == WEEKEND_PLAN
        assert outcomes[0]["probe"] is True, "兜底来源必须打标（面板可区分）"

    async def test_no_candidates_still_skips_quietly(self, env):
        """无候选可试时保持原语义：不谎报成功，也不抛异常。"""
        _client, mock, _stub, acc = env
        mock.state.claim_scenario = "claim_no_data"
        assert known_plan_ids() == []
        assert await auto_claim_all_plans(acc) == []

    async def test_held_plan_not_retried(self, env, fresh_app):
        """已持有的套餐不进兜底候选（必定 1003，白烧验证码）。"""
        _client, mock, stub, acc = env
        mock.state.claim_scenario = "claim_no_data"
        acc.plans = [{"plan_id": WEEKEND_PLAN}]
        fresh_app.set_setting("claim_known_plan_ids", f'["{WEEKEND_PLAN}"]')

        assert await auto_claim_all_plans(acc) == []
        assert stub.solve_count == 0, "不该为必然失败的候选求解验证码"

    async def test_preview_listed_plan_is_harvested(self, env):
        """preview 正常列出时写入候选，供后续空窗轮复用。"""
        _client, mock, _stub, acc = env
        mock.state.claim_scenario = None  # mock 默认列出 mock-claim-plan

        outcomes = await auto_claim_all_plans(acc)

        assert [o["plan_id"] for o in outcomes] == ["mock-claim-plan"]
        assert "mock-claim-plan" in known_plan_ids()


@pytest.mark.integration
class TestQuotaPlansMerge:
    async def test_current_without_data_keeps_balance_plans(self, env, fresh_app):
        """current 空窗（无 data）+ balance 带套餐 → plans 不被清成 []。"""
        _client, mock, _stub, acc = env
        from app import quota

        acc.plans = [{"plan_id": "old", "show_name": "Old"}]
        fresh_app.update_account(acc)

        mock.state.billing_current_scenario = "no_data"
        mock.state.billing_balance_plans = [{
            "plan_id": WEEKEND_PLAN, "show_name": "ZCODE WEEKEND BUILD",
            "status": "active",
            "entitlements": [{"show_name": "GLM-5.3-Flash", "meter": "model_usage",
                              "unit_type": "token", "grant_units": 500000000,
                              "period": "one_time",
                              "starts_at": int(time.time()) - 100,
                              "ends_at": int(time.time()) + 86400}],
        }]

        await quota.fetch_quota(acc)

        live = fresh_app.find("zai", acc.id)
        assert [p["plan_id"] for p in live.plans] == [WEEKEND_PLAN], \
            "current 空窗不得用 [] 覆盖 balance 已生效套餐"

    async def test_bonus_starts_at_prevents_false_exhausted(self, env, fresh_app):
        """日窗全 0 + 赠送池按 starts_at 生效 → 不得误标 exhausted。"""
        _client, mock, _stub, acc = env
        from app import quota
        from app.models import Status

        now = int(time.time())
        mock.state.billing_current_scenario = "no_data"
        mock.state.billing_balance_windows = [
            {"model": "GLM-5.3", "show_name": "GLM-5.3", "total_units": 3000000,
             "used_units": 3000000, "remaining_units": 0, "expires_at": now + 86400},
        ]
        mock.state.billing_balance_plans = [{
            "plan_id": WEEKEND_PLAN, "show_name": "ZCODE WEEKEND BUILD",
            "entitlements": [{"show_name": "GLM-5.3-Flash", "period": "one_time",
                              "starts_at": now - 100, "ends_at": now + 86400}],
        }]

        await quota.fetch_quota(acc)

        live = fresh_app.find("zai", acc.id)
        assert live.status != Status.EXHAUSTED, \
            "赠送池生效时不得判耗尽（上游实测键名为 starts_at）"

    async def test_harvested_plan_ids_feed_next_round(self, env):
        """balance 里的套餐 id 自动入候选池——跨号发现投放的唯一渠道。"""
        _client, mock, _stub, acc = env
        from app import quota

        mock.state.billing_current_scenario = "no_data"
        mock.state.billing_balance_plans = [{
            "plan_id": WEEKEND_PLAN, "show_name": "ZCODE WEEKEND BUILD",
            "entitlements": [{"show_name": "GLM-5.3-Flash", "period": "one_time",
                              "starts_at": int(time.time()) - 10}],
        }]

        await quota.fetch_quota(acc)

        assert WEEKEND_PLAN in known_plan_ids(), \
            "上游绕过 preview 直接入账的套餐必须被收割为候选"


@pytest.mark.integration
class TestManualClaimFallback:
    async def test_manual_claim_uses_harvested_candidate(self, env, fresh_app):
        """面板「手动领取」在 preview 空时也必须可用（同一根因的第二条路径）。"""
        client, mock, _stub, acc = env
        mock.state.claim_scenario = "claim_no_data"
        fresh_app.set_setting("claim_known_plan_ids", f'["{WEEKEND_PLAN}"]')

        res = await client.post("/admin/api/claim",
                                json={"account_ids": [acc.id]},
                                headers=_auth(fresh_app))

        assert res.status_code == 200
        outcome = res.json()["outcomes"][0]
        assert outcome["ok"] is True, f"回归（仍报无可领套餐）: {outcome}"
        assert outcome["plan_id"] == WEEKEND_PLAN
