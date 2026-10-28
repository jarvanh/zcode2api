"""领取成功后清空模型级冷却（2026-10-28 整改）。

覆盖三个面：
- Account.clear_all_model_cooling()：全量清冷却与失败计数
- _clear_cooling_after_claim()：清空 + 持久化 + 收尾异常不抛出
- claim()/claim_with_captcha() 成功路径接线：code==0 才清；业务失败（1003 等）不清
"""

from __future__ import annotations

import time

import pytest

from app import claim as claim_mod
from app.claim import _clear_cooling_after_claim, claim, claim_with_captcha
from app.models import Account


class _StubCaptcha:
    async def get_verify_param(self, port: int | None = None) -> tuple[str, str | None]:
        return "mock-verify-param", None

    def invalidate(self) -> None:
        pass

    async def fetch_config(self) -> dict:
        return {"region": "cn"}


def _jwt_account(fresh, name: str = "t1") -> Account:
    """三点 JWT 秘钥 → jwt 模式账号（provider=zai 时 Create 判定）。"""
    acc = fresh.add_account("zai", name, "a.b.c")
    return acc


def test_clear_all_model_cooling():
    acc = Account.create("bigmodel", "t", "key")
    acc.cool_model("glm-5.3-flash", 60, 3600)
    acc.cool_model("glm-4.5-air", 60, 3600)
    assert acc.model_cooling and acc.model_fail_count
    acc.clear_all_model_cooling()
    assert acc.model_cooling == {}
    assert acc.model_fail_count == {}
    assert acc.is_selectable(model="glm-5.3-flash")


def test_clear_cooling_after_claim_persists(fresh_app):
    acc = _jwt_account(fresh_app)
    acc.cool_model("glm-5.3-flash", 60, 3600)
    fresh_app.update_account(acc)

    _clear_cooling_after_claim(acc)

    live = fresh_app.find("zai", acc.id)
    assert live.model_cooling == {}
    assert live.model_fail_count == {}


def test_clear_cooling_noop_when_no_cooling(fresh_app, caplog):
    acc = _jwt_account(fresh_app, "t2")
    # 无冷却时静默返回，不写日志不报错
    _clear_cooling_after_claim(acc)
    assert acc.model_cooling == {}


def test_clear_cooling_survives_store_error(fresh_app, monkeypatch):
    acc = _jwt_account(fresh_app, "t3")
    acc.cool_model("m", 60, 3600)
    fresh_app.update_account(acc)

    def _boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(fresh_app, "update_account", _boom)
    # 收尾失败不得抛出（领取成功语义不受影响）
    _clear_cooling_after_claim(acc)
    assert acc.model_cooling == {}


@pytest.mark.asyncio
async def test_claim_success_clears_cooling(fresh_app, monkeypatch):
    acc = _jwt_account(fresh_app, "t4")
    acc.cool_model("glm-5.3-flash", 60, 3600)
    fresh_app.update_account(acc)

    async def _fake_billing(account, method, path, **kw):
        assert path == "/billing/claim"
        return {"code": 0, "data": {"server_time": 1, "plan": {}}}

    monkeypatch.setattr(claim_mod, "captcha_manager", _StubCaptcha())
    monkeypatch.setattr(claim_mod, "_billing_request", _fake_billing)

    outcome = await claim(acc, "plan-x")
    assert outcome["plan_id"] == "plan-x"
    live = fresh_app.find("zai", acc.id)
    assert live.model_cooling == {}
    assert live.model_fail_count == {}


@pytest.mark.asyncio
async def test_claim_failure_keeps_cooling(fresh_app, monkeypatch):
    acc = _jwt_account(fresh_app, "t5")
    acc.cool_model("glm-5.3-flash", 60, 3600)
    fresh_app.update_account(acc)

    async def _fake_billing(account, method, path, **kw):
        return {"code": 1003, "msg": "已领取过"}

    monkeypatch.setattr(claim_mod, "captcha_manager", _StubCaptcha())
    monkeypatch.setattr(claim_mod, "_billing_request", _fake_billing)

    with pytest.raises(claim_mod.ClaimError):
        await claim(acc, "plan-x")
    live = fresh_app.find("zai", acc.id)
    # 1003 = 包已领取过，不代表新资源到账，冷却必须保留
    assert live.is_model_cooling("glm-5.3-flash")


@pytest.mark.asyncio
async def test_claim_with_captcha_success_clears_cooling(fresh_app, monkeypatch):
    acc = _jwt_account(fresh_app, "t6")
    acc.cool_model("glm-5.3", 60, 3600)
    fresh_app.update_account(acc)

    async def _fake_post(account, headers, plan_id):
        return {"code": 0, "data": {"server_time": 1, "plan": {}}}

    monkeypatch.setattr(claim_mod, "captcha_manager", _StubCaptcha())
    monkeypatch.setattr(claim_mod, "_post_claim", _fake_post)

    outcome = await claim_with_captcha(acc, "verify-param", "cn", "plan-y")
    assert outcome["plan_id"] == "plan-y"
    live = fresh_app.find("zai", acc.id)
    assert live.model_cooling == {}
