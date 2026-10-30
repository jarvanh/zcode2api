"""_recovery 语义（2026-10-30）：quota 耗尽走 429 语义、lane 冷却带恢复时刻。

下游网关（OmniRoute）对 503 的处置是 node 熔断 + 激进重试（连坐健康模型），
对 429 + Retry-After 是定向退避。额度耗尽（套餐过期/用尽）必须走后者，
且 Retry-After 要给出真实恢复时刻（额度窗口到期 / 下一轮领包的较早者）。
"""

from __future__ import annotations

import time

from app.models import Account, Status
from app.routes.gateway import _recovery


class _FakeStore:
    def __init__(self, accounts):
        self._accounts = accounts

    def list_accounts(self, provider):
        return self._accounts

    def next_model_uncooldown_at(self, provider, model):
        return None

    def next_lane_uncooldown_at(self, provider, model):
        return None


def _exhausted_account(expiry: float) -> Account:
    acc = Account.create("zai", "t", "hC.a.b")
    acc.status = Status.EXHAUSTED
    acc.quota = {"GLM-5.3-Flash": {"total": 300, "used": 300,
                                   "remaining": 0, "expires_at": expiry}}
    return acc


def test_recovery_quota_defaults_to_23bjon(monkeypatch):
    """无待生效套餐 → wait=当天北京 23:00（额度重置运营兜底时点）。"""
    monkeypatch.setattr("app.routes.gateway.store",
                        _FakeStore([_exhausted_account(time.time() + 86400)]))
    kind, wait = _recovery("zai", "GLM-5.3-Flash")
    assert kind == "quota"
    assert wait is not None
    from datetime import datetime, timedelta, timezone
    bj = timezone(timedelta(hours=8))
    local = datetime.fromtimestamp(wait, bj)
    assert (local.hour, local.minute) == (23, 0)   # 落在北京 23:00
    assert wait - time.time() <= 86400             # 不会超过一天


def test_recovery_quota_prefers_pending_plan_grant(monkeypatch):
    """有待生效套餐（未来 starts_at）且早于 23:00 → 用生效时刻。"""
    acc = _exhausted_account(time.time() + 86400)
    acc.plan = {"entitlements": [{"starts_at": time.time() + 900}]}
    monkeypatch.setattr("app.routes.gateway.store", _FakeStore([acc]))
    kind, wait = _recovery("zai", "GLM-5.3-Flash")
    assert kind == "quota"
    assert wait is not None
    assert 890 <= wait - time.time() <= 910        # 取套餐生效时刻


def test_recovery_quota_ignores_pending_later_than_reset(monkeypatch):
    """待生效套餐晚于今天 23:00 → 仍用 23:00（不推迟承诺）。"""
    acc = _exhausted_account(time.time() + 86400)
    acc.plan = {"entitlements": [{"starts_at": time.time() + 90000}]}  # 25h 后
    monkeypatch.setattr("app.routes.gateway.store", _FakeStore([acc]))
    kind, wait = _recovery("zai", "GLM-5.3-Flash")
    assert kind == "quota"
    assert wait is not None
    from datetime import datetime, timedelta, timezone
    bj = timezone(timedelta(hours=8))
    assert datetime.fromtimestamp(wait, bj).hour == 23


def test_recovery_quota_skipped_when_lane_cooling_dominates(monkeypatch):
    """额度耗尽 + lane 风控冷却并存 → lane（503 语义）优先，风控封锁不能
    被报成额度语义误导客户端换模型。"""
    acc = _exhausted_account(time.time() + 3600)
    acc.risk_lane_cooling["plan:glm-5.3-flash"] = time.time() + 180
    monkeypatch.setattr("app.routes.gateway.store", _FakeStore([acc]))
    monkeypatch.setattr(_recovery_module_store(), "next_lane_uncooldown_at",
                        lambda p, m: acc.risk_lane_cooling["plan:glm-5.3-flash"],
                        raising=False)
    kind, wait = _recovery("zai", "GLM-5.3-Flash")
    assert kind == "lane"
    assert wait is not None and wait > time.time()


def _recovery_module_store():
    """取 gateway._recovery 实际引用的 store 对象（测试已 monkeypatch 它）。"""
    import app.routes.gateway as gw
    return gw.store


def test_recovery_none_when_selectable_exists(monkeypatch):
    """仍有可选号（exhausted 但别的号活着）→ 不报 quota。"""
    dead = _exhausted_account(time.time() + 3600)
    alive = Account.create("zai", "ok", "hD.a.b")
    monkeypatch.setattr("app.routes.gateway.store", _FakeStore([dead, alive]))
    kind, _wait = _recovery("zai", "GLM-5.3-Flash")
    assert kind is None
