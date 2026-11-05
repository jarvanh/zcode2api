"""同时使用账号数上限（maxAccounts，2026-11-05）。

对齐 magpie 的 maxAccounts（plugins#649）：同一出口同时用 3 个号会被 405 拒，
2 个不会。超出的号留作备用——在用的号冷却/耗尽/停用时名额自动让出。
"""

from __future__ import annotations

from app import settings


def _seed(store, secret: str, name: str):
    return store.add_account("zai", name, secret)


class TestMaxActiveAccounts:
    def test_default_is_two(self, fresh_app):
        assert fresh_app.max_active_accounts() == 2
        assert settings.MAX_ACTIVE_ACCOUNTS == 2

    def test_console_override(self, fresh_app):
        fresh_app.set_setting("max_active_accounts", "1")
        assert fresh_app.max_active_accounts() == 1
        fresh_app.set_setting("max_active_accounts", "0")
        assert fresh_app.max_active_accounts() == 0
        fresh_app.set_setting("max_active_accounts", "abc")
        assert fresh_app.max_active_accounts() == settings.MAX_ACTIVE_ACCOUNTS

    def test_cap_limits_distinct_accounts(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        b = _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "2")

        inflight = {a.id: 1, b.id: 1}
        picked = {fresh_app.select("zai", inflight=inflight).id for _ in range(20)}
        assert picked <= {a.id, b.id}
        assert c.id not in picked

    def test_free_slot_lets_a_reserve_account_in(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "2")

        # 只有一个号在飞：名额还有空位，备用号可以被拉进来
        inflight = {a.id: 1}
        picked = {fresh_app.select("zai", inflight=inflight).id for _ in range(20)}
        assert c.id in picked

    def test_zero_means_unlimited(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "0")

        inflight = {a.id: 1}
        picked = {fresh_app.select("zai", inflight=inflight).id for _ in range(20)}
        assert c.id in picked
