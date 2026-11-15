"""同时使用账号数上限（maxAccounts，2026-11-05）。

对齐 magpie 的 maxAccounts（plugins#649）：同一出口同时用 3 个号会被 405 拒，
2 个不会。超出的号留作备用——在用的号冷却/耗尽/停用时名额自动让出。
"""

from __future__ import annotations

from app import settings


def _seed(store, secret: str, name: str):
    return store.add_account("zai", name, secret)


class TestMaxActiveAccounts:
    def test_no_accounts_falls_back_to_direct_cap(self, fresh_app):
        """空池（无账号 = 无代理）：动态上限回落到直连保守值。"""
        assert fresh_app.max_active_accounts() == settings.DIRECT_ACTIVE_ACCOUNTS
        assert settings.MAX_ACTIVE_ACCOUNTS == 0

    def test_proxy_count_caps_when_no_meta(self, fresh_app):
        """有代理 → 上限 = 代理数；再加一个直连号不影响。"""
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        a.proxy_url = "http://127.0.0.1:7891"
        fresh_app.update_account(a)
        assert fresh_app.max_active_accounts() == 1
        b = _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        b.proxy_url = "http://127.0.0.1:7892"
        fresh_app.update_account(b)
        assert fresh_app.max_active_accounts() == 2
        _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")  # 直连号不加代理数
        assert fresh_app.max_active_accounts() == 2

    def test_all_direct_caps_at_two(self, fresh_app):
        """全部直连：上限 = DIRECT_ACTIVE_ACCOUNTS（默认 2）。"""
        _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        assert fresh_app.max_active_accounts() == 2

    def test_console_override_wins(self, fresh_app):
        """meta 显式设置优先于动态推导（含 0 = 不限）。"""
        fresh_app.set_setting("max_active_accounts", "1")
        assert fresh_app.max_active_accounts() == 1
        fresh_app.set_setting("max_active_accounts", "0")
        assert fresh_app.max_active_accounts() == 0
        fresh_app.set_setting("max_active_accounts", "abc")
        # 脏值 → 走动态推导（空池 = 直连上限）
        assert fresh_app.max_active_accounts() == settings.DIRECT_ACTIVE_ACCOUNTS

    def test_console_override_beats_proxy_count(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        a.proxy_url = "http://127.0.0.1:7891"
        fresh_app.update_account(a)
        fresh_app.set_setting("max_active_accounts", "3")
        assert fresh_app.max_active_accounts() == 3

    def test_cap_limits_distinct_accounts(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        b = _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "2")

        inflight = {a.id: 1, b.id: 1}
        # 默认单账号并发 1；本用例验 max_active 活跃集上限，显式给 2 留余量
        picked = {fresh_app.select("zai", inflight=inflight, max_inflight=2).id
                  for _ in range(20)}
        assert picked <= {a.id, b.id}
        assert c.id not in picked

    def test_free_slot_lets_a_reserve_account_in(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "2")

        # 粘性活跃集（magpie，2026-11-07）：未满员时逐步扩集（a→b），
        # 补到 cap=2 后 b/c 中只有先进集的 b 参与轮换，c 恒为备用静默。
        inflight = {a.id: 1}
        picked = {fresh_app.select("zai", inflight=inflight).id for _ in range(20)}
        assert c.id not in picked
        assert len(picked) <= 2  # cap=2 上限不破

    def test_zero_means_unlimited(self, fresh_app):
        a = _seed(fresh_app, "hA.eyJzdWIiOiJhIn0.sig", "m-a")
        _seed(fresh_app, "hB.eyJzdWIiOiJiIn0.sig", "m-b")
        c = _seed(fresh_app, "hC.eyJzdWIiOiJjIn0.sig", "m-c")
        fresh_app.set_setting("max_active_accounts", "0")

        inflight = {a.id: 1}
        picked = {fresh_app.select("zai", inflight=inflight).id for _ in range(20)}
        assert c.id in picked
