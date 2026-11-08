"""v2.9.32 端到端：上游持续 3008 时本地排队闸兜住，不秒回 503。

对照 2026-11-08 07:44 线上失败链：两活跃号各敲 3 次 3008 → 9 秒 503。
修复后：重问用尽即立闸门 → 请求排队等闸门到期 → 最终 429+Retry-After
（有确定恢复时刻），而不是 no_account 503。
"""

from __future__ import annotations

import pytest

_J1 = "h1.eyJzdWIiOiJnMSJ9.sig"
_J2 = "h2.eyJzdWIiOiJnMiJ9.sig"


@pytest.fixture(autouse=True)
def _clean_state():
    from app import reqlog
    from app.routes import gateway as gw

    reqlog._reset_for_tests()
    with gw._busy_gate_lock:
        gw._busy_gate_until.clear()
    gw._busy_hold["hits"].clear()
    gw._busy_hold["until"] = 0.0
    yield
    with gw._busy_gate_lock:
        gw._busy_gate_until.clear()
    gw._busy_hold["hits"].clear()
    gw._busy_hold["until"] = 0.0
    reqlog._reset_for_tests()


@pytest.mark.integration
class TestBusyGateEndToEnd:
    async def test_all_accounts_3008_returns_429_not_503(
        self, gateway_client, fresh_app, monkeypatch
    ):
        client, mock = gateway_client
        from app import settings
        from app.routes import gateway as gw
        from tests.conftest import seed_account

        # 排队预算压小，测试秒级结束；闸门 1s 到期后一次试探
        monkeypatch.setattr(settings, "QUEUE_WAIT", 2)
        monkeypatch.setattr(settings, "QUEUE_WAIT_SYNC", 2)
        monkeypatch.setattr(settings, "QUEUE_POLL", 0.2)
        monkeypatch.setattr(settings, "BUSY_GATE_WAIT", 1)
        monkeypatch.setattr(settings, "BUSY_WAITS", (0, 0))  # 测试不真等 1s/2s
        monkeypatch.setattr(settings, "RETRY_429_WAIT", 0)
        monkeypatch.setattr(settings, "BUSY_HOLD", 0)  # 隔离全池刹车，单测排队闸

        a = seed_account(fresh_app, _J1, name="gate-a")
        b = seed_account(fresh_app, _J2, name="gate-b")
        mock.state.sequences[_J1[:16]] = ["concurrency_3008"]
        mock.state.sequences[_J2[:16]] = ["concurrency_3008"]

        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}],
        })
        # 核心断言：不再是 no_account 503，而是带 Retry-After 的 429
        assert res.status_code == 429, (res.status_code, res.text[:200])
        assert "Retry-After" in res.headers
        # 两个账号都被 3008 立过闸（排队闸确实生效，而非直接放弃）
        with gw._busy_gate_lock:
            gated = {a.id: gw._busy_gate_until.get(a.id), b.id: gw._busy_gate_until.get(b.id)}
        assert gated[a.id] and gated[b.id], gated
        # 排队期间上游压力受控：每号每轮只吃官方 1s/2s 重问 3 次；
        # 2 秒排队预算内最多 2 轮闸门试探（真实线上预算 240s/60s，
        # 由 REQUEST_DEADLINE 兜底），远少于旧逻辑每轮重带满额重试梯。
        # 只查本用例自己的两个凭证（全量跑时 counters 会带上其他文件
        # 的凭证，按全字典断言会串台）。
        calls = {k: mock.state.counters.get(k, 0)
                 for k in (_J1[:16], _J2[:16])}
        assert all(v <= 10 for v in calls.values()), calls
