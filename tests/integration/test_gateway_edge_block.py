"""WAF 拦截页（IP 级）与 3012（账号级）的分流（2026-11-05，plugins#649）。

拦截页 = 本机出口 IP 被阿里云 ESA/WAF 拦：
  - 回客户端 405（不是 503），带 permission_error / edge_blocked_ip
  - **不记 strike、不改账号状态**（封的是地址，罚账号只会把好号打死）
  - 出口级静默：同一出口的后续请求不再打上游，其他出口不受影响

3012 = 账号风控：行为不变（记 strike、冷却、换号）。
"""

from __future__ import annotations

import pytest

from app import edge
from app.models import Status

ADMIN_AUTH = {"Authorization": "Bearer zcode"}

_MSG_BODY = {"model": "GLM-5.2", "messages": [{"role": "user", "content": "hi"}]}
_EDGE_JWT = "hE.eyJzdWIiOiJlIn0.sig"
_OTHER_JWT = "hO.eyJzdWIiOiJvIn0.sig"
_RISK_JWT = "hR2.eyJzdWIiOiJyIn0.sig"


def _messages_calls(mock) -> int:
    from tests.mock_upstream.server import _MESSAGES_PATHS
    return len([c for c in mock.state.calls if c[1] in _MESSAGES_PATHS])


@pytest.mark.integration
class TestEdgeBlockPage:
    async def test_edge_page_returns_405_without_strike(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        acc = seed_account(fresh_app, _EDGE_JWT, name="e-a")
        mock.state.sequences[_EDGE_JWT[:16]] = ["edge_block_page"]

        res = await client.post("/v1/messages", json=_MSG_BODY)

        assert res.status_code == 405
        body = res.json()
        assert body["error"]["type"] == "permission_error"
        assert body["error"]["code"] == "edge_blocked_ip"
        assert "防火墙" in body["error"]["message"]
        assert res.headers.get("x-zcode-edge") == "1"
        # 封的是地址：账号既没吃 strike，也没进冷却/禁用
        acc = fresh_app.list_accounts("zai")[0]
        assert acc.risk_strikes == 0
        assert acc.status == Status.ACTIVE
        # 已进入出口级静默（直连账号 → direct:<provider>）
        assert edge.held(edge.egress_key(None, "zai"))

    async def test_hold_fast_fails_next_request_without_upstream(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _EDGE_JWT, name="e-a")
        seed_account(fresh_app, _OTHER_JWT, name="e-b")
        mock.state.sequences[_EDGE_JWT[:16]] = ["edge_block_page"]
        mock.state.sequences[_OTHER_JWT[:16]] = ["edge_block_page"]

        first = await client.post("/v1/messages", json=_MSG_BODY)
        assert first.status_code == 405
        calls_after_first = _messages_calls(mock)

        second = await client.post("/v1/messages", json=_MSG_BODY)
        assert second.status_code == 405
        assert second.json()["error"]["code"] == "edge_blocked_ip"
        # 静默期内没有向上游再发一个请求（换号也不行：封的是地址）
        assert _messages_calls(mock) == calls_after_first

    async def test_3012_still_strikes_the_account(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _RISK_JWT, name="r-a")
        mock.state.sequences[_RISK_JWT[:16]] = ["risk_control_3012"]

        res = await client.post("/v1/messages", json=_MSG_BODY)

        # 3012 走原有风控路径：不是 405 的 edge 应答
        assert res.status_code != 405 or res.json().get("error", {}).get("code") != "edge_blocked_ip"
        acc = fresh_app.list_accounts("zai")[0]
        assert acc.risk_strikes >= 1
        assert not edge.held(edge.egress_key(None, "zai"))
