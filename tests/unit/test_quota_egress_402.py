"""池级额度耗尽的对外语义：402（2026-10-31 语义整改）。

背景：下游 OmniRoute 的错误分类器对 api-key 连接的 429 无条件判
RATE_LIMITED（不看文案与 Retry-After），「仅配额耗尽回退」因此失效，
还会给连接打限流标记（后续请求被预检 skip 报 503 unavailable）。
402 在其分类器里无条件判 QUOTA_EXHAUSTED（cooldownMs=0，不污染连接），
正好命中「仅配额耗尽回退」语义。
"""

from __future__ import annotations

import time

from app import settings
from app.models import Status


async def test_pool_quota_exhaustion_returns_402(gateway_client, fresh_app, monkeypatch):
    """全部账号 EXHAUSTED 且无号可选 → 402 + model_no_quota（非 429/503）。"""
    monkeypatch.setattr(settings, "QUEUE_WAIT", 0)
    acc = fresh_app.add_account("zai", "a", "jwt.token.a")
    acc.status = Status.EXHAUSTED
    fresh_app.update_account(acc)

    client, _ = gateway_client
    res = await client.post(
        "/v1/messages", json={"model": "glm-5.3-flash", "messages": []},
    )
    assert res.status_code == 402
    err = res.json()["error"]
    assert err["type"] == "model_no_quota"
    assert err["code"] == "no_quota_for_model"
    assert err["next_available_at"] > time.time()
    assert int(res.headers["Retry-After"]) >= 1
