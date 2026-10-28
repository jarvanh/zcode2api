"""第二阶段 B：无资源包的 429+Retry-After 语义 与 模型冷却探测轮。

- store.next_model_uncooldown_at：池内该模型最早解冻时刻
- bigmodel 收尾：全池模型冷却 → 429+Retry-After+next_available_at；
  无冷却信息 → 503 no_available_account（语义不变）
- ModelProbeMonitor：冷却接近尾段（<窗口）主动探测，成功清冷却；
  整号冷却/停用/无 Key 不探测
"""

from __future__ import annotations

import time

import pytest

from app import settings
from app.quota import ModelProbeMonitor
from app.store import Store


def _bm(store: Store, name: str, key: str):
    return store.add_account("bigmodel", name, key)


# ── store.next_model_uncooldown_at ──────────────────────────────────


def test_next_model_uncooldown_at_none_when_no_cooling(fresh_app):
    _bm(fresh_app, "a", "k1")
    assert fresh_app.next_model_uncooldown_at("bigmodel", "glm-5.3-flash") is None


def test_next_model_uncooldown_at_picks_soonest(fresh_app):
    a = _bm(fresh_app, "a", "k1")
    b = _bm(fresh_app, "b", "k2")
    far = a.cool_model("glm-5.3-flash", 3600, 3600)
    soon = b.cool_model("glm-5.3-flash", 60, 3600)
    other = a.cool_model("glm-4.5-air", 10, 3600)
    got = fresh_app.next_model_uncooldown_at("bigmodel", "glm-5.3-flash")
    assert got == pytest.approx(soon, abs=1.5)
    assert got < far
    # 模型名大小写归一
    assert fresh_app.next_model_uncooldown_at("bigmodel", "GLM-5.3-Flash") == pytest.approx(soon, abs=1.5)
    # 其他模型不受影响
    assert fresh_app.next_model_uncooldown_at("bigmodel", "glm-4.5-air") == pytest.approx(other, abs=1.5)


def test_next_model_uncooldown_at_ignores_expired(fresh_app):
    a = _bm(fresh_app, "a", "k1")
    until = a.cool_model("glm-5.3-flash", 60, 3600)
    a.model_cooling["glm-5.3-flash"] = time.time() - 1  # 已过期
    assert fresh_app.next_model_uncooldown_at("bigmodel", "glm-5.3-flash") is None


# ── _no_quota_response / bigmodel 收尾 ──────────────────────────────


def test_no_quota_response_429_shape():
    from fastapi.responses import JSONResponse

    from app.routes.gateway import _no_quota_response

    wait = time.time() + 120
    resp = _no_quota_response("req1", "glm-5.3-flash", "无包", wait)
    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 429
    import json as _json

    body = _json.loads(bytes(resp.body))
    err = body["error"]
    assert err["type"] == "model_no_quota"
    assert err["code"] == "no_quota_for_model"
    assert err["model"] == "glm-5.3-flash"
    assert err["next_available_at"] == pytest.approx(wait, abs=1.5)
    assert 100 <= body["retry_after"] <= 122
    assert resp.headers["retry-after"] == str(body["retry_after"])


def test_no_quota_response_falls_back_503():
    from app.routes.gateway import _no_quota_response

    resp = _no_quota_response("req1", "m", "无包", None)
    assert resp.status_code == 503
    # 过去时刻同样回 503（防御：到期后不应再建议等待）
    resp2 = _no_quota_response("req1", "m", "无包", time.time() - 5)
    assert resp2.status_code == 503


@pytest.mark.asyncio
async def test_bigmodel_all_cooled_returns_429(fresh_app):
    """全池模型冷却中 → 429 + Retry-After（而非 503）。"""
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    fresh_app.set_setting("bigmodel_channel_enabled", "true")
    try:
        acc = _bm(fresh_app, "cooled", "sk-x")
        acc.cool_model("glm-5.3-flash", 1800, 3600)
        fresh_app.update_account(acc)

        app = create_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post(
                "/v1/chat/completions",
                json={"model": "bigmodel/glm-5.3-flash",
                      "messages": [{"role": "user", "content": "hi"}], "max_tokens": 64},
            )
        assert res.status_code == 429
        body = res.json()
        assert body["error"]["type"] == "model_no_quota"
        assert body["error"]["code"] == "no_quota_for_model"
        assert int(res.headers["retry-after"]) >= 1
        # Retry-After 应接近冷却剩余（1800s 档，取上界防漂）
        assert 1700 <= int(res.headers["retry-after"]) <= 1801
    finally:
        fresh_app.set_setting("bigmodel_channel_enabled", "")


@pytest.mark.asyncio
async def test_bigmodel_no_cooling_returns_503(fresh_app):
    """池里账号但无模型冷却（纯无号语义）→ 维持 503 no_available_account。"""
    from httpx import ASGITransport, AsyncClient

    from app.main import create_app

    fresh_app.set_setting("bigmodel_channel_enabled", "true")
    try:
        _bm(fresh_app, "plain", "sk-y")
        app = create_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post(
                "/v1/chat/completions",
                json={"model": "bigmodel/glm-5.3-flash",
                      "messages": [{"role": "user", "content": "hi"}], "max_tokens": 64},
            )
        assert res.status_code == 503
        assert res.json()["error"]["type"] == "no_available_account"
    finally:
        fresh_app.set_setting("bigmodel_channel_enabled", "")


# ── ModelProbeMonitor._round ────────────────────────────────────────


def _near_cooling(acc, model: str, seconds: float):
    acc.model_cooling[model] = time.time() + seconds


@pytest.mark.asyncio
async def test_probe_round_skips_when_no_candidates(fresh_app, monkeypatch):
    m = ModelProbeMonitor()
    _bm(fresh_app, "a", "k1")
    called = []

    async def _fake(acc, model):
        called.append((acc.name, model))
        return True

    monkeypatch.setattr(m, "_probe_one", _fake)
    assert await m._round() == 0
    assert called == []


@pytest.mark.asyncio
async def test_probe_round_probes_near_expiry_and_clears(fresh_app, monkeypatch):
    m = ModelProbeMonitor()
    acc = _bm(fresh_app, "near", "k1")
    _near_cooling(acc, "glm-5.3-flash", 30)  # 剩余 < 默认窗口(120)
    fresh_app.update_account(acc)

    async def _fake(acc, model):
        acc.clear_model_cooling(model)
        return True

    monkeypatch.setattr(m, "_probe_one", _fake)
    assert await m._round() == 1
    live = fresh_app.find("bigmodel", acc.id)
    assert live.model_cooling == {}


@pytest.mark.asyncio
async def test_probe_round_ignores_far_cooling(fresh_app, monkeypatch):
    m = ModelProbeMonitor()
    acc = _bm(fresh_app, "far", "k1")
    _near_cooling(acc, "glm-5.3-flash", 3000)  # 剩余远超窗口
    fresh_app.update_account(acc)

    async def _fake(acc, model):
        raise AssertionError("不应打上游")

    monkeypatch.setattr(m, "_probe_one", _fake)
    assert await m._round() == 0


@pytest.mark.asyncio
async def test_probe_round_skips_disabled_and_globally_cooled(fresh_app, monkeypatch):
    m = ModelProbeMonitor()
    off = _bm(fresh_app, "off", "k1")
    off.enabled = False
    _near_cooling(off, "glm-5.3-flash", 30)
    cool = _bm(fresh_app, "cool", "k2")
    cool.cooling_until = time.time() + 600  # 整号冷却中
    cool.status = __import__("app.models", fromlist=["Status"]).Status.COOLING
    _near_cooling(cool, "glm-5.3-flash", 30)

    async def _fake(acc, model):
        raise AssertionError("不应探测停用/整号冷却账号")

    monkeypatch.setattr(m, "_probe_one", _fake)
    assert await m._round() == 0
