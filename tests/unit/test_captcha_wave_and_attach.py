"""验证码波次助推（B 档）与 plan 通道全量带头（C 档，默认关）。

B 档锁三件事：波次暂停确认抬升预热目标、助推期压制闲置停解、到期自动回落；
C 档锁两件事：开关开启时 plan 请求从池内带头（绝不现解）、默认关/池空
fail-open 不带头。
"""

from __future__ import annotations

import base64
import json
import time

from app import agent, settings
from app.captcha import CaptchaManager, captcha_manager
from app.models import Account

# ── B 档：波次助推 ────────────────────────────────────────────────────────────

def test_wave_boost_raises_target_and_suppresses_idle(monkeypatch):
    m = CaptchaManager()
    monkeypatch.setattr(settings, "CAPTCHA_POOL_MIN", 1)
    monkeypatch.setattr(settings, "CAPTCHA_WAVE_BOOST", 2)
    assert m._pool_target() == 1          # 常规水位
    m.notify_wave_pause(90)
    assert m._pool_target() == 3          # 1 + 2
    assert m._idle() is False             # 闲置停解被压制（即便从未取用）
    m._wave_boost_until = time.monotonic() - 1  # 到期
    assert m._pool_target() == 1          # 自动回落
    assert m._idle() is True              # 从未取用 → 回到闲置语义


def test_wave_boost_reconfirm_extends(monkeypatch):
    m = CaptchaManager()
    m.notify_wave_pause(10)
    first = m._wave_boost_until
    time.sleep(0.01)
    m.notify_wave_pause(30)               # 重复确认取更晚到期
    assert m._wave_boost_until > first


def test_try_take_now_empty_pool_is_none():
    m = CaptchaManager()
    assert m.try_take_now() is None       # 池空 fail-open，绝不现解


# ── C 档：plan 通道全量带头（默认关）────────────────────────────────────────

def _fake_jwt() -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"sub": "u-1"}).encode()).rstrip(b"=").decode()
    return f"h.{payload}.sig"


def _plan_account(name="c-att") -> Account:
    acc = Account.create("zai", name, _fake_jwt())
    acc.enabled = True
    acc.status = "active"
    return acc


def _build(acc, monkeypatch, token):
    if token is not None:
        monkeypatch.setattr(captcha_manager, "try_take_now", lambda: ("tok-123", "sgp"))
    url, headers, payload = agent.build_request(acc, {
        "model": "glm-5.3-flash", "max_tokens": 30, "stream": False,
        "messages": [{"role": "user", "content": "hi"}],
    }, None)
    return headers


def test_attach_off_by_default(monkeypatch):
    acc = _plan_account()
    monkeypatch.setattr(settings, "CAPTCHA_ATTACH_PLAN", False)
    headers = _build(acc, monkeypatch, [])
    assert "X-Aliyun-Captcha-Verify-Param" not in headers


def test_attach_on_emits_pool_token(monkeypatch):
    acc = _plan_account()
    monkeypatch.setattr(settings, "CAPTCHA_ATTACH_PLAN", True)
    headers = _build(acc, monkeypatch, [])
    assert headers["X-Aliyun-Captcha-Verify-Param"] == "tok-123"
    assert headers["X-Aliyun-Captcha-Verify-Region"] == "sgp"


def test_attach_pool_empty_fails_open(monkeypatch):
    acc = _plan_account()
    monkeypatch.setattr(settings, "CAPTCHA_ATTACH_PLAN", True)
    monkeypatch.setattr(captcha_manager, "try_take_now", lambda: None)
    headers = _build(acc, monkeypatch, None)
    assert "X-Aliyun-Captcha-Verify-Param" not in headers


def test_gateway_wave_confirms_notify_captcha(gateway_client, fresh_app):
    """波次确认 → captcha_manager.notify_wave_pause 被调用（B 档接线，
    走 gateway_client 的 stub_captcha 记录）。"""
    client, mock = gateway_client
    from app.routes import gateway as gw
    from tests.conftest import seed_account

    good_jwt = "hGA.eyJzdWIiOiJnYSJ9.sig"
    b_jwt = "hWB.eyJzdWIiOiJ3YiJ9.sig"
    seed_account(fresh_app, good_jwt, name="wa")
    seed_account(fresh_app, b_jwt, name="wb")
    mock.state.sequences[good_jwt[:16]] = ["risk_control_3012"]
    mock.state.sequences[b_jwt[:16]] = ["risk_control_3012"]

    stub = gw.captcha_manager

    async def fire():
        return await client.post("/v1/messages", json={
            "model": "GLM-5.2", "max_tokens": 8,
            "messages": [{"role": "user", "content": "hi"}]})

    import asyncio
    res = asyncio.run(fire())
    assert res.status_code == 503
    assert gw._risk_wave_level.get("zai", 0) >= 1
    assert getattr(stub, "wave_pauses", None) and stub.wave_pauses[0] > 0


