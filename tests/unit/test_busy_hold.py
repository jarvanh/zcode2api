"""BUSY_HOLD（magpie plugins#503 同款全池刹车）+ 3008 语义修正。

3008/3009/3010 = 并发槽「没轮到」：不扣号、不占 429 频控梯，按官方客户端
节奏（1s/2s）原地重问；同一请求重问仍被拒 = 持续饱和，60s 窗口内 BUSY_N 次
武装全池静默 BUSY_HOLD 秒（429+Retry-After），赶在 WAF 封 IP 之前刹车。
"""

from __future__ import annotations

import time

from app import settings
from app.routes import gateway as gw


def _setup(monkeypatch, n=5, window=60, hold=180):
    monkeypatch.setattr(settings, "BUSY_N", n)
    monkeypatch.setattr(settings, "BUSY_WINDOW", window)
    monkeypatch.setattr(settings, "BUSY_HOLD", hold)


async def test_busy_hold_arms_after_n_saturations(monkeypatch):
    _setup(monkeypatch)
    gw._busy_hold["hits"].clear()
    gw._busy_hold["until"] = 0.0
    for _ in range(settings.BUSY_N - 1):
        await gw._busy_record_saturated()
        assert not gw._busy_held()
    await gw._busy_record_saturated()
    assert gw._busy_held()
    assert 0 < gw._busy_hold_remaining() <= settings.BUSY_HOLD


async def test_busy_hold_does_not_self_extend(monkeypatch):
    _setup(monkeypatch)
    gw._busy_hold["hits"].clear()
    gw._busy_hold["until"] = 0.0
    for _ in range(settings.BUSY_N + 3):
        await gw._busy_record_saturated()
    until = gw._busy_hold["until"]
    for _ in range(3):
        await gw._busy_record_saturated()  # 静默期内的饱和不计数
    assert gw._busy_hold["until"] == until


async def test_busy_hold_disabled(monkeypatch):
    _setup(monkeypatch, hold=0)
    gw._busy_hold["hits"].clear()
    gw._busy_hold["until"] = 0.0
    for _ in range(20):
        await gw._busy_record_saturated()
    assert not gw._busy_held()


def test_3008_not_rate_hold(monkeypatch):
    """3008/3009/3010 并发槽不扣号（v2.9.28 曾扣 30s，语义纠正）。"""
    _setup(monkeypatch)
    body = gw._is_concurrency_hit('{"code":3008,"msg":"user concurrency limit exceeded"}')
    assert body is True
    # 3008 在 failover 之前单独返回，不落入真频控所号分支
    src = open(gw.__file__).read()
    busy_seg = src[src.index("busy_hit = _is_concurrency_hit(text)"):
                   src.index("hold_model = str(body.get(\"model\") or \"\")")]
    assert "return _NEXT_ACCOUNT" in busy_seg
    assert "hold_rate" not in busy_seg


def test_recovery_rate_hold(monkeypatch):
    """_recovery 的 rate kind 指向扣号到期时刻（不污染全局 settings）。"""
    _setup(monkeypatch)
    import pathlib
    import tempfile

    from app.store import Store

    d = pathlib.Path(tempfile.mkdtemp())
    monkeypatch.setattr(settings, "DATA_DIR", d)
    monkeypatch.setattr(settings, "DB_PATH", d / "accounts.db")
    s = Store()
    a = s.add_account("zai", "a", "hA.sig")
    a.hold_rate("GLM-5.3-Flash", 30)
    s.update_account(a)
    monkeypatch.setattr(gw, "store", s)
    kind, wait = gw._recovery("zai", "GLM-5.3-Flash")
    assert kind == "rate" and wait and wait > time.time()


def test_settings_defaults():
    assert settings.BUSY_N == 5 and settings.BUSY_WINDOW == 60 and settings.BUSY_HOLD == 180
    assert settings.BUSY_WAITS == (1, 2)
