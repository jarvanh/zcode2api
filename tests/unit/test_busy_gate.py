"""v2.9.32 startGate 本地排队闸：3008 饱和后本地等槽，不敲上游、不秒回 503。

对照 magpie 实测（2026-11-08）：startGate 在飞满 3 时新请求本地排队、上游
峰值严格 3；上游健康时 10 并发 100% 成功。我们这里闸门由「上游 3008」触发，
语义一致：上游槽位残留占用（实测中位 64.7s 释放）期间零上游压力地排队。
"""

from __future__ import annotations

import time

from app import settings
from app.routes import gateway as gw


def _reset(monkeypatch):
    with gw._busy_gate_lock:
        gw._busy_gate_until.clear()
    monkeypatch.setattr(settings, "BUSY_GATE_WAIT", 20)
    yield
    with gw._busy_gate_lock:
        gw._busy_gate_until.clear()


def test_gate_arm_and_live(monkeypatch):
    list(_reset(monkeypatch))
    until = gw._busy_gate_arm("acct-A")
    assert until > time.time()
    assert gw._busy_gate_live("acct-A") is True
    assert gw._busy_gate_active() is True
    assert gw._busy_gate_next() == until


def test_gate_expiry_releases_one_probe(monkeypatch):
    """到期瞬间只放一个请求进上游，其余继续排队（无并发涌上）。"""
    list(_reset(monkeypatch))
    monkeypatch.setattr(settings, "BUSY_GATE_WAIT", 0.001)
    gw._busy_gate_arm("acct-A", 0.001)
    time.sleep(0.005)
    # 10 个并发请求同时问：恰好 1 个拿到试探名额，其余仍在闸内
    results = [gw._busy_gate_live("acct-A") for _ in range(10)]
    assert results.count(False) == 1, results
    assert results.count(True) == 9


def test_gate_clear_on_success(monkeypatch):
    list(_reset(monkeypatch))
    gw._busy_gate_arm("acct-A")
    gw._busy_gate_clear("acct-A")
    assert not gw._busy_gate_active()
    assert gw._busy_gate_live("acct-A") is False
    assert gw._busy_gate_next() == 0.0


def test_gate_recovery_returns_busy_with_retry_after(monkeypatch):
    """闸门成立时 _recovery 给出确定的恢复时刻（→ 429+Retry-After）。"""
    list(_reset(monkeypatch))
    import pathlib
    import tempfile

    from app.store import Store

    d = pathlib.Path(tempfile.mkdtemp())
    monkeypatch.setattr(settings, "DATA_DIR", d)
    monkeypatch.setattr(settings, "DB_PATH", d / "accounts.db")
    s = Store()
    s.add_account("zai", "acct-A", "hA.sig")
    monkeypatch.setattr(gw, "store", s)
    until = gw._busy_gate_arm("acct-A")
    kind, wait = gw._recovery("zai", "GLM-5.3-Flash")
    assert kind == "busy" and wait and wait == until


def test_gate_disabled_does_not_block(monkeypatch):
    list(_reset(monkeypatch))
    monkeypatch.setattr(settings, "BUSY_GATE_WAIT", 0)
    assert gw._busy_gate_arm("acct-A") == 0.0
    assert not gw._busy_gate_active()
    assert gw._busy_gate_live("acct-A") is False


def test_busy_waits_keep_official_cadence():
    """v2.9.32 不拉长重问窗口：主机制是排队闸，重问仍是官方 1s/2s。"""
    assert settings.BUSY_WAITS == (1, 2)
