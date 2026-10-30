"""连接失败独立计数 + strikes 指数半衰（2026-10-30）。

- 连接失败与上游风控分开计数：请求未达上游、零风控代价，走独立
  net_strikes 短退避，绝不进 risk_strikes/lane 冷却。
- strikes 消退改为指数半衰（随时间连续消退），硬窗口仅作兜底。
"""

from __future__ import annotations

import time

import pytest

from app.models import Account, Status, settings


@pytest.fixture(autouse=True)
def _no_half_life(monkeypatch):
    """递进/封顶用例关掉半衰（时间增量≈0，避免浮点噪声）。"""
    monkeypatch.setattr(settings, "NET_STRIKE_HALF_LIFE", 0)
    monkeypatch.setattr(settings, "RISK_STRIKE_HALF_LIFE", 0)


def test_net_fail_penalty_escalates_and_caps():
    acc = Account.create("zai", "t", "hC.a.b")
    t1 = acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 1
    assert 59 <= t1 - time.time() <= 61          # 首次 60s
    t2 = acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 2
    assert 119 <= t2 - time.time() <= 121        # ×2
    for _ in range(10):
        acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 12
    t = acc.net_fail_penalty(60, 600, 12 * 3600)
    assert t - time.time() <= 601                # cap 封顶


def test_net_fail_penalty_decay_window_resets():
    acc = Account.create("zai", "t", "hC.a.b")
    acc.net_fail_penalty(60, 600, 300)
    acc.net_fail_penalty(60, 600, 300)
    assert acc.net_strikes == 2
    acc.net_last_at = time.time() - 301          # 跨硬窗口（兜底清零）
    acc.net_fail_penalty(60, 600, 300)
    assert acc.net_strikes == 1                  # 重新起算


def test_net_half_life_decay(monkeypatch):
    """半衰：距上次命中 1 个半衰期，存量减半后再 +1。"""
    monkeypatch.setattr(settings, "NET_STRIKE_HALF_LIFE", 100)
    acc = Account.create("zai", "t", "hC.a.b")
    acc.net_strikes = 4.0
    acc.net_last_at = time.time() - 100          # half_life=100 → 衰减一半
    acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 3.0                # 4×0.5 + 1
    # 且秒数曲线按衰减后的计分走：60×2^(3-1)=240
    until = acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 4.0
    assert 475 <= until - time.time() <= 485     # 60×2^(4-1)=480——按衰减后计分


def test_net_half_life_tail_forgotten(monkeypatch):
    """超过多个半衰期：尾数 <0.1 直接视为无历史，从 1 起算。"""
    monkeypatch.setattr(settings, "NET_STRIKE_HALF_LIFE", 100)
    acc = Account.create("zai", "t", "hC.a.b")
    acc.net_strikes = 9.0
    acc.net_last_at = time.time() - 3600         # half_life=100 → 2^-36 ≈ 0
    acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.net_strikes == 1.0


def test_net_fail_does_not_touch_risk_state():
    acc = Account.create("zai", "t", "hC.a.b")
    acc.risk_lane_strikes["plan:glm-5.3-flash"] = 3
    before = dict(acc.risk_lane_strikes)
    acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.risk_strikes == 0
    assert acc.risk_lane_strikes == before
    assert not acc.has_lane_cooling()
    assert acc.status == Status.ACTIVE           # 状态位不动（调用方负责）


def test_risk_lane_half_life_decay(monkeypatch):
    """lane 计分同样半衰：存量 2 过一个半衰期 → 1+1=2，秒数按 180×2^1。"""
    monkeypatch.setattr(settings, "RISK_STRIKE_HALF_LIFE", 1800)
    lane = "plan:glm-5.3-flash"
    acc = Account.create("zai", "t", "hC.a.b")
    acc.risk_lane_strikes[lane] = 2.0
    acc.risk_lane_last_at[lane] = time.time() - 1800  # half_life=1800 → 减半
    res = acc.risk_lane_penalty(lane, 180, 600, 4, 600, 12 * 3600)
    assert res["strikes"] == 2                   # 展示取整（1.0+1）
    assert acc.risk_lane_strikes[lane] == 2.0
    assert res["seconds"] == 360.0               # 180×2^(2-1)——按衰减后计分


def test_net_fields_round_trip():
    acc = Account.create("zai", "t", "hC.a.b")
    acc.net_fail_penalty(60, 600, 12 * 3600)
    data = acc.to_dict()
    assert data["net_strikes"] == 1
    clone = Account.from_dict(data)
    assert clone.net_strikes == 1
    assert clone.net_last_at == acc.net_last_at
