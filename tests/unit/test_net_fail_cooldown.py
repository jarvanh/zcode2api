"""连接失败独立计数（2026-10-30）：网络层失败与上游风控分开退避。

连接失败发生在本地/出口链路，请求未达上游、零风控代价——不得进
risk_strikes/lane 冷却（旧版死节点把账号烧到 strikes=9 的教训）；走独立
net_strikes 短退避（base×2 递进、cap 封顶、衰减窗口重置），成功后清零。
"""

from __future__ import annotations

import time

from app.models import Account, Status


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
    acc.net_last_at = time.time() - 301          # 跨衰减窗口
    acc.net_fail_penalty(60, 600, 300)
    assert acc.net_strikes == 1                  # 重新起算


def test_net_fail_does_not_touch_risk_state():
    acc = Account.create("zai", "t", "hC.a.b")
    acc.risk_lane_strikes["plan:glm-5.3-flash"] = 3
    before = dict(acc.risk_lane_strikes)
    acc.net_fail_penalty(60, 600, 12 * 3600)
    assert acc.risk_strikes == 0
    assert acc.risk_lane_strikes == before
    assert not acc.has_lane_cooling()
    assert acc.status == Status.ACTIVE           # 状态位不动（调用方负责）


def test_net_fields_round_trip():
    acc = Account.create("zai", "t", "hC.a.b")
    acc.net_fail_penalty(60, 600, 12 * 3600)
    data = acc.to_dict()
    assert data["net_strikes"] == 1
    clone = Account.from_dict(data)
    assert clone.net_strikes == 1
    assert clone.net_last_at == acc.net_last_at
