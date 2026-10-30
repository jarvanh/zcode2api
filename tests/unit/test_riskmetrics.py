"""风控指标统计：事件落盘（按天 JSONL）与聚合（后台 /admin/api/risk-metrics 数据源）。"""

from __future__ import annotations

import json

from app import riskmetrics, settings


def test_record_and_aggregate(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    riskmetrics.record("risk", "zai-156", signal="HTTP 405", lane="plan:glm-5.3-flash", strikes=2)
    riskmetrics.record("risk", "zai-131", signal="业务码 3012", lane="plan:glm-5.3-flash", strikes=1)
    riskmetrics.record("rate", "zai-175", model="GLM-5.3-Flash")
    riskmetrics.record("net", "zai-156", err="All connection attempts failed")
    riskmetrics.record("wave", "zai-1698", provider="zai", pause=180, level=1)
    # 未知 kind 拒收
    riskmetrics.record("bogus", "x")

    day_files = list(tmp_path.glob("risk-*.jsonl"))
    assert len(day_files) == 1
    lines = [json.loads(l) for l in day_files[0].read_text().splitlines()]
    assert len(lines) == 5  # bogus 未落盘
    assert lines[0]["kind"] == "risk"
    assert lines[0]["signal"] == "HTTP 405"

    agg = riskmetrics.aggregate(days=7)
    today = agg["days"][-1]
    assert today["risk"] == 2
    assert today["rate"] == 1
    assert today["net"] == 1
    assert today["wave"] == 1
    assert today["risk_by_signal"] == {"HTTP 405": 1, "业务码 3012": 1}
    assert today["risk_net_by_account"]["zai-156"] == 2  # 1 风控 + 1 连接失败


def test_aggregate_days_clamped(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    assert len(riskmetrics.aggregate(days=999)["days"]) == riskmetrics.MAX_DAYS
    assert len(riskmetrics.aggregate(days=0)["days"]) == 1
