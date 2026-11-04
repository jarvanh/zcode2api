"""值守告警台账单测 —— 按天聚合、dry-run 不落盘、失败不影响发送。"""

from __future__ import annotations

import pytest

from app import alertlog


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(alertlog.settings, "DATA_DIR", tmp_path)
    yield


def test_dry_run_never_writes(tmp_path):
    out = alertlog.record(["RISK", "POOL"], dry_run=True)
    assert out["recorded"] is False
    assert not list(tmp_path.glob("alertlog-*.jsonl"))


def test_record_and_history(tmp_path):
    assert alertlog.record(["RISK", "UP5XX", "RISK"])["recorded"] is True
    assert alertlog.record(["POOL"])["recorded"] is True
    h = alertlog.history(days=1)
    assert h["total"] == 2
    assert h["daily"][-1]["alerts"] == 2
    ids = {r["id"]: r["count"] for r in h["rules"]}
    assert ids == {"RISK": 1, "UP5XX": 1, "POOL": 1}
    assert h["silent_seconds"] is not None and h["silent_seconds"] >= 0


def test_rules_sorted_and_deduped(tmp_path):
    row = alertlog.record(["POOL", "RISK", "POOL"])
    assert row["rules"] == ["POOL", "RISK"] and row["n"] == 2


def test_empty_history_shape(tmp_path):
    h = alertlog.history(days=7)
    assert h["total"] == 0 and len(h["daily"]) == 7
    assert h["last_alert_at"] is None and h["silent_seconds"] is None
    assert h["rules"] == []


def test_days_clamped(tmp_path):
    assert len(alertlog.history(days=999)["daily"]) == alertlog._HISTORY_DAYS
    assert len(alertlog.history(days=0)["daily"]) == 1
