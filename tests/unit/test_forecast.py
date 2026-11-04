"""额度节奏外推单测 —— 重点是「证据不足时诚实拒绝」，不是凑一个 ETA。"""

from __future__ import annotations

import json
import time

import pytest

from app import forecast, quota_windows


class _Acc:
    def __init__(self, name, status="active", quota=None, verified=None, plans=None, plan=None):
        self.name = name
        self.status = status
        self.quota = quota or {}
        self.quota_verified_at = verified
        self.plans = plans or []
        self.plan = plan or {}


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(forecast.settings, "DATA_DIR", tmp_path)
    yield


class TestQuotaWindows:
    def test_bj_today_at_rolls_to_next_day(self):
        # 北京 23:30 → 次日 23:00
        now = time.time()
        ts = quota_windows.bj_today_at(23, 0, now)
        assert ts > now and ts - now <= 86400 + 1

    def test_pending_plan_handles_millis(self):
        now = time.time()
        acc = _Acc("a", plans=[{"entitlements": [{"starts_at": (now + 3600) * 1000}]}])
        v = quota_windows.pending_plan_start([acc], now)
        assert v is not None and abs(v - (now + 3600)) < 2

    def test_quota_reset_takes_earlier_plan(self):
        now = time.time()
        acc = _Acc("a", plans=[{"entitlements": [{"starts_at": now + 600}]}])
        assert quota_windows.quota_reset_at([acc], now) < quota_windows.bj_today_at(23, 0, now)
        assert abs(quota_windows.quota_reset_at([acc], now) - (now + 600)) < 2


class TestForecastHonesty:
    def test_no_snapshots_gives_no_eta(self):
        a = _Acc("zai-1", quota={"m": {"used": 10, "remaining": 100}},
                 verified=time.time())
        f = forecast.forecast([a])
        assert f["eta"] is None and f["eta_reliable"] is False
        assert f["reasons"] and "至少 2 次" in " ".join(f["reasons"])
        assert f["next_refill"]["kind"] == "day_reset"

    def test_contradictory_ledger_refuses_eta(self, tmp_path):
        """exhausted 但 remaining>0 = 两套账矛盾，即便有速率也必须拒绝外推。"""
        now = time.time()
        a = _Acc("zai-1", status="exhausted",
                 quota={"m": {"used": 100, "remaining": 999999}}, verified=now)
        # 手工埋两帧快照
        f = forecast._data_dir() / f"quota-history-{forecast._day_str()}.jsonl"
        f.write_text("\n".join([
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 600,
                        "used": 0, "rem": 1000, "status": "active"}),
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 60,
                        "used": 100, "rem": 999999, "status": "exhausted"})]) + "\n")
        out = forecast.forecast([a])
        assert out["eta"] is None and out["eta_reliable"] is False
        assert any("账本不一致" in r for r in out["reasons"])
        assert any(r["rate"] for r in out["rates"]["per_account"])

    def test_rate_from_two_snapshots(self, tmp_path):
        now = time.time()
        a = _Acc("zai-1", quota={"m": {"used": 200, "remaining": 800}}, verified=now)
        f = forecast._data_dir() / f"quota-history-{forecast._day_str()}.jsonl"
        f.write_text("\n".join([
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 300, "used": 0,
                        "rem": 1000, "status": "active"}),
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 60, "used": 200,
                        "rem": 800, "status": "active"})]) + "\n")
        out = forecast.forecast([a])
        r = next(x for x in out["rates"]["per_account"] if x["account"] == "zai-1")
        assert r["rate"] == round(200 / 240, 2)
        assert out["eta_reliable"] is True and out["eta"] is not None

    def test_stale_snapshot_no_eta(self, tmp_path):
        now = time.time()
        a = _Acc("zai-1", status="active", quota={"m": {"used": 200, "remaining": 800}},
                 verified=now)
        f = forecast._data_dir() / f"quota-history-{forecast._day_str()}.jsonl"
        f.write_text("\n".join([
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 7200, "used": 0,
                        "rem": 1000, "status": "active"}),
            json.dumps({"acc": "zai-1", "model": "m", "v": now - 3600, "used": 200,
                        "rem": 800, "status": "active"})]) + "\n")
        out = forecast.forecast([a])
        assert out["eta"] is None
        assert any("15 分钟" in r for r in out["reasons"])

    def test_observe_dedups_same_verified_at(self, tmp_path):
        now = time.time()
        a = _Acc("zai-1", quota={"m": {"used": 1, "remaining": 9}}, verified=now)
        assert forecast.observe([a]) == 1
        assert forecast.observe([a]) == 0      # 同一次刷新不重复记
        f = forecast._data_dir() / f"quota-history-{forecast._day_str()}.jsonl"
        assert len(f.read_text().strip().splitlines()) == 1

    def test_observe_skips_unverified(self, tmp_path):
        assert forecast.observe([_Acc("a", quota={"m": {"used": 1}}, verified=None)]) == 0


class TestGatewayUsesSharedWindows:
    """网关的 _quota_reset_at / _bj_today_at 必须转发共享真源，不许各算一套。"""

    def test_gateway_reset_matches_shared(self):
        from app import quota_windows
        from app.routes import gateway

        now = time.time()
        acc = _Acc("a", plans=[{"entitlements": [{"starts_at": now + 600}]}])
        assert gateway._quota_reset_at([acc], now) == quota_windows.quota_reset_at([acc], now)
        assert gateway._bj_today_at(23, 0, now) == quota_windows.bj_today_at(23, 0, now)
