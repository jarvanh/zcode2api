"""v2.9.38 实时观测层磁盘回放：重启后实时页仍可见重启前请求。"""

from __future__ import annotations

import json
import time

import pytest

from app import reqlog


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    reqlog._reset_for_tests()
    yield tmp_path
    reqlog._reset_for_tests()


def _row(ts: float, *, ok: bool = True, status: int = 200, account: str = "a"):
    return {
        "ts": ts, "ok": ok, "status": status, "kind": "" if ok else "rate",
        "code": None, "account": account, "mode": "jwt", "ch": "plan",
        "eg": "direct", "up": True, "model": "GLM-5.3-Flash",
        "endpoint": "chat", "path": "/v1/chat/completions",
        "ip": "127.0.0.1", "ua": "t", "probe": False, "stream": True,
        "msgs": 2, "mt": 64, "t_first": 0.5, "t_total": 1.2,
        "in": 10, "out": 5, "cache_read": 3,
        "sess": "abc", "sess_repeat": True,
        "att": 2, "rt": 1, "q": 0.3, "rl": 0.0, "5xx": 0.0, "cap": 0.0,
        "trace": "cf-ray-x", "sw": "in", "stop": "end_turn",
        "up_t": 1.0, "dd": "", "err": "" if ok else "rate limited",
    }


def test_restore_last_500_persisted(isolated, monkeypatch):
    """只回最近 KEEP 条终态；snapshot 最新在前；在途条目不落盘也不回放。"""
    from datetime import datetime, timedelta, timezone
    bj = timezone(timedelta(hours=8))
    today = datetime.now(bj).strftime("%Y-%m-%d")
    now = time.time()
    rows = [_row(now - 700 + i, account=f"a{i}") for i in range(700)]
    with open(isolated / f"reqlog-{today}.jsonl", "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    # 半行写入不应拖垮回放
    with open(isolated / f"reqlog-{today}.jsonl", "a", encoding="utf-8") as f:
        f.write('{"ts": broken\n')
    n = reqlog.restore_recent_from_disk()
    snap = reqlog.snapshot()
    assert n == reqlog.KEEP == 500
    assert len(snap) == reqlog.KEEP
    assert snap[0]["account"] == "a699"      # 最新在前
    assert snap[-1]["account"] == "a200"     # 恰好截掉前 200 条
    assert all(e["ok"] is not None for e in snap)  # 无幽灵在途行


def test_restore_cross_midnight(isolated):
    """跨北京日界：今天 + 昨天两个文件合并倒序取最新。"""
    from datetime import datetime, timedelta, timezone
    bj = timezone(timedelta(hours=8))
    now = datetime.now(bj)
    today = now.strftime("%Y-%m-%d")
    yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    t = time.time()
    with open(isolated / f"reqlog-{yday}.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps(_row(t - 9000, account="yesterday")) + "\n")
    with open(isolated / f"reqlog-{today}.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps(_row(t - 10, account="today")) + "\n")
    assert reqlog.restore_recent_from_disk() == 2
    snap = reqlog.snapshot()
    assert [e["account"] for e in snap] == ["today", "yesterday"]


def test_restore_best_effort_on_missing_file(isolated):
    """无历史文件：静默返回 0，不抛异常（冷启动）。"""
    assert reqlog.restore_recent_from_disk() == 0
    assert reqlog.snapshot() == []


def test_restored_entry_shape(isolated):
    """回放条目具备实时页详情所需字段；尝试梯子是聚合形态标记。"""
    from datetime import datetime, timedelta, timezone
    bj = timezone(timedelta(hours=8))
    today = datetime.now(bj).strftime("%Y-%m-%d")
    with open(isolated / f"reqlog-{today}.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps(_row(time.time() - 1)) + "\n")
    reqlog.restore_recent_from_disk()
    e = reqlog.snapshot()[0]
    for key in ("req_id", "ts", "model", "account", "channel", "egress",
                "status", "error", "err_kind", "sess", "sess_window",
                "input_tokens", "output_tokens", "cache_read", "upstream_id",
                "attempts" if False else "att"):
        assert key in e, key
    assert e["req_id"].startswith("h-")
    assert e["att"] == [{"restored": True}, {"restored": True}]
    assert e["att_restored_total"] == 1
    assert e["sess"] == "abc" and e["sess_window"] == "in"
    assert e["wait_q"] == 0.3 and e["cache_read"] == 3
