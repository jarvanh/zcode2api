"""reqlog 请求监控环形日志单元测试。"""

from __future__ import annotations

import time

import pytest

from app import reqlog, settings


@pytest.fixture(autouse=True)
def _clean(tmp_path, monkeypatch):
    """内存 deque 清零 + DATA_DIR 隔离到 tmp。

    两者缺一不可：只清内存不清 DATA_DIR 时，_persist() 会把测试行写进
    真实的 data/reqlog-<日期>.jsonl —— 2026-10-08 实测污染了 7163 行
    （model="m"、耗时微秒级、账号为空），监控页统计被虚高近 5 倍。
    """
    monkeypatch.setattr(settings, "DATA_DIR", tmp_path)
    reqlog.clear()
    yield
    reqlog.clear()


class TestRingBuffer:
    def test_begin_finish_ok_lifecycle(self):
        reqlog.begin("r1", "messages", "GLM-5.3-Flash", True, "你好")
        reqlog.mark_account("r1", "acc-1", "jwt")
        reqlog.finish_ok("r1", t_first=0.5, input_tokens=10, output_tokens=5)
        snap = reqlog.snapshot()
        assert len(snap) == 1
        e = snap[0]
        assert e["req_id"] == "r1"
        assert e["ok"] is True and e["status"] == 200
        assert e["account"] == "acc-1" and e["mode"] == "jwt"
        assert e["t_first"] == 0.5
        assert e["t_total"] is not None and e["t_total"] >= 0
        assert e["input_tokens"] == 10 and e["output_tokens"] == 5
        assert e["stream"] is True and e["endpoint"] == "messages"

    def test_inflight_entry_visible(self):
        reqlog.begin("r2", "chat", "GLM-5.3", False, "hi")
        snap = reqlog.snapshot()
        assert len(snap) == 1
        assert snap[0]["ok"] is None
        assert snap[0]["t_total"] is None

    def test_finish_error_truncates_message(self):
        reqlog.begin("r3", "messages", "GLM-5.3", False, "")
        reqlog.finish_error("r3", "x" * 500, status=400)
        e = reqlog.snapshot()[0]
        assert e["ok"] is False and e["status"] == 400
        assert len(e["error"]) == 200

    def test_finish_unknown_req_id_noop(self):
        reqlog.finish_ok("ghost")
        reqlog.finish_error("ghost", "x")
        assert reqlog.snapshot() == []

    def test_ring_buffer_cap(self):
        for i in range(reqlog.KEEP + 50):
            reqlog.begin(f"r{i}", "messages", "m", False, "")
            reqlog.finish_ok(f"r{i}")
        snap = reqlog.snapshot()
        assert len(snap) == reqlog.KEEP
        assert snap[0]["req_id"] == f"r{reqlog.KEEP + 49}"  # 最新在前

    def test_clear(self):
        reqlog.begin("r", "messages", "m", False, "")
        reqlog.clear()
        assert reqlog.snapshot() == []

    def test_mark_account_last_wins(self):
        reqlog.begin("r", "messages", "m", False, "")
        reqlog.mark_account("r", "a", "jwt")
        reqlog.mark_account("r", "b", "apiKey")
        reqlog.finish_ok("r")
        assert reqlog.snapshot()[0]["account"] == "b"

    def test_timestamps_monotonic(self):
        t0 = time.time()
        reqlog.begin("r", "messages", "m", False, "")
        reqlog.finish_ok("r")
        e = reqlog.snapshot()[0]
        assert t0 - 1 <= e["ts"] <= time.time() + 1
