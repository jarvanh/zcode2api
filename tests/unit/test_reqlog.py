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


class TestUpstreamMark:
    """up 标记：区分「调度失败·未达上游」与「上游失败·账号归属真实」。

    背景（2026-11-01 排查）：503 排队放弃行的 account 是重试梯子最后一次
    尝试的号，从未真正发出请求 —— 监控行张冠李戴误读为该号被打挂。
    mark_upstream() 在真实发起上游调用时置位，up=false 的失败行账号名仅供参考。
    """

    def test_default_up_false(self):
        reqlog.begin("r1", "messages", "m", True, "")
        reqlog.mark_account("r1", "zai-131", "jwt")
        reqlog.finish_error("r1", "无可用账号（已排队 140s）", status=503)
        e = reqlog.snapshot()[0]
        assert e["up"] is False
        assert e["account"] == "zai-131" and e["ok"] is False

    def test_mark_upstream_sets_true_once(self):
        reqlog.begin("r2", "messages", "m", False, "")
        assert reqlog.snapshot()[0]["up"] is False
        reqlog.mark_upstream("r2")
        reqlog.mark_upstream("r2")  # 重试多次置位幂等
        reqlog.finish_ok("r2")
        e = reqlog.snapshot()[0]
        assert e["up"] is True

    def test_upstream_fail_keeps_up_true(self):
        """真打到上游吃了 402 再调度放弃：up 必须是 True（账号归属真实）。"""
        reqlog.begin("r3", "messages", "m", False, "")
        reqlog.mark_account("r3", "zai-175", "jwt")
        reqlog.mark_upstream("r3")
        reqlog.finish_error("r3", "无可用账号", status=503)
        e = reqlog.snapshot()[0]
        assert e["up"] is True and e["ok"] is False and e["status"] == 503

    def test_up_persisted_to_history(self, tmp_path):
        from pathlib import Path

        reqlog.begin("r4", "messages", "m", False, "")
        reqlog.mark_upstream("r4")
        reqlog.finish_ok("r4")
        files = list(Path(tmp_path).glob("reqlog-*.jsonl"))
        assert files, "磁盘历史未落盘"
        import json as _json

        row = _json.loads(files[0].read_text().strip())
        assert row["up"] is True

        reqlog.begin("r5", "messages", "m", False, "")
        reqlog.finish_error("r5", "调度放弃", status=503)
        row2 = _json.loads(files[0].read_text().strip().splitlines()[-1])
        assert row2["up"] is False
