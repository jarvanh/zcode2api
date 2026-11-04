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


class TestEnrichedFields:
    """2026-11-04 增补维度：来源 / 通道 / 尝试梯子 / 等待拆解 / 错误分类。"""

    def test_begin_accepts_source_metadata(self):
        reqlog.begin("r1", "messages", "GLM-5.3-Flash", True, "hi",
                     path="/v1/messages", client_ip="1.2.3.4", ua="claude-cli/1.0",
                     probe=True, msgs=12, max_tokens=4096, model_req="bigmodel/glm-5.3")
        e = reqlog.snapshot()[0]
        assert e["path"] == "/v1/messages"
        assert e["client_ip"] == "1.2.3.4" and e["ua"] == "claude-cli/1.0"
        assert e["probe"] is True
        assert e["msgs"] == 12 and e["max_tokens"] == 4096
        assert e["model_req"] == "bigmodel/glm-5.3"

    def test_begin_defaults_tolerate_missing_kwargs(self):
        """旧调用形态（不传新参数）必须照旧工作，新字段有中性默认值。"""
        reqlog.begin("r2", "chat", "m", False, "")
        e = reqlog.snapshot()[0]
        assert e["client_ip"] == "" and e["probe"] is False
        assert e["msgs"] is None and e["att"] == []
        assert e["wait_q"] == 0.0 and e["retry_429"] == 0
        assert e["channel"] == "" and e["err_kind"] == ""

    def test_clipping_guards_log_size(self):
        reqlog.begin("r3", "messages", "m", False, "",
                     ua="u" * 500, client_ip="9" * 200, path="p" * 200)
        e = reqlog.snapshot()[0]
        assert len(e["ua"]) == 96 and len(e["client_ip"]) == 64 and len(e["path"]) == 80

    def test_channel_egress_and_trace_marked(self):
        reqlog.begin("r4", "messages", "m", False, "")
        reqlog.mark_channel("r4", "plan")
        reqlog.mark_egress("r4", "proxy")
        reqlog.mark_upstream_meta("r4", "cf-ray=abc123")
        reqlog.mark_upstream_meta("r4", "")  # 空值不覆盖已有标识
        reqlog.finish_ok("r4")
        e = reqlog.snapshot()[0]
        assert e["channel"] == "plan" and e["egress"] == "proxy"
        assert e["upstream_id"] == "cf-ray=abc123"

    def test_attempt_ladder_records_each_try(self):
        reqlog.begin("r5", "messages", "m", False, "")
        reqlog.mark_attempt("r5", "zai-131", "jwt", channel="plan", status=429,
                            ms=1234.5, error="429 频控，换号优先")
        reqlog.mark_attempt("r5", "zai-175", "jwt", channel="apikey", status=200,
                            ms=88.0, error="")
        reqlog.finish_ok("r5")
        e = reqlog.snapshot()[0]
        assert len(e["att"]) == 2
        first, second = e["att"]
        assert first["acc"] == "zai-131" and first["st"] == 429 and first["ms"] == 1234.5
        assert first["err"] == "429 频控，换号优先"
        assert second["ch"] == "apikey" and "err" not in second

    def test_attempt_ladder_truncated(self):
        """梯子有上限：异常重试风暴不能把环形日志撑爆。"""
        reqlog.begin("r6", "messages", "m", False, "")
        for i in range(reqlog.MAX_ATTEMPTS + 10):
            reqlog.mark_attempt("r6", f"acc-{i}", "jwt")
        reqlog.finish_ok("r6")
        e = reqlog.snapshot()[0]
        assert len(e["att"]) == reqlog.MAX_ATTEMPTS
        assert e["att"][0]["acc"] == "acc-0"

    def test_wait_and_retry_accumulate(self):
        reqlog.begin("r7", "messages", "m", False, "")
        reqlog.note_wait("r7", "q", 12.0)
        reqlog.note_wait("r7", "q", 8.0)
        reqlog.note_wait("r7", "rl", 60.0)
        reqlog.note_wait("r7", "q", -5)      # 非法：忽略
        reqlog.note_wait("r7", "nope", 3)    # 未知类别：忽略
        reqlog.note_retry("r7", "429")
        reqlog.note_retry("r7", "429")
        reqlog.note_retry("r7", "bogus")     # 未知类别：忽略
        reqlog.finish_ok("r7")
        e = reqlog.snapshot()[0]
        assert e["wait_q"] == 20.0 and e["wait_rl"] == 60.0
        assert e["wait_5xx"] == 0.0 and e["retry_429"] == 2 and e["retry_5xx"] == 0

    def test_error_classification_and_code(self):
        reqlog.begin("r8", "messages", "m", False, "")
        reqlog.finish_error("r8", "上游业务错误 3012", status=403, code=3012, kind="risk")
        e = reqlog.snapshot()[0]
        assert e["err_code"] == 3012 and e["err_kind"] == "risk"

    def test_stop_reason_on_success(self):
        reqlog.begin("r9", "messages", "m", False, "")
        reqlog.finish_ok("r9", input_tokens=10, output_tokens=5, stop_reason="max_tokens")
        e = reqlog.snapshot()[0]
        assert e["stop_reason"] == "max_tokens"

    def test_marks_on_finished_entry_are_noops(self):
        """收口后的 req_id 不再属于在途：迟到标记必须静默丢弃（不得 KeyError）。"""
        reqlog.begin("r10", "messages", "m", False, "")
        reqlog.finish_ok("r10")
        reqlog.mark_attempt("r10", "late", "jwt")
        reqlog.note_wait("r10", "q", 5)
        reqlog.note_retry("r10", "429")
        reqlog.mark_channel("r10", "plan")
        reqlog.mark_egress("r10", "direct")
        reqlog.mark_upstream_meta("r10", "cf-ray=x")
        assert reqlog.snapshot()[0]["att"] == []


class TestEnrichedHistory:
    def test_new_dimensions_persisted(self, tmp_path):
        import json as _json
        from pathlib import Path

        reqlog.begin("h1", "messages", "GLM-5.3", False, "正文不入盘",
                     path="/v1/messages", client_ip="8.8.8.8", ua="curl/8", probe=True)
        reqlog.mark_account("h1", "zai-131", "jwt")
        reqlog.mark_channel("h1", "plan")
        reqlog.mark_egress("h1", "proxy")
        reqlog.mark_upstream("h1")
        for _ in range(2):
            reqlog.note_retry("h1", "429")
        reqlog.note_wait("h1", "q", 15.0)
        reqlog.mark_attempt("h1", "zai-175", "jwt", channel="plan", status=429, ms=90)
        reqlog.mark_upstream_meta("h1", "cf-ray=xyz")
        reqlog.finish_error("h1", "上游业务错误 3012", status=403, code=3012, kind="risk")

        row = _json.loads(next(Path(tmp_path).glob("reqlog-*.jsonl")).read_text().strip())
        assert row["ip"] == "8.8.8.8" and row["ua"] == "curl/8" and row["probe"] is True
        assert row["ch"] == "plan" and row["eg"] == "proxy"
        assert row["rt"] == 2 and row["q"] == 15.0
        assert row["att"] == 1  # 明细不落盘，只落条数
        assert row["kind"] == "risk" and row["code"] == 3012
        assert row["trace"] == "cf-ray=xyz"
        assert "正文不入盘" not in _json.dumps(row)  # preview 绝不进磁盘

    def test_history_groups_channel_and_client(self, tmp_path):
        # (账号, 客户端 IP, 通道, 探测, 是否成功)
        rows = [
            ("a1", "1.1.1.1", "plan", True, True),
            ("a2", "2.2.2.2", "apikey", False, False),
            ("a1", "1.1.1.1", "plan", False, True),
        ]
        for i, (acc, ip, ch, probe, ok) in enumerate(rows):
            rid = f"h{i}"
            reqlog.begin(rid, "messages", "m", False, "", client_ip=ip, probe=probe)
            reqlog.mark_account(rid, acc, "jwt")
            reqlog.mark_channel(rid, ch)
            if ok:
                reqlog.finish_ok(rid)
            else:
                reqlog.finish_error(rid, "boom", status=502)

        agg = reqlog.history(days=1)
        assert {c["name"] for c in agg["channels"]} >= {"plan", "apikey"}
        assert {c["name"] for c in agg["clients"]} >= {"1.1.1.1", "2.2.2.2"}
        plan_row = next(c for c in agg["channels"] if c["name"] == "plan")
        assert plan_row["calls"] == 2 and plan_row["ok"] == 2
        key_row = next(c for c in agg["channels"] if c["name"] == "apikey")
        assert key_row["fail"] == 1
        assert agg["totals"]["probe"] == 1

    def test_history_totals_expose_new_metrics(self, tmp_path):
        reqlog.begin("t1", "messages", "m", False, "")
        reqlog.note_wait("t1", "q", 30.0)
        reqlog.note_retry("t1", "5xx")
        reqlog.finish_error("t1", "x", status=503)
        agg = reqlog.history(days=1)
        assert agg["totals"]["wait"] == 30.0
        assert agg["totals"]["retry"] == 1
