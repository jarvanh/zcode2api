"""reqlog 请求监控环形日志单元测试。"""

from __future__ import annotations

import json as _json
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
    reqlog._reset_for_tests()
    yield
    reqlog._reset_for_tests()


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


class TestConcurrencyMeter:
    """实时并发四口径 + 事件驱动峰值 + 分钟曲线（2026-11-04 第二批）。"""

    def _reset(self, monkeypatch):
        # client 口径读 len(_inflight)：必须连在途字典一起清空，
        # 否则前面用例遗留的未收口条目会让读数虚高
        monkeypatch.setattr(reqlog, "_inflight", {})
        monkeypatch.setattr(reqlog, "_queue_now", 0)
        monkeypatch.setattr(reqlog, "_conc", {"client": 0, "slot": 0, "queue": 0,
                                              "limit": 0, "per": {}})
        monkeypatch.setattr(reqlog, "_conc_peak",
                            {"client": 0, "slot": 0, "queue": 0, "at": 0.0, "kind": ""})
        monkeypatch.setattr(reqlog, "_conc_min", {})
        monkeypatch.setattr(reqlog, "_conc_day", "")
        reqlog.register_slot_source(None)

    def test_client_peak_tracks_inflight(self, monkeypatch):
        self._reset(monkeypatch)
        reqlog.begin("c1", "messages", "m", False, "")
        reqlog.begin("c2", "messages", "m", False, "")
        snap = reqlog.conc_snapshot()
        assert snap["now"]["client"] == 2
        assert snap["peak"]["client"] == 2 and snap["peak"]["kind"] == "client"
        reqlog.finish_ok("c1")
        snap = reqlog.conc_snapshot()
        assert snap["now"]["client"] == 1
        # 峰值不因回落而变小（事件驱动精确采样，不靠轮询撞）
        assert snap["peak"]["client"] == 2

    def test_queue_delta_never_negative(self, monkeypatch):
        self._reset(monkeypatch)
        reqlog.note_queue(1)
        reqlog.note_queue(1)
        assert reqlog.conc_snapshot()["now"]["queue"] == 2
        reqlog.note_queue(-5)
        snap = reqlog.conc_snapshot()
        assert snap["now"]["queue"] == 0
        assert snap["peak"]["queue"] == 2  # 峰值保留

    def test_slot_source_injected(self, monkeypatch):
        self._reset(monkeypatch)
        reqlog.register_slot_source(lambda: {"total": 3, "per": {"zai-131": 2, "zai-176": 1},
                                             "limit": 2})
        reqlog.sample()
        snap = reqlog.conc_snapshot()
        assert snap["now"]["slot"] == 3 and snap["now"]["limit"] == 2
        assert snap["now"]["per"] == {"zai-131": 2, "zai-176": 1}
        assert snap["peak"]["slot"] == 3

    def test_broken_slot_source_is_ignored(self, monkeypatch):
        """仪表读失败绝不影响请求与心跳（观测不反压）。"""
        self._reset(monkeypatch)

        def _boom():
            raise RuntimeError("store 炸了")
        reqlog.register_slot_source(_boom)
        reqlog.sample()  # 不得抛出
        assert reqlog.conc_snapshot()["now"]["slot"] == 0

    def test_minute_buckets_and_persist(self, monkeypatch):
        self._reset(monkeypatch)
        reqlog.begin("m1", "messages", "m", False, "")
        reqlog.sample()
        reqlog.sample()
        conc = reqlog.conc_snapshot()
        assert conc["minutes"], "分钟桶未生成"
        last = conc["minutes"][-1]
        assert last["c"] == 1 and last["n"] >= 2  # begin + 2 次 sample

        # 跨天刷盘：把 day 改成旧值，下一次采样触发上一天落盘
        from pathlib import Path

        reqlog._conc_day = "1999-01-01"
        reqlog.sample()
        files = list(Path(settings.DATA_DIR).glob("conc-*.jsonl"))
        assert files, "并发曲线未落盘"
        row = _json.loads(files[0].read_text().strip().splitlines()[0])
        assert {"t", "c", "s", "q", "n"} <= set(row)

    def test_conc_history_reads_disk(self, tmp_path, monkeypatch):
        self._reset(monkeypatch)
        reqlog.begin("h1", "messages", "m", False, "")
        reqlog.sample()
        reqlog.flush_conc()
        agg = reqlog.conc_history(days=1)
        assert agg["peak"]["client"] >= 1
        assert agg["daily"][-1]["name"] == agg["to"] if "to" in agg else True
        assert any(d["samples"] >= 1 for d in agg["daily"])

    def test_start_sampler_without_loop(self):
        """无 running loop 时返回 None 而不是抛（create_app 单测路径）。"""
        assert reqlog.start_sampler() is None

    def test_minute_rollover_keeps_curve_and_writes_once(self, monkeypatch, tmp_path):
        """跨分钟：面板曲线要保留已结束的分钟（不能只留当前一分钟），
        但磁盘每分钟只写一次（水位线去重，否则 jsonl 会被心跳刷爆）。"""
        from pathlib import Path

        self._reset(monkeypatch)
        clock = {"m": 600}
        monkeypatch.setattr(reqlog, "_bj_minute",
                            lambda ts=None: ("2026-11-04", clock["m"]))

        reqlog.begin("w1", "messages", "m", False, "")
        reqlog.sample()                 # 分钟 600 采样
        clock["m"] = 601
        reqlog.sample()                 # 600 结束 → 落盘一次；曲线应含 600+601
        clock["m"] = 602
        reqlog.sample()                 # 再次心跳：600 不得重复写

        minutes = [b["t"] for b in reqlog.conc_snapshot()["minutes"]]
        assert minutes == [600, 601, 602], f"面板曲线丢失已结束分钟: {minutes}"
        rows = [line for line in
                next(Path(tmp_path).glob("conc-*.jsonl")).read_text().splitlines() if line]
        ts = [_json.loads(r)["t"] for r in rows]
        assert ts.count(600) == 1, f"分钟重复落盘: {ts}"


class TestDedupAndLatencySplit:
    """去重标记 + 净上游耗时：把「网关自己等出来的」与「上游慢」分开。"""

    def test_dedup_flag_persisted(self, tmp_path):
        from pathlib import Path

        reqlog.begin("d1", "messages", "m", False, "")
        reqlog.mark_dedup("d1", "replayed")
        reqlog.finish_error("d1", "风暴去重重放", status=429, kind="dedup")
        row = _json.loads(next(Path(tmp_path).glob("reqlog-*.jsonl")).read_text().strip().splitlines()[-1])
        assert row["dd"] == "replayed" and row["kind"] == "dedup"

    def test_net_up_excludes_waits(self):
        reqlog.begin("d2", "messages", "m", False, "")
        reqlog.note_wait("d2", "rl", 30.0)
        reqlog.finish_ok("d2")
        e = reqlog.snapshot()[0]
        assert e["t_up"] is not None
        # 总耗时是微秒级（测试内），等 30s 后净耗时应被夹到 0 而不是负数
        assert e["t_up"] == 0.0

    def test_hist_aggregates_codes_stops_and_dedup(self, tmp_path):
        reqlog.begin("g1", "messages", "m", False, "")
        reqlog.mark_dedup("g1", "merged")
        reqlog.finish_error("g1", "429", status=429, code=3009, kind="rate")
        reqlog.begin("g2", "messages", "m", False, "")
        reqlog.finish_ok("g2", status=200, stop_reason="max_tokens")
        reqlog.begin("g3", "chat", "m", False, "")
        reqlog.finish_error("g3", "客户端断开", status=499)

        agg = reqlog.history(days=1)
        t = agg["totals"]
        codes = {c["code"]: c["count"] for c in t["codes"]}
        assert codes.get("429/3009") == 1      # 业务码并进直方图键
        assert codes.get("499") == 1
        stops = {s["reason"]: s["count"] for s in t["stops"]}
        assert stops.get("max_tokens") == 1
        assert t["dedup"] == 1 and t["disc"] == 1
        assert t["disc_rate"] == round(1 / 3 * 100, 1)
        assert "conc" in agg                     # 历史响应带并发曲线



class TestAccountHealth:
    """近端健康窗口：P95 要分钟级新鲜度，磁盘按天聚合不够。"""

    def test_per_account_buckets(self):
        for acc, ok in [("zai-131", True), ("zai-131", False), ("zai-176", True)]:
            rid = f"h-{acc}-{ok}"
            reqlog.begin(rid, "messages", "m", False, "")
            reqlog.mark_account(rid, acc, "jwt")
            if ok:
                reqlog.finish_ok(rid, t_first=0.5)
            else:
                reqlog.finish_error(rid, "429", status=429, t_first=0.2)
        h = reqlog.health(hours=1)
        rows = {a["account"]: a for a in h["accounts"]}
        assert rows["zai-131"]["calls"] == 2
        assert rows["zai-131"]["ok"] == 1 and rows["zai-131"]["fail"] == 1
        assert rows["zai-131"]["c429"] == 1
        assert rows["zai-131"]["ttfb_p50"] is not None
        assert rows["zai-176"]["calls"] == 1

    def test_risk_and_quota_kinds_counted(self):
        reqlog.begin("k1", "messages", "m", False, "")
        reqlog.mark_account("k1", "zai-175", "jwt")
        reqlog.finish_error("k1", "风控", status=405, kind="risk")
        reqlog.begin("k2", "messages", "m", False, "")
        reqlog.mark_account("k2", "zai-175", "jwt")
        reqlog.finish_error("k2", "额度", status=402, kind="quota")
        rows = {a["account"]: a for a in reqlog.health()["accounts"]}
        assert rows["zai-175"]["risk"] == 1 and rows["zai-175"]["quota"] == 1

    def test_inflight_not_counted(self):
        """未收口请求不进健康窗口（ok 未知，混进去会压歪成功率）。"""
        reqlog.begin("u1", "messages", "m", False, "")
        reqlog.mark_account("u1", "zai-131", "jwt")
        assert reqlog.health()["events"] == 0

    def test_window_boundary_respects_hours(self, monkeypatch):
        reqlog.begin("w1", "messages", "m", False, "")
        reqlog.finish_ok("w1")
        with reqlog._lock:
            reqlog._recent[-1]["ts"] -= 7200        # 假装两小时前
        assert reqlog.health(hours=1)["events"] == 0
        assert reqlog.health(hours=3)["events"] == 1

    def test_p95_not_pinned_to_max_on_small_sample(self):
        reqlog.begin("p1", "m", "m", False, "")
        reqlog.finish_ok("p1", t_first=0.1)
        a = reqlog.health()["accounts"][0]
        assert a["ttfb_p50"] == 0.1 and a["ttfb_p95"] == 0.1

    def test_recent_counts_for_watchdog(self):
        reqlog.begin("r1", "m", "m", False, "")
        reqlog.finish_error("r1", "风控", status=405, kind="risk")
        reqlog.begin("r2", "m", "m", False, "")
        reqlog.finish_error("r2", "5xx", status=502)
        reqlog.begin("r3", "m", "m", False, "")
        reqlog.finish_error("r3", "429", status=429)
        c = reqlog.recent_counts(minutes=10)
        assert c["risk"] == 1 and c["c5xx"] == 1 and c["c429"] == 1
        assert c["total"] == 3 and c["window_min"] == 10

    def test_outcome_stats_breaks_down_503(self):
        reqlog.begin("o1", "m", "m", False, "")
        reqlog.finish_ok("o1")
        reqlog.begin("o2", "m", "m", False, "")
        reqlog.finish_error("o2", "冷却", status=503, kind="channel_cooling")
        reqlog.begin("o3", "m", "m", False, "")
        reqlog.finish_error("o3", "无号", status=503, kind="no_available_account")
        s = reqlog.outcome_stats(minutes=10)
        assert s["total"] == 3 and s["ok"] == 1 and s["fail"] == 2
        assert s["http_503"] == 2
        assert s["http_503_by_kind"]["channel_cooling"] == 1
        assert s["http_503_by_kind"]["no_available_account"] == 1
        assert s["fail_by_kind"]["channel_cooling"] == 1
        assert s["first_success"] == 1
        assert s["first_success_rate"] == 33.33

    def test_reset_clears_recent(self):
        reqlog.begin("z1", "m", "m", False, "")
        reqlog.finish_ok("z1")
        reqlog._reset_for_tests()
        assert reqlog.health()["events"] == 0


class TestTrendAggregates:
    """历史聚合新增：唯一客户端数 / 会话数 / 缓存命中量。"""

    def test_clients_and_sessions_unique(self):
        for i, (ip, sess) in enumerate([("1.1.1.1", "s1"), ("1.1.1.1", "s1"),
                                        ("2.2.2.2", "s2")]):
            rid = f"c{i}"
            reqlog.begin(rid, "messages", "m", False, "", client_ip=ip)
            reqlog.tag_session(rid, sess, repeat=bool(i))
            reqlog.finish_ok(rid)
        agg = reqlog.history(days=1)
        assert agg["totals"]["clients_n"] == 2
        assert agg["totals"]["sess_n"] == 2

    def test_cache_rate(self):
        reqlog.begin("x1", "messages", "m", False, "")
        reqlog.finish_ok("x1", input_tokens=1000, cache_read=250)
        t = reqlog.history(days=1)["totals"]
        assert t["cache"] == 250 and t["cache_rate"] == 25.0

    def test_cache_rate_none_when_input_unknown(self):
        reqlog.begin("x2", "messages", "m", False, "")
        reqlog.finish_ok("x2", cache_read=100)     # 无 input_tokens：分母未知
        t = reqlog.history(days=1)["totals"]
        assert t["cache"] == 100 and t["cache_rate"] is None

    def test_daily_rows_carry_trend_fields(self):
        reqlog.begin("d1", "messages", "m", False, "", client_ip="9.9.9.9")
        reqlog.finish_ok("d1", input_tokens=100, cache_read=10)
        day = reqlog.history(days=1)["daily"][-1]
        assert day["clients_n"] == 1 and day["cache"] == 10

    def test_group_rows_expose_cache(self):
        reqlog.begin("g1", "messages", "glm", False, "")
        reqlog.mark_account("g1", "acc", "jwt")
        reqlog.finish_ok("g1", input_tokens=200, cache_read=50)
        models = reqlog.history(days=1)["models"]
        row = next(r for r in models if r["name"] == "glm")
        assert row["cache"] == 50 and row["cache_rate"] == 25.0


class TestWindowAndShapeAggregates:
    """5min idle 窗口占比 + 请求形状分位（截断率要对照 max_tokens 才读得懂）。"""

    def test_window_in_out_rates(self):
        for rid, w in [("w1", "in"), ("w2", "in"), ("w3", "out"), ("w4", "")]:
            reqlog.begin(rid, "messages", "m", False, "")
            reqlog.tag_session(rid, "sess-x", repeat=bool(w), window=w)
            reqlog.finish_ok(rid)
        t = reqlog.history(days=1)["totals"]
        assert t["sw_in"] == 2 and t["sw_out"] == 1
        assert t["sw_in_rate"] == round(2 / 3 * 100, 1)   # 流式（空）不进分母

    def test_window_rate_none_when_only_stream(self):
        reqlog.begin("s1", "messages", "m", True, "")
        reqlog.tag_session("s1", "k", False, "")
        reqlog.finish_ok("s1")
        assert reqlog.history(days=1)["totals"]["sw_in_rate"] is None

    def test_shape_percentiles(self):
        for i, (msgs, mt) in enumerate([(1, 100), (5, 4096), (9, 8192), (2, 200)]):
            rid = f"sh{i}"
            reqlog.begin(rid, "messages", "m", False, "", msgs=msgs, max_tokens=mt)
            reqlog.finish_ok(rid)
        t = reqlog.history(days=1)["totals"]
        # _pctile 用降序取整下标（与 ttfb 同口径）：4 样本 p95 → idx 2 = 第二大
        assert t["msgs_p50"] == 2 and t["msgs_p95"] == 5
        assert t["maxtok_p50"] == 200 and t["maxtok_p95"] == 4096

    def test_shape_none_when_unknown(self):
        """旧历史行没有形状字段：分位必须是 None，不能假装 0。"""
        reqlog.begin("u1", "messages", "m", False, "")
        reqlog.finish_ok("u1")
        t = reqlog.history(days=1)["totals"]
        assert t["msgs_p50"] is None and t["maxtok_p95"] is None

    def test_window_persisted_to_disk(self, tmp_path):
        from pathlib import Path

        reqlog.begin("p1", "messages", "m", False, "")
        reqlog.tag_session("p1", "k1", True, "in")
        reqlog.finish_ok("p1")
        row = _json.loads(next(Path(tmp_path).glob("reqlog-*.jsonl")).read_text().strip())
        assert row["sw"] == "in"
