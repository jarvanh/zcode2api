"""会话亲和观测（session_ctx observe 档）—— 键稳定性 + reqlog 接线 + 聚合。

锁四件事：同一会话键稳定（model+system+首条 user）、24h 复见判定、
reqlog 行落 sess/sess_repeat/cache_read、summary 的首见/复见均值聚合。
observe-only：任何失败都不影响请求（tag 吞异常由 try 包裹）。
"""

from __future__ import annotations

import json

import pytest

from app import reqlog, session_ctx


@pytest.fixture(autouse=True)
def _clean():
    session_ctx.reset_for_tests()
    yield
    session_ctx.reset_for_tests()


def _body(user="hi", model="glm-5.3-flash", system="sys"):
    return {"model": model, "stream": False,
            "system": [{"type": "text", "text": system}],
            "messages": [{"role": "user", "content": user}]}


def test_session_key_stable_within_conversation():
    """同会话（同 system + 首条 user）稳定同键；后续消息追加不影响。"""
    a = session_ctx.session_key(_body())
    longer = _body()
    longer["messages"].append({"role": "assistant", "content": "ok"})
    longer["messages"].append({"role": "user", "content": "第二条"})
    assert session_ctx.session_key(longer) == a
    assert session_ctx.session_key(_body(user="different")) != a
    assert session_ctx.session_key({"messages": 12345}) is None  # 畸形 no-op


def test_observe_repeat_detection():
    k1 = session_ctx.observe(_body())
    k2 = session_ctx.observe(_body())
    assert k1[1] is False and k1[2] == ""   # 首见：无上一请求可比，窗口留空
    assert k2[0] == k1[0] and k2[1] is True  # 复见
    assert k2[2] == "in"                      # 间隔秒级 → 窗口内


def test_observe_window_breaks_after_idle(monkeypatch):
    """≥5min 间隔：窗口判 out（缓存必然重建）——这是 cache_read 上不去的解释变量。"""
    key_body = _body()
    session_ctx.observe(key_body)
    # 把 last_seen 推回窗口之外
    with session_ctx._lock:
        k = session_ctx.session_key(key_body)
        session_ctx._seen[k] = _mono_now() - session_ctx._IDLE_WINDOW - 1
    obs = session_ctx.observe(key_body)
    assert obs[1] is True and obs[2] == "out"


def _mono_now() -> float:
    import time
    return time.monotonic()


def test_tag_stream_leaves_window_empty(monkeypatch, tmp_path):
    """流式不参与窗口判定：sess/sess_repeat 照写，sw 必须留空（判 out 是假阴性）。"""
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    reqlog.begin("st1", "chat", "glm-5.3-flash", True, "hi")
    session_ctx.tag("st1", _body(), stream=True)
    reqlog.finish_ok("st1", status=200)
    row = json.loads(_last_row(tmp_path))
    assert row["sess"] and row["sw"] == ""


def test_tag_writes_reqlog_row(monkeypatch, tmp_path):
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    reqlog.begin("r1", "chat", "glm-5.3-flash", False, "hi")
    session_ctx.tag("r1", _body(), False)
    reqlog.finish_ok("r1", status=200, cache_read=256)
    rows = [json.loads(l) for l in (tmp_path / "reqlog-1970-01-01.jsonl").read_text().splitlines()] \
        if False else json.loads(_last_row(tmp_path))
    assert rows["sess"] and rows["sess_repeat"] is False
    assert rows["sw"] == ""            # 首见无窗口归属
    assert rows["cache_read"] == 256


def _last_row(tmp_path) -> str:
    import glob
    files = sorted(glob.glob(str(tmp_path / "reqlog-*.jsonl")))
    with open(files[-1]) as f:
        return f.read().strip().splitlines()[-1]


def test_summary_aggregates_first_vs_repeat(monkeypatch, tmp_path):
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    monkeypatch.setattr(session_ctx.settings, "DATA_DIR", tmp_path)
    for i, (rid, repeat, cr) in enumerate([
        ("a1", False, 0),      # 首见无缓存命中
        ("a2", True, 500),     # 复见命中 500
        ("a3", True, 700),     # 复见命中 700
    ]):
        reqlog.begin(rid, "chat", "glm-5.3-flash", False, "x")
        reqlog.tag_session(rid, f"s{i % 2}", repeat, "in" if repeat else "")
        reqlog.finish_ok(rid, status=200, cache_read=cr)
    st = session_ctx.summary(days=1)
    assert st["requests"] == 3
    assert st["cache_read_first_avg"] == 0
    assert st["cache_read_repeat_avg"] == 600
    assert st["first_samples"] == 1 and st["repeat_samples"] == 2


def test_summary_empty_days_is_none_friendly(monkeypatch, tmp_path):
    monkeypatch.setattr(session_ctx.settings, "DATA_DIR", tmp_path)
    st = session_ctx.summary(days=1)
    assert st["requests"] == 0 and st["cache_read_first_avg"] is None


def test_summary_splits_window_in_out(monkeypatch, tmp_path):
    """三档拆分：窗口内复见才是 enforce 的判读口径；窗口外只用来解释整体不涨。"""
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    monkeypatch.setattr(session_ctx.settings, "DATA_DIR", tmp_path)
    rows = [
        ("s1", False, "", 0),      # 首见
        ("s2", False, "", 20),     # 首见
        ("s3", True, "in", 900),   # 窗口内复见：缓存真复用
        ("s4", True, "in", 1100),
        ("s5", True, "out", 30),   # 窗口外复见：节奏太散，缓存已重建
    ]
    for rid, rep, w, cr in rows:
        reqlog.begin(rid, "chat", "glm-5.3-flash", False, "x")
        reqlog.tag_session(rid, "k1" if rep else f"uniq-{rid}", rep, w)
        reqlog.finish_ok(rid, status=200, cache_read=cr)
    st = session_ctx.summary(days=1)
    assert st["cache_read_first_avg"] == 10
    assert st["cache_read_repeat_in_avg"] == 1000
    assert st["cache_read_repeat_out_avg"] == 30
    assert st["window_in"] == 2 and st["window_out"] == 1
    assert st["window_in_rate"] == 66.7
    # 旧字段保持原义（混档 repeat 均值），向后兼容
    assert st["cache_read_repeat_avg"] == round((900 + 1100 + 30) / 3, 1)


def test_summary_unknown_window_not_counted_as_in(monkeypatch, tmp_path):
    """旧数据无 sw：不得塞进 in 档（会把均值往「enforce 有效」方向偏）。"""
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    monkeypatch.setattr(session_ctx.settings, "DATA_DIR", tmp_path)
    reqlog.begin("o1", "chat", "m", False, "x")
    reqlog.tag_session("o1", "k", True, "")          # 复见但窗口未知
    reqlog.finish_ok("o1", status=200, cache_read=999)
    st = session_ctx.summary(days=1)
    assert st["repeat_samples"] == 1
    assert st["repeat_in_samples"] == 0 and st["repeat_out_samples"] == 0
    assert st["cache_read_repeat_in_avg"] is None
