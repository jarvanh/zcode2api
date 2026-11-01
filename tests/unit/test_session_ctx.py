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
    assert k1 == (k2[0], False)  # 首见
    assert k2[1] is True         # 复见


def test_tag_writes_reqlog_row(monkeypatch, tmp_path):
    monkeypatch.setattr(reqlog.settings, "DATA_DIR", tmp_path)
    reqlog.begin("r1", "chat", "glm-5.3-flash", False, "hi")
    session_ctx.tag("r1", _body())
    reqlog.finish_ok("r1", status=200, cache_read=256)
    rows = [json.loads(l) for l in (tmp_path / "reqlog-1970-01-01.jsonl").read_text().splitlines()] \
        if False else json.loads(_last_row(tmp_path))
    assert rows["sess"] and rows["sess_repeat"] is False
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
        reqlog.tag_session(rid, f"s{i % 2}", repeat)
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
