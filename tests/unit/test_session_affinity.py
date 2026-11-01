"""会话亲和 enforce 档（app/session_ctx.py SESSION_AFFINITY）单元测试。

enforce 语义：tag() 把 (会话键，稳定 UUID) 挂 contextvar，build_trace_headers
经 current_session() 取用——同会话每请求同值，六件 attribution 形状不变。
observe 语义不受影响（默认开，绝不改请求行为）。
"""

from __future__ import annotations

import pytest

from app import settings, session_ctx


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(settings, "SESSION_AFFINITY", True)
    monkeypatch.setattr(settings, "SESSION_OBSERVE", True)
    session_ctx.reset_for_tests()
    yield
    session_ctx.reset_for_tests()


def _body(model="glm-5.3", content="hello"):
    return {"model": model, "system": "sys", "messages": [{"role": "user", "content": content}]}


def test_tag_sets_stable_session_id():
    session_ctx.tag("r1", _body())
    sid1 = session_ctx.current_session()
    assert sid1
    session_ctx.tag("r2", _body())  # 同会话第二次请求
    assert session_ctx.current_session() == sid1


def test_different_bodies_get_different_ids():
    session_ctx.tag("r1", _body(content="a"))
    session_ctx.tag("r2", _body(content="b"))
    assert session_ctx.current_session() != "x"  # 当前 context 是 r2 的
    assert session_ctx.current_session() is not None


def test_affinity_off_returns_none(monkeypatch):
    monkeypatch.setattr(settings, "SESSION_AFFINITY", False)
    session_ctx.tag("r1", _body())
    assert session_ctx.current_session() is None


def test_build_trace_headers_uses_stable_id():
    from app.identity import build_trace_headers

    session_ctx.tag("r1", _body())
    h1 = build_trace_headers()
    session_ctx.tag("r2", _body())
    h2 = build_trace_headers()
    assert h1["x-session-id"] == h2["x-session-id"]
    # 其余五件仍逐请求全新
    assert h1["x-request-id"] != h2["x-request-id"]
    assert h1["x-zcode-trace-id"] != h2["x-zcode-trace-id"]
    # 六件形状不变
    assert set(h1) == {"x-request-id", "x-zcode-session-type", "x-zcode-trace-id",
                       "x-query-id", "x-zcode-query-source", "x-session-id"}


def test_malformed_body_no_crash():
    # 不可序列化 content → session_key 返回 None → 不装载也不报错
    session_ctx.tag("r1", {"messages": [{"role": "user", "content": object()}]})
    assert session_ctx.current_session() is None
    # 可序列化但字段类型怪（model 非字符串）仍能生成键：正常装载不报错
    session_ctx.tag("r2", {"model": 123})
    assert session_ctx.current_session() is not None
