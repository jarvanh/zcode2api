"""v2.9.34 early-flush 心跳改真 SSE 事件（OmniRoute readiness 放行）。

背景：OmniRoute stream readiness 只认「event:/data: + 空行」的完整事件，
注释行（":" 开头）被 s() 直接跳过 —— 纯注释心跳流在它那里是零事件流，
80s/100s 判 502/504 并按 free combo 回退 hy4free（近12h 350 条）。
"""

from __future__ import annotations

import json

from app.routes.gateway import _sse_error_chunk, _sse_ping_chunk


def test_anthropic_ping_is_real_event():
    chunk = _sse_ping_chunk("anthropic")
    assert chunk.startswith("event: ping\n")
    assert 'data: {"type": "ping"}' in chunk
    assert chunk.endswith("\n\n")


def test_openai_ping_is_standard_empty_delta():
    chunk = _sse_ping_chunk("openai")
    assert chunk.startswith("data: ")
    assert chunk.endswith("\n\n")
    body = json.loads(chunk[len("data: "):].strip())
    assert body["choices"][0]["delta"] == {}
    assert body["choices"][0]["finish_reason"] is None
    # 不能带内容：客户端不得把心跳当正文
    assert "content" not in body["choices"][0]["delta"]


def test_error_chunk_still_real_event():
    """错误事件本身也是完整 data 事件（readiness 同样放行，只是带错误）。"""
    chunk = _sse_error_chunk("anthropic", 429, "rate_limit_error", "x")
    assert chunk.startswith("event: error\n")
    assert chunk.endswith("\n\n")
    chunk2 = _sse_error_chunk("openai", 503, "api_error", "x")
    assert chunk2.startswith("data: ")
    assert "[DONE]" in chunk2
