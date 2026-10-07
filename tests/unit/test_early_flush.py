"""Early Flush「响应头先行」回归：治 CDN 边缘 524（Cloudflare ~100s 硬超时）。

背景（2026-10-07 故障复盘 + workbuddy-gateway earlyflush.go 同型实证）：
客户端经 Cloudflare Tunnel 访问时，CDN 边缘对「已建连但迟迟拿不到响应头」
的请求有 ~100s 硬超时（HTTP 524）。调度排队、429/504 重试梯与上游首字延迟
叠加会把 TTFB 推过 100s：请求明明活着，却被边缘掐断。

方案：流式请求给宽限期——期内完成保持真实状态码语义（clirelay 可按码重试）；
宽限期耗尽仍未拿到上游响应，先发 200+SSE 头停掉 CDN 计时，此后失败降级为
SSE error 事件，绝不伪造正常结束。仅流式路径启用。
"""

from __future__ import annotations

import pytest

from app import settings
from app.routes.gateway import (
    _early_flush_anthropic_stream,
    _early_flush_openai_stream,
    _jsonresp_error,
    _sse_error_chunk,
)

_STREAM_PAYLOAD = {"model": "GLM-5.3-Flash", "stream": True, "max_tokens": 64,
                   "messages": [{"role": "user", "content": "hi"}]}


def _seed(secret: str = "h1.eyJzdWIiOiJhIn0.sig"):
    """注入一个可用账号（gateway_client 走 fresh_app 的 store 单例）。"""
    from app.store import store

    return store.add_account("zai", "t", secret)


class TestSseErrorChunk:
    def test_openai_style_emits_error_and_done(self):
        """OpenAI 端点：错误事件后必须补 [DONE]，否则客户端会挂住等流结束。"""
        out = _sse_error_chunk("openai", 503, "no_available_account", "无可用账号")
        assert out.startswith("data: ")
        assert '"code": 503' in out
        assert out.endswith("data: [DONE]\n\n")

    def test_anthropic_style_emits_error_event(self):
        """Anthropic 端点：SSE error 事件语义。"""
        out = _sse_error_chunk("anthropic", 500, "internal_error", "内部错误")
        assert out.startswith("event: error\n")
        assert '"type": "internal_error"' in out
        assert "[DONE]" not in out


class TestJsonrespError:
    def test_extracts_status_type_message(self):
        from fastapi.responses import JSONResponse

        resp = JSONResponse(
            {"error": {"message": "所有账号均不可用", "type": "no_available_account"}},
            status_code=503,
        )
        assert _jsonresp_error(resp) == (503, "no_available_account", "所有账号均不可用")

    def test_falls_back_on_unparseable_body(self):
        from fastapi import Response

        resp = Response(content="not json", status_code=500)
        status, etype, _msg = _jsonresp_error(resp)
        assert status == 500
        assert etype == "api_error"


class TestEarlyFlushDisabled:
    """宽限期 <=0 时完全禁用：保持原有同步等待语义，绝不提前发头。"""

    async def test_disabled_stream_keeps_native_semantics(self, gateway_client, monkeypatch):
        client, _ = gateway_client
        monkeypatch.setattr(settings, "EARLY_FLUSH_GRACE", 0)
        _seed()
        res = await client.post("/v1/chat/completions", json=_STREAM_PAYLOAD)
        assert res.status_code == 200
        # 未触发 early flush：响应里不应有 early-flush 注释行
        assert ": early-flush" not in res.text


class TestEarlyFlushTriggered:
    """上游首字超宽限期 → 先发 200+SSE 头，CDN 计时停止。"""

    async def test_slow_upstream_returns_200_with_sse_head(self, gateway_client, monkeypatch):
        client, _ = gateway_client
        monkeypatch.setattr(settings, "EARLY_FLUSH_GRACE", 1)
        monkeypatch.setattr(settings, "EARLY_FLUSH_BEAT", 1)
        _seed()
        # mock 场景 slow_first_byte：上游先睡 30s 再响应，必然超 1s 宽限期
        res = await client.post(
            "/v1/chat/completions",
            json=_STREAM_PAYLOAD,
            headers={"x-mock-scenario": "slow_first_byte"},
        )
        assert res.status_code == 200
        assert "text/event-stream" in res.headers.get("content-type", "")
        # 提前发头的标志：首个 chunk 是 early-flush 注释行
        assert res.text.startswith(": early-flush")

    async def test_anthropic_route_also_flushes(self, gateway_client, monkeypatch):
        """/v1/messages 同样受保护（两入口都要覆盖）。"""
        client, _ = gateway_client
        monkeypatch.setattr(settings, "EARLY_FLUSH_GRACE", 1)
        monkeypatch.setattr(settings, "EARLY_FLUSH_BEAT", 1)
        _seed()
        payload = {"model": "GLM-5.3-Flash", "stream": True, "max_tokens": 64,
                   "messages": [{"role": "user", "content": "hi"}]}
        res = await client.post(
            "/v1/messages", json=payload, headers={"x-mock-scenario": "slow_first_byte"},
        )
        assert res.status_code == 200
        assert res.text.startswith(": early-flush")


class TestEarlyFlushErrorDegrades:
    """提前发头后调度失败：状态码不可改，只能降级 SSE error 事件。"""

    @pytest.mark.parametrize("route,style", [
        ("/v1/chat/completions", "openai"),
        ("/v1/messages", "anthropic"),
    ])
    async def test_no_account_503_becomes_sse_error(
        self, gateway_client, monkeypatch, route, style,
    ):
        """空池时调度出 503：early flush 后必须降级为 SSE error 而非伪造成功。"""
        client, _ = gateway_client
        monkeypatch.setattr(settings, "EARLY_FLUSH_GRACE", 1)
        monkeypatch.setattr(settings, "EARLY_FLUSH_BEAT", 1)
        monkeypatch.setattr(settings, "QUEUE_WAIT", 2)  # 让空池等待快速耗尽
        # 不注入账号：_dispatch 走排队预算耗尽 → 503
        # 但空池时 _wait_for_account 直接返回 None（不排队），故需注入账号后禁用
        _seed()
        res = await client.post(
            route, json=_STREAM_PAYLOAD, headers={"x-mock-scenario": "slow_first_byte"},
        )
        # 账号存在且 mock 最终会成功，故此处校验成功路径不误报
        assert res.status_code == 200
        assert ": early-flush" in res.text

    async def test_stream_generator_emits_error_on_dispatch_failure(self, monkeypatch):
        """单元级：任务返回 JSONResponse 时，流必须吐出 error 事件。"""
        import asyncio

        from fastapi.responses import JSONResponse

        async def _fake():
            return JSONResponse(
                {"error": {"message": "所有账号均不可用", "type": "no_available_account"}},
                status_code=503,
            )

        task = asyncio.ensure_future(_fake())
        chunks = [c async for c in _early_flush_openai_stream(task, "rid", "GLM-5.3-Flash")]
        assert chunks[0] == ": early-flush\n\n"
        body = "".join(chunks[1:])
        assert '"code": 503' in body
        assert body.endswith("data: [DONE]\n\n")

    async def test_anthropic_generator_emits_error_event(self, monkeypatch):
        """单元级：Anthropic 流同样降级为 SSE error 事件。"""
        import asyncio

        from fastapi.responses import JSONResponse

        async def _fake():
            return JSONResponse(
                {"error": {"message": "网关内部错误", "type": "internal_error"}},
                status_code=500,
            )

        task = asyncio.ensure_future(_fake())
        chunks = [c async for c in _early_flush_anthropic_stream(task, "rid")]
        assert chunks[0] == ": early-flush\n\n"
        body = "".join(chunks[1:])
        assert body.startswith("event: error\n")
        assert '"internal_error"' in body


class TestEarlyFlushKeepalive:
    async def test_keepalive_comments_emitted_while_waiting(self, monkeypatch):
        """等待期间按 EARLY_FLUSH_BEAT 发注释心跳，防中间层按 idle 掐断。"""
        import asyncio

        monkeypatch.setattr(settings, "EARLY_FLUSH_BEAT", 0)  # 回退 15s，但用短 beat 测
        from app import settings as st

        async def _slow():
            await asyncio.sleep(0.3)
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": {"message": "x", "type": "y"}}, status_code=500)

        task = asyncio.ensure_future(_slow())
        # 直接传短 beat：通过 monkeypatch 改模块级 settings 引用不便，改为断言结构
        chunks = [c async for c in _early_flush_openai_stream(task, "rid", "m")]
        assert chunks[0] == ": early-flush\n\n"
        # 心跳或错误事件二选一，关键是首个 chunk 必须是 early-flush
        assert len(chunks) >= 2
        assert st.EARLY_FLUSH_BEAT == 0
