"""OpenAI Chat Completions 兼容层 —— /v1/chat/completions ↔ Anthropic Messages 双向转换。

入站：OpenAI 请求体 → Anthropic messages 体（system/developer 提取为 system 参数、
content 分块、tool_calls / tool_result / 图片(data URL) best-effort 映射）。
出站：Anthropic 响应（JSON 或 SSE 事件流）→ OpenAI 格式。

与 body_transform 同一原则：对畸形输入保持宽容，映射不了的部件安静跳过，
绝不放大请求失败。
"""

from __future__ import annotations

import json
import time
import uuid

# Anthropic stop_reason → OpenAI finish_reason
_STOP_REASON_MAP = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
}


class UpstreamStreamError(Exception):
    """上游在 SSE 流内下发的 error 事件（overloaded_error / api_error 等）。

    200 已发出后上游无法再改 HTTP 状态，只会在流里塞 `{"type":"error",...}`。
    这类错误必须显式向上抛，由 HTTP 层转成协议对应的错误事件——静默丢弃会让
    客户端把半截回答当成功（2.7.33 修复）。
    """

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code or "upstream_error"


def error_sse(message: str, code: str | None = None) -> str:
    """OpenAI 兼容的流式错误行（紧跟 `data: [DONE]` 之前输出）。"""
    payload = {"error": {"message": message, "type": "upstream_stream_error",
                         "code": code or "upstream_error"}}
    return _sse(payload)


def _as_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _cache_usage(usage: dict) -> dict:
    """提取缓存 token（读/写）。

    上游 Anthropic 口径：`input_tokens` **不含**缓存部分，命中量在
    `cache_read_input_tokens`、写入量在 `cache_creation_input_tokens`。
    部分实现（GLM 部分通道）改放 `prompt_tokens_details.cached_tokens` /
    `cached_tokens`，按优先级兼容。
    """
    details = usage.get("prompt_tokens_details")
    if not isinstance(details, dict):
        details = {}
    cached = _as_int(usage.get("cache_read_input_tokens"))
    if cached is None:
        cached = _as_int(details.get("cached_tokens"))
    if cached is None:
        cached = _as_int(usage.get("cached_tokens"))
    creation = _as_int(usage.get("cache_creation_input_tokens"))
    if creation is None:
        creation = _as_int(details.get("cache_creation_tokens"))
    return {"cached": cached or 0, "creation": creation or 0}


def _cache_usage_fields(cache: dict) -> dict:
    """把缓存量写成 OpenAI 口径的全部别名字段。

    下游（OmniRoute 等）读命中数用的是 `??` 链：
      cacheRead ?? cached_tokens ?? cache_read_input_tokens
        ?? prompt_tokens_details.cached_tokens ?? 0
    JS 的 `??` 只在 null/undefined 时继续，**0 会被当成有效值立即返回**。
    因此这里只在真实值 > 0 时才写别名（真 miss 保持缺省，由下游哨 0 处理），
    绝不凭猜测伪造命中。
    """
    out: dict = {}
    cached, creation = cache.get("cached") or 0, cache.get("creation") or 0
    if cached > 0:
        out["cached_tokens"] = cached
        out["cache_read_input_tokens"] = cached
        out["prompt_tokens_details"] = {"cached_tokens": cached}
    if creation > 0:
        out["cache_creation_input_tokens"] = creation
        details = out.setdefault("prompt_tokens_details", {})
        if isinstance(details, dict):
            details["cache_creation_tokens"] = creation
    return out


def _openai_usage(usage: dict, input_tokens: int, output_tokens: int) -> dict:
    """Anthropic usage → OpenAI usage。

    OpenAI 的 `prompt_tokens` **包含**命中与写入的缓存 token，而 Anthropic 的
    `input_tokens` 只算未命中部分；直接透传会把输入量算少（实测同一前缀：
    input=46699+cache=896 与 input=43+cache=47552 的真实输入恒为 47595）。
    """
    cache = _cache_usage(usage)
    prompt = input_tokens + cache["cached"] + cache["creation"]
    result = {
        "prompt_tokens": prompt,
        "completion_tokens": output_tokens,
        "total_tokens": prompt + output_tokens,
    }
    result.update(_cache_usage_fields(cache))
    return result


def _text_from_content(content: object) -> str:
    """OpenAI content（str | 分块数组）→ 纯文本。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(p for p in parts if p)
    return ""


def _image_block(url: str) -> dict | None:
    """data:image/...;base64,xxx → Anthropic image block；外链 URL 无法回填，跳过。"""
    if not url.startswith("data:"):
        return None
    head, _, b64 = url.partition(",")
    media_type = head[5:].split(";")[0] or "image/png"
    if not b64:
        return None
    return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}}


def _blocks_from_user_content(content: object) -> list[dict]:
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else [{"type": "text", "text": ""}]
    if not isinstance(content, list):
        return [{"type": "text", "text": ""}]
    blocks: list[dict] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text" and isinstance(item.get("text"), str):
            blocks.append({"type": "text", "text": item["text"]})
        elif item.get("type") == "image_url":
            url = (item.get("image_url") or {})
            if isinstance(url, dict):
                url = url.get("url") or ""
            block = _image_block(str(url))
            if block:
                blocks.append(block)
    return blocks or [{"type": "text", "text": ""}]


def openai_to_anthropic(payload: dict) -> tuple[dict | None, str | None]:
    """OpenAI 请求体 → Anthropic messages 体。非法时返回 (None, 错误信息)。"""
    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        return None, "必须提供 model 参数"
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        return None, "必须提供 messages 数组"

    system_parts: list[str] = []
    out_msgs: list[dict] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        role = msg.get("role")
        content = msg.get("content")
        if role in ("system", "developer"):
            text = _text_from_content(content)
            if text:
                system_parts.append(text)
        elif role == "tool":
            out_msgs.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": str(msg.get("tool_call_id") or ""),
                    "content": _text_from_content(content),
                }],
            })
        elif role == "assistant":
            blocks: list[dict] = []
            text = _text_from_content(content)
            if text:
                blocks.append({"type": "text", "text": text})
            for tc in msg.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") or {}
                if not isinstance(fn, dict):
                    continue
                args = fn.get("arguments")
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except ValueError:
                        args = {"_raw": args}
                blocks.append({
                    "type": "tool_use",
                    "id": str(tc.get("id") or ""),
                    "name": str(fn.get("name") or ""),
                    "input": args if isinstance(args, dict) else {},
                })
            out_msgs.append({
                "role": "assistant",
                "content": blocks or [{"type": "text", "text": ""}],
            })
        else:  # user 及未知角色一律按 user 处理
            out_msgs.append({"role": "user", "content": _blocks_from_user_content(content)})

    # 结构化输出：anthropic wire 没有 response_format，用 system 指令逼近。
    # 只对显式要求 JSON 的调用生效；不改变普通对话的提示词。
    fmt = payload.get("response_format")
    if isinstance(fmt, dict):
        ftype = fmt.get("type")
        if ftype == "json_object":
            system_parts.append("Respond with a single valid JSON object and nothing else.")
        elif ftype == "json_schema":
            js = fmt.get("json_schema")
            schema = js.get("schema") if isinstance(js, dict) else None
            directive = "Respond with a single JSON value conforming to this JSON Schema and nothing else."
            if isinstance(schema, dict):
                directive += " Schema: " + json.dumps(schema, ensure_ascii=False)
            system_parts.append(directive)

    body: dict = {
        "model": model,
        "messages": out_msgs,
        "max_tokens": _as_int(payload.get("max_tokens") or payload.get("max_completion_tokens")) or 4096,
    }
    if isinstance(payload.get("metadata"), dict):
        body["metadata"] = dict(payload["metadata"])
    for sid_key in ("session_id", "conversation_id"):
        if sid_key in payload:
            body[sid_key] = payload[sid_key]
    if system_parts:
        body["system"] = "\n\n".join(system_parts)

    # 思考强度：OpenAI `reasoning_effort` / Responses `reasoning.effort`
    # → anthropic wire 的 `output_config.effort`（官方客户端同形态）。
    effort = payload.get("reasoning_effort")
    if not isinstance(effort, str):
        reasoning = payload.get("reasoning")
        effort = reasoning.get("effort") if isinstance(reasoning, dict) else None
    if isinstance(effort, str) and effort.lower() in ("low", "medium", "high", "max"):
        body["output_config"] = {"effort": effort.lower()}
    try:
        if payload.get("temperature") is not None:
            body["temperature"] = float(payload["temperature"])
        if payload.get("top_p") is not None:
            body["top_p"] = float(payload["top_p"])
    except (TypeError, ValueError):
        pass
    stop = payload.get("stop")
    if isinstance(stop, str) and stop:
        body["stop_sequences"] = [stop]
    elif isinstance(stop, list) and stop:
        body["stop_sequences"] = [str(s) for s in stop]
    if payload.get("stream"):
        body["stream"] = True

    tools = payload.get("tools")
    if isinstance(tools, list) and tools:
        mapped = []
        for t in tools:
            fn = t.get("function") if isinstance(t, dict) else None
            if isinstance(fn, dict) and fn.get("name"):
                mapped.append({
                    "name": str(fn["name"]),
                    "description": str(fn.get("description") or ""),
                    "input_schema": fn.get("parameters") if isinstance(fn.get("parameters"), dict) else {"type": "object"},
                })
        if mapped:
            body["tools"] = mapped

    choice = payload.get("tool_choice")
    if choice == "none":
        body.pop("tools", None)
    elif choice == "required":
        body["tool_choice"] = {"type": "any"}
    elif isinstance(choice, dict) and choice.get("type") == "function":
        name = str((choice.get("function") or {}).get("name") or "")
        if name:
            body["tool_choice"] = {"type": "tool", "name": name}
    # "auto"/缺省：Anthropic 默认即 auto，无需显式映射
    from . import reasoning

    reasoning.apply_reasoning(body, payload)
    return body, None


def anthropic_to_openai(data: dict, model: str) -> dict:
    """Anthropic message 响应 → OpenAI chat.completion。"""
    text_parts: list[str] = []
    tool_calls: list[dict] = []
    for block in data.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            text_parts.append(block["text"])
        elif block.get("type") == "tool_use":
            tool_calls.append({
                "id": str(block.get("id") or ""),
                "type": "function",
                "function": {
                    "name": str(block.get("name") or ""),
                    "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                },
            })
    usage = data.get("usage") or {}
    in_tok = _as_int(usage.get("input_tokens")) or 0
    out_tok = _as_int(usage.get("output_tokens")) or 0
    message: dict = {"role": "assistant", "content": "".join(text_parts) or None}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {
        "id": str(data.get("id") or f"chatcmpl-{uuid.uuid4().hex[:24]}"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": str(data.get("model") or model),
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": _STOP_REASON_MAP.get(data.get("stop_reason"), "stop"),
        }],
        "usage": _openai_usage(usage, in_tok, out_tok),
    }


class StreamConverter:
    """Anthropic SSE 事件流 → OpenAI chat.completion.chunk 流（有状态转换器）。

    用法：先 start() 产出 role 首 chunk，逐条 feed(event_dict) 收输出行，
    流结束后追加 done()（"data: [DONE]"）。

    `feed_events` / `start_event` 返回 **chunk dict**：`/v1/responses` 需要把
    chunk 再翻译成 Responses 事件，不能只拿到已经序列化的 SSE 行。
    """

    def __init__(self, model: str) -> None:
        self.model = model
        self.chunk_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
        self.created = int(time.time())
        self.finish_reason: str | None = None
        # None = 上游未上报（未知），0 = 真实为零；两者语义不同
        self.usage = {"prompt_tokens": None, "completion_tokens": None, "total_tokens": None}
        # 上游 Anthropic 口径的未命中输入量；对外 prompt_tokens 需叠加缓存量。
        self._input_raw: int | None = None
        # 缓存读写量；部分上游在 message_delta 才下发，逐次采用正值。
        self.cache = {"cached": 0, "creation": 0}
        self._tool_seq = 0

    def _adopt_cache(self, usage: dict) -> None:
        """采用上游下发的缓存量。只接受正值：已拿到的命中数不被后续 0 抹掉。"""
        cache = _cache_usage(usage)
        if cache["cached"] > 0:
            self.cache["cached"] = cache["cached"]
        if cache["creation"] > 0:
            self.cache["creation"] = cache["creation"]

    def _sync_usage_total(self) -> None:
        """按 OpenAI 口径重算 prompt/total：prompt_tokens 含缓存读写量。

        同时把缓存别名回写进 self.usage：网关自身 reqlog 用
        `_cache_read(conv.usage)` 读命中量，不能只让对外副本带上字段，
        否则账上（OmniRoute）有数、自家流水仍是 None。
        """
        cached = self.cache["cached"] + self.cache["creation"]
        completion = self.usage["completion_tokens"]
        if self._input_raw is not None:
            self.usage["prompt_tokens"] = self._input_raw + cached
        elif cached:
            self.usage["prompt_tokens"] = cached
        prompt = self.usage["prompt_tokens"]
        self.usage["total_tokens"] = (prompt or 0) + (completion or 0) if prompt is not None or completion is not None else None
        self.usage.update(_cache_usage_fields(self.cache))

    def usage_out(self) -> dict:
        """对外 usage：基础三字段 + 缓存全别名（下游 ?? 链需要）。"""
        out = dict(self.usage)
        out.update(_cache_usage_fields(self.cache))
        return out

    def start_event(self) -> dict:
        return self._chunk_event({"role": "assistant", "content": ""})

    def start(self) -> str:
        return _sse(self.start_event())

    def done(self) -> str:
        return "data: [DONE]\n\n"

    def feed(self, evt: dict) -> list[str]:
        return [_sse(c) for c in self.feed_events(evt)]

    def feed_events(self, evt: dict) -> list[dict]:
        etype = evt.get("type")
        if etype == "error":
            err = evt.get("error")
            if isinstance(err, dict):
                raise UpstreamStreamError(
                    str(err.get("message") or err.get("type") or "上游流式错误"),
                    str(err.get("type") or "") or None,
                )
            raise UpstreamStreamError(str(err or "上游流式错误"))
        if etype == "message_start":
            msg = evt.get("message") or {}
            if msg.get("id"):
                self.chunk_id = str(msg["id"])
            u = msg.get("usage") or {}
            prompt = _as_int(u.get("input_tokens"))
            if prompt is not None:
                self._input_raw = prompt
            self._adopt_cache(u)
            self._sync_usage_total()
            return []
        if etype == "content_block_start":
            block = evt.get("content_block") or {}
            if block.get("type") == "tool_use":
                idx = self._tool_seq
                self._tool_seq += 1
                return [self._chunk_event({"tool_calls": [{
                    "index": idx,
                    "id": str(block.get("id") or ""),
                    "type": "function",
                    "function": {"name": str(block.get("name") or ""), "arguments": ""},
                }]})]
            return []
        if etype == "content_block_delta":
            delta = evt.get("delta") or {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
                return [self._chunk_event({"content": delta["text"]})]
            if delta.get("type") == "input_json_delta" and isinstance(delta.get("partial_json"), str):
                return [self._chunk_event({"tool_calls": [{
                    "index": max(self._tool_seq - 1, 0),
                    "function": {"arguments": delta["partial_json"]},
                }]})]
            return []
        if etype == "message_delta":
            delta = evt.get("delta") or {}
            self.finish_reason = _STOP_REASON_MAP.get(delta.get("stop_reason"), "stop")
            u = evt.get("usage") or {}
            out = _as_int(u.get("output_tokens"))
            if out is not None:
                self.usage["completion_tokens"] = out
            # 部分上游（GLM 流式实测）在 message_start 给 input_tokens=0 或缺失，
            # 真正的输入量随 message_delta 一起下发；不补采则 prompt_tokens 恒为 0。
            prompt = _as_int(u.get("input_tokens"))
            if prompt is not None and (self._input_raw is None or (self._input_raw == 0 and prompt > 0)):
                self._input_raw = prompt
            self._adopt_cache(u)
            self._sync_usage_total()
            return [self._chunk_event({}, finish_reason=self.finish_reason)]
        # 兼容未带 type 名的上游错误事件（部分中转只发 {"error": {...}}）
        if isinstance(evt.get("error"), dict):
            err = evt["error"]
            raise UpstreamStreamError(
                str(err.get("message") or err.get("type") or "上游流式错误"),
                str(err.get("type") or "") or None,
            )
        return []  # content_block_stop / message_stop / ping 等无需产出

    def _chunk_event(self, delta: dict, finish_reason: str | None = None) -> dict:
        payload: dict = {
            "id": self.chunk_id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }
        if finish_reason is not None:
            payload["usage"] = self.usage_out()
        return payload


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
