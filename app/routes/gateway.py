"""核心网关：/v1/messages（Anthropic 风格）与 /v1/chat/completions（OpenAI 风格）。

共用多账号轮询 + 额度用完自动换号 + 阿里无痕验证自动续期；OpenAI 端点由
openai_compat 做双向格式转换，调度与错误处理策略完全一致。
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import threading
import time
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .. import constants, logs, reqlog, settings
from ..agent import build_request
from ..auth_admin import verify_gateway_key
from ..captcha import captcha_manager
from ..models import Account, Status
from ..openai_compat import StreamConverter, anthropic_to_openai, openai_to_anthropic
from ..quota import fetch_quota, refresh_accounts
from ..store import store

_sleep = asyncio.sleep  # 模块级引用：测试可 patch 此名而免污染全局 asyncio

router = APIRouter()

# ── X-Probe 探测打标 ──────────────────────────────────────────────
# 客户端在探测请求上带 X-Probe: 1（如模型能力测试、健康检查），网关把
# 每请求用量明细落盘，供 quota-board 做「探测 / 真实」用量分流。
# 用 ContextVar 传递标记：避免为打标而改动 _dispatch / _Upstream 等一整条
# 调用链的签名，且天然隔离并发请求。
_probe_flag: ContextVar[bool] = ContextVar("probe_flag", default=False)
_usage_lock = threading.Lock()
_BJ = timezone(timedelta(hours=8))


def is_probe_request(headers) -> bool:
    """判定探测请求：X-Probe 为 1/true/yes（大小写不敏感）即为探测。"""
    v = ""
    try:
        v = headers.get("x-probe") or ""
    except Exception:  # noqa: BLE001 - 头部解析失败按非探测处理
        return False
    return str(v).strip().lower() in {"1", "true", "yes"}


def record_usage(model: str, tin=None, tout=None, credit=None) -> None:
    """落盘单条用量明细（best-effort：失败只记日志，绝不影响主流程）。

    文件：data/usage-<北京日期>.jsonl
    """
    try:
        day = datetime.now(_BJ).strftime("%Y-%m-%d")
        path = os.path.join(settings.DATA_DIR, f"usage-{day}.jsonl")
        row = {
            "t": int(time.time()),
            "model": str(model or "-"),
            "probe": bool(_probe_flag.get()),
            "in": tin,
            "out": tout,
            "credit": credit,
        }
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        with _usage_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as err:  # noqa: BLE001 - 记账失败不能影响调度
        try:
            logs.warn("usage", f"用量明细落盘失败（不影响请求）: {err}")
        except Exception:
            pass

MAX_CAPTCHA_RETRIES = 3
MAX_ACCOUNT_ATTEMPTS = 5

# 常量收口：模型表与被拒信号关键字统一在 app/constants.py
MODEL_NAME_MAP = constants.MODEL_NAME_MAP
AVAILABLE_MODELS = constants.AVAILABLE_MODELS
_EXHAUST_KEYWORDS = constants.EXHAUST_KEYWORDS


def _detect_provider(body: dict, headers) -> str:
    model = body.get("model") or ""
    if model.startswith("bigmodel/") or headers.get("x-provider") == "bigmodel":
        return "bigmodel"
    return "zai"


def _normalize_body(body: dict) -> dict:
    model = body.get("model")
    if isinstance(model, str) and "/" in model:
        model = "/".join(model.split("/")[1:])
    if isinstance(model, str):
        model = MODEL_NAME_MAP.get(model.lower(), model)
        body["model"] = model

    # 上游对 max_tokens 有硬校验（400 code 1210），钳制到合法区间并记录钳制动作
    raw = body.get("max_tokens")
    if raw is not None and not isinstance(raw, bool):
        try:
            mt = int(float(raw))
        except (TypeError, ValueError):
            mt = None
        if mt is not None:
            clamped = max(1, min(mt, constants.MAX_TOKENS_LIMIT))
            if clamped != mt:
                logs.warn("gateway", f"max_tokens {mt} 超出上游范围 [1,{constants.MAX_TOKENS_LIMIT}]，钳制为 {clamped}")
            body["max_tokens"] = clamped

    messages = body.get("messages")
    if isinstance(messages, list):
        bridged = []
        for msg in messages:
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                bridged.append({**msg, "content": [{"type": "text", "text": msg["content"]}]})
            else:
                bridged.append(msg)
        body["messages"] = bridged
    return body


def _is_captcha_error(text: str) -> bool:
    low = text.lower()
    return "captcha" in low or "verify token" in low or "verify failed" in low


def _detect_captcha_challenge(resp: httpx.Response, text: str | None = None) -> str | None:
    """验证码挑战双检测（对齐 zapi handler.ts）。

    三种形态：
      1. 响应头 x-aliyun-captcha-verify-param 存在（官方挑战信号）
      2. HTTP 400/403 + body {"code":3007}（2026-08 观测的 body 内挑战）
      3. HTTP 403 + 文案 captcha/verify（老检测，保留兼容）
    返回挑战标记（非 None 即挑战），否则 None。
    """
    # 1) challenge 响应头
    header_val = resp.headers.get(constants.CAPTCHA_HEADER)
    if header_val and header_val.strip():
        return "header"

    if text is None:
        return None
    low = text.lower()

    # 2) body code 3007（400/403 任意状态）
    if resp.status_code in (400, 403) and any(m in text for m in constants.CAPTCHA_BODY_MARKERS):
        return "in-body-3007"

    # 3) 403 + 挑战文案
    if resp.status_code == 403 and _is_captcha_error(low):
        return "text"

    return None


def _is_exhausted(status_code: int, text: str) -> bool:
    # 429 是频控信号，优先于一切 body 关键词：429 body 带额度文案时
    # （api.z.ai 实测形态）必须走频控重试，不得判成额度耗尽踢号。
    if status_code == 429:
        return False
    if status_code in constants.EXHAUST_HTTP_STATUSES:
        return True
    low = text.lower()
    return any(k in low for k in _EXHAUST_KEYWORDS)


def _rate_break(account) -> int:
    """429 重试梯耗尽 → 连续计数；达阈值即熔断冷却（指数退避，到期自动回轮询）。

    返回本次冷却秒数（0 = 未达阈值，仅计数）。INVALID/DISABLED 不熔断
    （前者已走回退、后者是人工停用）。任何成功请求都会清零计数并解除。
    """
    account.rate_strikes += 1
    if account.rate_strikes < settings.RATE_BREAK_THRESHOLD:
        return 0
    if account.status in (Status.INVALID, Status.DISABLED):
        return 0
    exp = account.rate_strikes - settings.RATE_BREAK_THRESHOLD
    # 小池短档：可服务账号少（<= RATE_SMALL_POOL）时用短冷却 —— 最后一个
    # 账号被熔断不应意味着服务中断 15 分钟级；60s 起步翻倍、封顶 10 分钟。
    pool = sum(1 for a in store.list_accounts(account.provider)
               if a.enabled and a.status in (Status.ACTIVE, Status.COOLING))
    if pool <= settings.RATE_SMALL_POOL:
        cool = min(settings.RATE_COOL_SMALL_BASE * (2 ** exp), settings.RATE_COOL_SMALL_MAX)
    else:
        cool = min(settings.RATE_COOL_BASE * (2 ** exp), settings.RATE_COOL_MAX)
    account.status = Status.COOLING
    account.cooling_until = time.time() + cool
    account.last_error = (
        f"持续 429 自动熔断 {cool // 60} 分钟"
        f"（连续 {account.rate_strikes} 轮重试梯耗尽，池 {pool} 个），到期自动回轮询"
    )
    return cool


def _has_other_selectable(account: Account) -> bool:
    """池内除该账号外是否还有其他可服务账号（供 429 换号优先判定）。

    只看「其他账号」，不把同账号的 API Key 回退通道当作换号目标 —— 回退
    通道由 force_fallback 单独处理，两者语义不同（换号 vs 换通道）。
    """
    try:
        return store.select(account.provider, skip_ids={account.id}) is not None
    except Exception:  # noqa: BLE001 - 判定失败按「无其他账号」兑底，走原有兜底
        return False


def _has_recoverable_account(provider: str) -> bool:
    """池内是否存在「额度耗尽、预期分钟级自动恢复」的账号 —— 排队只对它有意义。

    EXHAUSTED：额度窗口滚动恢复（后台探测发现即回轮询），排队等待划算。
    COOLING 不排：5xx/风控冷却已知时长（300s 起、风控最长 24h），干等只会
    把连接吊死到 CDN 边缘 ~100s 硬超时（HTTP 524），应快速 503 让客户端
    自行重试；429 刚失败而仍 ACTIVE 的号秒级内也不会好转，同理不排队。
    （2026-10-07 实测：对这两类排队会把单账号 429 用例拖成 240s+ 挂死、
    上游被无谓重打十几次。）
    """
    return any(
        a.enabled and a.status == Status.EXHAUSTED
        for a in store.list_accounts(provider)
    )


def _is_risk_control(status_code: int, text: str) -> bool:
    """风控信号判定（3012「unusual activity」/ messages 端点 405）。

    与验证码挑战互斥：调用点已先排除 challenge 形态。命中即账号级风控，
    需指数退避冷却，而非直接回传客户端错误（会导致下次立刻重打、加剧风控）。
    """
    if status_code in constants.RISK_CONTROL_HTTP_STATUSES:
        return True
    low = text.lower()
    return any(m.lower() in low for m in constants.RISK_CONTROL_MARKERS)


def _parse_retry_after(value: str | None) -> int | None:
    """解析 Retry-After（仅秒数形态；HTTP-date 形态少见，放弃即用默认重试等待）。

    非正数不采信；超长值封顶采信 —— 尊重上游意图的同时防止把客户端吊死。
    """
    if not value:
        return None
    try:
        secs = int(float(value.strip()))
    except (ValueError, AttributeError):
        return None
    return min(secs, settings.RETRY_429_WAIT_MAX) if secs > 0 else None


def _mark(account: Account, status_value: str, error: str | None = None) -> None:
    account.status = status_value
    account.last_error = error
    if status_value == Status.COOLING:
        account.cooling_until = time.time() + settings.COOLING_SECONDS
    store.update_account(account)


def _last_user_text(body: dict) -> str:
    for msg in reversed(body.get("messages") or []):
        if not isinstance(msg, dict) or msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    return part.get("text", "")
    return ""


def _allowed_model_ids() -> list[str]:
    """账号池全部 entitlements 的 capabilities 并集（小写），与 /v1/models 同源。

    上游按此硬性判定允许的模型（套餐外返回业务码 3006），名单随套餐自动
    变化，新模型上线无需改代码。池空 / 无套餐信息时返回空列表 —— 静态
    AVAILABLE_MODELS 只是展示回退而非授权事实，拿它拦截会在冷启动误杀。
    """
    caps: set[str] = set()
    for provider in ("zai", "bigmodel"):
        for acc in store.list_accounts(provider):
            for plan in acc.plans or []:
                for ent in plan.get("entitlements") or []:
                    for c in ent.get("capabilities") or []:
                        if c.startswith("model:") and len(c) > 6:
                            caps.add(c[6:].lower())
    return sorted(caps)


def _reject_model_not_allowed(model: str) -> JSONResponse | None:
    """套餐外模型 → 400；返回 None 表示放行。

    必须在调度之前即时拒绝，而不是让排队去等一个绝无可能的账号：
    2026-10-07 实证，客户端请求 glm-5.3 而套餐只授权 glm-5.3-flash，排队
    尽职等 5s 后仍出 503，报错被误导成「额度耗尽」，既浪费排队预算又
    掩盖真实原因。这里直接给出可用模型清单，客户端一眼能看懂。
    """
    allowed = _allowed_model_ids()
    if not allowed:
        # 授权名单拿不到（空池 / 无套餐信息）→ 放行，交由调度走原有 503 路径，
        # 不按静态展示名单误杀（回归基线 test_no_account_503 锁定该行为）
        return None
    if (model or "").lower() in allowed:
        return None
    canon = [MODEL_NAME_MAP.get(m, m) for m in allowed]
    logs.warn("gateway", f"套餐外模型 {model or '(空)'} 被拒（可用：{', '.join(canon)}）")
    return JSONResponse(
        {"error": {
            "message": (
                f"模型 {model or '(空)'} 不在套餐允许范围内（上游 3006）。"
                f"可用模型：{', '.join(canon)}"
            ),
            "type": "model_not_allowed",
            "allowed_models": canon,
        }},
        status_code=400,
    )


@router.get("/v1/models", dependencies=[Depends(verify_gateway_key)])
async def list_models():
    """列出可用模型（Anthropic /v1/models 风格），动态生成。

    名单 = 账号池全部 entitlements 的 capabilities 并集（上游按此硬性判定
    允许的模型，套餐外 3006），套餐变化列表自动跟随、新模型上线无需改代码；
    取不到时回退静态 AVAILABLE_MODELS。模型名标准化为官方大小写
    （glm-5.3-flash → GLM-5.3-Flash），静态目录只用来补充展示规格。
    """
    caps: set[str] = set()
    for provider in ("zai", "bigmodel"):
        for acc in store.list_accounts(provider):
            for plan in acc.plans or []:
                for ent in plan.get("entitlements") or []:
                    for c in ent.get("capabilities") or []:
                        if c.startswith("model:") and len(c) > 6:
                            caps.add(c[6:])
    catalog = {m["id"]: m for m in getattr(constants, "MODEL_CATALOG", [])}
    ids: list[str] = []
    for c in sorted(caps):
        canon = MODEL_NAME_MAP.get(c.lower(), c)
        if canon not in ids:
            ids.append(canon)
    if not ids:
        ids = list(AVAILABLE_MODELS)
    data = []
    for i in ids:
        spec = catalog.get(i.lower()) or catalog.get(i)
        data.append({
            "id": i,
            "type": "model",
            "display_name": i,
            "created_at": "2025-01-01T00:00:00Z",
            **({"context_window": spec["context_window"],
                "max_output_tokens": spec["max_output_tokens"]} if spec else {}),
        })
    return {"object": "list", "data": data}


@router.post("/v1/messages", dependencies=[Depends(verify_gateway_key)])
async def messages(request: Request):
    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        return JSONResponse({"error": {"message": "请求体不是合法 JSON", "type": "invalid_request"}}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse(
            {"error": {"message": "请求体必须是 JSON 对象", "type": "invalid_request_error"}},
            status_code=400,
        )

    incoming_headers = dict(request.headers)
    provider = _detect_provider(body, request.headers)
    if provider == "bigmodel" and not store.bigmodel_channel_enabled():
        # 回退通道默认关闭（欠费时上游 429 原地重试会挂起约 2 分钟），
        # 这里即时拒绝而非让调度去试一个必然失败的通道。
        return JSONResponse(
            {"error": {"message": "bigmodel Key 回退通道已关闭（管理后台「设置」可开启）",
                       "type": "channel_disabled"}},
            status_code=403,
        )
    body = _normalize_body(body)
    # 套餐外模型即时拒绝（上游 3006）：否则会去排队等一个绝无可能的账号，
    # 最终出「额度耗尽」误导性 503。必须在 _dispatch 之前。
    if (rejected := _reject_model_not_allowed(str(body.get("model") or ""))) is not None:
        return rejected
    # 验证码页面由本服务托管，端口取实际请求端口（兼容任意启动端口）
    port = request.url.port or settings.PORT
    # X-Probe 打标：本次请求若为探测（模型测试/健康检查）则标记，供用量分流
    _probe_flag.set(is_probe_request(request.headers))

    req_id = secrets.token_hex(8)
    logs.req(req_id, str(body.get("model") or "-"), bool(body.get("stream")), _last_user_text(body))
    reqlog.begin(req_id, "messages", str(body.get("model") or "-"),
                 bool(body.get("stream")), _last_user_text(body))

    if body.get("stream") and settings.EARLY_FLUSH_GRACE > 0:
        # Early flush：宽限期内保持真实状态码语义；超期先发 200+SSE 头
        # 停掉 CDN 计时，此后失败降级 SSE error 事件（见 earlyflush 块注释）
        task = asyncio.create_task(_dispatch(req_id, body, incoming_headers, port, provider))
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=settings.EARLY_FLUSH_GRACE)
        except TimeoutError:
            logs.warn(req_id, f"上游首字超 {settings.EARLY_FLUSH_GRACE}s 宽限期，early-flush 先发 200+SSE 头（后续失败降级 SSE error）")
            return StreamingResponse(
                _early_flush_anthropic_stream(task, req_id),
                status_code=200, media_type="text/event-stream",
                headers=dict(_EARLY_FLUSH_SSE_HEADERS),
            )
        except asyncio.CancelledError:
            # 客户端断开必须显式取消派生任务——分离任务不会随请求取消而终止
            task.cancel()
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
        except Exception as err:  # noqa: BLE001 - 调度层意外异常也要收口监控条目
            reqlog.finish_error(req_id, f"网关内部错误: {err}", status=500)
            return JSONResponse(
                {"error": {"message": "网关内部错误", "type": "internal_error"}},
                status_code=500,
            )
    else:
        try:
            result = await _dispatch(req_id, body, incoming_headers, port, provider)
        except asyncio.CancelledError:
            # 客户端在调度期间断开（429 重试/验证码等待可达数分钟）——CancelError
            # 是 BaseException，不兜底会让监控条目永久滞留「进行中」
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
        except Exception as err:  # noqa: BLE001 - 调度层意外异常也要收口监控条目
            reqlog.finish_error(req_id, f"网关内部错误: {err}", status=500)
            return JSONResponse(
                {"error": {"message": "网关内部错误", "type": "internal_error"}},
                status_code=500,
            )
    if isinstance(result, _Upstream):
        # dispatch 返回与流式生成器启动之间的取消窗口：兜底关闭释放并发槽位
        try:
            return await _stream_or_biz_error(result, req_id)
        except asyncio.CancelledError:
            await result.close()
            raise
    return result


async def _stream_or_biz_error(up: _Upstream, req_id: str):
    """透传前先拦上游「HTTP 200 + 业务错误」，避免客户端把失败当成功。

    同步响应（content-type: application/json）可安全读取后重建；流式
    （text/event-stream）不预读，直接透传（流式首包才是内容，且预读会破坏
    SSE 时序）。
    """
    ctype = (up.resp.headers.get("content-type") or "").lower()
    if "text/event-stream" not in ctype:
        try:
            text = (await up.resp.aread()).decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001 - 读失败按原样透传兜底
            text = ""
        if text:
            hit = _upstream_biz_error(text)
            if hit:
                status, etype, emsg = hit
                detail = f"{emsg}（上游: {text[:160]}）"
                logs.req_err(req_id, detail)
                reqlog.finish_error(req_id, detail, status=status)
                await up.close()
                return JSONResponse(
                    {"error": {"message": emsg, "type": etype, "upstream": text[:400]}},
                    status_code=status,
                )
            # 非业务错误的已读响应：重建为等价响应，勿丢 body
            from fastapi import Response as _Resp
            await up.close()
            return _Resp(content=text, status_code=up.resp.status_code, media_type=ctype or "application/json")
    return up.to_streaming(req_id)


# ── Early Flush「响应头先行」（治 CDN 边缘 524）────────────────────────────
# 设计见 settings.EARLY_FLUSH_GRACE 注释：宽限期内保持真实状态码语义；
# 宽限期耗尽仍未拿到上游响应，则先发 200+SSE 头停掉 CDN 的「等待响应头」
# 计时，此后失败只能降级为 SSE error 事件（状态码已不可改），绝不伪造
# 正常结束。仅流式路径启用。
_EARLY_FLUSH_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


def _sse_error_chunk(style: str, status_code: int, etype: str, message: str) -> str:
    """按端点协议构造 SSE 错误事件（early flush 后状态码不可改，只能事件化）。"""
    if style == "openai":
        payload = {"error": {"message": message, "type": etype, "code": status_code}}
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\ndata: [DONE]\n\n"
    payload = {"type": "error", "error": {"type": etype, "message": message}}
    return f"event: error\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _jsonresp_error(resp) -> tuple[int, str, str]:
    """从调度层返回的 JSONResponse 提取 (status, type, message)，供 SSE 错误事件复用。"""
    try:
        data = json.loads(bytes(resp.body).decode("utf-8", "ignore"))
        err = data.get("error") or {}
        return (resp.status_code, str(err.get("type") or "api_error"),
                str(err.get("message") or "调度失败"))
    except Exception:  # noqa: BLE001 - 解析失败兑底为通用错误
        return resp.status_code, "api_error", "调度失败"


async def _early_flush_anthropic_stream(task: asyncio.Task, req_id: str):
    """/v1/messages 的 early flush 流（Anthropic SSE 语义）。

    首个 yield 立即把 200+SSE 响应头冲刷到线缆；等待调度期间按
    EARLY_FLUSH_BEAT 发注释心跳；成功透传上游流，失败降级 SSE error 事件。
    """
    beat = settings.EARLY_FLUSH_BEAT if settings.EARLY_FLUSH_BEAT > 0 else 15
    yield ": early-flush\n\n"
    while True:
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=beat)
            break
        except TimeoutError:
            yield ": keepalive\n\n"
        except asyncio.CancelledError:
            # 客户端断开必须显式取消派生任务——分离任务不会随请求取消而终止
            task.cancel()
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
    if not isinstance(result, _Upstream):
        # 调度层失败（reqlog 已在 _dispatch 内收口）：状态码不可改，降级 SSE error
        status, etype, emsg = _jsonresp_error(result)
        logs.warn(req_id, f"early-flush 后调度失败 {status}，降级 SSE error: {emsg}")
        yield _sse_error_chunk("anthropic", status, etype, emsg)
        return
    up = result
    ctype = (up.resp.headers.get("content-type") or "").lower()
    if "text/event-stream" not in ctype:
        # 少见：流式请求拿到同步 JSON——业务错误事件化，成功原样单事件透传
        try:
            raw = await up.resp.aread()
        except asyncio.CancelledError:
            reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
            raise
        except Exception as err:  # noqa: BLE001
            logs.req_err(req_id, f"流传输中断: {err}")
            reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
            yield _sse_error_chunk("anthropic", 502, "upstream_error", f"读取上游响应失败: {err}")
            return
        finally:
            await up.close()
        text = raw.decode("utf-8", "ignore")
        hit = _upstream_biz_error(text)
        if hit:
            status, etype, emsg = hit
            detail = f"{emsg}（上游: {text[:160]}）"
            logs.req_err(req_id, detail)
            reqlog.finish_error(req_id, detail, status=status, t_first=up.t_first)
            yield _sse_error_chunk("anthropic", status, etype, emsg)
            return
        logs.req_ok(req_id)
        reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code)
        record_usage(getattr(up, "model", "-"))
        yield f"data: {text}\n\n"
        return
    try:
        async for chunk in up.resp.aiter_bytes():
            yield chunk
        logs.req_ok(req_id)
        reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code)
        record_usage(getattr(up, "model", "-"))
    except asyncio.CancelledError:
        reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
        raise
    except Exception as err:  # noqa: BLE001
        logs.req_err(req_id, f"流传输中断: {err}")
        reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
        yield _sse_error_chunk("anthropic", 502, "upstream_error", f"流传输中断: {err}")
    finally:
        await up.close()


async def _early_flush_openai_stream(task: asyncio.Task, req_id: str, model: str):
    """/v1/chat/completions 的 early flush 流（OpenAI chunk 语义，走 StreamConverter）。"""
    beat = settings.EARLY_FLUSH_BEAT if settings.EARLY_FLUSH_BEAT > 0 else 15
    yield ": early-flush\n\n"
    while True:
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=beat)
            break
        except TimeoutError:
            yield ": keepalive\n\n"
        except asyncio.CancelledError:
            task.cancel()
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
    if not isinstance(result, _Upstream):
        status, etype, emsg = _jsonresp_error(result)
        logs.warn(req_id, f"early-flush 后调度失败 {status}，降级 SSE error: {emsg}")
        yield _sse_error_chunk("openai", status, etype, emsg)
        return
    up = result
    ctype = (up.resp.headers.get("content-type") or "").lower()
    if "text/event-stream" not in ctype:
        # 少见：流式请求拿到同步 JSON——业务错误事件化，成功原样单事件透传
        try:
            raw = await up.resp.aread()
        except asyncio.CancelledError:
            reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
            raise
        except Exception as err:  # noqa: BLE001
            logs.req_err(req_id, f"流传输中断: {err}")
            reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
            yield _sse_error_chunk("openai", 502, "upstream_error", f"读取上游响应失败: {err}")
            return
        finally:
            await up.close()
        text = raw.decode("utf-8", "ignore")
        hit = _upstream_biz_error(text)
        if hit:
            status, etype, emsg = hit
            logs.req_err(req_id, emsg)
            reqlog.finish_error(req_id, emsg, status=status, t_first=up.t_first)
            yield _sse_error_chunk("openai", status, etype, emsg)
            return
        data = _safe_json(text)
        usage = (data.get("usage") or {}) if isinstance(data, dict) else {}
        logs.req_ok(req_id)
        reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code,
                         input_tokens=usage.get("input_tokens"), output_tokens=usage.get("output_tokens"))
        record_usage(model, usage.get("input_tokens"), usage.get("output_tokens"))
        yield f"data: {text}\n\n"
        yield "data: [DONE]\n\n"
        return
    conv = StreamConverter(model)
    try:
        yield conv.start()
        async for line in up.resp.aiter_lines():
            if not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            if not data_str:
                continue
            evt = _safe_json(data_str)
            if isinstance(evt, dict):
                for out in conv.feed(evt):
                    yield out
        yield conv.done()
        logs.req_ok(req_id)
        reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code,
                         input_tokens=conv.usage.get("prompt_tokens"),
                         output_tokens=conv.usage.get("completion_tokens"))
        record_usage(model, conv.usage.get("prompt_tokens"), conv.usage.get("completion_tokens"))
    except asyncio.CancelledError:
        reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
        raise
    except Exception as err:  # noqa: BLE001
        logs.req_err(req_id, f"流传输中断: {err}")
        reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
        yield _sse_error_chunk("openai", 502, "upstream_error", f"流传输中断: {err}")
    finally:
        await up.close()


@router.post("/v1/chat/completions", dependencies=[Depends(verify_gateway_key)])
async def chat_completions(request: Request):
    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError):
        return JSONResponse({"error": {"message": "请求体不是合法 JSON", "type": "invalid_request_error"}}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"error": {"message": "请求体必须是 JSON 对象", "type": "invalid_request_error"}}, status_code=400)

    body, err = openai_to_anthropic(payload)
    if err or body is None:
        return JSONResponse({"error": {"message": err or "请求体不合法", "type": "invalid_request_error"}}, status_code=400)

    incoming_headers = dict(request.headers)
    provider = _detect_provider(body, request.headers)
    if provider == "bigmodel" and not store.bigmodel_channel_enabled():
        # 回退通道默认关闭（欠费时上游 429 原地重试会挂起约 2 分钟），
        # 这里即时拒绝而非让调度去试一个必然失败的通道。
        return JSONResponse(
            {"error": {"message": "bigmodel Key 回退通道已关闭（管理后台「设置」可开启）",
                       "type": "channel_disabled"}},
            status_code=403,
        )
    body = _normalize_body(body)
    # 套餐外模型即时拒绝（上游 3006）：否则会去排队等一个绝无可能的账号，
    # 最终出「额度耗尽」误导性 503。必须在 _dispatch 之前。
    if (rejected := _reject_model_not_allowed(str(body.get("model") or ""))) is not None:
        return rejected
    port = request.url.port or settings.PORT
    # X-Probe 打标：本次请求若为探测（模型测试/健康检查）则标记，供用量分流
    _probe_flag.set(is_probe_request(request.headers))

    req_id = secrets.token_hex(8)
    logs.req(req_id, str(body.get("model") or "-"), bool(payload.get("stream")), _last_user_text(body))
    reqlog.begin(req_id, "chat", str(body.get("model") or "-"),
                 bool(payload.get("stream")), _last_user_text(body))

    if payload.get("stream") and settings.EARLY_FLUSH_GRACE > 0:
        # Early flush：宽限期内保持真实状态码语义；超期先发 200+SSE 头
        # 停掉 CDN 计时，此后失败降级 SSE error 事件（见 earlyflush 块注释）
        task = asyncio.create_task(_dispatch(req_id, body, incoming_headers, port, provider))
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=settings.EARLY_FLUSH_GRACE)
        except TimeoutError:
            logs.warn(req_id, f"上游首字超 {settings.EARLY_FLUSH_GRACE}s 宽限期，early-flush 先发 200+SSE 头（后续失败降级 SSE error）")
            return StreamingResponse(
                _early_flush_openai_stream(task, req_id, str(body.get("model") or "")),
                status_code=200, media_type="text/event-stream",
                headers=dict(_EARLY_FLUSH_SSE_HEADERS),
            )
        except asyncio.CancelledError:
            task.cancel()
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
        except Exception as err:  # noqa: BLE001 - 调度层意外异常也要收口监控条目
            reqlog.finish_error(req_id, f"网关内部错误: {err}", status=500)
            return JSONResponse(
                {"error": {"message": "网关内部错误", "type": "internal_error"}},
                status_code=500,
            )
    else:
        try:
            result = await _dispatch(req_id, body, incoming_headers, port, provider)
        except asyncio.CancelledError:
            reqlog.finish_error(req_id, "客户端断开", status=499)
            raise
        except Exception as err:  # noqa: BLE001 - 调度层意外异常也要收口监控条目
            reqlog.finish_error(req_id, f"网关内部错误: {err}", status=500)
            return JSONResponse(
                {"error": {"message": "网关内部错误", "type": "internal_error"}},
                status_code=500,
            )
    if not isinstance(result, _Upstream):
        # 同步直通（_try_account 已读完 body 并收口监控含 tokens）：此处仅把
        # Anthropic message 转换为 OpenAI chat.completion；错误响应原样返回
        raw = getattr(result, "body", None)
        if result.status_code < 300 and raw:
            data = _safe_json(raw.decode("utf-8", "ignore"))
            if isinstance(data, dict) and data.get("type") == "message":
                return JSONResponse(anthropic_to_openai(data, str(body.get("model") or "")))
        return result

    model = str(body.get("model") or "")
    if payload.get("stream"):
        try:
            return _openai_stream_response(result, model, req_id)
        except asyncio.CancelledError:
            await result.close()
            raise

    try:
        raw = await result.resp.aread()
        logs.req_ok(req_id)
    except asyncio.CancelledError:
        reqlog.finish_error(req_id, "客户端断开", status=499, t_first=result.t_first)
        raise
    except Exception as err:  # noqa: BLE001
        logs.req_err(req_id, f"读取上游响应失败: {err}")
        reqlog.finish_error(req_id, f"读取上游响应失败: {err}", status=502)
        return JSONResponse({"error": {"message": f"读取上游响应失败: {err}", "type": "upstream_error"}}, status_code=502)
    finally:
        await result.close()
    raw_text = raw.decode("utf-8", "ignore")
    hit = _upstream_biz_error(raw_text)
    if hit:
        status, etype, emsg = hit
        detail = f"{emsg}（上游: {raw_text[:160]}）"
        logs.req_err(req_id, detail)
        reqlog.finish_error(req_id, detail, status=status, t_first=result.t_first)
        return JSONResponse(
            {"error": {"message": emsg, "type": etype, "upstream": raw_text[:400]}},
            status_code=status,
        )
    data = _safe_json(raw_text)
    if not isinstance(data, dict) or data.get("type") != "message":
        reqlog.finish_error(req_id, "上游响应格式异常", status=502, t_first=result.t_first)
        return JSONResponse({"error": {"message": "上游响应格式异常", "type": "upstream_error"}}, status_code=502)
    usage = data.get("usage") or {}
    reqlog.finish_ok(req_id, t_first=result.t_first, status=result.resp.status_code,
                     input_tokens=usage.get("input_tokens"), output_tokens=usage.get("output_tokens"))
    record_usage(model, usage.get("input_tokens"), usage.get("output_tokens"))
    return JSONResponse(anthropic_to_openai(data, model))


def _openai_stream_response(up: _Upstream, model: str, req_id: str) -> StreamingResponse:
    """把上游 Anthropic SSE 事件流转换为 OpenAI chunk 流。"""
    conv = StreamConverter(model)

    async def _iter():
        try:
            yield conv.start()
            async for line in up.resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data_str = line[5:].strip()
                if not data_str:
                    continue
                evt = _safe_json(data_str)
                if isinstance(evt, dict):
                    for out in conv.feed(evt):
                        yield out
            yield conv.done()
            logs.req_ok(req_id)
            reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code,
                             input_tokens=conv.usage.get("prompt_tokens"),
                             output_tokens=conv.usage.get("completion_tokens"))
            record_usage(model, conv.usage.get("prompt_tokens"), conv.usage.get("completion_tokens"))
        except asyncio.CancelledError:
            reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
            raise
        except Exception as err:  # noqa: BLE001
            logs.req_err(req_id, f"流传输中断: {err}")
            reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
        finally:
            await up.close()

    return StreamingResponse(_iter(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})


async def _dispatch(req_id, body, incoming_headers, port, provider):
    """多账号轮询调度：_Upstream（成功）或 JSONResponse（错误）。

    单账号并发限制：选号后若该账号在飞请求已达上限（store.account_concurrency，
    0 = 不限），跳过换下一个账号——不排队（流式请求可占槽位数分钟，排队会
    放大延迟甚至吊死客户端）。满号跳过不计入 MAX_ACCOUNT_ATTEMPTS（只计真正
    进入 _try_account 的次数）。全满/无号 → 503。
    """
    t0 = time.time()
    tried: set[str] = set()
    limit = _limit()
    attempts = 0
    queued = 0.0  # 已排队等待的秒数（选不到号时累计）
    # 排队预算：非流式请求无法 early flush（提前发头会破坏本地聚合协议），
    # 排队静默直接计入 TTFB —— 预算必须压在 CDN 边缘 ~100s 硬超时内
    #（QUEUE_WAIT_SYNC=60s，与 QUEUE_WAIT 取小），超时快速 503 而非 CF 524。
    stream = bool(body.get("stream"))
    budget: float | None = None
    if not stream and settings.QUEUE_WAIT_SYNC > 0:
        budget = float(min(settings.QUEUE_WAIT, settings.QUEUE_WAIT_SYNC))
    # 请求级总死线：跨账号重试/等待的总耗时上限（0 = 不限）。梯内等待与
    # 到期试探都属长挂起，死线保证客户端在明确时限内拿到失败与已试账号
    # 明细，可自行快速重试，而不是无限挂死。
    deadline = settings.REQUEST_DEADLINE

    while attempts < MAX_ACCOUNT_ATTEMPTS:
        if deadline and time.time() - t0 > deadline:
            logs.warn(req_id, f"请求级死线 {deadline}s 已到，停止重试（已试 {len(tried)} 账号）")
            return JSONResponse(
                {"error": {"message": f"所有账号重试均未成功（请求级死线 {deadline}s）",
                           "type": "no_available_account", "tried": sorted(tried)}},
                status_code=503,
            )
        account = store.select(provider, skip_ids=tried)
        if account is None:
            # 选不到号：限界排队等一等，吃掉「额度分钟级滚动恢复」型故障。
            # 返回 None 表示不排队（关闭 / 预算耗尽 / 空池）→ 走下方 503。
            waited = await _wait_for_account(req_id, provider, t0, deadline, queued, budget)
            if waited is None:
                break
            queued += waited
            tried.clear()  # 期间账号状态可能已刷新，重新给每个账号机会
            continue
        tried.add(account.id)
        if limit > 0 and _inflight.get(account.id, 0) >= limit:
            logs.warn(req_id, f"账号 {account.name} 并发已满（{_inflight.get(account.id, 0)}/{limit}），切换下一个")
            continue
        attempts += 1
        needs_captcha = provider == "zai" and account.uses_plan_channel()

        slot_box: list[str | None] = [None]
        if limit > 0:
            _inflight[account.id] = _inflight.get(account.id, 0) + 1
            slot_box[0] = account.id
        try:
            result = await _try_account(
                req_id, account, body, incoming_headers, port, needs_captcha, slot_box,
            )
        except BaseException:
            if slot_box[0] is not None:
                _release_slot(slot_box[0])
                slot_box[0] = None
            raise
        if result is _NEXT_ACCOUNT:
            if slot_box[0] is not None:
                _release_slot(slot_box[0])
                slot_box[0] = None
            continue
        if isinstance(result, _Upstream):
            held = slot_box[0]
            slot_box[0] = None
            if held is not None:
                result.on_close = _make_slot_releaser(held)
            return result
        if slot_box[0] is not None:
            _release_slot(slot_box[0])
            slot_box[0] = None
        return result

    if queued:
        logs.req_err(req_id, f"无可用账号 / 额度均已耗尽 / 并发已满（已排队等待 {queued:.0f}s）")
        reqlog.finish_error(
            req_id, f"无可用账号 / 额度均已耗尽 / 并发已满（已排队 {queued:.0f}s）", status=503,
        )
        return JSONResponse(
            {"error": {
                "message": f"所有账号均不可用、额度已用完或并发已满（已排队等待 {queued:.0f}s），请在后台检查账号状态",
                "type": "no_available_account", "queued_seconds": round(queued),
            }},
            status_code=503,
        )
    logs.req_err(req_id, "无可用账号 / 额度均已耗尽 / 并发已满")
    reqlog.finish_error(req_id, "无可用账号 / 额度均已耗尽 / 并发已满", status=503)
    return JSONResponse(
        {"error": {"message": "所有账号均不可用、额度已用完或并发已满，请在后台检查账号状态", "type": "no_available_account"}},
        status_code=503,
    )


def _release_slot(account_id: str) -> None:
    n = _inflight.get(account_id, 0) - 1
    if n <= 0:
        _inflight.pop(account_id, None)
    else:
        _inflight[account_id] = n


def _park_slot(slot_box: list[str | None] | None) -> None:
    """等待（429/验证码/5xx）前释放并发槽，避免把账号冻住数分钟。"""
    if slot_box and slot_box[0] is not None:
        _release_slot(slot_box[0])
        slot_box[0] = None


def _reacquire_slot(account: Account, slot_box: list[str | None] | None) -> bool:
    """等待结束后重新占槽；占不到则让调用方换号。"""
    if slot_box is None:
        return True
    limit = _limit()
    if limit <= 0:
        return True
    if _inflight.get(account.id, 0) >= limit:
        return False
    _inflight[account.id] = _inflight.get(account.id, 0) + 1
    slot_box[0] = account.id
    return True


def _make_slot_releaser(account_id: str):
    def _release() -> None:
        _release_slot(account_id)
    return _release


# ── 限界排队：选不到可用账号时等一等 ────────────────────────────────────────
# 动机：上游额度是分钟级滚动恢复的（2026-10-06 实测故障窗 4 分钟：账号被
# 标记 EXHAUSTED → 后台探测发现额度恢复 → 回轮询）。旧行为秒回 503，把这类
# 自愈型故障全部变成用户可见报错；排队能把它吃掉。
# 限界的理由：当天额度真耗尽时等也无用，排队只是把报错推迟。
_queue_probe_lock = threading.Lock()
_queue_probe_last = 0.0


def _trigger_quota_probe(req_id, provider: str) -> None:
    """排队期间主动探测一次额度，不等后台周期（节流防 billing 查询放大）。

    后台探测周期较长（默认分钟级），排队时若只干等，等于把恢复时间对齐到
    后台节奏上；主动探一次能把分钟级抖动再压短。并发排队会同时涌进多个
    请求，故按 QUEUE_PROBE_MIN_INTERVAL 全局节流——billing/* 连续查询是
    上游风控「unusual activity」的信号源，不能放大。
    """
    global _queue_probe_last
    now = time.time()
    with _queue_probe_lock:
        if now - _queue_probe_last < settings.QUEUE_PROBE_MIN_INTERVAL:
            return
        _queue_probe_last = now
    targets = [
        a for a in store.list_accounts(provider)
        if a.mode == "jwt" and a.allows_billing()
    ]
    if not targets:
        return
    logs.warn(req_id, f"排队触发主动额度探测（{len(targets)} 个账号）")
    _spawn_bg(refresh_accounts(targets))


async def _wait_for_account(req_id, provider: str, t0: float,
                            deadline: float, queued: float,
                            budget: float | None = None) -> float | None:
    """选不到可用账号时限界排队等待，返回实际等待秒数或 None。

    budget：本次请求的排队总预算（秒）；None = 用 settings.QUEUE_WAIT。
    非流式请求无法 early flush（提前发头会破坏本地聚合协议），排队静默
    直接计入 TTFB —— 由调用方把预算压到 QUEUE_WAIT_SYNC（CDN ~100s 红
    线内），超时快速 503，而不是让 CF 判 524。
    返回秒数 → 调用方继续轮询选号（账号可能已恢复）；
    返回 None → 不该排队（功能关闭 / 排队预算耗尽 / 请求死线已到 /
    池子空 / 池内无可恢复账号），由调用方走原有 503 路径，不做无谓空等。
    """
    max_wait = settings.QUEUE_WAIT if budget is None else budget
    if max_wait <= 0:
        return None
    # 池子压根没有账号（区别于「有账号但都不可用」）→ 排队毫无意义
    if not store.list_accounts(provider):
        return None
    # 排队只对「预期自动恢复」的故障有意义：EXHAUSTED 额度分钟级滚动恢复、
    # COOLING 到期自动回轮询。429/5xx 刚失败而仍 ACTIVE 的号秒级内不会好转
    # —— 对它们排队只会反复重打同一账号（每轮重带满额重试梯），放大上游
    # 压力并把 TTFB 拖过 CDN 边缘 ~100s 硬超时（HTTP 524；2026-10-07 实测
    # 单账号 429 用例被排队拖成 240s+ 挂死、上游被无谓重打十几次）。
    # 无可恢复账号 → 快速 503，让客户端自行重试。
    if not _has_recoverable_account(provider):
        return None
    remaining = max_wait - queued
    if remaining <= 0:
        logs.warn(req_id, f"排队已累计 {queued:.0f}s，达上限 {max_wait}s，停止等待")
        return None
    if deadline:
        elapsed = time.time() - t0
        if elapsed >= deadline:
            return None
        remaining = min(remaining, deadline - elapsed)

    poll = max(1, settings.QUEUE_POLL)
    logs.warn(req_id, f"暂无可用账号，排队等待（剩余预算 {remaining:.0f}s，每 {poll}s 重试选号）")
    _trigger_quota_probe(req_id, provider)

    waited = 0.0
    while waited < remaining:
        step = min(float(poll), remaining - waited)
        await _sleep(step)
        waited += step
        if store.select(provider) is not None:
            logs.warn(req_id, f"排队 {waited:.0f}s 后账号恢复可用，继续调度")
            return waited
        if deadline and time.time() - t0 >= deadline:
            return waited  # 死线到：交给主循环的死线分支出 503
    return None  # 排队预算耗尽


_NEXT_ACCOUNT = object()


# fire-and-forget 后台任务强引用：事件循环对 task 只持弱引用，裸 create_task
# 会被 GC 中途静默丢弃（同 main.py 启动安装序已修过的缺陷，2026-09 review）。
_bg_tasks: set[asyncio.Task] = set()


def _spawn_bg(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


# 账号并发限制：account_id → 在飞请求数。asyncio 单线程事件循环下
# check+inc 原子；释放走 _Upstream.close / 失败路径，泄漏面在测试钉住。
_inflight: dict[str, int] = {}


def _limit() -> int:
    """当前并发上限（0 = 不限），实时读取设置（后台改后即生效）。"""
    return store.account_concurrency()


# ── 上游业务错误（HTTP 200 内嵌 code/msg）────────────────────────────────────
# 上游把业务失败包在 HTTP 200 里返回（如 {"code":1005,"msg":"exceed quota
# limit"}）。原样透传会让客户端把失败当成功 —— Anthropic SDK 拿到没有 content
# 块的响应体，要么报莫名其妙的解析错，要么静默当作空回复（2026-09-29：无额度
# 时客户端一直收到「成功但空信息」）。这里把可识别的业务错误映射为真实 HTTP
# 状态码 + 标准错误体，自检与客户端据此分诊。
# 只认「无 content/choices 块 + 有非零 code」的形状，正常响应绝不误伤。
_UPSTREAM_BIZ_CODES = {
    1005: (429, "upstream_quota_exceeded", "上游额度已用完（套餐耗尽或到期）"),
    1003: (409, "upstream_already_claimed", "上游拒绝：已领取过"),
    1004: (403, "upstream_not_eligible", "上游拒绝：不符合条件"),
    3001: (400, "invalid_request_error", "上游参数错误"),
    3003: (503, "upstream_busy", "上游系统繁忙"),
    3006: (400, "model_not_allowed", "模型不在套餐允许范围内"),
    3012: (403, "upstream_risk_control", "上游风控拦截"),
}


# 业务码语义分类：额度类（标记 exhausted 换号）与风控类（禁用账号）
_QUOTA_BIZ_CODES = {1005, 1003, 1004}
_RISK_BIZ_CODES = {3012}


def _upstream_biz_code(text: str) -> int | None:
    """取上游响应体里的业务码（无法解析返回 None）。"""
    if not text or len(text) > 4096:
        return None
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("content") is not None or data.get("choices") is not None:
        return None
    try:
        return int(data.get("code"))
    except (TypeError, ValueError):
        return None


def _upstream_biz_error(text: str) -> tuple[int, str, str] | None:
    """从上游响应体识别业务错误，返回 (http_status, type, message)；正常返回 None。"""
    if not text or len(text) > 4096:
        return None
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("content") is not None or data.get("choices") is not None:
        return None
    try:
        code = int(data.get("code"))
    except (TypeError, ValueError):
        return None
    if code == 0:
        return None
    hit = _UPSTREAM_BIZ_CODES.get(code)
    if hit:
        return hit
    msg = str(data.get("msg") or data.get("message") or "").strip()
    if msg:
        return (502, "upstream_error", f"上游业务错误 {code}：{msg[:200]}")
    return None


class _Upstream:
    """已建立的上游成功流：由调用方消费并负责关闭。"""

    __slots__ = ("resp", "cm", "client", "t_first", "account_name", "mode", "on_close", "_closed")

    def __init__(self, resp: httpx.Response, cm, client: httpx.AsyncClient,
                 t_first: float | None = None, account_name: str = "", mode: str = "",
                 on_close=None) -> None:
        self.resp = resp
        self.cm = cm
        self.client = client
        self.t_first = t_first
        self.account_name = account_name
        self.mode = mode
        self.on_close = on_close
        self._closed = False

    async def close(self) -> None:
        """幂等关闭：释放上游流与并发槽位（on_close），重复调用安全。"""
        if self._closed:
            return
        self._closed = True
        await self.cm.__aexit__(None, None, None)
        await self.client.aclose()
        if self.on_close is not None:
            try:
                self.on_close()
            except Exception:  # noqa: BLE001 - 槽位释放失败不掩盖主流程
                pass

    def to_streaming(self, req_id: str) -> StreamingResponse:
        """原样透传（/v1/messages 直通路径）。"""
        up = self

        async def _body_iter():
            try:
                async for chunk in up.resp.aiter_bytes():
                    yield chunk
                logs.req_ok(req_id)
                reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code)
                # 直通路径无 usage 明细，仅记录模型与探测标记
                record_usage(getattr(up, "model", "-"))
            except asyncio.CancelledError:
                reqlog.finish_error(req_id, "客户端断开", status=499, t_first=up.t_first)
                raise
            except Exception as err:  # noqa: BLE001
                logs.req_err(req_id, f"流传输中断: {err}")
                reqlog.finish_error(req_id, f"流传输中断: {err}", t_first=up.t_first)
            finally:
                await up.close()

        return StreamingResponse(_body_iter(), status_code=up.resp.status_code,
                                 media_type=up.resp.headers.get("content-type", "application/json"),
                                 headers={"Cache-Control": "no-cache"})


async def _try_account(req_id, account, body, incoming_headers, port, needs_captcha,
                       slot_box: list | None = None):
    """尝试用单个账号转发，含验证码续期与可配置重试。

    错误处理策略（参数见 settings，均可用环境变量调整）：
      - 验证码挑战：清池换码重建请求，最多 MAX_CAPTCHA_RETRIES 次
      - 429 频控：**不冷却账号**，按上游 Retry-After（封顶 RETRY_429_WAIT_MAX）
        或 RETRY_429_WAIT 等待后原地重试，最多 RETRY_429_TIMES 次；
        耗尽后换下一个账号，账号保持可用。Plan 通道耗尽且有 API Key 时切
        回退通道重试（force_fallback 显式路由——429 不改账号状态，不能靠
        status 推导通道；回退通道自己的 429 重试预算独立计满后再换号）
      - 5xx 等一般错误：重试最多 RETRY_5XX_TIMES 次；耗尽后账号冷却
        COOLING_SECONDS 并换下一个账号
      - 风控（3012/405「unusual activity」）：指数退避冷却（base 900s 起，封顶
        24h），累计 RISK_BAN_STRIKES 次升级禁用（UI 展示），人工确认恢复
    """
    captcha_retries = 0
    retries_429 = 0
    retries_5xx = 0
    force_fallback = False  # 本请求瞬态走 Key 回退（不改账号持久化状态）
    model_name = str(body.get("model") or "-")
    while True:
        attempt_t0 = time.time()
        reqlog.mark_account(req_id, account.name, account.mode)
        verify_param = verify_region = None
        if needs_captcha:
            _park_slot(slot_box)
            try:
                verify_param, verify_region = await captcha_manager.get_verify_param(port)
            except Exception as err:  # noqa: BLE001
                logs.req_err(req_id, f"人机校验失败: {err}")
                reqlog.finish_error(req_id, f"人机校验失败: {err}", status=500)
                return JSONResponse(
                    {"error": {"message": f"无法完成人机校验: {err}", "type": "captcha_error"}},
                    status_code=500,
                )
            if not _reacquire_slot(account, slot_box):
                logs.warn(req_id, f"账号 {account.name} 验证码等待后并发已满，切换下一个")
                return _NEXT_ACCOUNT

        try:
            url, headers, payload = build_request(account, body, verify_param,
                                                  incoming_headers, verify_region,
                                                  force_fallback=force_fallback)
        except RuntimeError as err:
            account.record_result(False, f"凭证无效: {err}")
            _mark(account, Status.INVALID, str(err))
            logs.warn(req_id, f"账号 {account.name} 凭证无效，切换下一个")
            return _NEXT_ACCOUNT

        client = httpx.AsyncClient(timeout=httpx.Timeout(connect=30.0, read=None, write=120.0, pool=30.0))
        cm = client.stream("POST", url, headers=headers, content=payload)
        try:
            resp = await cm.__aenter__()
        except httpx.HTTPError as err:
            await client.aclose()
            account.record_result(False, f"连接失败: {err}")
            # 废 JWT / 风控禁用走 Key 回退失败时不得洗成 cooling，否则冷却结束会重开 Plan
            if account.status in (Status.INVALID, Status.DISABLED):
                store.update_account(account)
            else:
                _mark(account, Status.COOLING, f"连接失败: {err}")
            logs.warn(req_id, f"账号 {account.name} 连接失败，切换下一个")
            return _NEXT_ACCOUNT

        status_code = resp.status_code

        if status_code >= 400:
            text = (await resp.aread()).decode("utf-8", "ignore")
            await cm.__aexit__(None, None, None)
            await client.aclose()

            # 验证码挑战：三形态任一命中即清池重试（不改账号状态）
            challenge = _detect_captcha_challenge(resp, text) if needs_captcha else None
            if challenge:
                captcha_manager.invalidate()
                captcha_retries += 1
                if captcha_retries >= MAX_CAPTCHA_RETRIES:
                    account.record_result(False, "验证码挑战连续失败")
                    logs.warn(req_id, f"账号 {account.name} 验证码连续失败，切换下一个")
                    return _NEXT_ACCOUNT
                logs.warn(req_id, f"账号 {account.name} 验证码挑战（{challenge}），刷新重试")
                continue  # 同账号重建请求重试

            # 风控（3012「unusual activity」/ 405）：指数退避冷却自动恢复，累计
            # RISK_BAN_STRIKES 次升级禁用（人工恢复）。不立即重试（避免对风控
            # 账号持续施压）；冷却期零上游流量（is_cooling 全通道门禁）。
            if _is_risk_control(status_code, text):
                account.record_result(False, f"风控 HTTP {status_code}（3012/unusual activity）")
                account.risk_penalty(
                    settings.RISK_COOLDOWN_BASE, settings.RISK_COOLDOWN_MAX,
                    settings.RISK_BAN_STRIKES, settings.RISK_STRIKE_DECAY_SECONDS,
                )
                if account.status == Status.DISABLED:
                    account.last_error = (
                        f"风控封禁 (3012/unusual activity) HTTP {status_code}，"
                        f"确认恢复后请在后台手动启用（第 {account.risk_strikes} 次）"
                    )
                    penalty_note = f"已升级禁用（第 {account.risk_strikes} 次）"
                    if settings.RISK_AUTO_ROTATE:
                        # 「风控后换设备重生」自动化：升级禁用即换发全新设备
                        # （新 SKU + 新 device_mid），后台补跑安装序；账号重新
                        # 启用时已是全新身份（billing/claim 头同源跟随档案）
                        from ..fingerprint import profile_for, rotate
                        from ..install import schedule_install

                        old_mid = profile_for(account).device_mid[:8]
                        rotate(account)
                        account.installed_at = None
                        schedule_install(account)
                        logs.warn(
                            req_id,
                            f"账号 {account.name} 风控禁用，已自动换发设备指纹"
                            f"（旧 mid {old_mid}）",
                        )
                else:
                    cool_until = time.strftime(
                        "%H:%M", time.localtime(account.cooling_until)
                    ) if account.cooling_until else "?"
                    account.last_error = (
                        f"命中风控 (3012/unusual activity) HTTP {status_code}，"
                        f"指数退避冷却至 {cool_until}（第 {account.risk_strikes} 次）"
                    )
                    penalty_note = f"冷却至 {cool_until}（第 {account.risk_strikes} 次）"
                store.update_account(account)
                if needs_captcha and account.has_apikey_fallback():
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 命中风控 HTTP {status_code}，{penalty_note}，切 API Key 回退",
                    )
                    needs_captcha = False
                    force_fallback = True
                    continue
                logs.warn(
                    req_id,
                    f"账号 {account.name} 命中风控 HTTP {status_code}，{penalty_note}，切换下一个",
                )
                return _NEXT_ACCOUNT

            if _is_exhausted(status_code, text):
                account.record_result(False, f"额度用完 HTTP {status_code}")
                if account.status in (Status.INVALID, Status.DISABLED):
                    store.update_account(account)
                else:
                    _mark(account, Status.EXHAUSTED, "额度已用完")
                    _spawn_bg(_safe_refresh(account))
                logs.warn(req_id, f"账号 {account.name} 额度用完，切换下一个")
                return _NEXT_ACCOUNT

            if status_code == 401:
                account.record_result(False, "鉴权失败 HTTP 401")
                _mark(account, Status.INVALID, "鉴权失败 HTTP 401")
                if needs_captcha and account.has_apikey_fallback():
                    logs.warn(req_id, f"账号 {account.name} 鉴权失败 401，切 API Key 回退")
                    needs_captcha = False
                    force_fallback = True
                    continue
                logs.warn(req_id, f"账号 {account.name} 鉴权失败 401，切换下一个")
                return _NEXT_ACCOUNT

            if status_code == 403:
                # 403 已排除挑战形态（上方 challenge 分支），此处为真实鉴权拒绝
                account.record_result(False, "鉴权失败 HTTP 403")
                _mark(account, Status.INVALID, "鉴权失败 HTTP 403")
                if needs_captcha and account.has_apikey_fallback():
                    logs.warn(req_id, f"账号 {account.name} 鉴权失败 403，切 API Key 回退")
                    needs_captcha = False
                    force_fallback = True
                    continue
                logs.warn(req_id, f"账号 {account.name} 鉴权失败 403，切换下一个")
                return _NEXT_ACCOUNT

            if status_code == 429:
                # 1113「无余额/无资源包」是 Key 的永久状态（api.z.ai 实测秒回），
                # 原地等待重试毫无意义，只会把客户端吊死在 RETRY_429_TIMES×WAIT 里
                # ——快速换号/失败。
                if "1113" in text or "insufficient balance" in text.lower():
                    account.record_result(False, "429 1113 无余额（Key 无资源包，永久态）")
                    logs.warn(req_id, f"账号 {account.name} 429 为 1113 无余额，跳过等待重试")
                    if needs_captcha and account.has_apikey_fallback():
                        needs_captcha = False
                        force_fallback = True
                        retries_429 = 0
                        continue
                    return _NEXT_ACCOUNT
                # 频控不是账号故障：不冷却，原地等一等再试，耗尽后换号且账号保持可用。
                # Plan 通道耗尽 ≠ Key 回退也耗尽：同账号切回退并归还该通道的重试预算
                #（与上方 3012/401/403 切回退同一语义）
                #
                # 429 换号优先（2026-10-07 实证，对齐 workbuddy-gateway 做法）：
                # 池内还有其他可服务账号时立刻换号，而不是原地静默等待 —— 实测
                # 60s×2 轮即 120s 会撞穿 CDN 边缘 ~100s 硬超时（HTTP 524），且等待
                # 期间零字节发给客户端（实测 183 次 429、67 次踩到第 2 轮以上）。
                # 仅当全池都被限流（无号可换）时，才回退到下方原地等待兜底。
                if settings.RETRY_429_FAILOVER_FIRST and _has_other_selectable(account):
                    account.record_result(False, "429 频控，换号优先（不原地静默等待）")
                    logs.warn(req_id, f"账号 {account.name} 被限流 429，池内有其他账号，立刻换号")
                    return _NEXT_ACCOUNT
                if retries_429 < settings.RETRY_429_TIMES:
                    retries_429 += 1
                    wait = _parse_retry_after(resp.headers.get("retry-after")) or settings.RETRY_429_WAIT
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 被限流 429，{wait}s 后重试"
                        f"（{retries_429}/{settings.RETRY_429_TIMES}）",
                    )
                    _park_slot(slot_box)
                    await _sleep(wait)
                    if not _reacquire_slot(account, slot_box):
                        logs.warn(req_id, f"账号 {account.name} 429 等待后并发已满，切换下一个")
                        return _NEXT_ACCOUNT
                    continue
                if needs_captcha and account.has_apikey_fallback():
                    account.record_result(False, "Plan 通道 429 耗尽，切 API Key 回退")
                    logs.warn(req_id, f"账号 {account.name} Plan 通道 429 耗尽，切 API Key 回退")
                    needs_captcha = False
                    force_fallback = True
                    retries_429 = 0
                    continue
                account.record_result(False, f"429 重试 {settings.RETRY_429_TIMES} 次耗尽")
                cool = _rate_break(account)
                store.update_account(account)
                if cool:
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 连续 {account.rate_strikes} 轮 429 重试梯耗尽，"
                        f"自动熔断 {cool // 60} 分钟（到期自动回轮询）",
                    )
                else:
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 429 重试 {settings.RETRY_429_TIMES} 次耗尽，"
                        f"切换下一个（账号保持可用）",
                    )
                return _NEXT_ACCOUNT

            if status_code >= 500:
                # 一般性上游错误：重试，耗尽才冷却账号并换号
                if retries_5xx < settings.RETRY_5XX_TIMES:
                    retries_5xx += 1
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 上游 HTTP {status_code}，"
                        f"{settings.RETRY_5XX_WAIT}s 后重试（{retries_5xx}/{settings.RETRY_5XX_TIMES}）",
                    )
                    _park_slot(slot_box)
                    await _sleep(settings.RETRY_5XX_WAIT)
                    if not _reacquire_slot(account, slot_box):
                        logs.warn(req_id, f"账号 {account.name} 5xx 等待后并发已满，切换下一个")
                        return _NEXT_ACCOUNT
                    continue
                account.record_result(False, f"HTTP {status_code} 重试 {settings.RETRY_5XX_TIMES} 次耗尽，冷却")
                if account.status in (Status.INVALID, Status.DISABLED) or account.is_cooling():
                    # Key 回退 5xx 不得覆盖废 JWT / 风控禁用，否则冷却结束会重开 Plan；
                    # 风控冷却同理（冷却期不覆盖，避免缩短风控退避）
                    store.update_account(account)
                    logs.warn(req_id, f"账号 {account.name} 上游 {status_code} 重试耗尽，Plan 已停用，切换下一个")
                else:
                    cool = settings.COOLING_SECONDS
                    account.status = Status.COOLING
                    account.cooling_until = time.time() + cool
                    account.last_error = f"上游 HTTP {status_code} 重试 {settings.RETRY_5XX_TIMES} 次耗尽，冷却"
                    store.update_account(account)
                    logs.warn(req_id, f"账号 {account.name} 上游 {status_code} 重试耗尽，冷却 {cool}s，切换下一个")
                return _NEXT_ACCOUNT

            # 其它 4xx：直接回传客户端；响应体全量落日志供排查
            # （错误 JSON 通常很小；防御性上限 4KB，超长按 HTML 类 WAF 页处理只留头部）
            account.fail_count += 1
            account.record_result(False, f"HTTP {status_code}: {text[:120]}".replace("\n", " "))
            store.update_account(account)
            logs.req_err(req_id, f"上游错误 HTTP {status_code}（账号 {account.name}）")
            body_log = text if len(text) <= 4000 else text[:4000] + f"...(共 {len(text)} 字节，疑似 WAF 页)"
            logs.warn(req_id, f"上游 {status_code} 完整响应体: {body_log}")
            reqlog.finish_error(req_id, f"HTTP {status_code}: {text[:120]}".replace("\n", " "),
                                status=status_code, t_first=time.time() - attempt_t0)
            return JSONResponse(
                _safe_json(text) or {"error": {"message": text[:500], "type": "upstream_error"}},
                status_code=status_code,
            )

        # 成功：记录用量并把打开的上游流交给调用方。
        # 注意：上游把业务失败包在 HTTP 200 里（{"code":1005,...}），此处必须先
        # 读 body 判一次 —— 否则账号会被记为「请求成功」并保持 active，
        # 后台显示正常却一直接不出内容（2026-09-29：175 无额度却显示正常）。
        if status_code < 300:
            is_sse = "text/event-stream" in (resp.headers.get("content-type") or "").lower()
            if not is_sse:
                # 同步响应：先读 body 判业务错误（流式不预读，免破坏 SSE 时序）
                text = (await resp.aread()).decode("utf-8", "ignore")
                hit = _upstream_biz_error(text)
                if hit is not None:
                    _status, etype, _emsg = hit
                    code_hit = _upstream_biz_code(text)
                    await cm.__aexit__(None, None, None)
                    await client.aclose()
                    if code_hit in _RISK_BIZ_CODES:
                        account.record_result(False, f"风控封禁（业务码 {code_hit}）")
                        account.ban_for_risk()
                        account.last_error = (
                            f"风控封禁 (业务码 {code_hit})，确认恢复后请在后台手动启用"
                            f"（第 {account.risk_strikes} 次）"
                        )
                        store.update_account(account)
                        logs.warn(req_id, f"账号 {account.name} 命中风控业务码 {code_hit}，已禁用，切换下一个")
                        return _NEXT_ACCOUNT
                    if code_hit in _QUOTA_BIZ_CODES:
                        account.record_result(False, f"额度用完（业务码 {code_hit}）")
                        if account.status in (Status.INVALID, Status.DISABLED):
                            store.update_account(account)
                        else:
                            _mark(account, Status.EXHAUSTED, "额度已用完")
                            _spawn_bg(_safe_refresh(account))
                        logs.warn(req_id, f"账号 {account.name} 额度用完（业务码 {code_hit}），切换下一个")
                        return _NEXT_ACCOUNT
                    # 其余业务码：不动账号状态，直接回客户端（避免持续施压）
                    account.record_result(False, f"上游业务错误 {code_hit}")
                    store.update_account(account)
                    logs.req_err(req_id, f"账号 {account.name} 上游业务错误 {code_hit}")
                    reqlog.finish_error(req_id, f"上游业务错误 {code_hit}", status=_status)
                    return JSONResponse(
                        {"error": {"message": f"上游业务错误 {code_hit}", "type": etype,
                                   "upstream": text[:400]}},
                        status_code=_status,
                    )
                # 业务正常的同步响应：body 已读完，重建等价响应交给上层
                await cm.__aexit__(None, None, None)
                await client.aclose()
                from fastapi import Response as _Resp
                account.use_count += 1
                account.last_used_at = time.time()
                account.record_result(True, f"HTTP 200 · {model_name} · {time.time() - attempt_t0:.1f}s")
                # API Key 回退成功不得把废 JWT / 风控禁用洗成 active，也不得清风控
                # 计数；风控冷却未到期同样不洗（与下方流式路径同一守卫语义，
                # 否则 3012 后同请求走 Key 回退成功会把冷却洗掉，立刻重打 Plan）
                if account.status not in (Status.INVALID, Status.DISABLED) and not account.is_cooling():
                    account.risk_strikes = 0
                    account.last_risk_at = None
                    account.rate_strikes = 0
                    account.last_error = None
                    account.cooling_until = None
                    if account.status in (Status.COOLING, Status.EXHAUSTED):
                        account.status = Status.ACTIVE
                store.update_account(account)
                _spawn_bg(_safe_refresh(account))
                # 同步直通在此收口监控条目（含 tokens 与用量记账）：原先同步响应
                # 只有流式路径闭环，非流式条目永远滞留「进行中」——reqlog._inflight
                # 无界增长，监控页也把已完成请求永远显示为进行中
                sync_data = _safe_json(text)
                sync_usage = (sync_data.get("usage") or {}) if isinstance(sync_data, dict) else {}
                reqlog.finish_ok(req_id, t_first=time.time() - attempt_t0, status=status_code,
                                 input_tokens=sync_usage.get("input_tokens"),
                                 output_tokens=sync_usage.get("output_tokens"))
                record_usage(model_name, sync_usage.get("input_tokens"),
                             sync_usage.get("output_tokens"))
                return _Resp(content=text, status_code=status_code,
                             media_type=resp.headers.get("content-type") or "application/json")

        account.use_count += 1
        account.last_used_at = time.time()
        account.record_result(True, f"HTTP 200 · {model_name} · {time.time() - attempt_t0:.1f}s")
        # API Key 回退成功不得把废 JWT / 风控禁用洗成 active，也不得清风控计数；
        # 风控冷却未到期同样不洗（上游风控标记为小时级粘性，回退通道成功不代表
        # Plan 通道已解除，洗掉会立刻重打 Plan 并累加计数）。冷却已过期后的
        # Plan 通道成功即证明恢复，全量清理（清风控计数）。
        if account.status not in (Status.INVALID, Status.DISABLED) and not account.is_cooling():
            account.risk_strikes = 0
            account.last_risk_at = None
            account.rate_strikes = 0
            account.last_error = None
            account.cooling_until = None
            if account.status in (Status.COOLING, Status.EXHAUSTED):
                account.status = Status.ACTIVE
        store.update_account(account)
        _spawn_bg(_safe_refresh(account))

        return _Upstream(resp, cm, client, t_first=time.time() - attempt_t0,
                         account_name=account.name, mode=account.mode)


def _safe_json(text: str):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


async def _safe_refresh(account: Account) -> None:
    try:
        live = store.find(account.provider, account.id)
        if live is None:
            return
        if live.provider == "zai" and live.allows_billing():
            # 去抖：每条消息都刷 billing 是流量放大器（会加剧风控），与 monitor 共享
            # last_checked_at，最小间隔内的刷新直接跳过
            last = live.last_checked_at
            if last and time.time() - last < settings.BILLING_REFRESH_MIN_INTERVAL:
                return
            await fetch_quota(live)
    except Exception:  # noqa: BLE001
        pass
