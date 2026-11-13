"""核心网关：/v1/messages（Anthropic 风格）与 /v1/chat/completions（OpenAI 风格）。

共用多账号轮询 + 额度用完自动换号 + 阿里无痕验证自动续期；OpenAI 端点由
openai_compat 做双向格式转换，调度与错误处理策略完全一致。
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import secrets
import threading
import time
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.requests import ClientDisconnect

from .. import (
    body_transform,
    business_error,
    constants,
    dedup,
    diaglog,
    edge,
    egress,
    freshness,
    logs,
    plan_guard,
    quota_windows,
    reasoning,
    reqlog,
    riskmetrics,
    session_ctx,
    settings,
)
from ..affinity import affinity_manager, extract_context_key
from ..agent import build_request
from ..auth_admin import verify_gateway_key
from ..captcha import captcha_manager
from ..gate import gate
from ..models import Account, Status
from ..openai_compat import StreamConverter, anthropic_to_openai, openai_to_anthropic
from ..quota import fetch_quota, refresh_accounts
from ..responses_compat import ResponsesStreamConverter, chat_to_response, responses_to_chat
from ..store import store
from ..token_estimate import estimate_input_tokens_after_transform

_sleep = asyncio.sleep  # 模块级引用：测试可 patch 此名而免污染全局 asyncio

# ── BUSY_HOLD（magpie plugins#503 同款全池刹车）──────────────────────────
# 429 body 3008/3009/3010 = 并发槽「没轮到」：同一请求 1s/2s 重问两次仍被拒
# 才算「持续饱和」（busy_saturated 计一次）；60s 窗口内累计 BUSY_N 次即全池
# 静默 BUSY_HOLD 秒（所有账号的 zcode.z.ai Start Plan 请求在本地直接
# 429+Retry-After，零上游压力），赶在阿里云 WAF 把整个出口 IP 405 封禁之前
# 刹车。状态只在进程内存：重启即清，与 magpie 同款。
_busy_hold = {"hits": [], "until": 0.0}
_busy_hold_lock = asyncio.Lock()  # 武装分支串行化：并发请求同时满员会双写 armed 事件

# ── startGate 本地排队闸（v2.9.32，magpie 实测对照）──────────────────────
# 3008 = 上游并发槽残留占用（实测「饱和→恢复」中位 64.7s，p75 260s），
# 1s/2s 重问用尽后**不换号敲第二遍、不秒回 503**：给该账号立一个本地
# 闸门，闸门期内新请求在 _wait_for_account 里排队等（零上游压力），到期
# 放一个请求进去试探；成功则清闸，仍 3008 则续闸。magpie 的 startGate
# 是「在飞满 3 就本地排队」，我们这里是「上游说满了就本地排队」，同语义
# 更贴实际（上游的槽位计数不透明，本地 inflight 未必等于上游）。
_busy_gate_until: dict[str, float] = {}   # account_id → 本地闸门截止 epoch
_busy_gate_lock = threading.Lock()


def _busy_gate_arm(account_id: str, seconds: float | None = None) -> float:
    """账号上游饱和：立/续本地闸门；期间该号新请求只排队不敲上游。"""
    hold = float(seconds if seconds is not None else settings.BUSY_GATE_WAIT)
    if hold <= 0:
        return 0.0
    with _busy_gate_lock:
        until = max(_busy_gate_until.get(account_id, 0.0), time.time() + hold)
        _busy_gate_until[account_id] = until
    return until


def _busy_gate_clear(account_id: str) -> None:
    with _busy_gate_lock:
        _busy_gate_until.pop(account_id, None)


def _busy_gate_live(account_id: str) -> bool:
    """账号是否处于上游闸门期；到期时原子领取唯一试探名额。

    返回 True = 仍在闸内（本请求应跳过、排队等）；返回 False = 闸已到
    期且本请求抢到试探名额（进上游），其余并发请求仍会拿到 True，
    不会在到期瞬间一起涌到上游（magpie startGate 的 first-come 语义）。
    """
    now = time.time()
    with _busy_gate_lock:
        until = _busy_gate_until.get(account_id, 0.0)
        if not until:
            return False
        if until <= now:
            # 到期：发一个试探名额（把闸门续 BUSY_GATE_WAIT 作为试探窗口），
            # 试探成功由 _busy_gate_clear 撤闸；仍 3008 由 _busy_gate_arm 续闸。
            _busy_gate_until[account_id] = now + max(1.0, float(settings.BUSY_GATE_WAIT))
            return False  # 本请求放行
        return True


def _busy_gate_active() -> bool:
    """是否有闸门（含到期后仍在等待唯一试探名额的窗口）。

    到期后闸门条目仍保留（由 _busy_gate_live 原子发放试探名额），
    不可在这里按 until<=now 清除 —— 否则排队轮询会误判「无闸门」
    而把全部账号重新敲一遍（v2.9.32 集成测试钉住）。
    """
    with _busy_gate_lock:
        return bool(_busy_gate_until)


def _busy_gate_next() -> float:
    """全池最早闸门到期时刻（无闸门返回 0）：_recovery 用它给 Retry-After。"""
    now = time.time()
    with _busy_gate_lock:
        live = [v for v in _busy_gate_until.values() if v > now]
    return min(live) if live else 0.0


def _busy_held() -> bool:
    return settings.BUSY_HOLD > 0 and time.time() < _busy_hold["until"]


def _busy_hold_remaining() -> int:
    return max(1, int(_busy_hold["until"] - time.time() + 0.999))


async def _busy_record_saturated() -> None:
    """记一次持续饱和；60s 窗口内满 BUSY_N 次即武装全池静默。

    asyncio.Lock 串行化「检查满员 → 武装 → 清窗」：两个并发请求同时
    满员会各自武装一次（实测 2026-11-08 双写 armed，ts 差 1ms）。
    """
    if settings.BUSY_HOLD <= 0 or settings.BUSY_N <= 0:
        return
    # 同一饱和信号同步给进程级闸门（上游 2.7.23 gate.note_busy）
    gate.note_busy()
    if _busy_held():
        return  # 静默期内的饱和不再计数，防自我延长
    async with _busy_hold_lock:
        if _busy_held():  # 双检：等锁期间另一请求可能已武装
            return
        now = time.time()
        hits = _busy_hold["hits"]
        hits.append(now)
        window = settings.BUSY_WINDOW
        while hits and now - hits[0] > window:
            hits.pop(0)
        if len(hits) >= settings.BUSY_N:
            _busy_hold["until"] = now + settings.BUSY_HOLD
            hits.clear()
            logs.warn("BUSY", f"60s 内 {settings.BUSY_N} 次持续饱和，全池静默 {settings.BUSY_HOLD}s"
                              "（赶在 WAF 封 IP 之前刹车，magpie plugins#503 同款）")
            riskmetrics.record("busy", "", carrier="ip", event="armed",
                               n=settings.BUSY_N, hold=settings.BUSY_HOLD)

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


def _client_meta(request) -> dict:
    """请求来源元数据（监控用）：真实客户端 IP / UA / 路径。

    CF 隧道下 nginx 无访问日志，真实 IP 以 CF-Connecting-IP 为准；直连本机时
    回退 X-Forwarded-For（多跳取第一段）/ local。UA 截断防日志/环形区膨胀。
    """
    h = request.headers
    ip = (h.get("cf-connecting-ip") or "").strip()
    if not ip:
        ip = (h.get("x-forwarded-for") or "").split(",")[0].strip()
    return {"path": request.url.path, "client_ip": (ip or "local")[:64],
            "ua": (h.get("user-agent") or "")[:96]}


def _body_meta(body: dict) -> dict:
    """请求形状（监控展示）：消息条数与 max_tokens（都不是则 None）。"""
    msgs = body.get("messages")
    mt = body.get("max_tokens")
    return {"msgs": len(msgs) if isinstance(msgs, list) else None,
            "max_tokens": mt if isinstance(mt, int) else None}


_CHANNEL_KEYS = ("zai", "zai_fallback", "bigmodel")


def _lane_for(force_fallback: bool, plan_channel: bool, url: str, channel: str = "") -> str:
    """本次尝试实际走的业务通道（监控用）。

    渠道语义：优先直接用通道键（zai/zai_fallback/bigmodel）；无键时与成功
    路径同口径按 url 判（/zcode-plan/ → plan）。plan_std 系通道已随上游
    df42a2d 移除（OAuth token 打不了标准 Anthropic 端点）。
    force_fallback 仅作旧口径兜底。
    """
    if channel in _CHANNEL_KEYS:
        # 通道键 → 监控词表（fork 二元 plan/apikey）：zai（zcode-plan JWT）
        # 消耗 Plan 套餐额度记 plan；Key 系（zai_fallback / bigmodel）记
        # apikey。前端 PLAN/KEY 徽章与 reqlog 聚合词表不变。
        if channel == "zai":
            return "plan"
        return "apikey"
    if url and "/zcode-plan/" in url:
        return "plan"
    if force_fallback:
        return "apikey"
    return "plan" if plan_channel else ""


def _upstream_trace(resp) -> str:
    """上游响应头追踪标识：优先 cf-ray（CF 边缘），其次上游 request id。"""
    for key in ("cf-ray", "x-request-id", "x-zcode-request-id", "x-trace-id"):
        try:
            val = resp.headers.get(key)
        except Exception:  # noqa: BLE001 - 观测字段绝不放大请求失败
            return ""
        if val:
            return f"{key}={val}"[:120]
    return ""


# 用量明细保留天数：与 reqlog 的 HISTORY_DAYS 同语义，按天文件整体删除。
# 不清理时高流量实例 data/ 目录以 ~每天一个文件无限增长（2026-10-07 审查发现）。
USAGE_HISTORY_DAYS = 30
_usage_prune_day = ""


def _usage_maybe_prune(data_dir: str) -> None:
    """跨天首写时清理过期 usage-*.jsonl（与 reqlog._maybe_prune 同范式）。"""
    global _usage_prune_day
    today = datetime.now(_BJ).strftime("%Y-%m-%d")
    if _usage_prune_day == today:
        return
    _usage_prune_day = today
    cutoff = (datetime.now(_BJ) - timedelta(days=USAGE_HISTORY_DAYS)).strftime("%Y-%m-%d")
    try:
        for name in os.listdir(data_dir):
            if not (name.startswith("usage-") and name.endswith(".jsonl")):
                continue
            day = name[6:-6]
            if len(day) == 10 and day < cutoff:
                os.remove(os.path.join(data_dir, name))
    except OSError:
        pass  # 清理失败不影响记账


def record_usage(model: str, tin=None, tout=None, credit=None) -> None:
    """落盘单条用量明细（best-effort：失败只记日志，绝不影响主流程）。

    文件：data/usage-<北京日期>.jsonl（保留 USAGE_HISTORY_DAYS 天）
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
            _usage_maybe_prune(settings.DATA_DIR)
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
MODEL_CATALOG = constants.MODEL_CATALOG
_EXHAUST_KEYWORDS = constants.EXHAUST_KEYWORDS


def _messages_egress() -> str:
    """messages 通道有效出口 tag（同时驱动 captcha 签发出口，见 app/egress.py）。

    ZCODE_MESSAGES_FORCE_DIRECT=true 强制直连：2026-11-04 线上实证 env 代理
    出口（GCP IP）被上游持续标记——messages 走代理 17h+ 零成功（3012
    unusual activity），而同账号 billing/claim/telemetry 直连全部正常。
    统一全链路出口，消除「CN billing + US messages」双 IP 异常信号；
    置 false 恢复历史行为（走 env 代理）。
    """
    if settings.MESSAGES_FORCE_DIRECT:
        return egress.DIRECT
    return egress.egress_for(settings.UPSTREAM["zai"])


def _detect_provider(body: dict, headers) -> str:
    """入站 provider 识别（Magpie 模式命名空间：`provider/Model` 前缀）。

    优先级：模型名前缀 > `x-provider` 请求头 > 默认 zai（裸名向后兼容）。
    """
    model = body.get("model") or ""
    prefix = model.split("/", 1)[0].strip().lower() if "/" in model else ""
    if prefix in ("bigmodel", "zai"):
        return prefix
    xp = (headers.get("x-provider") or "").strip().lower()
    if xp in ("bigmodel", "zai"):
        return xp
    return "zai"


def _normalize_body(body: dict) -> dict:
    model = body.get("model")
    if isinstance(model, str) and "/" in model:
        # Magpie 命名空间前缀（zai/、bigmodel/）只用于路由识别，上游不认；
        # 必须无条件剥掉并写回——否则映射后名称恰好不变时（如
        # zai/GLM-5.3-Flash → GLM-5.3-Flash）前缀会被原样透传，上游 3006。
        model = "/".join(model.split("/")[1:])
    if isinstance(model, str):
        # 先按大小写别名改写，再兜底「已退役型号」→ 当前主力型号。上游对退役名
        # 一律 3006（model not allowed），原样透传只会让客户端拿必然失败的结果。
        mapped = MODEL_NAME_MAP.get(model.lower(), model)
        mapped = constants.RETIRED_MODEL_ALIASES.get(mapped, mapped)
        if mapped != model:
            logs.warn("gateway", f"模型 {model} → {mapped}（别名/退役型号改写）")
        body["model"] = mapped

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
    # 原生 /v1/messages：GLM-5.3 家族未显式给 thinking → 默认 max 档。
    # compat 路径已由 apply_reasoning 注入，此处见到 thinking 会跳过。
    reasoning.apply_native_defaults(body)
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
    # 业务码优先：Plan 通道 1005/1113 实测形态 {"code":1005,"msg":"exceed quota limit"}
    data = _safe_json(text)
    if isinstance(data, dict) and data.get("code") in constants.EXHAUST_BUSINESS_CODES:
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


def _has_other_selectable(account: Account, model: str | None = None) -> bool:
    """池内除该账号外是否还有其他可服务账号（供 429 换号优先判定）。

    只看「其他账号」，不把同账号的 API Key 回退通道当作换号目标 —— 回退
    通道由 force_fallback 单独处理，两者语义不同（换号 vs 换通道）。
    ignore_rate_hold：忽略 429 扣号只看结构可用性 —— 扣号是「本请求刚把
    某号扣住」的结果，不该让换号判定误判成「无号可换」而退化成原地重试 5
    次（那样反而把同一个号打得更多）。
    """
    try:
        return store.select(account.provider, skip_ids={account.id}, model=model,
                            ignore_rate_hold=True) is not None
    except Exception:  # noqa: BLE001 - 判定失败按「无其他账号」兑底，走原有兜底
        return False


def _quota_windows_live(account, now: float) -> bool:
    """额度窗口是否仍有「滚动恢复」的可能（至少一窗未到期）。

    EXHAUSTED 之所以值得排队，是因为日窗会到期重置、后台 60s 探测发现即
    拉回轮询。但窗口本身已过期（expires_at <= now）时，重置不会再发生，
    恢复只能靠上游新发授权（grant / 领取），排队干等纯属浪费预算。

    quota 缺失（尚未刷新、或上游不提供额度）时按「仍可恢复」处理：宁可
    保留旧的排队行为，也不拿空数据把账号判死。
    """
    quota = getattr(account, "quota", None) or {}
    windows = [w for w in quota.values() if isinstance(w, dict)]
    if not windows:
        return True

    def _epoch(v) -> float:
        """expires_at 归一成 epoch：线上是 epoch 秒，快照/mock 出现过 ISO8601
        字符串——裸 `> now` 会 str/float 混型 TypeError（1310 排队复现，2026-11-09）。
        归一失败（None/垃圾值）按「仍可恢复」处理，与 quota 缺失同向。"""
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                try:
                    from datetime import datetime as _dt
                    return _dt.fromisoformat(v.replace("Z", "+00:00")).timestamp()
                except (ValueError, OSError):
                    return 0.0
        return 0.0

    return any(_epoch(w.get("expires_at")) > now for w in windows)


def _has_recoverable_account(provider: str, now: float | None = None) -> bool:
    """池内是否存在「预期自动恢复、排队等待划算」的账号。

    EXHAUSTED 且额度窗口未到期：额度分钟级滚动恢复，排队等待划算。
    EXHAUSTED 但窗口已过期：不会自动重置，排队只会把连接吊到 CDN 边缘
    ~100s 硬超时（HTTP 524），且会越过 30s early-flush 宽限期先发 200 ——
    客户端据此把「额度耗尽」误判成成功（2026-10-08 实测：4 号全部窗口
    过期时，请求排队 240s 后被 early-flush 成 HTTP 200 + SSE error）。
    COOLING 不排：5xx/风控冷却已知时长（300s 起、风控最长 24h），干等只会
    把连接吊死，应快速 503 让客户端自行重试；429 刚失败而仍 ACTIVE 的号
    秒级内也不会好转，同理不排队。
    （2026-10-07 实测：对这两类排队会把单账号 429 用例拖成 240s+ 挂死、
    上游被无谓重打十几次。）
    ACTIVE 但并发占满：槽位随在途流结束必然释放（秒级到分钟级），且排队
    轮询只做本地选号（满号被 skip，不进 _try_account、不重带重试梯），
    对上游零压力 —— 排队等槽远优于秒回 503（2026-10-30 实测：openclaw
    多会话把全部 ACTIVE 号打满，原逻辑直接 503，被 OmniRoute 一次 503
    锁模型 12min，放大成持续故障）。
    """
    now = now if now is not None else time.time()
    if any(
        a.enabled and a.status == Status.EXHAUSTED
        and _quota_windows_live(a, now)
        for a in store.list_accounts(provider)
    ):
        return True
    limit = _limit()
    if limit > 0 and any(
        a.enabled and a.status == Status.ACTIVE
        and _inflight.get(a.id, 0) >= limit
        for a in store.list_accounts(provider)
    ):
        return True
    # v2.9.32（magpie startGate 对照实测）：上游并发槽饱和（3008 系）是
    # 上游侧残留占用（实测中位 64.7s 释放）。被 3008 打过的账号进
    # _busy_gate_until 本地闸门：闸门期新请求只排队等，不敲上游；到期
    # 自动放一个进来试探。仅这类「有明确恢复时刻」的故障值得排队，
    # 回到 2026-10-07 修掉的「普通 429 也空等 240s」老路。
    if _busy_gate_active():
        return True
    return False


def _is_risk_control(status_code: int, text: str) -> bool:
    """风控信号判定（3012「unusual activity」/ messages 端点 405）。

    与验证码挑战互斥：调用点已先排除 challenge 形态。命中即账号级风控，
    需指数退避冷却，而非直接回传客户端错误（会导致下次立刻重打、加剧风控）。
    """
    if status_code in constants.RISK_CONTROL_HTTP_STATUSES:
        return True
    low = text.lower()
    return any(m.lower() in low for m in constants.RISK_CONTROL_MARKERS)


def _is_concurrency_hit(text: str) -> bool:
    """429 body 是否为「并发争用」（区别于账号级频控，等待策略不同）。

    业务码必须解析后精确比较：logid 是 20 位随机数字，裸子串 "3009" 会撞车
    （constants.CONCURRENCY_BUSINESS_CODE 注记的同款教训）；非 JSON 形态退回
    文案匹配兜底。3008/user concurrency 同样秒级释放，与 3009 一起短退避。
    """
    code = business_error.business_code(_safe_json(text))
    return code in (constants.CONCURRENCY_BUSINESS_CODE, 3008) or "concurrency limit" in text.lower()


def _is_model_concurrency_hit(text: str) -> bool:
    """仅 3009「模型并发已满」：槽位秒级释放，不该扣号（扣了会让本来马上
    可用的账号白等 30s）。3008 是账号级频控，必须扣号——2026-11-07 线上
    429 主力就是 3008，误按 3009 语义放行会导致扣号不生效、att=4 照旧。
    """
    code = business_error.business_code(_safe_json(text))
    return code == constants.CONCURRENCY_BUSINESS_CODE


def _is_rate_limit_hit(text: str) -> bool:
    """429/业务码是否为 1302/1303「真限流」（Z.ai 频控，非并发争用）。

    与并发相反：继续原地重试只会加重，应账号休息并下沉下一渠道。只做业务码
    精确比较，不做裸子串（logid 撞车教训）。
    """
    return business_error.business_code(_safe_json(text)) in constants.RATE_LIMIT_429_CODES


def _is_edge_block(status_code: int, text: str) -> bool:
    """边缘防火墙按 IP 封禁的 405（区别于账号级 3012 风控）。

    3012 的 405 文案是 unusual activity（账号风控）；边缘封禁页指向
    network's address was blocked，不带 code 3012。前者账号退避即可，后者
    说明出口 IP 已被整体拒绝——继续换账号只会把全池送进去，必须全进程暂停。
    """
    if status_code not in constants.RISK_CONTROL_HTTP_STATUSES:
        return False
    if business_error.business_code(_safe_json(text)) == constants.RISK_BUSINESS_CODE:
        return False
    low = (text or "").lower()
    if any(m.lower() in low for m in constants.RISK_CONTROL_MARKERS):
        return False
    if any(m in low for m in constants.EDGE_BLOCK_MARKERS):
        return True
    return "<html" in low or "errors.aliyun.com" in low


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


def _apply_risk_strike(account: Account, signal: str, req_id: str,
                       model: str | None = None,
                       channel: str | None = None) -> str:
    """风控打击落地（405/3012 与 HTTP 200 内嵌业务码 3012 共用）。

    两种曲线，由 RISK_LANE_SCOPED 切换（关闭即完全回到旧行为，灰度用）：

    - 通道级（默认）：只冷却真正被打的那条 lane（plan/apikey × 模型归一键），
      不动整号 status。依据 zai-org/feedback #716：3012 是通道级封锁（同账号
      付费 Coding Plan 腿 0 拦截、billing 全程正常），旧整号罚会把健康回退腿、
      自动领包轮、额度刷新一起杀死。指数 15min→2h 封顶，累计 4 次该 lane
      长冷 6h，到期自动回轮询（探测轮可提前复活）。
    - 旧整号曲线：指数退避冷却自动恢复，累计 RISK_BAN_STRIKES 次升级禁用
      （人工恢复）；升级禁用且 RISK_AUTO_ROTATE 开启时自动换发全新设备指纹
      并补跑安装序。

    channel：命中请求实际走的通道（按实际 URL 判定，见调用点）。
    返回 penalty_note 供调用方日志。
    """
    if settings.RISK_LANE_SCOPED:
        lane = account.lane_key(channel or "plan", model or "")
        res = account.risk_lane_penalty(
            lane, settings.RISK_LANE_COOLDOWN, settings.RISK_LANE_COOLDOWN_MAX,
            settings.RISK_LANE_BAN_STRIKES, settings.RISK_LANE_BAN_COOLDOWN,
            settings.RISK_STRIKE_DECAY_SECONDS,
        )
        until_str = time.strftime("%H:%M", time.localtime(res["until"]))
        account.last_error = (
            f"命中风控 ({signal})，通道 {lane} 冷却至 {until_str}"
            f"（第 {res['strikes']} 次"
            f"{'，全部服务通道均在冷却，等到期自动恢复' if res['all_blocked'] else ''}）"
        )
        if res["all_blocked"] and account.status not in (Status.INVALID, Status.DISABLED):
            # 全部服务通道都在冷却 → 整号退化 COOLING（到期自愈，不等人工），
            # 状态位与 lane 字典保持一致；「命中风控」前缀的 last_error 是
            # 探测轮识别此形态（区别于 5xx 普通冷却）的标记。
            account.status = Status.COOLING
            account.cooling_until = account.worst_lane_until() or res["until"]
        store.update_account(account)
        riskmetrics.record("risk", account.name, signal=signal, lane=res["lane"],
                           strikes=res["strikes"], model=model or "",
                           carrier="account")
        return (f"通道 {lane} 冷却至 {until_str}（第 {res['strikes']} 次"
                f"{'，整号全部服务通道均已冷却' if res['all_blocked'] else '，其余通道不受影响'}）")

    account.risk_penalty(
        settings.RISK_COOLDOWN_BASE, settings.RISK_COOLDOWN_MAX,
        settings.RISK_BAN_STRIKES, settings.RISK_STRIKE_DECAY_SECONDS,
    )
    if account.status == Status.DISABLED:
        account.last_error = (
            f"风控封禁 ({signal})，确认恢复后请在后台手动启用"
            f"（第 {account.risk_strikes:.0f} 次）"
        )
        note = f"已升级禁用（第 {account.risk_strikes:.0f} 次）"
        if settings.RISK_AUTO_ROTATE and _rotate_allowed(account):
            # 「风控后换设备重生」自动化：升级禁用即换发全新设备
            # （新 SKU + 新 device_mid），后台补跑安装序；账号重新
            # 启用时已是全新身份（billing/claim 头同源跟随档案）
            from ..fingerprint import profile_for, rotate
            from ..install import schedule_install

            old_mid = profile_for(account).device_mid[:8]
            rotate(account)
            account.installed_at = None
            account.rotate_count = int(account.rotate_count or 0) + 1
            account.last_rotate_at = time.time()
            schedule_install(account)
            logs.warn(
                req_id,
                f"账号 {account.name} 风控禁用，已自动换发设备指纹"
                f"（旧 mid {old_mid}，累计第 {account.rotate_count} 次）",
            )
        elif settings.RISK_AUTO_ROTATE:
            # 换机频控护栏（修身份抖动循环）：间隔内的再次禁用不再换机，
            # 上游看到的设备身份保持稳定（换机过频本身就是风控信号）
            since_min = int((time.time() - float(account.last_rotate_at or 0)) // 60)
            logs.warn(
                req_id,
                f"账号 {account.name} 风控禁用，距上次换指纹仅 {since_min} 分钟，"
                f"按频控（≥{settings.RISK_ROTATE_MIN_INTERVAL // 3600}h）不换机",
            )
    else:
        cool_until = time.strftime(
            "%H:%M", time.localtime(account.cooling_until)
        ) if account.cooling_until else "?"
        account.last_error = (
            f"命中风控 ({signal})，指数退避冷却至 {cool_until}（第 {account.risk_strikes:.0f} 次）"
        )
        note = f"冷却至 {cool_until}（第 {account.risk_strikes:.0f} 次）"
    riskmetrics.record("risk", account.name, signal=signal, lane="account",
                       strikes=account.risk_strikes, model=model or "",
                       carrier="account")
    store.update_account(account)
    return note


_pool_starved_since: float = 0.0  # 整池断粮起始时刻（0=健康，边沿判定用）
_pool_starved_logged_at: float = 0.0  # 上次记录断粮的时刻（持续期防抖）
_POOL_STARVED_DEBOUNCE = 3600.0  # 断粮持续期 1h 记一次（恢复即清）


def _note_pool_starved(provider: str, model_key: str) -> None:
    """整池断粮边沿记录（2026-11-10 主人要求：只写服务日志，不再发 TG）。

    触发：全池 exhausted 且上游无可领套餐（_recovery 判 quota）→ warn 记开始，
    持续期每 1h 记一次；额度恢复（首个上游成功）→ info 记结束并带持续时长。
    TG 通知已下线（曾因恢复不防抖刷屏），需要看断粮就 grep 日志：
    `journalctl --user -u zcode2api | grep 整池断粮`。
    best-effort，绝不影响请求。
    """
    global _pool_starved_since, _pool_starved_logged_at
    try:
        now = time.time()
        accts = store.list_accounts(provider)
        starving = bool(accts) and all(
            (not a.enabled) or a.status == Status.EXHAUSTED for a in accts
        ) and any(a.enabled and a.status == Status.EXHAUSTED for a in accts)
        if starving:
            if not _pool_starved_since:
                _pool_starved_since = _pool_starved_logged_at = now
                logs.warn("pool", f"整池断粮开始：全部账号额度用尽且上游无可领套餐"
                                  f"（模型 {model_key or '全部'}），自动领取轮持续探测中")
            elif now - _pool_starved_logged_at >= _POOL_STARVED_DEBOUNCE:
                _pool_starved_logged_at = now
                held = int((now - _pool_starved_since) // 60)
                logs.warn("pool", f"整池断粮持续 {held} 分钟"
                                  f"（模型 {model_key or '全部'}），上游仍无套餐")
        else:
            if not _pool_starved_since:
                return
            held = int((now - _pool_starved_since) // 60)
            _pool_starved_since = _pool_starved_logged_at = 0.0
            logs.info("pool", f"额度已恢复（断粮持续 {held} 分钟），请求可正常服务")
    except Exception as err:  # noqa: BLE001 - 记录失败绝不影响请求
        logs.warn("pool", f"断粮状态记录失败（忽略）: {err}")


def _recovery(provider: str, model_key: str) -> tuple[str | None, float | None]:
    """池内最早能重新承接该模型的时刻与来源（kind, wait）。

    kind "model" = 该模型无资源包（429 model_no_quota 语义）；"lane" = 服务
    通道被风控冷却（503 no_available_account 语义，携带 Retry-After）。两类
    冷却字段分立、不能共用错误类型：把风控封锁报成 model_no_quota 会误导
    客户端换模型重试（封锁按通道，换啥模型都没用）。

    "quota"（2026-10-30，2026-10-31 改 402）= 池内全部号额度耗尽（EXHAUSTED）。
    报 402 而非 503/429：503 会被下游（OmniRoute）当 node 故障熔断连坐健康
    模型；429 在其分类器里对 api-key 连接无条件判 RATE_LIMITED（不看文案），
    令「仅配额耗尽回退」失效且给连接打限流标记。402 无条件判
    QUOTA_EXHAUSTED（cooldownMs=0），恰好命中其「仅配额耗尽回退」语义。

    恢复时刻（quota）：默认**当天 23:00（北京时区）**——实测上游 balance
    窗口的 expires_at 不可信（余额 2.8 亿仍报额度耗尽，grant 腿与 balance
    是两套账），23:00 是日套餐重置的运营兜底时点；池内若有**待生效套餐**
    （plans 的 entitlements 带未来 starts_at，已领未生效/预约下发），取最早
    生效时刻（早于 23:00 才采用）。下一轮自动领取轮也可能带回新套餐，但
    领取是「尽力而为」（名额可能用完），不作为承诺时刻。
    """
    model_wait = store.next_model_uncooldown_at(provider, model_key)
    lane_wait = store.next_lane_uncooldown_at(provider, model_key)
    now = time.time()
    quota_wait: float | None = None
    exhausted = [a for a in store.list_accounts(provider)
                 if a.enabled and a.status == Status.EXHAUSTED
                 and not a.has_lane_cooling() and not a.is_cooling()]
    if exhausted and not any(
            a.is_selectable(model=model_key) for a in store.list_accounts(provider)):
        quota_wait = _quota_reset_at(exhausted, now)
    if model_wait and (not lane_wait or model_wait <= lane_wait):
        return "model", model_wait
    if quota_wait and (not lane_wait or quota_wait <= lane_wait):
        _note_pool_starved(provider, model_key)
        _note_pool_exhausted(trigger=None)  # 402 出口也让池闸评估/武装
        return "quota", quota_wait
    if lane_wait:
        return "lane", lane_wait
    # 429 扣号/全池静默/本地闸门的到期时刻 → 429+Retry-After（非 503）：
    # 短窗口必然恢复，语义是「稍后重试必有」，OmniRoute 对 429 走限流退避。
    if _busy_held():
        return "busy", _busy_hold["until"]
    gate_next = _busy_gate_next()
    if gate_next:
        return "busy", gate_next
    key = Account.model_key(model_key)
    holds = [float(a.rate_hold.get(key) or 0)
             for a in store.list_accounts(provider) if a.enabled]
    holds = [u for u in holds if u > now]
    if holds:
        return "rate", min(holds)
    return None, None


def _quota_reset_at(accounts: list, now: float) -> float:
    """额度耗尽后的承诺恢复时刻（口径真源见 app/quota_windows.py）。

    旧实现在本文件里自带 _bj_today_at 与毫秒归一，「几点回血」类观测想复用
    就得抄一份或循环 import 网关。现抽到共享纯函数模块，本处只做转发，
    保持所有既有调用点（含单测 monkeypatch）签名不变。
    """
    return quota_windows.quota_reset_at(accounts, now)


def _bj_today_at(hour: int, minute: int, now: float) -> float:
    """兼容旧调用/测试：转发到共享实现。"""
    return quota_windows.bj_today_at(hour, minute, now)


def _rotate_allowed(account: Account) -> bool:
    """风控自动换指纹频控：距上次换发满 RISK_ROTATE_MIN_INTERVAL 才允许再换。

    last_rotate_at 为空 = 从未自动换过，放行。lane 曲线下极少触达（基本不再
    升级禁用），主要保护仍走旧整号曲线的灰度场景与存量身份抖动循环。
    """
    last = float(account.last_rotate_at or 0)
    if last <= 0:
        return True
    return time.time() - last >= settings.RISK_ROTATE_MIN_INTERVAL


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
    bare = (model or "").split("/", 1)[1] if "/" in (model or "") else (model or "")
    if bare.lower() in allowed:
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
    """列出可用模型（Anthropic /v1/models 风格，Magpie 命名空间前缀），动态生成。

    合并语义（上游 e038650 + fork 动态名单）：ID 加 `provider/` 前缀（上游
    Magpie 命名空间，裸名仅作向后兼容路由不入目录），名单仍取账号池全部
    entitlements capabilities 的并集（fork 增强，套餐变化自动跟随），展示
    规格沿用 fork 的 MODEL_CATALOG 字典目录。
    """
    # 动态名单（fork）：entitlements capabilities 并集，标准化大小写；
    # ID 加 provider 前缀（上游 Magpie 命名空间）。池空时回退静态目录前缀形态。
    caps: set[str] = set()
    caps_provider: dict[str, set[str]] = {}
    for prov in ("zai", "bigmodel"):
        for acc in store.list_accounts(prov):
            for plan in acc.plans or []:
                for ent in plan.get("entitlements") or []:
                    for c in ent.get("capabilities") or []:
                        if c.startswith("model:") and len(c) > 6:
                            caps.add(c[6:])
                            caps_provider.setdefault(c[6:].lower(), set()).add(prov)
    catalog = {m["id"]: m for m in getattr(constants, "MODEL_CATALOG", [])}
    ids: list[str] = []
    for c in sorted(caps):
        canon = MODEL_NAME_MAP.get(c.lower(), c)
        if canon not in ids:
            ids.append(canon)
    if not ids:
        # 池空回退（上游语义）：静态命名空间目录原样输出（保留 bigmodel 分组），
        # 保证冷启动仍可发现模型。
        data = []
        for m in getattr(constants, "MODEL_CATALOG_NAMESPACED", []):
            if "/" not in m:
                continue
            spec = catalog.get(m.split("/", 1)[1].lower())
            data.append({
                "id": m,
                "type": "model",
                "display_name": m.split("/", 1)[1],
                "created_at": "2025-01-01T00:00:00Z",
                **({"context_window": spec["context_window"],
                    "max_output_tokens": spec["max_output_tokens"]} if spec else {}),
            })
        return {"object": "list", "data": data}
    data = []
    for i in ids:
        # provider 归属：fork 的 bigmodel OpenAI 专属模型归 bigmodel，其余按
        # entitlements 实际来源 provider 分组（无则默认 zai）。
        provs = caps_provider.get(i.lower()) or (
            ["bigmodel"] if i.lower() in getattr(constants, "BIGMODEL_OPENAI_MODELS", ()) else ["zai"])
        spec = catalog.get(i.lower()) or catalog.get(i)
        for prov in provs:
            data.append({
                "id": f"{prov}/{i}",
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
    except (ClientDisconnect, json.JSONDecodeError, ValueError):
        return JSONResponse({"error": {"message": "请求体不是合法 JSON", "type": "invalid_request"}}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse(
            {"error": {"message": "请求体必须是 JSON 对象", "type": "invalid_request_error"}},
            status_code=400,
        )

    incoming_headers = dict(request.headers)
    requested_model = str(body.get("model") or "")
    if requested_model.startswith("bigmodel/"):
        model_name = requested_model.split("/", 1)[1].strip().lower()
        if model_name in constants.BIGMODEL_OPENAI_MODELS:
            return JSONResponse(
                {"error": {"message": "该模型仅支持 OpenAI 兼容端点 /v1/chat/completions",
                           "type": "unsupported_endpoint"}},
                status_code=400,
            )
    provider = _detect_provider(body, request.headers)
    if provider == "bigmodel" and not store.bigmodel_channel_enabled():
        # fork 成本闸（保留）：bigmodel Key 通道默认关闭（欠费时上游 429 原地
        # 重试会挂起约 2 分钟），显式 `bigmodel/` 前缀请求在入口即时拒绝而非
        # 让调度去试一个必然失败的通道；开关打开时放行（Magpie 模式可用）。
        return JSONResponse(
            {"error": {"message": "bigmodel Key 回退通道已关闭（管理后台「设置」可开启）",
                       "type": "channel_disabled"}},
            status_code=403,
        )
    body = _normalize_body(body)
    # 套餐外模型即时拒绝（上游 3006）：否则会去排队等一个绝无可能的账号，
    # 最终出「额度耗尽」误导性 503。必须在 _dispatch 之前。
    if (rejected := _reject_model_not_allowed(requested_model)) is not None:
        return rejected
    # 验证码页面由本服务托管，端口取实际请求端口（兼容任意启动端口）
    port = request.url.port or settings.PORT
    # X-Probe 打标：本次请求若为探测（模型测试/健康检查）则标记，供用量分流
    _probe_flag.set(is_probe_request(request.headers))

    meta = _client_meta(request)
    req_id = secrets.token_hex(8)
    logs.req(req_id, str(body.get("model") or "-"), bool(body.get("stream")), _last_user_text(body))
    reqlog.begin(req_id, "messages", str(body.get("model") or "-"),
                 bool(body.get("stream")), _last_user_text(body),
                 path=meta["path"], client_ip=meta["client_ip"], ua=meta["ua"],
                 probe=_probe_flag.get(), model_req=requested_model,
                 **_body_meta(body))
    session_ctx.tag(req_id, body, bool(body.get("stream")))
    # 客户端指纹（上游 2.7.17，2026-11-04 排障加装）：周期短探测打线上时无法
    # 定位来源——CF 隧道绕过 nginx 无访问日志、hub 也不记录调用方。真实客户端
    # IP 以 CF-Connecting-IP 为准（经 cloudflared），直连本机时回退
    # X-Forwarded-For / 本地环回。UA 截断防日志爆炸。
    logs.info(req_id, f"client={meta['client_ip']} ua={meta['ua'] or '?'}")

    if body.get("stream") and settings.EARLY_FLUSH_GRACE > 0:
        # Early flush：宽限期内保持真实状态码语义；超期先发 200+SSE 头
        # 停掉 CDN 计时，此后失败降级 SSE error 事件（见 earlyflush 块注释）
        task = asyncio.create_task(_dispatch_guarded(req_id, body, incoming_headers, port, provider))
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
            result = await _dispatch_guarded(req_id, body, incoming_headers, port, provider)
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


def _sse_ping_chunk(style: str) -> str:
    """等待期心跳的真 SSE 事件（非注释行）。

    OmniRoute 的 stream readiness 只认「event:/data: + 空行」的完整事件，
    注释行（":" 开头）永远不算 —— 纯注释心跳的流在它那里是零事件流，
    STREAM_READINESS/首事件超时后判 502/504 并按 combo 回退下一档。
    Anthropic 官方协议有 event: ping；OpenAI 无 ping 语义，用带 usage 占位
    的 chunk 会污染客户端计数，故用 data 空对象（多数客户端忽略未知字段，
    StreamConverter 对未知 chunk 容忍）。
    """
    if style == "openai":
        # OpenAI 无 ping 事件语义；发标准空 delta chunk（id/choices 齐全、
        # delta 空对象）—— OpenAI 客户端与 OmniRoute 都把它当空操作忽略。
        payload = {"id": "chatcmpl-keepalive", "object": "chat.completion.chunk",
                   "created": 0, "model": "keepalive",
                   "choices": [{"index": 0, "delta": {}, "finish_reason": None}]}
        return f"data: {json.dumps(payload)}\n\n"
    return 'event: ping\ndata: {"type": "ping"}\n\n'


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
    sent_ping = False  # 首个真事件已发（readiness 放行后改回注释心跳省流）
    while True:
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=beat)
            break
        except TimeoutError:
            # v2.9.34：心跳用真 ping 事件。注释行在 OmniRoute stream readiness
            # 里不算事件（s() 对 ":" 行直接跳过），纯注释流 80s 判 502 回退。
            if not sent_ping:
                yield _sse_ping_chunk("anthropic")
                sent_ping = True
            else:
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
        record_usage(up.model)
        yield f"data: {text}\n\n"
        return
    try:
        async for chunk in up.resp.aiter_bytes():
            yield chunk
        logs.req_ok(req_id)
        reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code)
        record_usage(up.model)
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
    sent_ping = False
    while True:
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=beat)
            break
        except TimeoutError:
            # 同 anthropic 侧：真 ping 事件放行 OmniRoute readiness（注释行不算）
            if not sent_ping:
                yield _sse_ping_chunk("openai")
                sent_ping = True
            else:
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
                         input_tokens=usage.get("input_tokens"), output_tokens=usage.get("output_tokens"),
                         cache_read=_cache_read(usage))
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
                         output_tokens=conv.usage.get("completion_tokens"),
                         cache_read=_cache_read(conv.usage))
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


def _no_quota_response(req_id: str, model: str, message: str, wait: float | None) -> JSONResponse:
    """按模型冷却给出可预期等待时，用 429 + Retry-After 表达（2026-10-28 语义整改）。

    503 不带 Retry-After 会被上游网关（OmniRoute）当「服务坏了」计入 node 级
    熔断失败并激进重试，造成同节点健康模型连坐；429 + Retry-After 则进入其
    useUpstreamRetryHints 定向退避，等到期再试，包到账即产能。无冷却信息
    （纯无号/并发满/队列超时）仍回 503 no_available_account，语义不变。
    """
    if wait and wait > time.time():
        retry_after = max(1, int(wait - time.time() + 0.999))
        return JSONResponse(
            {"error": {"message": message, "type": "model_no_quota",
                       "code": "no_quota_for_model", "model": model,
                       "next_available_at": round(wait)},
             "retry_after": retry_after},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
    return JSONResponse(
        {"error": {"message": message, "type": "no_available_account"}},
        status_code=503,
    )


async def _bigmodel_openai_chat(payload: dict, requested_model: str, request: Request):
    """bigmodel 资源包模型的 OpenAI 直通路径。

    这些模型在 Anthropic 兼容端点会被 Coding Plan 门禁拦（1309），
    但 OpenAI 兼容端点可用；这里直接透传 OpenAI 请求，并按模型粒度
    做 1113/1309 冷却与换号。
    """
    model_name = requested_model.split("/", 1)[1].strip().lower()
    stream = bool(payload.get("stream"))
    req_id = secrets.token_hex(8)
    preview = ""
    msgs = payload.get("messages")
    if isinstance(msgs, list):
        for m in reversed(msgs):
            if isinstance(m, dict) and isinstance(m.get("content"), str):
                preview = m["content"][:80]
                break
    logs.req(req_id, model_name, stream, preview)
    meta = _client_meta(request)
    reqlog.begin(req_id, "chat", model_name, stream, preview,
                 path=meta["path"], client_ip=meta["client_ip"], ua=meta["ua"],
                 probe=is_probe_request(request.headers), model_req=requested_model,
                 **_body_meta(payload))
    session_ctx.tag(req_id, payload, stream)
    tried: set[str] = set()
    last_error = ""
    t0 = time.time()
    for _ in range(MAX_ACCOUNT_ATTEMPTS):
        account = store.select("bigmodel", skip_ids=tried, model=model_name)
        if account is None:
            break
        tried.add(account.id)
        if not account.api_key:
            continue
        body = {**payload, "model": model_name}
        url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
        headers = {"Authorization": f"Bearer {account.api_key}", "Content-Type": "application/json"}
        # 账号级出口代理：proxy_kwargs() 未配置时返回 {}，行为与直连一致
        client = httpx.AsyncClient(timeout=httpx.Timeout(connect=30.0, read=None, write=120.0, pool=30.0),
                                   **account.proxy_kwargs())
        reqlog.mark_account(req_id, account.name, account.mode)
        reqlog.mark_channel(req_id, "apikey")
        reqlog.mark_egress(req_id, "account" if account.proxy_kwargs()
                           else egress.egress_for(url),
                           account_name=account.name)
        # 打标：本请求已发起上游调用（监控区分调度/上游失败，同 zai 主链路）
        reqlog.mark_upstream(req_id)
        try:
            if stream:
                req = client.build_request("POST", url, headers=headers, json=body)
                resp = await client.send(req, stream=True)
                if resp.status_code == 200:
                    account.use_count += 1
                    account.last_used_at = time.time()
                    account.record_result(True, f"HTTP 200 · {model_name} · openai-stream")
                    account.clear_model_cooling(model_name)
                    account.clear_rate_hold(model_name)  # 上游 2xx 成功即放回 429 扣号
                    store.update_account(account)
                    t_first = time.time() - t0
                    reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                    reqlog.finish_ok(req_id, t_first=t_first, status=200)
                    record_usage(model_name)

                    async def gen():
                        try:
                            async for chunk in resp.aiter_raw():
                                yield chunk
                        finally:
                            await resp.aclose()
                            await client.aclose()

                    return StreamingResponse(gen(), media_type="text/event-stream")
                text = (await resp.aread()).decode("utf-8", "replace")
                await resp.aclose()
                await client.aclose()
            else:
                resp = await client.post(url, headers=headers, json=body)
                text = resp.text
                if resp.status_code == 200:
                    data = resp.json()
                    usage = data.get("usage") or {}
                    account.use_count += 1
                    account.last_used_at = time.time()
                    account.record_result(True, f"HTTP 200 · {model_name} · openai")
                    account.clear_model_cooling(model_name)
                    account.clear_rate_hold(model_name)  # 上游 2xx 成功即放回 429 扣号
                    store.update_account(account)
                    reqlog.finish_ok(req_id, t_first=time.time() - t0, status=200,
                                     input_tokens=usage.get("prompt_tokens"),
                                     output_tokens=usage.get("completion_tokens"),
                                     cache_read=_cache_read(usage))
                    record_usage(model_name, usage.get("prompt_tokens"), usage.get("completion_tokens"))
                    await client.aclose()
                    return JSONResponse(data)
                await client.aclose()
        except httpx.HTTPError as err:
            await client.aclose()
            last_error = f"连接失败: {err}"
            reqlog.mark_attempt(req_id, account.name, account.mode, channel="apikey",
                                ms=(time.time() - t0) * 1000, error=last_error)
            account.record_result(False, last_error)
            store.update_account(account)
            continue
        last_error = text[:300]
        if resp.status_code != 200:
            reqlog.mark_attempt(req_id, account.name, account.mode, channel="apikey",
                                status=resp.status_code, ms=(time.time() - t0) * 1000,
                                error=f"HTTP {resp.status_code}: {text[:80]}")
        if resp.status_code == 429 and ("1113" in text or "1309" in text or "insufficient balance" in text.lower()):
            until = account.cool_model(model_name, settings.MODEL_NO_QUOTA_COOLDOWN_BASE,
                                       settings.MODEL_NO_QUOTA_COOLDOWN_MAX)
            account.record_result(False, f"{resp.status_code} 无资源包（{model_name} 冷却至 "
                                        f"{time.strftime('%H:%M', time.localtime(until))}）")
            store.update_account(account)
            logs.warn(req_id, f"账号 {account.name} {model_name} 无资源包，冷却后换号")
            continue
        reqlog.finish_error(req_id, last_error, status=resp.status_code, kind="upstream")
        return JSONResponse({"error": {"message": last_error, "type": "upstream_error"}},
                            status_code=resp.status_code)
    wait = store.next_model_uncooldown_at("bigmodel", model_name)
    reqlog.finish_error(req_id, last_error or "所有 bigmodel 账号均不可用",
                        status=429 if wait else 503,
                        kind="quota" if wait else "no_account")
    return _no_quota_response(req_id, model_name, last_error or "所有 bigmodel 账号均不可用", wait)


async def _run_compat_endpoint(request: Request, kind: str, translate,
                               stream_hook, finalize):
    """OpenAI 风格端点公共骨架（/v1/chat/completions 与 /v1/responses 共用）。

    流程：解析 JSON → translate(payload) 得 anthropic body → 归一 → 调度
    （多账号/重试/验证码与 /v1/messages 同一条路径）→ 按客户端要求走流式或
    非流式消费。三方差异全部参数化：
      translate(payload)  -> (anthropic_body | None, err | None)
      stream_hook(up, model, req_id) -> StreamingResponse
      finalize(data, model) -> dict   # 非流式：anthropic message → 客户端格式

    fork 增强（2.6.x 并入）：bigmodel/ 前缀模型直通、套餐外模型即时拒绝（3006）、
    X-Probe 探测打标、early flush（复用 /v1/messages 同一套宽限语义）。
    """
    try:
        payload = await request.json()
    except (ClientDisconnect, json.JSONDecodeError, ValueError):
        # ClientDisconnect：客户端在读请求体阶段断开——回包无人收，仅为不刷
        # starlette 完整堆栈（err.log 曾每次断开一条 Traceback）
        return JSONResponse({"error": {"message": "请求体不是合法 JSON", "type": "invalid_request_error"}}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"error": {"message": "请求体必须是 JSON 对象", "type": "invalid_request_error"}}, status_code=400)

    requested_model = str(payload.get("model") or "")
    if requested_model.startswith("bigmodel/"):
        model_name = requested_model.split("/", 1)[1].strip().lower()
        if model_name in constants.BIGMODEL_OPENAI_MODELS:
            return await _bigmodel_openai_chat(payload, requested_model, request)

    body, err = translate(payload)
    if err or body is None:
        return JSONResponse({"error": {"message": err or "请求体不合法", "type": "invalid_request_error"}}, status_code=400)

    incoming_headers = dict(request.headers)
    provider = _detect_provider(body, request.headers)
    if provider == "bigmodel" and not store.bigmodel_channel_enabled():
        # fork 成本闸（保留）：bigmodel Key 通道默认关闭（欠费时上游 429 原地
        # 重试会挂起约 2 分钟），显式 `bigmodel/` 前缀请求在入口即时拒绝而非
        # 让调度去试一个必然失败的通道；开关打开时放行（Magpie 模式可用）。
        return JSONResponse(
            {"error": {"message": "bigmodel Key 回退通道已关闭（管理后台「设置」可开启）",
                       "type": "channel_disabled"}},
            status_code=403,
        )
    body = _normalize_body(body)
    # 套餐外模型即时拒绝（上游 3006）：否则会去排队等一个绝无可能的账号，
    # 最终出「额度耗尽」误导性 503。必须在 _dispatch 之前。
    if (rejected := _reject_model_not_allowed(requested_model)) is not None:
        return rejected
    port = request.url.port or settings.PORT
    # X-Probe 打标：本次请求若为探测（模型测试/健康检查）则标记，供用量分流
    _probe_flag.set(is_probe_request(request.headers))

    meta = _client_meta(request)
    req_id = secrets.token_hex(8)
    logs.req(req_id, str(body.get("model") or "-"), bool(payload.get("stream")), _last_user_text(body))
    reqlog.begin(req_id, kind, str(body.get("model") or "-"),
                 bool(payload.get("stream")), _last_user_text(body),
                 path=meta["path"], client_ip=meta["client_ip"], ua=meta["ua"],
                 probe=_probe_flag.get(), model_req=requested_model,
                 **_body_meta(body))
    session_ctx.tag(req_id, body, bool(payload.get("stream")))

    if payload.get("stream") and settings.EARLY_FLUSH_GRACE > 0:
        # Early flush：宽限期内保持真实状态码语义；超期先发 200+SSE 头
        # 停掉 CDN 计时，此后失败降级 SSE error 事件（见 earlyflush 块注释）
        task = asyncio.create_task(_dispatch_guarded(req_id, body, incoming_headers, port, provider))
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
            result = await _dispatch_guarded(req_id, body, incoming_headers, port, provider)
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
        # 同步直通（_try_account 已读完 body 并收口监控含 tokens）：成功时按
        # 端点各自的 finalize 转形状（chat → chat.completion / responses →
        # response 对象）；错误响应原样返回
        raw = getattr(result, "body", None)
        if result.status_code < 300 and raw:
            data = _safe_json(raw.decode("utf-8", "ignore"))
            if isinstance(data, dict) and data.get("type") == "message":
                return JSONResponse(finalize(data, str(body.get("model") or "")))
        return result

    model = str(body.get("model") or "")
    if payload.get("stream"):
        try:
            return stream_hook(result, model, req_id)
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
                     input_tokens=usage.get("input_tokens"), output_tokens=usage.get("output_tokens"),
                     cache_read=_cache_read(usage))
    record_usage(model, usage.get("input_tokens"), usage.get("output_tokens"))
    return JSONResponse(finalize(data, model))


@router.post("/v1/chat/completions", dependencies=[Depends(verify_gateway_key)])
async def chat_completions(request: Request):
    """OpenAI Chat Completions 兼容端点（openai_compat 双向转换）。"""
    return await _run_compat_endpoint(
        request, "chat",
        translate=openai_to_anthropic,
        stream_hook=_openai_stream_response,
        finalize=anthropic_to_openai,
    )


def _translate_responses(payload: dict) -> tuple[dict | None, str | None]:
    """Responses → Chat Completions → Anthropic 两跳翻译。"""
    chat, err = responses_to_chat(payload)
    if err or chat is None:
        return None, err
    return openai_to_anthropic(chat)


@router.post("/v1/responses", dependencies=[Depends(verify_gateway_key)])
async def responses(request: Request):
    """OpenAI Responses API 兼容端点（入站 Responses → chat → anthropic，出站反向）。

    调度、重试、换号、验证码与 /v1/chat/completions 完全一致（公共骨架
    `_run_compat_endpoint`），只在两头做形状翻译。
    """
    return await _run_compat_endpoint(
        request, "responses",
        translate=_translate_responses,
        stream_hook=_responses_stream_response,
        finalize=lambda data, model: chat_to_response(anthropic_to_openai(data, model), model),
    )


@router.post("/v1/messages/count_tokens", dependencies=[Depends(verify_gateway_key)])
async def count_tokens(request: Request):
    """Anthropic count_tokens 端点：本地确定性估算。

    上游（Plan 通道与 api.z.ai）**没有**这个端点（2026-11-02 实测 404），但
    Anthropic SDK / Claude Code 会先调它做上下文预算，缺了会直接中断会话。
    这里只做估算：不占账号额度、不打上游、不需要可用账号（池空也能回）。

    估算对象是 **transform 之后**的形状（含官方 system 三块 + 日期轮，
    线上标定固定开销 ≈400 tok）——2026-11-03 实测：只算客户端原始 body 时
    6 tok 的请求实际计费 341，预算严重失真。
    """
    try:
        body = await request.json()
    except (ClientDisconnect, json.JSONDecodeError, ValueError):
        return JSONResponse({"error": {"message": "请求体不是合法 JSON", "type": "invalid_request"}}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse(
            {"error": {"message": "请求体必须是 JSON 对象", "type": "invalid_request_error"}},
            status_code=400,
        )
    model = body.get("model")
    if not isinstance(model, str) or not model.strip():
        return JSONResponse({"error": {"message": "必须提供 model 参数", "type": "invalid_request_error"}}, status_code=400)
    return {"input_tokens": estimate_input_tokens_after_transform(_normalize_body(body), model)}


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
                             output_tokens=conv.usage.get("completion_tokens"),
                             cache_read=_cache_read(conv.usage))
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


def _dedup_replay_worthy(provider: str, model: str) -> bool:
    """失败重放仅当波次暂停仍未到期（同一封锁态仍在）。

    暂停到期后的第一发试探请求必须放行——它承担波次再确认职责
    （RISK_FAIL_FAST 已把它限制为最多烧 1 次 strike）；lane 冷却不作为
    重放依据：冷却到期后的试探同样是恢复路径，零延迟优先。
    突发并发重试的防烧由在途合并（begin_inflight/wait_inflight）承担。
    """
    return time.time() < _risk_wave_until.get(provider, 0)


async def _dispatch_guarded(req_id, body, incoming_headers, port, provider):
    """风暴去重守卫（app/dedup.py）：同内容请求的失败重放与在途合并。

    包在 _dispatch 外层，四个调用点统一走本守卫。三档语义：
      - TTL 内同指纹失败 → 直接重放缓存响应（零调度零 strike）；
      - 非流式 + leader 在途 → 有界等待结局，失败重放、成功/超时放行；
      - 流式 → 不合并不缓存（真实结局在生成器里），仅登记在途防同拍。
    """
    if not settings.DEDUP_ENABLED:
        return await _dispatch(req_id, body, incoming_headers, port, provider)
    fp = dedup.fingerprint(body)
    if fp is None:
        return await _dispatch(req_id, body, incoming_headers, port, provider)
    hit = dedup.peek_failure(fp)
    if hit is not None and _dedup_replay_worthy(provider, str(body.get("model") or "")):
        status, payload = hit
        logs.info(req_id, f"风暴去重: {settings.DEDUP_TTL}s 内同内容失败，直接重放（不进调度）")
        # 收口修复（2026-11-04）：重放不经过 _dispatch，监控条目原先永远滞留
        # 「进行中」——既刷脏实时并发（client 口径虚高），也让 _inflight 无界增长。
        reqlog.mark_dedup(req_id, "replayed")
        reqlog.finish_error(req_id, f"风暴去重重放（{settings.DEDUP_TTL}s 内同内容失败）",
                            status=status, kind="dedup")
        return Response(content=payload, status_code=status, media_type="application/json")
    stream = bool(body.get("stream"))
    # 流式跳过登记（真实结局在生成器里）；非流式登记，True=leader / False=follower
    is_leader = stream or dedup.begin_inflight(fp)
    if not is_leader:
        outcome = await dedup.wait_inflight(fp, timeout=settings.DEDUP_FOLLOWER_WAIT)
        if outcome == "failed":
            status, payload = dedup.failure_of(fp)
            logs.info(req_id, "风暴去重: leader 同内容调度失败，重放其失败响应")
            reqlog.mark_dedup(req_id, "merged")
            reqlog.finish_error(req_id, "风暴去重: 并入 leader 失败结局",
                                status=status, kind="dedup")
            return Response(content=payload, status_code=status, media_type="application/json")
        # leader 成功/超时 → follower 正常走自己的调度（上游已被证明健康）
    result = await _dispatch(req_id, body, incoming_headers, port, provider)
    _dedup_note(fp, result, stream)
    return result


def _cache_read(usage) -> int | None:
    """上游 usage 里的缓存命中 token 数（Anthropic cache_read_input_tokens /
    OpenAI prompt_tokens_details.cached_tokens）；无则 None（会话亲和观测用）。"""
    if not isinstance(usage, dict):
        return None
    v = usage.get("cache_read_input_tokens")
    if v is None:
        v = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
    return v


def _dedup_note(fp: str, result, stream: bool) -> None:
    """把调度结局翻译给去重层：_Upstream = 成功（无 body 可缓存）；JSONResponse
    按状态码记。early-flush 流式的真实失败在生成器里，此处只登记「已产出上游流」。"""
    if isinstance(result, _Upstream):
        dedup.note_result(fp, 200, b"", stream)
        return
    dedup.note_result(fp, result.status_code, getattr(result, "body", b"") or b"", stream)


def _responses_stream_response(up: _Upstream, model: str, req_id: str) -> StreamingResponse:
    """上游 Anthropic SSE → Responses 事件族（经 chat chunk 中转，复用两套转换器）。"""
    chat_conv = StreamConverter(model)
    resp_conv = ResponsesStreamConverter(model)

    async def _iter():
        try:
            for out in resp_conv.start():
                yield out
            async for line in up.resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data_str = line[5:].strip()
                if not data_str:
                    continue
                evt = _safe_json(data_str)
                if not isinstance(evt, dict):
                    continue
                for chunk in chat_conv.feed_events(evt):
                    for out in resp_conv.feed_chunk(chunk):
                        yield out
            for out in resp_conv.done():
                yield out
            logs.req_ok(req_id)
            reqlog.finish_ok(req_id, t_first=up.t_first, status=up.resp.status_code,
                             input_tokens=chat_conv.usage.get("prompt_tokens"),
                             output_tokens=chat_conv.usage.get("completion_tokens"))
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


def _provider_chain(provider: str) -> list[str]:
    """跨 provider 降级链（Magpie 模式，上游 e038650）。

    首选显式指定的 provider，其后按 `settings.PROVIDER_FALLBACK_ORDER` 里
    位于它之后的 provider 补齐（默认 zai → bigmodel）。裸模型名与 `zai/`
    前缀都落在 zai，zai 池没号时自动下沉 bigmodel；`bigmodel/` 前缀只在
    bigmodel 池内调度（order 里它已是最后一站）。
    """
    order = list(settings.PROVIDER_FALLBACK_ORDER)
    if provider in order:
        return order[order.index(provider):]
    return [provider]


async def _dispatch(req_id, body, incoming_headers, port, req_provider):
    """多账号负载均衡调度：_Upstream（成功）或 JSONResponse（错误）。

    调度准则（上游 e038650 频道梯框架 + fork 增强逐项挂回）：
    1. 同一上下文亲和性：多轮请求绑定同一账号，提高 Cache 命中与局部性；
    2. 新请求多账号负载均衡：分配到在飞负载最低的账号（同负载轮询平局）；
    3. 单账号并发限制（控制台可改，meta 即时生效）：满载或故障时无缝溢出至其他可用账号；
    4. 满载跳过不计入 MAX_ACCOUNT_ATTEMPTS；全满/无号 → 按恢复窗口定性返回；
    5. 跨 provider 降级（Magpie 模式）：当前 provider 池无可服务账号时按
       PROVIDER_FALLBACK_ORDER 下沉下一 provider（zai 池空自动用 bigmodel）；
       `bigmodel/` 前缀显式指定时已是链尾，不跨回 zai。
    6. fork 增强保留：请求级死线、风控波次快速失败（两账号确认制）、
       BUSY_HOLD 全池静默、startGate 排队闸、限界排队预算（非流式压在
       CDN ~100s 红线内）。

    port：历史签名保留（验证码页面端口），调度不再直接使用。
    req_provider：入站请求解析出的 provider（_detect_provider）；尾部 _recovery
    按它定性——for 循环变量 provider 会泄成链尾 provider，不得混用。
    """
    t0 = time.time()
    ctx_info = extract_context_key(body, incoming_headers, provider=req_provider)
    pinned_id = affinity_manager.get(ctx_info.key) if ctx_info.is_continuation else None
    if pinned_id:
        logs.info(req_id, f"会话亲和命中: key={ctx_info.key} -> account={pinned_id}")

    tried: set[str] = set()
    model_key = str(body.get("model") or "")
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

    providers = _provider_chain(req_provider)
    if len(providers) > 1:
        # fork 成本闸保留：bigmodel Key 通道默认关（欠费 429 原地重试拖 2 分钟），
        # 关闭时降级链不进 bigmodel 池；显式 `bigmodel/` 前缀请求已在入口 403。
        providers = [providers[0]] + [p for p in providers[1:] if store.bigmodel_channel_enabled()]

    # 套餐窗口闸门（pool_hold）入口快速失败：全池确认枯竭且**没有任何可用的
    # Key 回退归宿**时直接 429，不再白走 5 轮账号×验证码检查（上游 2.7.30）。
    if (
        "zai" in providers
        and gate.refusal_reason() == "pool_hold"
        and not any(
            a.has_apikey_fallback() and not _key_no_balance_cooling(a.id)
            for a in store.list_accounts("zai")
            if a.mode == "jwt"
        )
    ):
        wait = gate.retry_after()
        logs.req_err(req_id, f"套餐窗口闸门 pool_hold，入口快速失败（Retry-After {wait}s）")
        reqlog.finish_error(req_id, "gate_pool_hold", status=429)
        return JSONResponse(
            {"error": {"message": f"套餐窗口真空期（全池额度耗尽），请 {wait}s 后重试",
                       "type": "gate_pool_hold"}},
            status_code=429,
            headers={"Retry-After": str(wait)},
        )

    for pi, provider in enumerate(providers):
        while attempts < MAX_ACCOUNT_ATTEMPTS:
            if deadline and time.time() - t0 > deadline:
                logs.warn(req_id, f"请求级死线 {deadline}s 已到，停止重试（已试 {len(tried)} 账号）")
                reqlog.finish_error(req_id, f"请求级死线 {deadline}s 已到（已试 {len(tried)} 账号）", status=503,
                                    kind="deadline")
                return JSONResponse(
                    {"error": {"message": f"所有账号重试均未成功（请求级死线 {deadline}s）",
                               "type": "no_available_account", "tried": sorted(tried)}},
                    status_code=503,
                )
            # 2026-10-25 防风控放大器（两账号确认制）：波次暂停期直接快速 503
            # （不选号不试探；波次首请求最多烧 2 次 strike）。流式请求改 503 会丢
            # early-flush 的 200 头 —— 仅当尚未收到任何上游流时走到这里，安全。
            if settings.RISK_FAIL_FAST and _risk_wave_paused(provider):
                until = _risk_wave_until_for(provider) or time.time()
                remaining = int(until - time.time()) + 1
                logs.warn(req_id, f"风控波次暂停中（剩 ~{remaining}s），快速失败不试探账号（防 strike 放大）")
                reqlog.finish_error(req_id, "风控波次暂停中", status=503, kind="risk_wave")
                return JSONResponse(
                    {"error": {
                        "message": (f"上游风控波次暂停中（频道级封锁窗口），"
                                    f"预计 {time.strftime('%H:%M', time.localtime(until))} 恢复；"
                                    "窗口由 Plan 通道成功流量自动解除"),
                        "type": "risk_wave_paused", "code": "channel_cooling",
                        "next_available_at": round(until),
                    }},
                    status_code=503,
                    headers={"Retry-After": str(max(remaining, 1))},
                )
            # BUSY_HOLD 武装（3008 持续饱和达阈值）：全池静默，请求不发上游。
            # 流式排队预算 240s ≥ HOLD 180s：本地等完静默自动恢复，成功率不吃亏；
            # 同步预算 60s < HOLD：直接 429+Retry-After 由客户端重试，不强行吊满
            # 3 分钟撞 CDN 100s 红线。
            if _busy_held():
                wait = _busy_hold_remaining()
                max_wait = settings.QUEUE_WAIT if budget is None else budget
                can_wait = max_wait >= wait and (deadline == 0 or time.time() - t0 + wait <= deadline)
                if can_wait and queued + wait <= max_wait:
                    logs.warn(req_id, f"BUSY_HOLD 静默中（剩 ~{wait}s），本地排队等待恢复")
                    reqlog.note_wait(req_id, "q", wait)
                    await _sleep(wait)
                    queued += wait
                    tried.clear()  # 静默结束后按粘性活跃集重新选号
                    continue
                logs.warn(req_id, f"BUSY_HOLD 静默中（剩 ~{wait}s），429 快速返回不试探账号")
                reqlog.finish_error(req_id, "BUSY_HOLD 全池静默", status=429, kind="rate")
                return JSONResponse(
                    {"error": {
                        "message": (f"上游并发槽持续饱和，全池静默 {wait}s 后恢复"
                                    "（防止出口 IP 被防火墙封禁；稍后自动重试即可）"),
                        "type": "gate_busy_hold", "code": "busy_hold",
                        "next_available_at": round(time.time() + wait),
                    }},
                    status_code=429,
                    headers={"Retry-After": str(wait)},
                )
            account = store.select(
                provider,
                skip_ids=tried,
                model=model_key,
                # limit=0（不限）时仍传 _inflight 作排序输入，max_inflight 给
                # 足量大值让过滤恒通过 —— 否则亲和/负载排序退化（v2.9.33 修复）。
                inflight=_inflight,
                pinned_id=pinned_id,
                max_inflight=limit if limit > 0 else 10_000,
            )
            if account is None:
                # 本 provider 池选不到号：限界排队等一等，吃掉「额度分钟级
                # 滚动恢复」型故障。返回 None 表示不排队（关闭 / 预算耗尽 /
                # 空池 / 无可恢复账号 / 波次暂停）→ 按上游语义下沉下一 provider。
                waited = await _wait_for_account(req_id, provider, t0, deadline, queued, budget)
                if waited is not None:
                    queued += waited
                    reqlog.note_wait(req_id, "q", waited)
                    tried.clear()  # 期间账号状态可能已刷新，重新给每个账号机会
                    continue
                if pi + 1 < len(providers):
                    logs.info(req_id, f"provider {provider} 池无可选账号（排队不可行），跨 provider 降级 {providers[pi + 1]}")
                break
            # v2.9.32 startGate 闸门：上游 3008 刚立过闸的账号直接跳过（不计
            # attempts、不占槽），交给下方排队等闸门到期 —— 零上游压力。
            if _busy_gate_live(account.id):
                logs.warn(req_id, f"账号 {account.name} 上游并发闸门中，跳过并排队等闸门到期")
                tried.add(account.id)
                pinned_id = None
                continue
            tried.add(account.id)
            pinned_id = None

            # Key 通道 1113 冷却中的账号跳过（Plan 已死、仅 Key 可用时不该反复重探）
            if _key_no_balance_cooling(account.id) and not account.uses_plan_channel():
                logs.warn(req_id, f"账号 {account.name} Key 无资源包冷却中，跳过")
                continue

            # 模型级额度耗尽冷却跳号（2.7.29）：该号在本模型已确认 1005/1113。
            if plan_guard.cooling(account.id, str(body.get("model") or "")):
                logs.warn(req_id, f"账号 {account.name} 模型 {body.get('model')} 额度耗尽冷却中，跳过")
                continue

            slot_box: list[str | None] = [None]
            if limit > 0:
                if _inflight.get(account.id, 0) >= limit:
                    logs.warn(req_id, f"账号 {account.name} 并发已满（{_inflight.get(account.id, 0)}/{limit}），切换下一个")
                    continue
                # 在任何异步挂起点（await）前同步占槽，消除 Check-then-Act 竞态击穿上限
                _inflight[account.id] = _inflight.get(account.id, 0) + 1
                slot_box[0] = account.id

            attempts += 1
            # 结构上能否用到验证码（Plan 通道）；是否**真的带**由上游 captcha 配置决定：
            # 线上 skip_model_request=true ⇒ 官方客户端模型请求不带验证码头，hub 也必须
            # 不带（带了就是「官方不会干的事」，3012 的结构性嫌疑）。上游若仍回 3007 要码，
            # _try_account 会补码重试。渠道梯内 zcode-plan(`zai`) 才用验证码。
            plan_channel = account.provider == "zai" and account.uses_plan_channel()
            try:
                needs_captcha = plan_channel and await captcha_manager.model_requests_need_captcha(
                    _messages_egress()
                )
                result = await _try_account(
                    req_id, account, body, incoming_headers, plan_channel, needs_captcha,
                    slot_box,
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
                _busy_gate_clear(account.id)  # 上游通了：撤本地排队闸（v2.9.32）
                if _pool_starved_since:
                    _note_pool_starved(provider, model_key)  # 额度恢复边沿
                if ctx_info.key:
                    affinity_manager.bind(ctx_info.key, account.id)
                held = slot_box[0]
                slot_box[0] = None
                if held is not None:
                    result.on_close = _make_slot_releaser(held)
                return result
            # 非流式成功由 _try_account 直接聚合成 Response 返回（fork 既有路径），
            # 这里同样只在上游成功（2xx）时记录亲和；4xx/5xx 不建立绑定。
            if ctx_info.key and 200 <= int(getattr(result, "status_code", 0) or 0) < 300:
                affinity_manager.bind(ctx_info.key, account.id)
            if slot_box[0] is not None:
                _release_slot(slot_box[0])
                slot_box[0] = None
            # 仅在上游流成功建立（_Upstream）时才记录会话亲和；4xx 等错误不建立绑定
            return result
    if queued:
        logs.req_err(req_id, f"无可用账号 / 额度均已耗尽 / 并发已满（已排队等待 {queued:.0f}s）")
        # 排队耗时已在 _wait_for_account 返回时逐段 note_wait 累加，此处不再重复计入
        kind, wait = _recovery(req_provider, model_key)
        reqlog.finish_error(
            req_id, f"无可用账号 / 额度均已耗尽 / 并发已满（已排队 {queued:.0f}s）",
            status=429 if (kind in ("model", "rate", "busy") and wait) else (
                402 if kind == "quota" else 503),
            kind="quota" if kind == "quota" else ("rate" if kind in ("model", "rate", "busy") else "no_account"),
        )
        if wait and wait > time.time():
            retry_after = max(1, int(wait - time.time() + 0.999))
            if kind == "model":
                return JSONResponse(
                    {"error": {
                        "message": f"所有账号均不可用、额度已用完或并发已满（已排队等待 {queued:.0f}s），请在后台检查账号状态",
                        "type": "model_no_quota", "code": "no_quota_for_model",
                        "model": model_key, "queued_seconds": round(queued),
                        "next_available_at": round(wait),
                     }, "retry_after": retry_after},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            if kind == "quota":
                # 池级额度耗尽走 402 而非 429（2026-10-31）：下游 OmniRoute 的
                # 错误分类器对 api-key 连接的 429 无条件判 RATE_LIMITED（不看
                # 文案与 Retry-After），会提前终止 combo 并给连接打限流标记；
                # 402 无条件判 QUOTA_EXHAUSTED（cooldownMs=0，不污染连接），
                # 才能命中其「仅配额耗尽回退」。kind=="quota" 蕴含 wait 非空。
                return JSONResponse(
                    {"error": {
                        "message": "所有账号套餐额度已用尽，等待窗口重置或自动领取新套餐",
                        "type": "model_no_quota", "code": "no_quota_for_model",
                        "model": model_key, "queued_seconds": round(queued),
                        "next_available_at": round(wait),
                     }, "retry_after": retry_after},
                    status_code=402,
                    headers={"Retry-After": str(retry_after)},
                )
            # 429 频控扣号/并发槽饱和/全池静默：短窗口必然恢复 → 429 而非
            # 503（「稍后重试必有」；OmniRoute 对 429 走限流退避而非熔断）
            if kind in ("rate", "busy"):
                why = ("上游并发槽持续饱和，全池静默中" if kind == "busy"
                       else "全部账号被上游限流（频控/并发槽），短窗口自动恢复")
                return JSONResponse(
                    {"error": {
                        "message": (f"{why}（已排队等待 {queued:.0f}s），"
                                    f"预计 {time.strftime('%H:%M', time.localtime(wait))} 恢复"),
                        "type": "rate_limit_error", "code": "rate_limited",
                        "model": model_key, "queued_seconds": round(queued),
                        "next_available_at": round(wait),
                    }, "retry_after": retry_after},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            # 风控通道冷却：封锁按通道，换模型无济于事 → 503 而非 429 语义
            return JSONResponse(
                {"error": {
                    "message": (f"全部服务通道均在风控冷却中（已排队等待 {queued:.0f}s），"
                                f"预计 {time.strftime('%H:%M', time.localtime(wait))} 恢复"),
                    "type": "no_available_account", "code": "channel_cooling",
                    "queued_seconds": round(queued),
                    "next_available_at": round(wait),
                }, "retry_after": retry_after},
                status_code=503,
                headers={"Retry-After": str(retry_after)},
            )
        return JSONResponse(
            {"error": {
                "message": f"所有账号均不可用、额度已用完或并发已满（已排队等待 {queued:.0f}s），请在后台检查账号状态",
                "type": "no_available_account", "queued_seconds": round(queued),
            }},
            status_code=503,
        )
    # 进程级闸门拒绝（边缘封禁/忙碌断路器/分钟预算/套餐窗口）兜底：所有 plan
    # 账号都被本地拒绝到这里，必须带真实原因与 Retry-After（上游 2.7.23）。
    _refuse = gate.refusal_reason()
    if _refuse:
        _wait = gate.retry_after()
        _status = 429 if _refuse in ("rate_limit", "busy_hold", "pool_hold") else 503
        logs.req_err(req_id, f"闸门 {_refuse}，拒绝本请求（Retry-After {_wait}s）")
        reqlog.finish_error(req_id, f"gate_{_refuse}", status=_status)
        return JSONResponse(
            {"error": {"message": f"上游保护闸门触发（{_refuse}），请 {_wait}s 后重试",
                       "type": f"gate_{_refuse}"}},
            status_code=_status,
            headers={"Retry-After": str(_wait)},
        )

    logs.req_err(req_id, "无可用账号 / 额度均已耗尽 / 并发已满")
    kind, wait = _recovery(req_provider, model_key)
    reqlog.finish_error(req_id, "无可用账号 / 额度均已耗尽 / 并发已满",
                        status=429 if (kind in ("model", "rate", "busy") and wait) else (
                            402 if kind == "quota" else 503),
                        kind="quota" if kind == "quota" else ("rate" if kind in ("model", "rate", "busy") else "no_account"))
    if wait and wait > time.time():
        retry_after = max(1, int(wait - time.time() + 0.999))
        if kind == "model":
            return JSONResponse(
                {"error": {
                    "message": "所有账号均不可用、额度已用完或并发已满，请在后台检查账号状态",
                    "type": "model_no_quota", "code": "no_quota_for_model",
                    "model": model_key, "next_available_at": round(wait),
                 }, "retry_after": retry_after},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        if kind == "quota":
            # 池级额度耗尽走 402 而非 429（语义同上排队路径；不带排队秒数）
            return JSONResponse(
                {"error": {
                    "message": "所有账号套餐额度已用尽，等待窗口重置或自动领取新套餐",
                    "type": "model_no_quota", "code": "no_quota_for_model",
                    "model": model_key, "next_available_at": round(wait),
                 }, "retry_after": retry_after},
                status_code=402,
                headers={"Retry-After": str(retry_after)},
            )
        if kind in ("rate", "busy"):
            why = ("上游并发槽持续饱和，全池静默中" if kind == "busy"
                   else "全部账号被上游限流（频控/并发槽），短窗口自动恢复")
            return JSONResponse(
                {"error": {
                    "message": f"{why}，预计 {time.strftime('%H:%M', time.localtime(wait))} 恢复",
                    "type": "rate_limit_error", "code": "rate_limited",
                    "model": model_key, "next_available_at": round(wait),
                }, "retry_after": retry_after},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        return JSONResponse(
            {"error": {
                "message": (f"全部服务通道均在风控冷却中，"
                            f"预计 {time.strftime('%H:%M', time.localtime(wait))} 恢复"
                            "（封锁按通道，换模型无济于事）"),
                "type": "no_available_account", "code": "channel_cooling",
                "next_available_at": round(wait),
            }, "retry_after": retry_after},
            status_code=503,
            headers={"Retry-After": str(retry_after)},
        )
    # 池里全是「已标 EXHAUSTED」的号时走不到 exhaust 分支（select 先滤掉），
    # 套餐窗口闸门在这里补触发——重启后真空期首请求也能进入快速失败闭环。
    _note_pool_exhausted(trigger=None)
    return JSONResponse(
        {"error": {"message": "所有账号均不可用、额度已用完或并发已满，请在后台检查账号状态", "type": "no_available_account"}},
        status_code=503,
    )


def _business_error_code(data: dict) -> int | None:
    """HTTP 200 承载的业务错误码（Plan 通道实测形态 `{"code":1005,"msg":...}`）。

    实现在 `app.business_error`（gateway / quota 共用一份判定，避免两处漂移）。
    """
    return business_error.business_code(data)


def _status_for_business_code(code: int) -> int:
    """业务码 → 等价 HTTP 状态（复用下方已有的错误分支，不另起一套处置）。

    这里**不写死映射表**：风控码与额度码都取自 constants，改常量即同步生效，
    不会出现「常量加了 1113、映射忘了」这类静默漂移（单测把两边对齐钉住）。
    """
    if code == constants.RISK_BUSINESS_CODE:
        return constants.RISK_CONTROL_HTTP_STATUSES[0]
    if code in constants.EXHAUST_BUSINESS_CODES:
        return constants.EXHAUST_HTTP_STATUSES[0]
    if code in constants.RATE_LIMIT_429_CODES:
        return 429
    return 400


def _buffered_response(resp: httpx.Response, raw: bytes) -> httpx.Response:
    """把已读净的正文包成内存响应，供下游按原语义消费。

    去掉 content-encoding/content-length/transfer-encoding：正文已是解码后的
    字节，保留这些头会让下游再解一次（httpx 对内存响应仍会解 content-encoding）。
    """
    drop = ("content-encoding", "content-length", "transfer-encoding")
    headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in drop]
    return httpx.Response(resp.status_code, headers=headers, content=raw)


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
    if slot_box is not None:
        slot_box[0] = None


# ── 风控波次暂停（2026-10-25 防放大器，两账号确认制）────────────────────
# 上游 405/3012 波次是「频道级」封锁（官方客户端同拦，zai-org/feedback
# #716/#1185 实证家宽直连也拦）：波次窗口内换号必然同样 405，只会把池内
# 账号挨个烧 strike（08:41 实测单请求烧 3 号至禁用）。判定：窗口内第 2 个
# 不同账号也命中风控 → 确认频道级波次，全池短窗暂停（新请求入口快速
# 503、不排队不试探，暂停指数延长）——波次首请求最多烧 2 次 strike。
# 单账号命中只冷却自身，不影响其他账号 failover。解除：Plan 通道真实成功
# 流量（Key 回退成功不算，api.z.ai 通道与被拦的 plan 通道无关）。
_risk_wave_until: dict[str, float] = {}
_risk_wave_hit: dict[str, tuple[str, float]] = {}  # provider → (首个命中账号 id, 时刻)
_WAVE_HIT_TTL = 3600.0  # 命中记忆时效：超时视为全新单点事件，防陈旧记忆误确认波次


def _risk_wave_until_for(provider: str) -> float | None:
    """当前波次暂停截止时刻；未暂停返回 None（测试可注入未来时间戳）。"""
    until = _risk_wave_until.get(provider)
    if until is None or until <= time.time():
        return None
    return until


def _risk_wave_paused(provider: str) -> bool:
    return _risk_wave_until_for(provider) is not None


def _risk_wave_confirm(provider: str, account_id: str) -> bool:
    """风控命中时累计判定；返回 True 表示本次命中确认了频道级波次。

    首个命中记录 (账号 id, 时刻)；不同账号在记忆时效内再次命中即确认波次，
    暂停指数延长（2^n × RISK_WAVE_PAUSE，封顶 RISK_WAVE_PAUSE_MAX）。
    """
    if settings.RISK_WAVE_PAUSE <= 0:
        return False
    now = time.time()
    first = _risk_wave_hit.get(provider)
    if first is None or first[0] == account_id or now - first[1] > _WAVE_HIT_TTL:
        _risk_wave_hit[provider] = (account_id, now)
        return False
    # 不同账号也命中 → 波次确认，指数延长
    level = _risk_wave_level.get(provider, 0) + 1
    _risk_wave_level[provider] = level
    pause = min(settings.RISK_WAVE_PAUSE * (2 ** (level - 1)),
                settings.RISK_WAVE_PAUSE_MAX)
    new_until = time.time() + pause
    cur = _risk_wave_until.get(provider)
    if cur is None or new_until > cur:
        _risk_wave_until[provider] = new_until
    riskmetrics.record("wave", account_id, provider=provider,
                       pause=int(pause), level=level, carrier="ip")
    # B 档验证码波次助推：暂停到期前后的试探/恢复期验证码消耗最集中，
    # 预热目标临时抬升 + 闲置停解压制（到期自动回落，见 captcha.notify_wave_pause）
    captcha_manager.notify_wave_pause(pause)
    # 文本校验波次叠加提示（2026-10-31 教训）：sidecar 若已过时，波次大概率
    # 就是措辞触发而非频控——趁运维还在看日志的时候把线索递到位。
    hint = freshness.stale_hint()
    if hint:
        logs.warn("risk", f"风控波次与 sidecar 文本过时叠加，高度疑似措辞触发: {hint}")
    return True


_risk_wave_level: dict[str, int] = {}


def _risk_wave_clear(provider: str) -> None:
    """Plan 通道真实成功流量到达时解除波次暂停与命中记忆（上游窗口已开）。"""
    _risk_wave_until.pop(provider, None)
    _risk_wave_hit.pop(provider, None)
    _risk_wave_level.pop(provider, None)


def _risk_wave_reset_for_tests() -> None:
    """测试隔离钩子：清空全部波次状态。"""
    _risk_wave_until.clear()
    _risk_wave_hit.clear()
    _risk_wave_level.clear()
    edge.reset()


def _edge_response(provider: str) -> JSONResponse:
    """静默期内/刚命中拦截页时的本地应答。

    刻意回 405（而不是 503）：下游 OmniRoute / magpie 把 405 当作
    「这个 provider 被网络拒绝了」→ 立刻切路由组里的下一个 provider；
    回 503 反而会被当成 node 故障计入熔断。文案对齐 magpie 的
    provider.BlockedHint，便于它直接识别。
    """
    until = edge.until(provider) or (time.time() + settings.EDGE_HOLD_SECONDS)
    remaining = max(1, int(until - time.time() + 0.999))
    stamp = time.strftime("%H:%M:%S", time.localtime(until))
    return JSONResponse(
        {
            "type": "error",
            "error": {
                "type": "permission_error",
                "code": "edge_blocked_ip",
                "message": (
                    "405 Method Not Allowed — 上游网络防火墙拦截了本机出口 IP；"
                    f"等待或切换网络/代理（ZCode Start Plan 在 {stamp} 前不再尝试）"
                ),
                "next_available_at": round(until),
            },
        },
        status_code=405,
        headers={"Retry-After": str(remaining), "x-zcode-edge": "1"},
    )


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
    # ACTIVE 满号等在途流释放槽位（轮询仅本地选号，零上游压力）。
    # 429/5xx 刚失败而仍 ACTIVE 的号秒级内不会好转——对它们排队只会反复
    # 重打同一账号（每轮重带满额重试梯），放大上游压力并把 TTFB 拖过 CDN
    # 边缘 ~100s 硬超时（HTTP 524；2026-10-07 实测单账号 429 用例被排队
    # 拖成 240s+ 挂死、上游被无谓重打十几次）。
    # 无可恢复账号 → 快速 503，让客户端自行重试。
    if not _has_recoverable_account(provider):
        return None
    # 2026-10-25 防风控放大器：波次暂停期是「频道级封锁」窗口，窗口内试探
    # 必然再吃 405 再烧 strike —— 与 COOLING 同理不排队，快速 503 由客户端
    # 自行重试（窗口由真实成功流量到期自然解除）。
    if _risk_wave_paused(provider):
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
    reqlog.note_queue(1)
    waited = 0.0
    try:
        while waited < remaining:
            step = min(float(poll), remaining - waited)
            await _sleep(step)
            waited += step
            # v2.9.32 startGate：闸门未到期前不重新选号（否则 tried.clear
            # 后会把两号各再敲一遍 3008，放大上游压力 —— 2026-10-07 教训）。
            # 到期瞬间由 _busy_gate_live 原子放唯一试探名额。
            if _busy_gate_active() and time.time() < _busy_gate_next():
                continue
            if store.select(provider) is not None:
                logs.warn(req_id, f"排队 {waited:.0f}s 后账号恢复可用，继续调度")
                return waited
            if deadline and time.time() - t0 >= deadline:
                return waited  # 死线到：交给主循环的死线分支出 503
        return None  # 排队预算耗尽
    finally:
        # 无论正常结束/提前返回/客户端取消，都必须退出排队计数，
        # 否则 queue 口径只进不出，实时并发面板会被刷成永久高位
        reqlog.note_queue(-1)


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


def _slot_view() -> dict:
    """上游占槽快照（reqlog 并发仪表数据源）。

    per 键用账号名而非 id（面板直接渲染）；已删除的账号跳过。limit=0 表示
    后台未设并发上限，前端据此不画 n/limit 而画裸数。
    """
    per: dict[str, int] = {}
    for acc_id, n in list(_inflight.items()):
        if n <= 0:
            continue
        acc = store.find_any(acc_id)
        per[acc.name if acc else acc_id[:8]] = n
    return {"total": sum(n for n in _inflight.values() if n > 0),
            "per": per, "limit": _limit()}


reqlog.register_slot_source(_slot_view)


# Key 通道 1113「无资源包」（永久态）的选号冷却记忆：account_id → 解禁 epoch。
# 不记的话每个请求都会重选该号去撞一次必然失败的 1113（2026-11-11 线上实证：
# 151 个请求 = 151 次白烧上游调用）。进程内记忆：重启即忘，重探一次代价可接受；
# 后台手动启用账号时清除（用户明确意图 = 立即重试）。
_key_no_balance: dict[str, float] = {}

# pool_hold 证据链补查的进程级节流记忆：account_id → 上次探查发起时刻
# （epoch）。真空期 balances 为空时证据点永不闭合，无此闸则每条请求都对
# 全池 stale 号 spawn billing（review P1 整改，2026-11-12）。重启即忘
_pool_probe_last: dict[str, float] = {}


def clear_key_no_balance(account_id: str) -> None:
    """后台启用/凭证变更时清除该号的 Key 无余额冷却，允许立即重探。"""
    _key_no_balance.pop(account_id, None)


def _key_no_balance_cooling(account_id: str, now: float | None = None) -> bool:
    until = _key_no_balance.get(account_id, 0)
    if not until:
        return False
    if (now or time.time()) >= until:
        _key_no_balance.pop(account_id, None)  # 到期自然重探
        return False
    return True


def _note_pool_exhausted(trigger: Account | None = None) -> None:
    """账号 Plan 通道判额度耗尽后，评估是否触发套餐窗口闸门（pool_hold）。

    调用时机：① _try_account 的 exhaust 分支（本请求真实撞到 1113/额度
    耗尽，trigger=该账号）；② _dispatch 的「无可用账号」兜底（全池已被标
    EXHAUSTED、select 直接滤光，见下）；③ fork 的 402 出口（trigger=None，
    让池闸评估/武装）。判定口径（保守优先，绝不误拦——2026-11-12 线上
    247 次真空期白撞教训）：只有当**全部** JWT 账号都返回「确认枯竭」（每个
    号都有过期后实查证据 quota_verified_at > 最新 expires_at，且无当前可用
    窗口）时才拦；任意一号是可用（0.0）或不可判定（None——快照空/陈旧/新号）
    都不拦，但会补 _safe_refresh 后台实查把证据链续上（见下方 stale 分支注释）。
    next_start 取全池最小未来
    effective_at；无未来窗口给 -1，由 gate 用上限兜底。
    """
    accounts = [a for a in store.list_accounts("zai") if a.mode == "jwt"]
    if not accounts:
        return
    verdicts = []
    stale: list[Account] = []
    for a in accounts:
        verdict = a.next_window_start()
        if verdict is None:
            stale.append(a)
            continue
        verdicts.append(verdict)
    if stale:
        # 证据链未闭合（快照空/陈旧）→ 不拦（保守优先），但**补后台实查**把
        # 证据链续上。没有这一步是死锁：EXHAUSTED 号被 select 滤掉后不会有任何
        # 请求真实撞到它们（exhaust 分支的刷新也就永不执行），quota_verified_at
        # 永远停在上一个日窗之前 → 全池 None → 闸门永不触发（2026-11-12 线上
        # 实证：真空期 34/34 请求全裸 503，0 次 pool_hold 拦截）。_safe_refresh
        # 自带 60s 去抖，一轮 billing 后 verified_at 越过 max_ends，下一请求
        # 即进入快速失败闭环——裸 503 时代从「整个真空期」缩到「一轮实查」。
        # 探查另有**进程级节流**（review P1 整改）：60s 去抖挡不住「balances
        # 为空 → 证据点不打 → 账号永远 stale → 每请求都 spawn」的持续形态
        # （线上实锤 22:49 刷新跑了但 verified_at 未动），首探立即、后续按
        # GATE_POOL_PROBE_MIN_SECONDS 每号限频，把 billing 轰炸压回 monitor 量级
        now = time.time()
        for a in stale:
            last_probe = _pool_probe_last.get(a.id, 0.0)
            if now - last_probe < settings.GATE_POOL_PROBE_MIN_SECONDS:
                continue
            _pool_probe_last[a.id] = now
            _spawn_bg(_safe_refresh(a))
        return  # 不可判定 → 放行，让真实请求去定生死
    if any(v == 0.0 for v in verdicts):
        return  # 任一号当前可用 → 不拦（-1.0 = 枯竭但无未来窗口，照拦）
    future = [v for v in verdicts if v > 0]
    next_start = min(future) if future else -1.0
    now = time.time()
    if gate.pool_hold_remaining(now) > 0:
        return  # 已在 hold 中：刷新到更早的窗口点才有意义，否则不动
    gate.note_pool_exhausted(next_start, now=now)
    when = (
        time.strftime("%m-%d %H:%M", time.localtime(next_start))
        if next_start > 0
        else f"未知（{settings.GATE_POOL_HOLD_MAX_SECONDS}s 后重探）"
    )
    trigger_note = f"（{trigger.name} 判额度耗尽）" if trigger else ""
    logs.warn(
        "gate",
        f"套餐窗口闸门触发：全池 {len(accounts)} 号确认枯竭{trigger_note}，"
        f"暂停 plan 渠道至 {when}",
    )


def _limit() -> int:
    """当前并发上限（0 = 不限）。控制台可改、meta 即时生效（v2.9.33 恢复）。"""
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

    __slots__ = ("resp", "cm", "client", "t_first", "account_name", "mode",
                 "model", "on_close", "gate_release", "_closed")

    def __init__(self, resp: httpx.Response, cm, client: httpx.AsyncClient | None,
                 t_first: float | None = None, account_name: str = "", mode: str = "",
                 model: str = "-", on_close=None, gate_release=None) -> None:
        self.resp = resp
        self.cm = cm
        self.client = client
        self.t_first = t_first
        self.account_name = account_name
        self.mode = mode
        self.model = model
        self.on_close = on_close
        self.gate_release = gate_release
        self._closed = False

    async def close(self) -> None:
        """幂等关闭：释放上游流与并发槽位（on_close），重复调用安全。

        cm/client 可能为 None（200 业务错误的正文已读净并包成内存响应），
        只释放仍然存在的部分。
        """
        if self._closed:
            return
        self._closed = True
        if self.cm is not None:
            await self.cm.__aexit__(None, None, None)
        if self.client is not None:
            await self.client.aclose()
        for callback in (self.on_close, self.gate_release):
            if callback is None:
                continue
            try:
                callback()
            except Exception:  # noqa: BLE001 - 槽位/闸门释放失败不掩盖主流程
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
                record_usage(up.model)
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


def _channel_ladder(account: Account) -> list[str]:
    """账号渠道优先梯（Magpie 模式）。

    zai：zcode_plan（jwt @ zcode-plan 代理）→ apikey（api.z.ai）；
    bigmodel：bigmodel（api key）。仅当账号确持对应凭证才入梯。
    """
    if account.provider == "zai":
        ladder: list[str] = []
        if account.uses_plan_channel():
            ladder.append("zai")
        if bool((account.api_key or "").strip()):
            ladder.append("zai_fallback")
        return ladder
    if account.provider == "bigmodel":
        return ["bigmodel"] if bool((account.api_key or "").strip()) else []
    return []


def _parse_exhaust_reset(text: str) -> float | None:
    """从额度耗尽文案中尽力解析重置时间戳（仅未来时间）；解析不到返回 None。

    1310「已达周/月使用上限」可能带下一次重置时间（ISO/日期时间形态）。
    解析失败不阻塞主流程——额度恢复最终由 billing 刷新兜底。
    """
    m = re.search(
        r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})(?:日|[T\s]+)?"
        r"(?:(\d{1,2})[:时](\d{2})(?:[:分](\d{2}))?)?",
        text,
    )
    if not m:
        return None
    try:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh = int(m.group(4) or 0)
        mm = int(m.group(5) or 0)
        ss = int(m.group(6) or 0)
        # 注意：本文件使用 `from datetime import datetime`（类名），
        # 上游原写法 datetime.datetime(...) 会因类名遮蔽模块名而失败。
        ts = datetime(y, mo, d, hh, mm, ss).timestamp()
    except (ValueError, OSError):
        return None
    return ts if ts > time.time() else None


async def _try_account(req_id, account, body, incoming_headers, plan_channel,
                       needs_captcha, slot_box: list | None = None):
    """尝试用单个账号转发，含渠道优先梯、验证码续期与可配置重试。

    渠道梯（Magpie 模式，见 _channel_ladder）：
      zai：zcode_plan（jwt @ zcode-plan 代理）→ apikey（api.z.ai）
    错误处理策略（参数见 settings，均可用环境变量调整）：
      - 验证码挑战：清池换码重建请求，最多 MAX_CAPTCHA_RETRIES 次（仅 zcode_plan）
      - 429 频控：**不冷却账号**，按上游 Retry-After（封顶 RETRY_429_WAIT_MAX）
        或 RETRY_429_WAIT 等待后原地重试，最多 RETRY_429_TIMES 次；
        耗尽后下沉下一渠道（plan → apikey），再耗尽换号且账号保持可用
      - 5xx 等一般错误：重试最多 RETRY_5XX_TIMES 次；耗尽后账号冷却
        COOLING_SECONDS 并换下一个账号
      - 风控（3012/405「unusual activity」）：指数退避冷却（base 900s 起，封顶
        24h），累计 RISK_BAN_STRIKES 次升级禁用（UI 展示），人工确认恢复；
        窗口内两个不同账号连中即确认频道级波次 → 全池暂停防 strike 放大
        （2026-10-25，见 _risk_wave_confirm）
      - 1310（周/月使用上限，上游 e038650）：额度耗尽信号，标记 EXHAUSTED
        并尽力解析重置时间写入 cooling_until（不是频控，不重试）
    """
    ladder = _channel_ladder(account)
    if not ladder:
        account.record_result(False, "无可用渠道凭证")
        _mark(account, Status.INVALID, "无可用渠道凭证")
        logs.warn(req_id, f"账号 {account.name} 无可用渠道凭证，切换下一个")
        return _NEXT_ACCOUNT

    channel_idx = 0
    channel = ladder[0]
    captcha_retries = 0
    retries_429 = 0
    retries_busy = 0  # 3008/3009/3010 并发槽原地重问次数（BUSY_WAITS 节奏）
    retries_5xx = 0
    force_fallback = False  # 仅供 _lane_for 监控口径；通道切换已改由 _jump_to_fallback 驱动
    model_name = str(body.get("model") or "-")
    # 验证码必须与本次 zcode-plan 请求同出口（见 app/egress.py）；记下来供失败日志佐证
    captcha_egress = _messages_egress() if plan_channel else None
    gate_acquired = False

    def _drop_gate() -> None:
        """归还闸门槽位；等待后 continue 会重新申请。"""
        nonlocal gate_acquired
        if gate_acquired:
            gate.release()
            gate_acquired = False

    def _jump_to_channel(target: str | None = None) -> bool:
        """下沉到渠道梯下一站（target 指定目标渠道）；无可达下一站返回 False。"""
        nonlocal channel_idx, channel, captcha_retries, retries_429, retries_5xx
        if target is None:
            target_idx = channel_idx + 1
        else:
            if target not in ladder:
                return False
            target_idx = ladder.index(target)
            if target_idx <= channel_idx:
                return False  # 已在该渠道或其后，无可再降
        if target_idx >= len(ladder):
            return False
        channel_idx = target_idx
        channel = ladder[channel_idx]
        captcha_retries = 0
        retries_429 = 0
        retries_5xx = 0
        return True

    def _jump_to_fallback() -> bool:
        """风控/鉴权后跳过所有 plan 系渠道，直达 api-key 渠道（如持有）。"""
        fallback = "zai_fallback" if account.provider == "zai" else "bigmodel"
        return _jump_to_channel(target=fallback)

    try:
        while True:
            attempt_t0 = time.time()
            reqlog.mark_account(req_id, account.name, account.mode)
            verify_param = verify_region = None
            # 出口 IP 静默期内不要解验证码：直接按 fork 契约回 405（不再花码）
            if channel == "zai" and edge.held(account.provider):
                if _jump_to_fallback():
                    logs.warn(req_id, f"出口 IP 封锁静默中（剩 ~{edge.remaining(account.provider)}s），"
                                      f"账号 {account.name} 改走 API Key 回退")
                    needs_captcha = False
                    continue
                reqlog.finish_error(req_id, "出口 IP 被 WAF 拦截，静默中", status=405, kind="edge")
                return _edge_response(account.provider)

            # 仅 zcode-plan（`zai`）渠道用验证码；apikey 免码
            if channel == "zai" and needs_captcha:
                # 闸门会拒时不要先花一枚验证码再被拒（上游 2.7.23）
                _refuse = gate.refusal_reason()
                # edge_hold 交给后面的 edge.held 分支回 405 契约（不在这里 503）
                if _refuse and _refuse != "edge_hold":
                    if _jump_to_fallback():
                        logs.warn(req_id, f"账号 {account.name} 闸门 {_refuse}，直接切 API Key 回退")
                        needs_captcha = False
                        continue
                    return _NEXT_ACCOUNT
                _park_slot(slot_box)
                cap_t0 = time.time()
                try:
                    # 验证码必须与本次 Plan 请求同出口：token 由某出口签发后，
                    # 换出口使用会被上游判 unusual activity（3012）。
                    verify_param, verify_region = await captcha_manager.get_verify_param(
                        egress_tag=_messages_egress()
                    )
                except Exception as err:  # noqa: BLE001
                    logs.req_err(req_id, f"人机校验失败: {err}")
                    reqlog.note_wait(req_id, "cap", time.time() - cap_t0)
                    reqlog.finish_error(req_id, f"人机校验失败: {err}", status=500, kind="captcha")
                    return JSONResponse(
                        {"error": {"message": f"无法完成人机校验: {err}", "type": "captcha_error"}},
                        status_code=500,
                    )
                reqlog.note_wait(req_id, "cap", time.time() - cap_t0)
                if not _reacquire_slot(account, slot_box):
                    logs.warn(req_id, f"账号 {account.name} 验证码等待后并发已满，切换下一个")
                    return _NEXT_ACCOUNT

            try:
                url, headers, payload = build_request(
                    account, body, verify_param, incoming_headers, verify_region,
                    upstream_key=channel,
                )
            except RuntimeError as err:
                # 本渠道凭证缺失/无效：先下沉下一渠道，梯底才判死账号（上游 e038650）。
                # url 尚未赋值（或为上一趟残留），不得用假 url 推通道（fork 口径）。
                reqlog.mark_attempt(req_id, account.name, account.mode,
                                    channel=_lane_for(False, plan_channel, "", channel=channel),
                                    error=f"渠道 {channel} 凭证无效: {err}")
                account.record_result(False, f"渠道 {channel} 凭证无效: {err}")
                logs.warn(req_id, f"账号 {account.name} 渠道 {channel} 凭证无效: {err}，下沉下一渠道")
                if _jump_to_channel():
                    continue
                _mark(account, Status.INVALID, str(err))
                return _NEXT_ACCOUNT

            # IP 级封锁静默（见 app/edge.py，2026-11-05 对齐 magpie 0.1.43）：
            # 静默期内 zcode-plan 通道一个字节都不发上游（封的是出口 IP → plan 主机）。
            # 有 Key 回退的账号下沉 api.z.ai（另一个 host，不受同一防火墙影响）；
            # 否则本地回同款 405，让下游切 provider。plan_std/apikey 渠道不受影响。
            if channel == "zai" and edge.held(account.provider):
                if _jump_to_fallback():
                    logs.warn(
                        req_id,
                        f"出口 IP 封锁静默中（剩 ~{edge.remaining(account.provider)}s），"
                        f"账号 {account.name} 改走 API Key 回退",
                    )
                    needs_captcha = False
                    continue
                reqlog.finish_error(req_id, "出口 IP 被 WAF 拦截，静默中",
                                    status=405, kind="edge")
                # fork 契约：405 + edge_blocked_ip（OmniRoute/magpie 据此切 provider）
                return _edge_response(account.provider)

            # 进程级闸门（上游 2.7.23）：只对 zcode-plan 渠道生效。被拒时优先
            # 下沉 API Key 渠道；无可下沉时换号，交给 dispatch 兜底。
            if channel == "zai":
                _ok, _why = gate.acquire()
                if not _ok:
                    if _jump_to_fallback():
                        logs.warn(req_id, f"账号 {account.name} 闸门 {_why}，切 API Key 回退")
                        needs_captcha = False
                        continue
                    return _NEXT_ACCOUNT
                gate_acquired = True

            # 账号级出口代理（zai 主链路 + fallback 同 client）：未配置 = 直连不变；
            # billing/claim/验证码不在此列（刻意直连，见 claim._billing_request）。
            # ZCODE_MESSAGES_FORCE_DIRECT 时账号代理照常生效（2.7.16 语义：该开关只
            # 压制 env/系统代理，账号级显式代理仍是账号自己的出口决策）。
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=30.0, read=None, write=120.0, pool=30.0),
                trust_env=not settings.MESSAGES_FORCE_DIRECT,
                **account.proxy_kwargs())
            # 出口观测：账号自带代理 / env 系统代理 / 直连（egress_for 与 httpx 判定逐字一致）
            if account.proxy_kwargs():
                egress_seen = "account"
            else:
                egress_seen = egress.egress_for(url)
            reqlog.mark_egress(req_id, egress_seen, account_name=account.name)
            # 打标：本请求已发起上游调用（含连接失败/4xx/5xx）；调度层放弃的行
            # 无此标记，监控端据此区分「调度失败·未达上游」与「上游失败」
            reqlog.mark_upstream(req_id)
            cm = client.stream("POST", url, headers=headers, content=payload)
            try:
                resp = await cm.__aenter__()
            except httpx.HTTPError as err:
                reqlog.mark_attempt(req_id, account.name, account.mode,
                                    channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                    error=f"连接失败: {err}")
                await client.aclose()
                _drop_gate()
                account.record_result(False, f"连接失败: {err}")
                riskmetrics.record("net", account.name, err=str(err))
                # 废 JWT / 风控禁用走 Key 回退失败时不得洗成 cooling，否则冷却结束会重开 Plan；
                # 通道级风控退避在场时同理（连接失败不得覆盖风控退避文案与计数）
                if (account.status in (Status.INVALID, Status.DISABLED)
                        or account.has_lane_cooling()):
                    store.update_account(account)
                else:
                    # 连接失败与上游风控分开计数（2026-10-30）：请求未达上游、零风控
                    # 代价，走独立 net_strikes 短退避（base×2 递进、cap 封顶），不进
                    # risk_strikes/lane 冷却——死节点不再把账号烧向风控禁用。
                    until = account.net_fail_penalty(
                        settings.NET_COOLING_BASE, settings.NET_COOLING_CAP,
                        settings.RISK_STRIKE_DECAY_SECONDS)
                    account.status = Status.COOLING
                    account.cooling_until = until
                    account.last_error = (
                        f"连接失败（第 {account.net_strikes:.0f} 次，冷却至 "
                        f"{time.strftime('%H:%M', time.localtime(until))}）: {err}")
                    store.update_account(account)
                logs.warn(req_id, f"账号 {account.name} 连接失败，切换下一个")
                return _NEXT_ACCOUNT

            status_code = resp.status_code
            text: str | None = None

            # 200 承载业务错误：Plan 通道对额度/风控类拒绝直接回 HTTP 200 +
            # {"code":1005|3012,"msg":...}（2026-10-30 实测，content-type application/json）。
            # 不识别有两个后果：① 当成成功洗掉 risk_strikes / 冷却 / last_error，把仍在
            # 标记期的账号拉回 ACTIVE（「冷却期零上游流量」不变量被绕开）；② 错误体当正文
            # 透传，客户端拿到「200 但没有 message」。JSON 正文一次读净：合法响应改走内存
            # 流（下游消费逻辑不变），业务错误码映射成等价 HTTP 状态走下面统一分支。
            if status_code < 400 and resp.headers.get("content-type", "").startswith("application/json"):
                raw = await resp.aread()
                await cm.__aexit__(None, None, None)
                await client.aclose()
                cm = client = None  # 已关闭，_Upstream.close 容忍 None
                decoded = raw.decode("utf-8", "ignore")
                data = _safe_json(decoded)
                code = _business_error_code(data) if isinstance(data, dict) else None
                if code is None:
                    resp = _buffered_response(resp, raw)
                else:
                    text = decoded
                    status_code = _status_for_business_code(code)
                    logs.warn(
                        req_id,
                        f"账号 {account.name} HTTP 200 承载业务错误 code={code}"
                        f"（按 {status_code} 处置）：{' '.join(text.split())[:160]}",
                    )
                diaglog.record_failure(req_id, account_name=account.name, mode=account.mode,
                                       model=str(body.get("model") or ""), url=url,
                                       status=status_code, text=text, request_body=body)
                reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))

            if status_code >= 400:
                _drop_gate()  # 本趟上游尝试已结束；continue 的下一趟会重新申请
                if text is None:
                    text = (await resp.aread()).decode("utf-8", "ignore")
                    await cm.__aexit__(None, None, None)
                    await client.aclose()

                # 验证码挑战：三形态任一命中即清池重试（不改账号状态）。zcode-plan
                # 渠道上即使这一趟没带码也要探测——若上游仍要求验证码，下一趟补码重试。
                challenge = _detect_captcha_challenge(resp, text) if channel == "zai" else None
                if challenge:
                    captcha_manager.invalidate(_messages_egress())
                    captcha_retries += 1
                    reqlog.note_retry(req_id, "cap")
                    reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                    if captcha_retries >= MAX_CAPTCHA_RETRIES:
                        reqlog.mark_attempt(req_id, account.name, account.mode, channel="plan",
                                            status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                            error=f"验证码挑战连续失败（{challenge}）")
                        account.record_result(False, "验证码挑战连续失败")
                        logs.warn(req_id, f"账号 {account.name} 验证码连续失败，切换下一个")
                        return _NEXT_ACCOUNT
                    if not needs_captcha:
                        # 按配置本不该带码，但上游明确要码 → 下一趟补码
                        needs_captcha = True
                        logs.warn(req_id, f"账号 {account.name} 验证码挑战（{challenge}），补码重试")
                    else:
                        logs.warn(req_id, f"账号 {account.name} 验证码挑战（{challenge}），刷新重试")
                    continue  # 同账号重建请求重试

                # 405 + HTML 拦截页 = 本机出口 IP 被阿里云 ESA/WAF 拦（
                # app/edge.py）。2026-11-05 对齐 magpie 0.1.43（plugins#649）：
                # 进程级静默、**不给账号记 strike** —— 封的是地址，罚账号只会
                # 把健康号打进冷却（11-03 那 12 次拦截页就是这么烧掉的）。
                # 上游宽口径（405 + 边缘页文案/HTML，排除 3012）与 fork edge.py
                # 载体判定合并；命中即记进程级闸门（gate_edge_hold）。
                if status_code == 405 and (_is_edge_block(status_code, text) or edge.is_page(text)):
                    gate.trip_edge()
                    until = edge.arm(account.provider)
                    snippet = " ".join(text.split())[:120]
                    reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                    reqlog.mark_attempt(
                        req_id, account.name, account.mode,
                        channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                        status=status_code, ms=(time.time() - attempt_t0) * 1000,
                        error="出口 IP 被拦（WAF 拦截页）",
                    )
                    riskmetrics.record("edge", account.name, status=status_code,
                                       egress=captcha_egress, carrier="ip")
                    logs.warn(
                        req_id,
                        f"账号 {account.name} 命中阿里云 WAF 拦截页（出口 IP 级，非账号风控），"
                        f"全池静默至 {time.strftime('%H:%M:%S', time.localtime(until))}，不记 strike；"
                        f"上游body={snippet!r}",
                    )
                    if _jump_to_fallback():
                        needs_captcha = False
                        continue
                    return _edge_response(account.provider)

                # 风控（3012「unusual activity」/ 405）：指数退避冷却自动恢复，累计
                # RISK_BAN_STRIKES 次升级禁用（人工恢复）。不立即重试（避免对风控
                # 账号持续施压）；冷却期零上游流量（is_cooling 全通道门禁）。
                if _is_risk_control(status_code, text):
                    # 405 一律按风控计（RISK_CONTROL_HTTP_STATUSES），所以必须把上游
                    # body 留一段在日志里——否则分不清是 3012 标记、验证码挑战还是
                    # WAF/签名拒绝，strikes 花完了仍不知道上游在拒什么。
                    # 200-biz（上游统一映射成 405 到这里）与原生 405 共用分支：
                    # body 是业务码 JSON 时 signal 标「业务码 N」，日志/面板可区分
                    # 载体（fork 语义保留），否则按原生 HTTP 405 记。
                    biz_code = _upstream_biz_code(text)
                    signal = (f"业务码 {biz_code}" if biz_code is not None
                              else f"3012/unusual activity HTTP {status_code}")
                    snippet = " ".join(text.split())[:200]
                    reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                    reqlog.mark_attempt(req_id, account.name, account.mode,
                                        channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                        status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                        error=f"风控 {signal}")
                    account.record_result(False, f"风控 {signal}（HTTP {status_code}）")
                    penalty_note = _apply_risk_strike(
                        account, signal, req_id,
                        model=str(body.get("model") or ""),
                        channel="plan" if "/zcode-plan/" in url else "apikey",
                    )
                    # 2026-10-25 防放大器（两账号确认制，fork 保留）：首个命中只记名；
                    # 窗口内第 2 个不同账号也命中才确认波次。确认后本请求不再换号
                    # （下轮入口拦截），API Key 回退除外（api.z.ai 通道与被拦的 plan
                    # 通道无关）。惩罚落地（lane 级或旧整号曲线含 RISK_AUTO_ROTATE）
                    # 已由 _apply_risk_strike 统一处理。
                    wave_confirmed = _risk_wave_confirm(account.provider, account.id)
                    if _jump_to_fallback():
                        logs.warn(
                            req_id,
                            f"账号 {account.name} 命中风控 HTTP {status_code}（出口={captcha_egress}），"
                            f"{penalty_note}，切 API Key 回退；上游body={snippet!r}",
                        )
                        needs_captcha = False
                        continue
                    if wave_confirmed:
                        logs.warn(
                            req_id,
                            f"账号 {account.name} 命中风控 HTTP {status_code}（出口={captcha_egress}），"
                            f"{penalty_note}，确认频道级波次（多账号连中），全池暂停 {settings.RISK_WAVE_PAUSE}s+，"
                            f"本请求不再换号；上游body={snippet!r}",
                        )
                    else:
                        logs.warn(
                            req_id,
                            f"账号 {account.name} 命中风控 HTTP {status_code}（出口={captcha_egress}），"
                            f"{penalty_note}，切换下一个；上游body={snippet!r}",
                        )
                    return _NEXT_ACCOUNT

                if _is_exhausted(status_code, text):
                    # 1113 通道感知（覆盖 402/200 承载形态）：Key 通道的 1113 只记
                    # 选号冷却，不得把账号标 EXHAUSTED（Plan 可能仍在）。
                    _ex_code = business_error.business_code(_safe_json(text))
                    if _ex_code == constants.NO_BALANCE_BUSINESS_CODE and channel != "zai":
                        _key_no_balance[account.id] = time.time() + settings.KEY_NO_BALANCE_COOLDOWN
                        reqlog.mark_attempt(req_id, account.name, account.mode,
                                            channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                            status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                            error="1113 无余额（Key 无资源包），选号冷却", code=1113)
                        account.record_result(False, "1113 无余额（Key 无资源包），跳过")
                        store.update_account(account)
                        logs.warn(req_id, f"账号 {account.name} 1113 无余额（Key），"
                                  f"选号冷却 {settings.KEY_NO_BALANCE_COOLDOWN}s，账号状态不动")
                        return _NEXT_ACCOUNT
                    reqlog.mark_attempt(req_id, account.name, account.mode,
                                        channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                        status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                        error="额度用完")
                    account.record_result(False, f"额度用完 HTTP {status_code}")
                    if account.status in (Status.INVALID, Status.DISABLED):
                        store.update_account(account)
                    else:
                        reset_ts = _parse_exhaust_reset(text)
                        if reset_ts:
                            account.cooling_until = reset_ts
                            account.last_error = (
                                f"额度已用完（重置于 "
                                f"{time.strftime('%m-%d %H:%M', time.localtime(reset_ts))}）"
                            )
                            account.status = Status.EXHAUSTED
                            store.update_account(account)
                        else:
                            _mark(account, Status.EXHAUSTED, "额度已用完")
                        _spawn_bg(_safe_refresh(account))
                        _note_pool_exhausted(account)
                        # 模型级冷却（上游 2.7.29）：Plan 通道确认耗尽的（账号，模型）
                        if channel == "zai" and model_name and model_name != "-":
                            plan_guard.note(
                                account.id, model_name,
                                reset_ts or (time.time() + settings.PLAN_MODEL_EXHAUST_COOLDOWN),
                            )
                    logs.warn(req_id, f"账号 {account.name} 额度用完，切换下一个")
                    return _NEXT_ACCOUNT

                if status_code == 401:
                    account.record_result(False, "鉴权失败 HTTP 401")
                    _mark(account, Status.INVALID, "鉴权失败 HTTP 401")
                    if _jump_to_fallback():
                        logs.warn(req_id, f"账号 {account.name} 鉴权失败 401，切 API Key 回退")
                        needs_captcha = False
                        continue
                    logs.warn(req_id, f"账号 {account.name} 鉴权失败 401，切换下一个")
                    return _NEXT_ACCOUNT

                if status_code == 403:
                    # 403 已排除挑战形态（上方 challenge 分支），此处为真实鉴权拒绝
                    account.record_result(False, "鉴权失败 HTTP 403")
                    _mark(account, Status.INVALID, "鉴权失败 HTTP 403")
                    if _jump_to_fallback():
                        logs.warn(req_id, f"账号 {account.name} 鉴权失败 403，切 API Key 回退")
                        needs_captcha = False
                        continue
                    logs.warn(req_id, f"账号 {account.name} 鉴权失败 403，切换下一个")
                    return _NEXT_ACCOUNT

                if status_code == 429:
                    # 1113「无余额/无资源包」是 Key 的永久状态（api.z.ai 实测秒回），
                    # 原地等待重试毫无意义，只会把客户端吊死在 RETRY_429_TIMES×WAIT 里
                    # ——快速换号/失败。
                    _bis_code = business_error.business_code(_safe_json(text))
                    if _bis_code == constants.NO_BALANCE_BUSINESS_CODE \
                            or "insufficient balance" in text.lower():
                        # 通道感知：Plan 1113 = 套餐额度耗尽 → 模型级冷却（fork 语义：
                        # 同号其它模型额度独立，不整号标死）；Key 1113 = 该 Key 无资源包
                        # → 只做选号冷却，账号状态/风控计数一律不动。
                        model_key = str(body.get("model") or "")
                        if channel == "zai":
                            until = account.cool_model(
                                model_key,
                                settings.MODEL_NO_QUOTA_COOLDOWN_BASE,
                                settings.MODEL_NO_QUOTA_COOLDOWN_MAX,
                            )
                            reqlog.mark_attempt(req_id, account.name, account.mode,
                                                channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                                status=429, ms=(time.time() - attempt_t0) * 1000,
                                                error=f"429 1113 无余额，{model_key} 冷却", code=1113)
                            account.record_result(
                                False,
                                f"429 1113 无余额（{model_key or '-'} 冷却至 "
                                f"{time.strftime('%H:%M', time.localtime(until))}）",
                            )
                            account.last_error = (
                                f"429 1113 无余额（{model_key or '-'} 冷却至 "
                                f"{time.strftime('%H:%M', time.localtime(until))}）")
                            store.update_account(account)
                            _note_pool_exhausted(account)
                            if channel == "zai" and model_key and model_key != "-":
                                plan_guard.note(account.id, model_key,
                                                until or (time.time() + settings.PLAN_MODEL_EXHAUST_COOLDOWN))
                            logs.warn(req_id, f"账号 {account.name} 429 为 1113 无余额（Plan），"
                                      f"{model_key} 冷却，跳过等待重试")
                        else:
                            _key_no_balance[account.id] = time.time() + settings.KEY_NO_BALANCE_COOLDOWN
                            reqlog.mark_attempt(req_id, account.name, account.mode,
                                                channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                                status=429, ms=(time.time() - attempt_t0) * 1000,
                                                error="429 1113 无余额（Key 无资源包），选号冷却", code=1113)
                            account.record_result(False, "429 1113 无余额（Key 无资源包），跳过")
                            store.update_account(account)
                            logs.warn(req_id, f"账号 {account.name} 429 为 1113 无余额（Key），"
                                      f"选号冷却 {settings.KEY_NO_BALANCE_COOLDOWN}s，账号状态不动")
                        if _jump_to_fallback():
                            reqlog.note_retry(req_id, "fb")
                            needs_captcha = False
                            retries_429 = 0
                            continue
                        return _NEXT_ACCOUNT
                    if _is_rate_limit_hit(text):
                        # 1302/1303「真限流」：与并发相反，原地重试只会加重——
                        # 账号休息 RATE_LIMIT_COOLDOWN 秒（同 3012 语义）。
                        cooldown = settings.RATE_LIMIT_COOLDOWN
                        if account.status not in (Status.INVALID, Status.DISABLED) \
                                and not account.is_cooling():
                            account.status = Status.COOLING
                            account.cooling_until = time.time() + cooldown
                            account.last_error = f"429 真限流（1302/1303），休息 {cooldown}s"
                        account.record_result(False, f"429 code 1302/1303 真限流，休息 {cooldown}s")
                        store.update_account(account)
                        reqlog.mark_attempt(req_id, account.name, account.mode,
                                            channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                            status=429, ms=(time.time() - attempt_t0) * 1000,
                                            error=f"429 真限流（1302/1303），休息 {cooldown}s", code=1302)
                        logs.warn(req_id, f"账号 {account.name} 429 真限流（1302/1303），"
                                  f"休息 {cooldown}s，不原地重试")
                        if _jump_to_fallback():
                            reqlog.note_retry(req_id, "fb")
                            needs_captcha = False
                            retries_429 = 0
                            continue
                        return _NEXT_ACCOUNT
                    if "1309" in text or "Coding Plan套餐已到期" in text:
                        model_key = str(body.get("model") or "")
                        until = account.cool_model(
                            model_key,
                            settings.MODEL_NO_QUOTA_COOLDOWN_BASE,
                            settings.MODEL_NO_QUOTA_COOLDOWN_MAX,
                        )
                        reqlog.mark_attempt(req_id, account.name, account.mode, channel="plan",
                                            status=429, ms=(time.time() - attempt_t0) * 1000,
                                            error=f"429 1309 Plan 到期，{model_key} 冷却", code=1309)
                        account.record_result(
                            False,
                            f"429 1309 Coding Plan 到期（{model_key or '-'} 冷却至 "
                            f"{time.strftime('%H:%M', time.localtime(until))}）",
                        )
                        store.update_account(account)
                        logs.warn(
                            req_id,
                            f"账号 {account.name} 429 为 1309 Coding Plan 到期，{model_key} 冷却，跳过等待重试",
                        )
                        return _NEXT_ACCOUNT
                    # ── 3008/3009/3010 并发槽「没轮到」（magpie plugins#503）──
                    # 优先于换号：换号会放大上游压力（magpie 对 busy 不换号）。
                    # 官方客户端节奏 1s/2s 原地重问两次，不占 429 频控梯、不扣号；
                    # 重问仍被拒 = 持续饱和，进程级计数 60s 内 BUSY_N 次武装
                    # BUSY_HOLD 全池刹车（赶在 WAF 封 IP 之前）。
                    busy_hit = _is_concurrency_hit(text)
                    if busy_hit and retries_busy < len(settings.BUSY_WAITS):
                        wait = settings.BUSY_WAITS[retries_busy]
                        retries_busy += 1
                        # 首次重问（retries_busy==1）后若再次被拒，即「首拒+1s
                        # 重问仍拒」；此处的第二次执行正是重问失败点。
                        if retries_busy >= 2:
                            riskmetrics.record("busy", account.name, carrier="busy",
                                               reason="concurrency", model=str(body.get("model") or ""))
                            await _busy_record_saturated()
                        reqlog.note_retry(req_id, "429")
                        reqlog.note_wait(req_id, "rl", wait)
                        logs.warn(req_id, f"账号 {account.name} 429 并发槽饱和（3008/3009），"
                                  f"{wait}s 后原地重问（{retries_busy}/{len(settings.BUSY_WAITS)}）")
                        _park_slot(slot_box)
                        await _sleep(wait)
                        if not _reacquire_slot(account, slot_box):
                            logs.warn(req_id, f"账号 {account.name} 并发槽等待后已满，切换下一个")
                            return _NEXT_ACCOUNT
                        continue
                    if busy_hit and retries_busy >= len(settings.BUSY_WAITS):
                        # 用尽官方节奏仍饱和：先下沉 API Key（上游 2.7.23），无回退
                        # 才立本地排队闸门（v2.9.32 startGate 语义）。
                        if _jump_to_fallback():
                            reqlog.note_retry(req_id, "fb")
                            reqlog.mark_attempt(req_id, account.name, account.mode, channel="plan",
                                                status=429, ms=(time.time() - attempt_t0) * 1000,
                                                error="Plan 并发槽饱和用尽，切 API Key 回退")
                            account.record_result(False, "429 并发槽饱和，切 API Key 回退")
                            logs.warn(req_id, f"账号 {account.name} 429 并发槽饱和，切 API Key 回退")
                            needs_captcha = False
                            retries_busy = 0
                            continue
                        gate_until = _busy_gate_arm(account.id)
                        gate_note = (f"，闸门至 {time.strftime('%H:%M:%S', time.localtime(gate_until))}"
                                     if gate_until else "")
                        reqlog.note_retry(req_id, "429")
                        reqlog.mark_attempt(req_id, account.name, account.mode,
                                            channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                            status=429, ms=(time.time() - attempt_t0) * 1000,
                                            error="429 并发槽饱和")
                        account.record_result(False, "429 并发槽饱和（不扣号）")
                        riskmetrics.record("busy", account.name, carrier="busy",
                                           reason="concurrency_exhausted", model=str(body.get("model") or ""))
                        logs.warn(req_id, f"账号 {account.name} 429 并发槽饱和，原地重问用尽，"
                                  f"立本地闸门{gate_note}后换号")
                        return _NEXT_ACCOUNT
                    # 频控不是账号故障：不冷却，原地等一等再试，耗尽后换号且账号保持可用。
                    # Plan 系渠道耗尽 ≠ apikey 也耗尽：同账号切回退并归还该渠道的重试预算
                    #（与上方 3012/401/403 切回退同一语义）
                    #
                    # 429 换号优先（2026-10-07 实证，对齐 workbuddy-gateway 做法）：
                    # 池内还有其他可服务账号时立刻换号，而不是原地静默等待 —— 实测
                    # 60s×2 轮即 120s 会撞穿 CDN 边缘 ~100s 硬超时（HTTP 524），且等待
                    # 期间零字节发给客户端（实测 183 次 429、67 次踩到第 2 轮以上）。
                    # 仅当全池都被限流（无号可换）时，才回退到下方原地等待兜底。
                    if settings.RETRY_429_FAILOVER_FIRST and _has_other_selectable(
                        account, str(body.get("model") or "")
                    ):
                        # 真·频控（1302/1303/无码 429）才扣号；3008 已在上方单独处理。
                        hold_model = str(body.get("model") or "")
                        hold_secs = 0
                        if settings.RETRY_429_HOLD_DEFAULT > 0:
                            hold_secs = _parse_retry_after(resp.headers.get("retry-after")) \
                                or settings.RETRY_429_HOLD_DEFAULT
                            hold_secs = min(hold_secs, settings.RETRY_429_HOLD_MAX)
                            until = account.hold_rate(hold_model, hold_secs)
                            store.update_account(account)
                        reqlog.note_retry(req_id, "429")
                        reqlog.mark_attempt(req_id, account.name, account.mode,
                                            channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                            status=429, ms=(time.time() - attempt_t0) * 1000, error="429 频控，换号优先")
                        account.record_result(False, "429 频控，换号优先（不原地静默等待）")
                        riskmetrics.record("rate", account.name, carrier="rate",
                                           status=status_code, model=str(body.get("model") or ""))
                        logs.warn(req_id, f"账号 {account.name} 被限流 429，池内有其他账号，立刻换号"
                                  + (f"（429 扣号 {hold_secs}s 至 "
                                     f"{time.strftime('%H:%M', time.localtime(until))}）" if hold_secs else ""))
                        return _NEXT_ACCOUNT
                    if retries_429 < settings.RETRY_429_TIMES:
                        retries_429 += 1
                        reqlog.note_retry(req_id, "429")
                        concurrency_hit = busy_hit
                        wait = _parse_retry_after(resp.headers.get("retry-after"))
                        if wait is None:
                            if concurrency_hit:
                                wait = settings.RETRY_429_CONCURRENCY_WAIT + random.randint(0, 2)
                            else:
                                wait = settings.RETRY_429_WAIT
                        if concurrency_hit:
                            logs.warn(
                                req_id,
                                f"账号 {account.name} 模型并发已满（3009），{wait}s 后重试"
                                f"（{retries_429}/{settings.RETRY_429_TIMES}）",
                            )
                        else:
                            logs.warn(
                                req_id,
                                f"账号 {account.name} 被限流 429，{wait}s 后重试"
                                f"（{retries_429}/{settings.RETRY_429_TIMES}）",
                            )
                        _park_slot(slot_box)
                        reqlog.note_wait(req_id, "rl", wait)
                        await _sleep(wait)
                        if not _reacquire_slot(account, slot_box):
                            logs.warn(req_id, f"账号 {account.name} 429 等待后并发已满，切换下一个")
                            return _NEXT_ACCOUNT
                        continue
                    if _jump_to_fallback():
                        # 渠道梯语义（上游 e038650）：下沉到 apikey 渠道并归还重试预算；
                        # _jump_to_fallback 已重置 retries_429/retries_5xx/captcha_retries，
                        # retries_busy（fork 3008 重问梯）同样归零。
                        reqlog.note_retry(req_id, "fb")
                        reqlog.mark_attempt(req_id, account.name, account.mode, channel="plan",
                                            status=429, ms=(time.time() - attempt_t0) * 1000,
                                            error=f"Plan 通道 429 耗尽（重试 {retries_429} 次），切回退")
                        account.record_result(False, "Plan 通道 429 耗尽，切 API Key 回退")
                        logs.warn(
                            req_id,
                            f"账号 {account.name} Plan 通道 429 耗尽，切 API Key 回退",
                        )
                        needs_captcha = False
                        retries_busy = 0
                        continue
                    account.record_result(False, f"429 重试 {settings.RETRY_429_TIMES} 次耗尽")
                    reqlog.mark_attempt(req_id, account.name, account.mode,
                                        channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                        status=429, ms=(time.time() - attempt_t0) * 1000,
                                        error=f"429 重试 {retries_429} 次耗尽")
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
                        reqlog.note_retry(req_id, "5xx")
                        reqlog.note_wait(req_id, "5xx", settings.RETRY_5XX_WAIT)
                        reqlog.mark_attempt(req_id, account.name, account.mode,
                                            channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                            status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                            error=f"上游 {status_code}，{settings.RETRY_5XX_WAIT}s 后重试")
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
                    reqlog.mark_attempt(req_id, account.name, account.mode,
                                        channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                        status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                        error=f"5xx 重试 {retries_5xx} 次耗尽，冷却换号")
                    if (account.status in (Status.INVALID, Status.DISABLED)
                            or account.is_cooling() or account.has_lane_cooling()):
                        # Key 回退 5xx 不得覆盖废 JWT / 风控禁用，否则冷却结束会重开 Plan；
                        # 风控退避同理：整号冷却或通道级 lane 退避在场时，不得用通用
                        # 300s 冷却与文案覆盖真因（lane 退避更长，覆盖=提前重打）
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
                reqlog.mark_attempt(req_id, account.name, account.mode,
                                    channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                    status=status_code, ms=(time.time() - attempt_t0) * 1000,
                                    error=f"HTTP {status_code}: {text[:80]}")
                # 上游把业务码装进 4xx body（3006/1002/1309 等）时一并落 code：
                # 「HTTP 400」不够排障，业务码才是区分套餐外/额度/验证码的真因
                biz_code = _upstream_biz_code(text)
                reqlog.finish_error(req_id, f"HTTP {status_code}: {text[:120]}".replace("\n", " "),
                                    status=status_code, t_first=time.time() - attempt_t0,
                                    code=biz_code, kind="biz" if biz_code is not None else "upstream")
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
                        diaglog.record_failure(req_id, account_name=account.name, mode=account.mode,
                                               model=str(body.get("model") or ""), url=url,
                                               status=200, text=text, request_body=body)
                        reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                        await cm.__aexit__(None, None, None)
                        await client.aclose()
                        if code_hit in _RISK_BIZ_CODES:
                            reqlog.mark_attempt(req_id, account.name, account.mode,
                                                channel="plan", status=_status,
                                                ms=(time.time() - attempt_t0) * 1000,
                                                error=f"风控封禁（业务码 {code_hit}）", code=code_hit)
                            account.record_result(False, f"风控封禁（业务码 {code_hit}）")
                            penalty_note = _apply_risk_strike(
                                account, f"业务码 {code_hit}", req_id,
                                model=str(body.get("model") or ""),
                                channel="plan" if "/zcode-plan/" in url else "apikey",
                            )
                            # 2026-10-25 防放大器：与 HTTP 405 分支同处置（两账号确认制）
                            wave_confirmed = _risk_wave_confirm(account.provider, account.id)
                            if _jump_to_fallback():
                                logs.warn(
                                    req_id,
                                    f"账号 {account.name} 命中风控业务码 {code_hit}，{penalty_note}，切 API Key 回退",
                                )
                                needs_captcha = False
                                continue
                            if wave_confirmed:
                                logs.warn(req_id, f"账号 {account.name} 命中风控业务码 {code_hit}，{penalty_note}，确认频道级波次（多账号连中），全池暂停 {settings.RISK_WAVE_PAUSE}s+，本请求不再换号")
                            else:
                                logs.warn(req_id, f"账号 {account.name} 命中风控业务码 {code_hit}，{penalty_note}，切换下一个")
                            return _NEXT_ACCOUNT
                        if code_hit in _QUOTA_BIZ_CODES:
                            reqlog.mark_attempt(req_id, account.name, account.mode,
                                                channel=_lane_for(force_fallback, plan_channel, url, channel=channel),
                                                status=_status, ms=(time.time() - attempt_t0) * 1000,
                                                error=f"额度用完（业务码 {code_hit}）", code=code_hit)
                            account.record_result(False, f"额度用完（业务码 {code_hit}）")
                            if account.status in (Status.INVALID, Status.DISABLED):
                                store.update_account(account)
                            else:
                                _mark(account, Status.EXHAUSTED, "额度已用完")
                                _spawn_bg(_safe_refresh(account))
                                _note_pool_exhausted(account)
                                # 模型级冷却：200 承载的 1005 同样打点
                                if channel == "zai" and model_name and model_name != "-":
                                    plan_guard.note(account.id, model_name,
                                                    time.time() + settings.PLAN_MODEL_EXHAUST_COOLDOWN)
                            logs.warn(req_id, f"账号 {account.name} 额度用完（业务码 {code_hit}），切换下一个")
                            return _NEXT_ACCOUNT
                        # 其余业务码：不动账号状态，直接回客户端（避免持续施压）
                        account.record_result(False, f"上游业务错误 {code_hit}")
                        store.update_account(account)
                        logs.req_err(req_id, f"账号 {account.name} 上游业务错误 {code_hit}")
                        reqlog.finish_error(req_id, f"上游业务错误 {code_hit}", status=_status,
                                            code=code_hit, kind="biz")
                        return JSONResponse(
                            {"error": {"message": f"上游业务错误 {code_hit}", "type": etype,
                                       "upstream": text[:400]}},
                            status_code=_status,
                        )
                    # 业务正常的同步响应：body 已读完，重建等价响应交给上层
                    if cm is not None:
                        await cm.__aexit__(None, None, None)
                    if client is not None:
                        await client.aclose()
                    cm = client = None
                    from fastapi import Response as _Resp
                    account.use_count += 1
                    account.last_used_at = time.time()
                    account.record_result(True, f"HTTP 200 · {model_name} · {time.time() - attempt_t0:.1f}s")
                    # API Key 回退成功不得把废 JWT / 风控禁用洗成 active，也不得清风控
                    # 计数；风控冷却未到期同样不洗（与下方流式路径同一守卫语义，
                    # 否则 3012 后同请求走 Key 回退成功会把冷却洗掉，立刻重打 Plan）
                    account.clear_model_cooling(str(body.get("model") or ""))
                    account.clear_rate_hold(str(body.get("model") or ""))  # 上游 2xx 成功即放回 429 扣号
                    # 真实成功按实际通道聚合解封（url 判通道：内部 lane 重路由时
                    # force_fallback 仍为 False，不能用请求参数猜通道）
                    channel_used = "plan" if "/zcode-plan/" in url else "apikey"
                    account.clear_channel_cooling(channel_used)
                    reqlog.mark_channel(req_id, channel_used)
                    # 计数清洗要求整号无任何在场风控退避：lane 冷却中成功（如走回退腿）
                    # 保留 last_error 与 strikes 审计；lane 清空后的 plan 成功才全量清洗
                    if (account.status not in (Status.INVALID, Status.DISABLED)
                            and not account.is_cooling() and not account.has_lane_cooling()):
                        account.risk_strikes = 0
                        account.last_risk_at = None
                        account.rate_strikes = 0
                        account.net_strikes = 0
                        account.net_last_at = None
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
                    sync_stop = sync_data.get("stop_reason") if isinstance(sync_data, dict) else None
                    reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
                    reqlog.finish_ok(req_id, t_first=time.time() - attempt_t0, status=status_code,
                                     input_tokens=sync_usage.get("input_tokens"),
                                     output_tokens=sync_usage.get("output_tokens"),
                                     cache_read=_cache_read(sync_usage),
                                     stop_reason=str(sync_stop) if sync_stop else None)
                    record_usage(model_name, sync_usage.get("input_tokens"),
                                 sync_usage.get("output_tokens"))
                    # 2026-10-25 防放大器：Plan 通道真实成功 = 上游风控窗口已开，
                    # 解除波次暂停与命中记忆；Key 回退成功不算（api.z.ai 通道与
                    # 被拦的 plan 通道无关，不代表 Plan 线恢复）。同步直通在此收口。
                    if channel_used == "plan":
                        _risk_wave_clear(account.provider)
                    _drop_gate()  # 同步响应已收口
                    # ToolSearch 别名反向还原（同步路径；流式在 to_streaming）
                    if body_transform.TOOL_SEARCH_ALIAS in text:
                        text = text.replace(body_transform.TOOL_SEARCH_ALIAS,
                                            body_transform.TOOL_SEARCH)
                    return _Resp(content=text, status_code=status_code,
                                 media_type=resp.headers.get("content-type") or "application/json")

            account.use_count += 1
            account.last_used_at = time.time()
            account.record_result(True, f"HTTP 200 · {model_name} · {time.time() - attempt_t0:.1f}s")
            # API Key 回退成功不得把废 JWT / 风控禁用洗成 active，也不得清风控计数；
            # 风控冷却未到期同样不洗（上游风控标记为小时级粘性，回退通道成功不代表
            # Plan 通道已解除，洗掉会立刻重打 Plan 并累加计数）。冷却已过期后的
            # Plan 通道成功即证明恢复，全量清理（清风控计数）。
            account.clear_model_cooling(str(body.get("model") or ""))
            account.clear_rate_hold(str(body.get("model") or ""))  # 上游 2xx 成功即放回 429 扣号
            # 真实成功按实际通道聚合解封（url 判通道，见同步直通出口注释）
            channel_used = "plan" if "/zcode-plan/" in url else "apikey"
            account.clear_channel_cooling(channel_used)
            reqlog.mark_channel(req_id, channel_used)
            if (account.status not in (Status.INVALID, Status.DISABLED)
                    and not account.is_cooling() and not account.has_lane_cooling()):
                account.risk_strikes = 0
                account.last_risk_at = None
                account.rate_strikes = 0
                account.net_strikes = 0
                account.net_last_at = None
                account.last_error = None
                account.cooling_until = None
                if account.status in (Status.COOLING, Status.EXHAUSTED):
                    account.status = Status.ACTIVE
            store.update_account(account)
            _spawn_bg(_safe_refresh(account))

            # 2026-10-25 防放大器：与同步直通出口同语义 —— Plan 通道真实成功才
            # 解除波次暂停与命中记忆；Key 回退成功不算。
            if channel_used == "plan":
                _risk_wave_clear(account.provider)

            reqlog.mark_upstream_meta(req_id, _upstream_trace(resp))
            # 所有权先摘再随流转移：finally 只在「未转移」时归还
            gate_release = gate.release if gate_acquired else None
            gate_acquired = False
            return _Upstream(resp, cm, client, t_first=time.time() - attempt_t0,
                             account_name=account.name, mode=account.mode, model=model_name,
                             gate_release=gate_release)
    finally:
        _drop_gate()


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
