"""后台管理 API：/admin/api/*（账号池、设置、用量监控）。"""

from __future__ import annotations

import asyncio
import time

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException

from .. import (
    alertlog,
    captcha_ledger,
    dedup,
    diaglog,
    forecast,
    freshness,
    logs,
    reqlog,
    riskmetrics,
    session_ctx,
    usage_stats,
    vitals,
)
from ..auth_admin import verify_admin_key
from ..body_transform import jwt_user_id
from ..captcha import CaptchaSolveError
from ..claim import (
    AUTH_EXPIRED_MESSAGE,
    ClaimError,
    auto_claim_all_plans,
    billing_block_reason,
    claim_with_captcha,
    preview_plans,
    report_activation_events,
)
from ..claim import claim as do_claim
from ..models import PROVIDERS, Account, Status
from ..oauth import ZaiAuthFlow, parse_userinfo
from ..quota import fetch_coding_limits, fetch_quota, refresh_accounts
from ..store import store

router = APIRouter(prefix="/admin/api", dependencies=[Depends(verify_admin_key)])


# ── 鉴权探针 ─────────────────────────────────────────────────────────────────
@router.get("/verify")
async def verify():
    return {"status": "ok"}


# ── 账号列表 + 概览统计 ──────────────────────────────────────────────────────
def _username_for(account) -> str | None:
    """账号对应的上游用户名（OAuth 授权瞬间抓取，按 uid 存 alias:{uid}）。

    存量账号补查不到：userinfo 只认 poll 返回的 access_token（授权瞬间独有、
    过期即弃），池内存量 JWT 打过去一律 401（2026-09-30 实测）——所以这里
    只有 OAuth 入池的账号才可能有值，其余返回 None 属预期。

    之前 alias 只写不读（入池存了却无人消费），故抓到了也不会显示；此处补上消费端。
    """
    uid = account.uid()
    if not uid:
        return None
    alias = store.get_setting(f"alias:{uid}")
    return str(alias) if alias else None


@router.get("/sections/freshness")
async def sections_freshness():
    """system sidecar 文本新鲜度（2026-10-31 文本校验波次防复发哨兵）。

    现算并回写（status() 零网络纯本地读）：官方升版后最长 6h 的巡检盲窗里
    控制台一直亮绿（2026-11-02 实测），读路径现算即消。回写同时喂饱
    stale_hint() 热路径与 healthcheck。
    """
    return {"data": freshness.refresh()}


@router.get("/session-affinity")
async def session_affinity(days: int = 7):
    """会话亲和观测聚合：首见 vs 复见请求的 cache_read 均值对比（observe 档）。"""
    return {"data": session_ctx.summary(days=days)}


@router.get("/diaglog")
async def query_diaglog(days: int = 3, limit: int = 200, status: int | None = None):
    """失败请求诊断留存（响应体 + 请求形状指纹，默认 3 天）。"""
    return {"data": {"rows": diaglog.query(days=days, limit=limit, status=status)}}


@router.get("/accounts")
async def list_accounts():
    now = time.time()
    accounts = []
    for a in store.list_accounts():
        view = a.public_view()
        view["username"] = _username_for(a)
        accounts.append(view)
    stats = {"total": len(accounts), "active": 0, "exhausted": 0,
             "cooling": 0, "invalid": 0, "disabled": 0,
             "calls": 0, "fail": 0}
    for a in accounts:
        st = a["status"]
        if st in stats:
            stats[st] += 1
        stats["calls"] += a["use_count"]
        stats["fail"] += a["fail_count"]
    return {"accounts": accounts, "stats": stats, "providers": list(PROVIDERS), "ts": now}


@router.get("/status")
async def status_info():
    return {
        "providers": list(PROVIDERS),
        "gateway_key_set": bool(store.gateway_key()),
        "quota_pool": {
            p: sum(1 for a in store.list_accounts(p) if a.is_selectable())
            for p in PROVIDERS
        },
    }


# ── 风控态势总览（2026-11-01，控制台「风控态势」区块数据源）──────────────────
@router.get("/risk-overview")
async def risk_overview(days: int = 7):
    """一屏风控态势：按天计数 / 账号 lane 冷却与 strikes / 波次暂停 / 新鲜度 / 去重。"""
    import time as _time

    agg = riskmetrics.aggregate(min(max(int(days), 1), 90))
    day_rows = agg.get("days") or []
    today = day_rows[-1] if day_rows else {}

    now = _time.time()
    lanes = []
    for a in store.list_accounts("zai"):
        cooling = [
            {"lane": ln, "remain_s": max(0, int(until - now))}
            for ln, until in (a.risk_lane_cooling or {}).items() if until > now
        ]
        strikes = {k: round(v, 1) for k, v in (a.risk_lane_strikes or {}).items()}
        if cooling or strikes or a.status == Status.COOLING:
            lanes.append({
                "name": a.name, "status": a.status, "enabled": bool(a.enabled),
                "lanes": cooling, "strikes": strikes,
                "last_error": (a.last_error or "")[:120],
            })

    from . import gateway as gw  # 惰性：避免模块加载顺序耦合（同包相对导入）
    waves = []
    for provider, until in dict(getattr(gw, "_risk_wave_until", {})).items():
        if until > now:
            waves.append({
                "provider": provider, "remain_s": int(until - now),
                "level": int(getattr(gw, "_risk_wave_level", {}).get(provider, 0)),
            })

    # 新鲜度现算并回写（同 /sections/freshness，消巡检盲窗；30s 轮询也吃到官方升版漂移）
    fresh = freshness.refresh()

    return {
        "data": {
            "days": day_rows,
            "today": {k: today.get(k, 0) for k in ("risk", "rate", "net", "wave")},
            "accounts": lanes,
            "waves": waves,
            "freshness": fresh,
            "dedup": dedup.stats(),
        }
    }


# ── 风控指标（2026-10-30）───────────────────────────────────────────────────
# 405/3012 命中、429 限流、连接失败、波次确认的按天聚合；days 上限 90。
@router.get("/risk-metrics")
async def risk_metrics(days: int = 7):
    return riskmetrics.aggregate(days)


# ── 新增账号 ─────────────────────────────────────────────────────────────────
@router.post("/accounts")
async def add_accounts(payload: dict = Body(...)):
    provider = payload.get("provider", "zai")
    if provider not in PROVIDERS:
        raise HTTPException(400, "不支持的 provider")
    tokens = payload.get("tokens") or []
    if isinstance(tokens, str):
        tokens = [t.strip() for t in tokens.splitlines() if t.strip()]
    tokens = [t.strip() for t in tokens if t and t.strip()]
    if not tokens:
        raise HTTPException(400, "请输入至少一个 Token / API Key")

    added = []
    existing = {a.id for a in store.list_accounts(provider)}  # 识别真新增（重复 token 跳过）
    for tok in dict.fromkeys(tokens):  # 去重保序
        name = payload.get("name") or f"{provider}-{len(store.list_accounts(provider)) + 1}"
        acc = store.add_account(provider, name, tok)
        added.append(acc.id)
    # 立即刷新一次额度（仅 zai jwt）
    fresh = [a for a in store.list_accounts(provider) if a.id in added and a.mode == "jwt"]
    if fresh:
        await refresh_accounts(fresh)
    # 真新增（id 不在加号前的集合里；重复 token 返回旧账号自动跳过）
    new_accounts = [a for a in store.list_accounts(provider)
                    if a.id in added and a.id not in existing]
    for acc in new_accounts:
        _schedule_install(acc)  # 按账号安装序（含 apiKey 账号，幂等）
        if acc.mode == "jwt":
            _schedule_auto_claim(acc)  # 授权完成即激活+自动领取，入池即吃满活动
    return {"count": len(added), "ids": added}


# ── 删除账号 ─────────────────────────────────────────────────────────────────
@router.delete("/accounts")
async def delete_accounts(ids: list[str] = Body(...)):
    deleted = 0
    for aid in ids:
        acc = store.find_any(aid)
        if acc and store.remove_account(acc.provider, aid):
            deleted += 1
    return {"deleted": deleted}


# ── 编辑账号 ─────────────────────────────────────────────────────────────────
@router.put("/accounts/{account_id}")
async def edit_account(account_id: str, payload: dict = Body(...)):
    acc = store.find_any(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    if "name" in payload and payload["name"]:
        acc.name = payload["name"].strip()
    secret = payload.get("token") or payload.get("secret")
    if secret:
        secret = secret.strip()
        is_jwt = secret.count(".") == 2 and acc.provider == "zai"
        if is_jwt:
            acc.mode = "jwt"
            acc.jwt_token = secret
            # 换 JWT 不清已有 API Key：OAuth 兑换出的回退通道要保住
        else:
            acc.mode = "apiKey"
            acc.api_key = secret
            acc.jwt_token = None
        acc.status = Status.ACTIVE
        acc.last_error = None
    if "proxy_url" in payload:
        # 账号级出口代理（空串 = 清除恢复直连）；非法协议在写入口拒绝，不落库
        try:
            acc.proxy_url = Account.normalize_proxy(payload.get("proxy_url"))
        except ValueError as err:
            raise HTTPException(400, str(err)) from None
    store.update_account(acc)
    return {"ok": True}


# ── 启用 / 禁用 ──────────────────────────────────────────────────────────────
@router.post("/accounts/{account_id}/enabled")
async def set_enabled(account_id: str, payload: dict = Body(...)):
    acc = store.find_any(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    enabled = bool(payload.get("enabled", True))
    store.set_enabled(acc.provider, account_id, enabled)
    return {"ok": True}


# ── 客户端指纹（每账号独立设备档案）──────────────────────────────────────────
@router.post("/accounts/{account_id}/fingerprint/rotate")
async def rotate_fingerprint(account_id: str):
    """换发账号客户端指纹（下一套设备模板 + 全新 device_mid）。

    场景：账号被风控后换设备重生；或怀疑指纹污染时手动更换。
    """
    from ..fingerprint import profile_for, rotate

    acc = store.find_any(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    old = profile_for(acc)
    profile = rotate(acc)
    acc.installed_at = None  # 换机后必须重跑安装序，旧 MID 的日活不能顶新设备
    store.update_account(acc)
    _schedule_install(acc)
    logs.info("fingerprint", f"账号 {acc.name} 指纹换发: "
                             f"{old.platform_full}/{old.device_mid[:8]} → "
                             f"{profile.platform_full}/{profile.device_mid[:8]}")
    return {"ok": True, "fingerprint": acc.fingerprint}


# ── 刷新额度（实时用量监控）─────────────────────────────────────────────────
@router.post("/accounts/refresh")
async def refresh(payload: dict = Body(default=None)):
    payload = payload or {}
    if payload.get("all"):
        pool = [a for a in store.list_accounts("zai") if a.mode == "jwt"]
    else:
        ids = set(payload.get("ids") or [])
        pool = [a for a in store.list_accounts() if a.id in ids and a.mode == "jwt"]
    # 失效 / 风控封禁 / 冷却 一律不打 billing（冷却期零上游流量 + 废 JWT 不再 401）；
    # 手动停用的好票要能手动刷新（allows_claim），否则领到的新包在面板看不见
    targets = [a for a in pool if a.allows_claim()]
    skipped_cooling = sum(1 for a in pool if a.is_cooling())
    skipped_invalid = len(pool) - len(targets) - skipped_cooling
    summary = await refresh_accounts(targets)
    return {
        "summary": summary,
        "count": len(targets),
        "skipped_cooling": skipped_cooling,
        "skipped_invalid": skipped_invalid,
    }


@router.post("/accounts/{account_id}/refresh")
async def refresh_one(account_id: str):
    acc = store.find_any(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    if acc.mode != "jwt":
        return {"ok": False, "message": "仅 Coding Plan (JWT) 账号支持额度查询"}
    blocked = billing_block_reason(acc, action="上游刷新")
    if blocked:
        return {"ok": False, "message": blocked, "account": acc.public_view()}
    res = await fetch_quota(acc)
    return {"ok": "error" not in res, "result": res, "account": acc.public_view()}


@router.get("/accounts/{account_id}/coding_limits")
async def coding_limits(account_id: str):
    """Coding 平面额度（api.z.ai monitor 端点，按 API Key 鉴权）。

    与 billing 族是两套账本：Plan 通道额度看 `quota` 快照，API Key 直连
    api.z.ai 的额度只能从这里看。体验套餐号常返回「当前用户不存在 coding plan」。
    """
    acc = store.find_any(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    res = await fetch_coding_limits(acc)
    return {"ok": "error" not in res, **res}


# ── OAuth 登录（Z.AI）────────────────────────────────────────────────────────
# 授权链接有效期：上游 cli/init 发起的流程约 5 分钟过期（zcode.z.ai 行为）。
# 会话过期后 poll 返回 {"status": "expired"}（幂等，可安全重试）。
LOGIN_FLOW_TTL = 300.0
# 兑换 API Key（getCustomerInfo → api_keys → copy）总时长上限
LOGIN_EXCHANGE_TIMEOUT = 60.0

# flow_id -> {"flow": ZaiAuthFlow, "created": float, "label": str}
# 单进程内存态即可：登录会话不该跨进程存活，重启后用户重新生成链接。
_login_flows: dict[str, dict] = {}


def _login_gc() -> None:
    now = time.time()
    expired = [fid for fid, entry in _login_flows.items() if now - entry["created"] > LOGIN_FLOW_TTL]
    for fid in expired:
        _login_flows.pop(fid, None)


@router.post("/login/start")
async def login_start(payload: dict = Body(default=None)):
    """发起 Z.AI OAuth，返回授权链接供前端展示。

    payload 可选 {"label": "acct-1"} —— 作为账号名前缀入池，便于多号识别。
    """
    payload = payload or {}
    label = (payload.get("label") or "").strip()[:32]
    _login_gc()
    flow = ZaiAuthFlow()
    try:
        flow_id, authorize_url = await flow.init()
    except Exception as err:  # noqa: BLE001
        logs.warn("oauth", f"登录初始化失败: {type(err).__name__}: {err}")
        raise HTTPException(502, f"登录初始化失败: {err}") from err
    _login_flows[flow_id] = {"flow": flow, "created": time.time(), "label": label}
    logs.info("oauth", f"发起登录 flow_id={flow_id} label={label or '-'}")
    return {
        "flow_id": flow_id,
        "authorize_url": authorize_url,
        "expires_in": int(LOGIN_FLOW_TTL),
    }


@router.get("/login/poll/{flow_id}")
async def login_poll(flow_id: str):
    """轮询授权状态；JWT 入池后立即 ready，API Key 兑换/刷新在后台回填。

    返回 status ∈ pending / ready / failed / expired。
    failed 附带 message（上游拒绝原因）；expired 表示会话超时需重新发起；
    未知 flow_id 一律 expired（而非 404），前端据此提示重新生成链接。
    官方 poll HTTP 4xx 视为终态 failed；5xx/网络抖动保持 pending 并打日志。
    """
    _login_gc()
    entry = _login_flows.get(flow_id)
    if not entry:
        return {"status": "expired"}
    flow = entry["flow"]
    try:
        data = await flow.poll(flow_id)
    except httpx.HTTPStatusError as err:
        code = err.response.status_code
        logs.warn("oauth", f"poll {flow_id} 上游 HTTP {code}")
        if 400 <= code < 500:
            _login_flows.pop(flow_id, None)
            # 3004（官方 poll 4xx 承载的会话过期）：明确映射 expired 让前端提示
            # 重新生成链接，不再归为 failed（zcode-switch poll 3004 语义同形）
            try:
                body_code = (err.response.json() or {}).get("code")
            except ValueError:
                body_code = None
            if body_code == 3004:
                logs.info("oauth", f"授权会话过期 flow_id={flow_id}（上游 3004）")
                return {"status": "expired", "message": "授权会话已过期，请重新生成授权链接"}
            return {"status": "failed", "message": f"上游拒绝轮询（HTTP {code}）"}
        return {"status": "pending"}
    except Exception as err:  # noqa: BLE001 - 单次网络抖动按 pending 处理
        logs.warn("oauth", f"poll {flow_id} 抖动: {type(err).__name__}")
        return {"status": "pending"}

    state = data.get("status")
    if state == "failed":
        _login_flows.pop(flow_id, None)
        reason = (data.get("message") or data.get("reason") or "授权失败或被拒绝")
        logs.warn("oauth", f"授权失败 flow_id={flow_id}: {reason}")
        return {"status": "failed", "message": str(reason)}
    if state != "ready":
        return {"status": "pending"}

    # 会话先摘除再入池：并发/重复 poll 不会再进入兑换链。
    _login_flows.pop(flow_id, None)

    # JWT 先入池并立刻 ready；兑换 API Key / 额度刷新改后台，避免卡住前端下一轮 poll。
    zcode_jwt = data.get("token")
    access_token = (data.get("zai") or {}).get("access_token")
    label = entry.get("label") or "oauth-login"
    # 抓取用户名：userinfo 只认 poll 返回的 data.zai.access_token（授权瞬间独有，
    # 过期即弃），池内存量凭据打上游身份接口一律被拒——uid 与真实账号用户名的
    # 对应关系错过这一刻就再也查不到（2026-09-30 实测 5247/1698 补查失败）。
    uid = jwt_user_id(zcode_jwt)
    if zcode_jwt and access_token and uid:
        try:
            username = parse_userinfo(await flow.userinfo(access_token), uid)
        except Exception:  # noqa: BLE001 - 命名是锦上添花，绝不能阻塞入池主链路
            username = None
        if username:
            store.set_setting(f"alias:{uid}", username)
            if label == "oauth-login":
                label = username
            logs.info("oauth", f"flow_id={flow_id} 抓取用户名 uid={uid} → {username}")
    account = None
    if zcode_jwt:
        account = store.add_account("zai", label, zcode_jwt)
    elif access_token:
        try:
            api_key = await asyncio.wait_for(
                flow.exchange_api_key(access_token), timeout=LOGIN_EXCHANGE_TIMEOUT
            )
            account = store.add_account("zai", label, api_key)
        except Exception as err:  # noqa: BLE001
            logs.warn("oauth", f"无 JWT 时兑换 API Key 失败: {err}")

    if account is None:
        logs.warn("oauth", f"授权结果无凭证 flow_id={flow_id}")
        return {"status": "failed", "message": "未能从授权结果中获取凭证"}

    if account.mode == "jwt":
        _schedule_auto_claim(account)  # 授权完成即激活+自动领取，入池即吃满活动
    _schedule_install(account)  # 按账号安装序（幂等；apiKey 账号同样安装）
    _schedule_login_followup(account, flow, access_token if zcode_jwt else None)
    logs.info("oauth", f"授权成功入池 {account.name} ({account.id}) mode={account.mode}")
    return {"status": "ready", "account": account.public_view()}


# ── 额度领取 ─────────────────────────────────────────────────────────────────
_auto_claim_tasks: set[asyncio.Task] = set()  # 强引用防 GC
_login_followup_tasks: set[asyncio.Task] = set()


def _schedule_login_followup(account, flow, access_token: str | None) -> None:
    """ready 后后台兑换 API Key 并刷新额度；失败只打日志，不影响已入池的 JWT。"""

    async def _job():
        live = store.find("zai", account.id)
        if live is None:
            return
        if access_token:
            try:
                api_key = await asyncio.wait_for(
                    flow.exchange_api_key(access_token), timeout=LOGIN_EXCHANGE_TIMEOUT
                )
                live = store.find("zai", account.id)
                if live is None:
                    return
                live.api_key = api_key
                store.update_account(live)
            except Exception as err:  # noqa: BLE001 - 兑换失败不影响 JWT 已入池
                logs.warn("oauth", f"账号 {account.name} 兑换 API Key 失败: {err}")
        live = store.find("zai", account.id)
        if live is None:
            return
        if live.mode == "jwt":
            try:
                await refresh_accounts([live])
            except Exception as err:  # noqa: BLE001
                logs.warn("oauth", f"账号 {account.name} 入池后额度刷新失败: {err}")

    task = asyncio.create_task(_job())
    _login_followup_tasks.add(task)
    task.add_done_callback(_login_followup_tasks.discard)


def _schedule_auto_claim(account) -> None:
    """入池后调度后台自动领取（激活上报 + 全量可领套餐）。

    不阻塞入池响应（验证码求解可长达数十秒）；仅 JWT 账号。失败不影响入池。
    """
    if not (account.mode == "jwt" and account.jwt_token):
        return

    async def _job():
        live = store.find(account.provider, account.id)
        if live is None:
            return
        try:
            outcomes = await auto_claim_all_plans(live)
            live = store.find(account.provider, account.id)
            if live is None:
                return
            if outcomes:
                await refresh_accounts([live])  # 领到额度立即反映到 UI
        except Exception as err:  # noqa: BLE001 - 兜底：绝不冒泡
            logs.warn("claim", f"账号 {account.name} 自动领取任务异常: {err}")

    task = asyncio.create_task(_job())
    _auto_claim_tasks.add(task)
    task.add_done_callback(_auto_claim_tasks.discard)


def _jwt_accounts(account_ids: list[str] | None) -> list:
    accounts = store.list_accounts("zai")
    if account_ids:
        wanted = set(account_ids)
        accounts = [a for a in accounts if a.id in wanted]
    return [a for a in accounts if a.mode == "jwt" and a.jwt_token]


def _schedule_install(account) -> None:
    """入池后调度后台按账号安装序（configs + 激活事件，幂等）。

    不阻塞入池响应；任何账号模式都跑（apiKey 账号 user_id 退空串，同官方
    未登录安装形态）。失败不影响入池；重复调用安全（installed_at 幂等跳过）。
    实现收口在 install.schedule_install（风控禁用自动换设备等场景共用）。
    """
    from ..install import schedule_install

    schedule_install(account)


@router.get("/claim/preview")
async def claim_preview(account_id: str | None = None):
    """立即拉取可领取套餐（全部/单个 JWT 账号）。

    先上报激活事件（zcode-switch claim_refresh 同形，模拟官方客户端当日活跃；
    疑似活动投放资格信号），上报失败不阻断 preview。
    """
    ids = [account_id] if account_id else None
    out = []
    for acc in _jwt_accounts(ids):
        blocked = billing_block_reason(acc, action="上游查询", allow_disabled=True)
        if blocked:
            out.append({"account_id": acc.id, "account_name": acc.name,
                        "plans": [], "error": blocked,
                        "activated": False, "activation_error": None})
            continue
        try:
            activation_error = await report_activation_events(acc)
        except Exception as err:  # noqa: BLE001 - 上报失败不阻断 preview
            activation_error = str(err)
        try:
            plans = await preview_plans(acc)
            out.append({"account_id": acc.id, "account_name": acc.name,
                        "plans": plans, "error": None,
                        "activated": activation_error is None,
                        "activation_error": activation_error})
        except ClaimError as err:
            out.append({"account_id": acc.id, "account_name": acc.name,
                        "plans": [], "error": str(err),
                        "activated": activation_error is None,
                        "activation_error": activation_error})
    return {"preview": out}


@router.post("/claim")
async def claim(payload: dict = Body(default=None)):
    """领取套餐（body 可选 account_ids / plan_id）；缺省对全部 JWT 账号自动选最优套餐。

    返回 outcomes[]：{account_id, account_name, ok, plan_name?, grants?, message?}。
    """
    payload = payload or {}
    account_ids = payload.get("account_ids") or None
    plan_id = (payload.get("plan_id") or "").strip() or None
    candidates = _jwt_accounts(account_ids)
    if not candidates:
        return {"outcomes": [], "summary": {"ok": 0, "fail": 0}}

    # 冷却 / 失效 JWT 不领取（billing/claim 是上游写流量，废票只会 401）；
    # 手动停用的好票仍要领取（allows_claim 放行，停用不丢每日套餐）
    outcomes = [
        {"account_id": a.id, "account_name": a.name, "ok": False,
         "message": billing_block_reason(a, allow_disabled=True) or AUTH_EXPIRED_MESSAGE}
        for a in candidates if not a.allows_claim()
    ]
    for acc in candidates:
        if not acc.allows_claim():
            continue
        try:
            result = await do_claim(acc, plan_id, allow_disabled=True)
        except ClaimError as err:
            logs.warn("claim", f"账号 {acc.name} 领取失败: {err}")
            outcome = {"account_id": acc.id, "account_name": acc.name,
                       "ok": False, "message": str(err)}
            if err.code != -1:
                outcome["code"] = err.code
            if err.next_at:
                outcome["next_at"] = err.next_at
            outcomes.append(outcome)
            continue
        except CaptchaSolveError as err:
            # 验证码求解失败（get_verify_param）：明确业务回执而非裸 500
            logs.err("claim", f"账号 {acc.name} 领取失败: {err}")
            outcomes.append({"account_id": acc.id, "account_name": acc.name,
                             "ok": False, "message": str(err)})
            continue
        except RuntimeError as err:
            # 兜底：captcha 层历史语义的运行时故障，防回归裸 500
            logs.err("claim", f"账号 {acc.name} 领取失败: {err}")
            outcomes.append({"account_id": acc.id, "account_name": acc.name,
                             "ok": False, "message": str(err)})
            continue
        await refresh_accounts([acc])
        outcomes.append({"account_id": acc.id, "account_name": acc.name,
                         "ok": True, **result})
    ok = sum(1 for o in outcomes if o["ok"])
    return {"outcomes": outcomes, "summary": {"ok": ok, "fail": len(outcomes) - ok}}


@router.get("/claim/captcha-config")
async def claim_captcha_config():
    """手动领取用：阿里验证码 SDK 初始化参数（前端浏览器内完成人机验证）。"""
    from ..captcha import captcha_manager
    from ..egress import DIRECT

    # claim 全链路走直连（billing 请求 trust_env=False），验证码配置同源拉取
    config = await captcha_manager.fetch_config(DIRECT)
    return {
        "enabled": bool(config.get("enabled", True)),
        "scene_id": config.get("sceneId") or "",
        "region": config.get("region") or "",
        "prefix": config.get("prefix") or "",
    }


@router.post("/claim/manual")
async def claim_manual(payload: dict = Body(...)):
    """手动领取：body {account_id, captcha_verify_param, captcha_region?, plan_id?}。

    verify_param 必须来自用户浏览器内阿里 SDK 滑块成功回调（无头环境无法求解）。
    """
    account_id = (payload.get("account_id") or "").strip()
    verify_param = (payload.get("captcha_verify_param") or "").strip()
    region = (payload.get("captcha_region") or "").strip() or None
    plan_id = (payload.get("plan_id") or "").strip() or None
    if not account_id:
        raise HTTPException(400, "缺少 account_id")

    acc = store.find("zai", account_id)
    if not acc or acc.mode != "jwt" or not acc.jwt_token:
        raise HTTPException(404, "JWT 账号不存在")
    blocked = billing_block_reason(acc, allow_disabled=True)
    if blocked:
        return {"outcomes": [{"account_id": acc.id, "account_name": acc.name,
                              "ok": False, "message": blocked}],
                "summary": {"ok": 0, "fail": 1}}

    try:
        result = await claim_with_captcha(acc, verify_param, region, plan_id,
                                          allow_disabled=True)
    except ClaimError as err:
        return {"outcomes": [{"account_id": acc.id, "account_name": acc.name,
                              "ok": False, "message": str(err)}],
                "summary": {"ok": 0, "fail": 1}}
    await refresh_accounts([acc])
    return {"outcomes": [{"account_id": acc.id, "account_name": acc.name,
                          "ok": True, **result}],
            "summary": {"ok": 1, "fail": 0}}


# ── 设置 ─────────────────────────────────────────────────────────────────────
def _mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "••••"
    return f"{value[:4]}…{value[-4:]}"


@router.get("/settings")
async def get_settings():
    from .. import constants
    from .. import settings as app_settings
    from ..version_tracker import is_newer_version, version_tracker

    admin_key = store.admin_key()
    gateway_key = store.gateway_key()
    auto_sync = (
        store.get_setting(
            "auto_sync_client_version", str(app_settings.AUTO_SYNC_CLIENT_VERSION).lower()
        )
        == "true"
    )
    latest = version_tracker.latest_info
    latest_ver = latest.version if latest else constants.CLIENT_APP_VERSION
    has_update = bool(latest and is_newer_version(latest.version, constants.CLIENT_APP_VERSION))

    return {
        "admin_key_set": bool(admin_key),
        "admin_key_masked": _mask_secret(admin_key),
        "admin_key_is_default": bool(admin_key) and admin_key == app_settings.DEFAULT_ADMIN_KEY,
        "gateway_key_set": bool(gateway_key),
        "gateway_key_masked": _mask_secret(gateway_key),
        "bigmodel_channel_enabled": store.bigmodel_channel_enabled(),
        "quota_refresh_interval": store.quota_refresh_interval(),
        "account_concurrency": store.account_concurrency(),
        "claim_round_interval": store.claim_round_interval(),
        "claim_round_stagger": store.claim_round_stagger(),
        "claim_round_hours": ",".join(map(str, store.claim_round_hours())),
        "auto_sync_client_version": auto_sync,
        "zcode_client_version": constants.CLIENT_APP_VERSION,
        "zcode_latest_version": latest_ver,
        "zcode_version_has_update": has_update,
        "zcode_release_date": latest.release_date if latest else "",
        "zcode_release_name": latest.release_name if latest else "",
        "zcode_release_notes": latest.release_notes if latest else "",
        "zcode_version_checked_at": latest.checked_at if latest else 0.0,
    }


@router.put("/settings")
async def update_settings(payload: dict = Body(...)):
    if "admin_key" in payload:
        key = (payload["admin_key"] or "").strip()
        if not key:
            raise HTTPException(400, "后台密钥不能为空")
        if "…" in key or key == "••••":
            pass  # 前端回填的掩码，不改密
        else:
            store.set_setting("admin_key", key)
    if "gateway_key" in payload:
        key = (payload["gateway_key"] or "").strip()
        if "…" in key or key == "••••":
            pass
        else:
            store.set_setting("gateway_key", key)
    if "bigmodel_channel_enabled" in payload:
        store.set_setting("bigmodel_channel_enabled",
                          "1" if payload["bigmodel_channel_enabled"] else "0")
    if "quota_refresh_interval" in payload:
        try:
            interval = max(0, int(payload["quota_refresh_interval"]))
        except (TypeError, ValueError):
            raise HTTPException(400, "刷新间隔必须是非负整数") from None
        store.set_setting("quota_refresh_interval", str(interval))
    # account_concurrency 已写死为 2（2026-11-04 线上实测），不再接受后台修改；
    # PUT 中携带该键直接忽略（不报错，兼容旧前端 payload）
    if "claim_round_interval" in payload:
        try:
            interval = max(0, int(payload["claim_round_interval"]))
        except (TypeError, ValueError):
            raise HTTPException(400, "自动领取轮间隔必须是非负整数（0 = 关闭）") from None
        store.set_setting("claim_round_interval", str(interval))
    if "claim_round_stagger" in payload:
        try:
            stagger = max(0, int(payload["claim_round_stagger"]))
        except (TypeError, ValueError):
            raise HTTPException(400, "领取错峰间隔必须是非负整数（0 = 关闭）") from None
        store.set_setting("claim_round_stagger", str(stagger))
    if "claim_round_hours" in payload:
        # 领取窗口（北京时间小时，逗号分隔；空 / * = 全天）。归一成升序去重后落库。
        raw = str(payload["claim_round_hours"] or "").strip()
        hours: list[int] = []
        if raw not in ("", "*"):
            for part in raw.replace("，", ",").split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    hour = int(part)
                except ValueError:
                    raise HTTPException(400, "领取窗口小时必须是 0-23 的整数，逗号分隔（空 = 全天）") from None
                if not 0 <= hour <= 23:
                    raise HTTPException(400, "领取窗口小时必须在 0-23 之间")
                if hour not in hours:
                    hours.append(hour)
        store.set_setting("claim_round_hours", ",".join(map(str, sorted(hours))))
    if "auto_sync_client_version" in payload:
        enabled = bool(payload["auto_sync_client_version"])
        store.set_setting("auto_sync_client_version", "true" if enabled else "false")
    return {"ok": True}


# ── ZCode 官方版本与更新日志 ────────────────────────────────────────────────
# 版本状态（当前/最新/更新日志）由 GET /settings 统一返回（前端 settings.html
# 直接消费）；此处只保留手动核验动作接口。版本伪装值完全由 version_tracker
# 自动对齐官方 manifest，不提供手工改写入口（避免脱离单一真相源）。
@router.post("/version/zcode/check")
async def check_zcode_version_now():
    from ..version_tracker import version_tracker

    # force=True：管理后台「立即核验」语义，跳过新鲜度缓存强制拉取
    res = await version_tracker.check_and_sync(force=True)
    return res


# ── 导入 / 导出 ─────────────────────────────────────────────────────────────
@router.get("/export")
async def export_accounts():
    return store.export()


@router.post("/import")
async def import_accounts(payload: dict = Body(...)):
    existing = {a.id for a in store.list_accounts()}
    count = store.import_accounts(payload)
    # 导入的新账号：安装序（幂等）+ JWT 账号激活+自动领取（幂等：重复 token 不新增）
    imported = [a for a in store.list_accounts("zai") if a.id not in existing]
    for acc in imported:
        _schedule_install(acc)
        if acc.mode == "jwt":
            _schedule_auto_claim(acc)
    return {"count": count}


# ── 请求监控 ─────────────────────────────────────────────────────────────────
@router.get("/usage/summary")
async def usage_summary():
    """今日/本周/本月/历史 token 用量汇总（不含探测请求）。"""
    return usage_stats.summary()


@router.get("/monitoring")
async def monitoring():
    """网关请求环形日志（内存态，重启清零）+ 并发快照 + 近端健康窗口。

    health 放在这里而非单独轮：它与 entries 同属「实时层」，5s 一跳的新鲜度
    已够（P95 要的是分钟级，不是秒级），省前端一个定时器与一次往返。
    """
    return {"entries": reqlog.snapshot(), "keep": reqlog.KEEP,
            "conc": reqlog.conc_snapshot(), "health": reqlog.health(1.0)}


@router.get("/monitoring/concurrency")
async def monitoring_concurrency():
    """1s 心跳的数据包：并发四口径 + 峰值 + 分钟曲线 + 进程体征 + 账号徽标。

    合成一个端点而不是拆三个：面板要的是「同拍快照」（在途数与 RSS 必须属于
    同一时刻，否则「占槽高但 solver 为 0」这类交叉判读会错位），也少三次往返。
    accounts 只读 store 内存表（无磁盘 IO），供「号池健康」页签拼 strike 徒标。
    """
    return {**reqlog.conc_snapshot(), "vitals": vitals.snapshot(),
            "accounts": _account_watch_rows()}


@router.get("/monitoring/concurrency/history")
async def monitoring_concurrency_history(days: int = 7):
    """磁盘并发曲线（逐日峰值 + 最近一天分钟级），峰值不因重启清零而丢。"""
    return reqlog.conc_history(days)


@router.get("/monitoring/history")
async def monitoring_history(days: int = 7):
    """磁盘请求历史聚合（近 days 天，默认 7）。

    实时页的内存日志只有 500 条且重启清零，看不了趋势；这里按天读
    data/reqlog-<日期>.jsonl 做聚合，仅回传统计而非明细（30 天可达数万条）。
    """
    return reqlog.history(days)


def _account_watch_rows() -> list[dict]:
    """账号运行侧徽标（健康页签与值守脚本共用）。

    Status 是字符串常量类（非 Enum）：直接 str(a.status)，不得 .value
    （写成 .value 会让端点整体 500，集成测试钉住）。
    """
    return [{"name": a.name, "provider": a.provider, "status": str(a.status),
             "enabled": bool(a.enabled),
             "risk_strikes": round(float(a.risk_strikes or 0), 2),
             "rate_strikes": round(float(a.rate_strikes or 0), 2),
             "net_strikes": round(float(a.net_strikes or 0), 2),
             "last_error": (a.last_error or "")[:200]}
            for a in store.list_accounts()]


@router.get("/monitoring/health")
async def monitoring_health(hours: float = 1.0):
    """近 hours 小时的逐账号健康窗口（P50/P95 首字、429/5xx/风控/额度计数）。

    与 /monitoring/history 的分工：那边按天读磁盘看趋势，这边读内存近端事件
    答「这个号现在还能不能用」——P95 需要分钟级新鲜度，按天粒度不够。
    """
    return reqlog.health(max(0.1, min(hours, 24.0)))


@router.get("/monitoring/vitals")
async def monitoring_vitals():
    """进程体征：RSS / solver 与 Chromium 进程数 / 令牌池水位 / 事件循环延迟。"""
    return vitals.snapshot()


@router.get("/monitoring/watch")
async def monitoring_watch():
    """值守脚本（主机侧 cron）的单一数据源：一次拿齐阈值判定所需。

    阈值判定与告警发送刻意放在仓库外（token 不入公开仓；告警降噪逻辑与
    gateway-admission-sentinel 同范式）。这里只出数，不出判断。
    """
    accounts = store.list_accounts()
    return {"conc": reqlog.conc_snapshot(),
            "recent": reqlog.recent_counts(10.0),
            "vitals": vitals.snapshot(minutes=5),
            "health": reqlog.health(1.0),
            "forecast": forecast.forecast(accounts),
            "alerts": alertlog.history(7),
            "accounts": _account_watch_rows()}


@router.get("/monitoring/forecast")
async def monitoring_forecast(days: int = 3):
    """额度节奏：几点回血（可信）+ 消耗速率 + 有条件的「几点打光」外推。

    口径诚实：balance 的 remaining 与 grant 腿是两套账（2026-10-31 实证），
    存在「exhausted 却 remaining>0」时明确拒给 ETA，不造假精度。
    每次查询顺带登记当前 quota 快照（纯观测，不触发 billing 请求）。
    """
    return forecast.forecast(store.list_accounts(), days=days)


@router.post("/monitoring/alerts")
async def monitoring_alert_record(payload: dict = Body(...)):
    """值守脚本发送成功后登记一条告警（只记规则集合，不记正文）。

    dry_run=true 时不落盘，避免本地自检污染真实告警曲线。
    """
    rules = payload.get("rules") or []
    if not isinstance(rules, list):
        rules = []
    return alertlog.record([str(r) for r in rules],
                           dry_run=bool(payload.get("dry_run")),
                           delivered=bool(payload.get("delivered", True)))


@router.get("/monitoring/alerts")
async def monitoring_alert_history(days: int = 7):
    """告警历史曲线：逐日条数、规则分布 TOP、最近告警与静默时长。"""
    return alertlog.history(days)


@router.post("/monitoring/clear")
async def monitoring_clear():
    reqlog.clear()
    return {"ok": True}


# ── 验证码台账 ─────────────────────────────────────────────────────────────────
@router.get("/captcha/ledger")
async def captcha_ledger_history(days: int = 7):
    """按 solver 路线聚合铸码成功率（近 days 天，默认 7）。

    多路线并存（happy-dom / 真浏览器）时用数据回答「该用哪条」，
    而不是靠注释里的风控回忆。
    """
    return captcha_ledger.history(days)


@router.get("/captcha/ledger/recent")
async def captcha_ledger_recent():
    """内存近期铸码明细（重启清零，按时间序）。"""
    return {"entries": captcha_ledger.snapshot(), "keep": captcha_ledger.KEEP}
