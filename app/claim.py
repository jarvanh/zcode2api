"""套餐领取（Z.AI billing/preview + billing/claim）。

链路与 zcode-switch claim.rs 同形：
  1. GET  {BILLING_BASE}/billing/preview?app_version=&platform= → data.plans[]
  2. 领取需阿里云无痕验证码：CaptchaManager 服务端求解 → X-Aliyun-Captcha-Verify-Param
  3. POST {BILLING_BASE}/billing/claim  body {"plan_id":...}（+ 可选 Verify-Region 头）

上游业务码语义（沿用 zcode-switch 映射）：1001 套餐不存在 / 1002 活动结束 /
1003 已领取过 / 1004 不符合条件 / 1005 今日名额用完 / 3001 参数错误 /
3007 验证码失败（换验证码重试一次）/ 401 未登录。
"""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
import time

import httpx

from . import constants, egress, freshness, logs, settings
from .captcha import CaptchaSolveError, captcha_manager
from .models import Account, Status


def _stale_suffix() -> str:
    """版本新鲜度告警后缀（仅 stale 时非空）。

    领取失败与版本过时在日志里长期不可区分（2026-10-30/31 两轮实证：
    BILLING_APP_VERSION 停旧版 → preview 空窗/领取拒绝，日志只见「上游无
    投放套餐」）——在失败原语上把线索直接递到位。
    """
    hint = freshness.claim_hint()
    return f"（{hint}）" if hint else ""


class ClaimError(Exception):
    """业务失败（含上游 code 语义），message 面向用户。

    next_at 仅 1005（名额用完）携带：上游 data.plan.ends_at（秒 → 毫秒），
    即名额恢复时间（zcode-switch claim.rs claim_error 同形）。
    """

    def __init__(self, message: str, *, code: int = -1, next_at: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.next_at = next_at


AUTH_EXPIRED_MESSAGE = "凭证失效，请重新授权"

_CLAIM_FAIL = {
    1001: "套餐不存在",
    1002: "活动已结束或套餐暂不可领取",
    1003: "该套餐已经领取过",
    1004: "不符合领取条件",
    1005: "今日领取名额已用完",
    3001: "领取参数错误，请刷新后重试",
    3002: "请求过于频繁，等待下轮领取窗口",
    3007: "验证码校验失败，请重试",
    401: "请先登录后再领取",
}


def billing_block_reason(account: Account, *, action: str = "领取",
                         allow_disabled: bool = False) -> str | None:
    """JWT 不可打 billing 时的用户文案；可打则返回 None。

    allow_disabled=True 放行「手动停用的好票」（enabled=False，JWT 仍有效）：
    停用只表示不参与对话轮询，billing 写入凭证照常可用。INVALID / 冷却 /
    风控 strikes 封禁仍一律拦住（封禁与手动停用的区别见 allows_claim）。
    """
    if account.is_cooling():
        return f"账号冷却中（风控/限流），已跳过{action}"
    if account.mode != "jwt" or not (account.jwt_token or "").strip():
        return f"非 Coding Plan 账号，已跳过{action}"
    if account.status == Status.INVALID:
        return AUTH_EXPIRED_MESSAGE
    if not account.enabled:
        return None if allow_disabled else f"账号已停用，已跳过{action}"
    if account.status == Status.DISABLED:
        return f"账号风控封禁，已跳过{action}"
    if not account.uses_plan_channel():
        return AUTH_EXPIRED_MESSAGE
    return None


def _mark_auth_failure(account: Account) -> None:
    from .store import store

    live = store.find(account.provider, account.id)
    if live is None:
        return
    live.status = Status.INVALID
    live.last_error = AUTH_EXPIRED_MESSAGE
    store.update_account(live)
    account.status = live.status
    account.last_error = live.last_error


def _fail_error(code: int, body: dict) -> ClaimError:
    """业务码 → ClaimError；1005 附带名额恢复时间（data.plan.ends_at 秒 → 毫秒）。"""
    base = _CLAIM_FAIL.get(code, "领取失败")
    server = body.get("msg") or body.get("message") or ""
    message = f"{base}（{server}）" if server else base
    message += _stale_suffix()
    next_at = None
    if code == 1005:
        ends = ((body.get("data") or {}).get("plan") or {}).get("ends_at")
        if isinstance(ends, (int, float)) and ends > 0:
            next_at = int(ends * 1000)
    return ClaimError(message, code=code, next_at=next_at)


def _business_code(body: dict) -> int:
    code = body.get("code")
    try:
        return int(code) if code is not None else -1
    except (TypeError, ValueError):
        return -1


def parse_plan(raw: dict) -> dict | None:
    """提取可领取套餐（plan_id/name/描述/优先级 + model_usage token 授权项）。"""
    plan_id = str(raw.get("plan_id") or raw.get("planId") or "").strip()
    if not plan_id:
        return None
    grants = []
    for ent in raw.get("entitlements") or []:
        if not isinstance(ent, dict):
            continue
        if ent.get("meter") != "model_usage" or ent.get("unit_type") != "token":
            continue
        name = str(ent.get("show_name") or ent.get("showName") or "").strip()
        if not name:
            continue
        units = ent.get("grant_units", ent.get("grantUnits")) or 0
        try:
            units = float(units)
        except (TypeError, ValueError):
            units = 0.0  # 上游偶发字符串/None 形态：不让单个脏字段炸掉整个 preview
        grants.append({
            "name": name,
            "units": units,
            "period": ent.get("period") or "one_time",
        })
    try:
        priority = int(raw.get("priority") or 0)
    except (TypeError, ValueError):
        priority = 0  # preview 排序要求可比较，脏值归零而不是抛 TypeError
    return {
        "plan_id": plan_id,
        "name": str(raw.get("name") or "").strip(),
        "description": str(raw.get("description") or "").strip(),
        "priority": priority,
        "grants": grants,
    }


def extract_plans_from_marketing_deliveries(deliveries: list) -> list[dict]:
    """从 marketing/touch 的 deliveries 载荷中提取待领取套餐。

    官方客户端结构实证（2026-11-03 逆向 + 线上实测）：
    delivery.banner.buttons[].action: {"type": "claim_zcode_plan", "args": {"plan_id": "..."}}
    delivery.banner.args.zcode_plan: {"plan_id": "...", "name": "...", "priority": ..., "entitlements": [...]}
    """
    plans: list[dict] = []
    seen_ids: set[str] = set()

    def _scan(obj: object) -> None:
        if isinstance(obj, dict):
            # 1. 优先提取挂在 args 里的完整 zcode_plan
            zp = obj.get("zcode_plan") or obj.get("zcodePlan")
            if isinstance(zp, dict):
                parsed = parse_plan(zp)
                if parsed:
                    pid = parsed["plan_id"]
                    existing = next((p for p in plans if p["plan_id"] == pid), None)
                    if existing is not None:
                        existing.update(parsed)
                    else:
                        plans.append(parsed)
                        seen_ids.add(pid)
            # 2. 检查 claim_zcode_plan 按钮动作
            action = obj.get("action")
            if isinstance(action, dict) and action.get("type") == "claim_zcode_plan":
                args = action.get("args") or {}
                pid = str(args.get("plan_id") or args.get("planId") or "").strip()
                if pid and pid not in seen_ids:
                    plans.append({
                        "plan_id": pid,
                        "name": pid,
                        "description": "活动投放套餐",
                        "priority": 100,
                        "grants": [],
                    })
                    seen_ids.add(pid)
            for v in obj.values():
                _scan(v)
        elif isinstance(obj, list):
            for item in obj:
                _scan(item)

    for delivery in deliveries or []:
        _scan(delivery)

    return plans


async def fetch_marketing_touch_plans(account: Account) -> list[dict]:
    """拉取 marketing/touch 接口下发的活动套餐（2026-11 官方实证）。

    失败时仅记录并返回空列表，不阻断主流程。
    """
    from .quota import _auth_headers

    try:
        body = await _billing_request(
            account, "GET", settings.ZCODE_MARKETING_TOUCH_URL,
            headers=_auth_headers(account),
            params={"seq": "0"},
        )
    except Exception as err:
        logs.info("claim", f"账号 {account.name} marketing/touch 查询跳过（{err}）")
        return []

    code = _business_code(body)
    if code != 0:
        return []
    deliveries = (body.get("data") or {}).get("deliveries") or []
    return extract_plans_from_marketing_deliveries(deliveries)


async def _billing_request(account: Account, method: str, path: str, **kwargs) -> dict:
    headers = dict(kwargs.pop("headers"))
    url = path if path.startswith("http") else f"{settings.ZCODE_BILLING_BASE}{path}"
    # 瞬态网络重试（2.7.68/2.7.71/2026-11-21 高可用迭代）：连接/读超时是出口
    # 链路瞬态（EasyTier 重启窗口、家宽线抖动，tebi 实锤多次），单发即败会让
    # 本轮整号报废（-1 非信誉型，wrapper 不重试）。GET/HEAD 幂等可安全重试；
    # POST claim 是写操作，超时后上游可能已入账，重发会撞 1003——保持单发。
    # 2.7.71 提到 3 次（2s/4s 阶梯）仍不够：08:10-08:13 出口抖动是**分钟级**
    # （49 条探针/连接失败实锤），6s 总窗的重试全落抖动窗内白烧，号报废要等
    # 下一窗口 +3600s（白天窗口 0,7,8 内甚至没有下一轮）。提到 5 次阶梯
    # 2/4/8/15s（总窗 ~29s + 请求本身）吃住分钟级抖动的尾部。
    attempts = 5 if method.upper() in ("GET", "HEAD") else 1
    try:
        # 出口由 settings.BILLING_EGRESS 决定（trust_env 按 direct/proxy 切）。
        # 2026-11-13 tebi 实证：GCP 大厂 ASN 直连可用（历史缺省），机房小 ASN
        # 直连 IP 会被 billing 风控拒（preview 空投放/429、验证码 3007）——
        # 走家宽代理 3/3 号立即可领。messages 端点与此独立（egress.py 判定）。
        # per-account 出口隔离（2.7.36）：账号绑定专属代理时优先——billing 与
        # messages 必须同 IP（token/风控同源）。
        account_proxy = egress.account_proxy(account)
        trust_env = settings.BILLING_EGRESS == egress.PROXY
        client_kw = ({"proxy": account_proxy, "trust_env": False} if account_proxy
                     else {"trust_env": trust_env})
        async with httpx.AsyncClient(timeout=25, **client_kw) as client:
            res: httpx.Response | None = None
            for attempt in range(1, attempts + 1):
                try:
                    res = await client.request(
                        method, url,
                        headers=headers, **kwargs,
                    )
                    break
                except httpx.TransportError as err:
                    if attempt >= attempts:
                        raise
                    wait = (2, 4, 8, 15)[attempt - 1]
                    logs.info("claim",
                              f"账号 {account.name} billing {method} 瞬态网络错误"
                              f"（{type(err).__name__}），{wait}s 后重试"
                              f"（{attempt}/{attempts - 1}）")
                    await asyncio.sleep(wait)
        assert res is not None  # 循环必经 break/raise
    except httpx.HTTPError as err:
        # 连接/超时等网络故障统一转业务错误：路由层只需面对 ClaimError 一种失败
        raise ClaimError(f"上游网络错误: {err}") from err
    if res.status_code in (401, 403):
        text = (res.text or "").lower()
        is_challenge = "captcha" in text or "verify" in text
        if not is_challenge:
            # 403 + 风控文案 ≠ 凭证失效：标 INVALID 会把健康号永久踢出领取链
            #（与 messages 链 3012 判定同一口径，2.7.33）。
            is_risk = res.status_code == 403 and any(
                m.lower() in text for m in constants.RISK_CONTROL_MARKERS
            )
            if is_risk:
                raise ClaimError(
                    f"上游风控拒绝 HTTP 403（非凭证失效）: {' '.join((res.text or '').split())[:160]}",
                    code=403,
                )
            _mark_auth_failure(account)
            raise ClaimError(AUTH_EXPIRED_MESSAGE)
    try:
        body = res.json()
    except ValueError:
        raise ClaimError(f"上游响应非 JSON HTTP {res.status_code}") from None
    return body


def jwt_user_id(account: Account) -> str | None:
    """JWT payload 的 user_id（zcode-switch telemetry_user_id 同源语义）。

    官方客户端事件上报以 user_id 标识用户；hub 不存 user_info，直接从 JWT
    解出（user_id 优先，sub 兜底，两者同为 36 位 uuid）。
    """
    token = (account.jwt_token or "").strip()
    if not token:
        return None
    try:
        seg = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    except (IndexError, ValueError):
        return None
    uid = payload.get("user_id") or payload.get("sub")
    if not isinstance(uid, str) or not uid.strip():
        return None
    return uid.strip()


async def report_activation_events(account: Account) -> str | None:
    """上报官方客户端激活事件（app_launch + app_daily_active），返回错误或 None。

    zcode-switch claim_refresh 同形：preview 前模拟桌面端当日活跃（疑似活动
    套餐投放资格信号）。事件体/端点/业务码判定收敛在 telemetry 单一事实源；
    请求无 Authorization（上游事件端点不校验）；任何失败仅返回文案，不阻断
    preview，首个失败即中止（日活键在上游按 device_mid+日期去重，重试无意义）。
    """
    from .fingerprint import profile_for
    from .telemetry import post_activation_event

    profile = profile_for(account)
    user_id = jwt_user_id(account)
    if not user_id:
        return "JWT 无 user_id，跳过激活上报"
    for element in constants.ACTIVATION_ELEMENTS:
        try:
            await post_activation_event(profile, user_id, element,
                                        proxy=egress.account_proxy(account))
        except (httpx.HTTPError, RuntimeError) as err:
            return _activation_err_text(element, err)
    return None


def _activation_err_text(element: str, err: Exception) -> str:
    """激活失败文案。httpx 的部分异常（ReadError 等）str 为空——空串时回退
    异常类名，否则日志只剩「上报失败: 」查无可查（2026-11-03 线上实锤）。"""
    detail = str(err).strip() or type(err).__name__
    return f"激活事件 {element} 上报失败: {detail}"


# 激活上报每日去重门：账号 id → 最近一次**成功**上报的本地日期。
# 自动路径（领取轮/入池）每号每天最多成功上报一次（官方客户端 app_launch
# 语义 = 每次启动一次；日活键上游本就按 device_mid+日期去重）。上报失败不
# 记账，下一轮自然重试（轮间隔 3600s，故障期上限 24 次/天，自愈）。手动
# preview 端点不走此门（用户触发 = zcode-switch claim_refresh 语义）。
_ACTIVATION_REPORTED: dict[str, str] = {}

# 营销横幅 feed 的 seq 每进程递增（官方 bootRequestSeq 同形，3.17.2 bundle）
_TOUCH_SEQ = 0

# ── preview 空窗兜底（投放发现）────────────────────────────────────
# 2026-10-31 实证：三个号的 billing/preview 返回 {"code":0,"msg":"","logid":...}
# ——连 data 字段都没有——但带明确 plan_id 打 billing/claim 返回 code=0 真实发放。
# 即「preview 空」不等于「无可领套餐」（官方客户端也不当空：server-core.js
# getManualClaimPlanPreviews 对缺 data 直接抛 manual_claim_preview_missing_data）。
# 旧实现把空 preview 一律记「上游无投放套餐，跳过领取」，活动投放从此永远看不
# 到——这就是主人手动领到、网关领不到的根因。
# 兜底候选源（按可信度排序）：
#   1. 日期推导 `zcode-v3-start-plan-{MMDD}`（10-10/10-24/10-27/10-28/10-29/
#      11-02 均实证）+ 周末 `-wk`（10-30 周五启动）——不依赖任何账号已持有，
#      是「没人先手动领一个」时的主发现渠道；
#   2. preview 列出过的（remember_plan_ids）；
#   3. 任一池内账号已入账的 plans[]（quota.fetch_quota 收割）。
# 公开 client/configs 的 startPlanPreview 不作为候选源：实测它只列基座套餐
# （zcode-v3-start-plan），对活动 id 零信息。
# 候选先做无验证码存在性探测（1001=不存在 / 1003=已持有 / 其它=存在），只对
# 确认存在的 id 解验证码，避免把每日领取预算烧在错误猜测上。
# 候选跳过账号已持有的（1003 确定），每号每天限次（防候选长期不变打爆码池）。
_KNOWN_PLANS_META = "claim_known_plan_ids"
_PROBE_META = "claim_probe_tried"
# 每号每日「新候选」上限。记账含免码探测，故额度必须容得下一天的全部候选
# （日期推导 1~2 个 + 收割 id），否则日期候选会把收割 id 挤出当日预算；
# 真实验证码消耗只发生在探测确认存在的 id 上（通常 1 个）。
_PROBE_DAILY_BUDGET = 4


def known_plan_ids() -> list[str]:
    """历史发现过的可领套餐 id（meta 持久化，重启不丢）。"""
    from .store import store

    try:
        arr = json.loads(str(store.get_setting(_KNOWN_PLANS_META, "") or "[]"))
    except ValueError:
        return []
    return [x for x in arr if isinstance(x, str) and x.strip()] if isinstance(arr, list) else []


def remember_plan_ids(plan_ids: list[str]) -> None:
    """并集入库（只增不减；失效 id 由 _forget_plan_id 按上游 1001 剔除）。

    真新 id 入池即唤醒自动领取轮（request_round）：preview 列出 / balance
    收割发现新投放时，把「等下一个整点轮」的提前量从最长 1 小时压到秒级
    （2026-11-02 主人要求）。唤醒失败不影响发现链路，下轮自然兜底。
    """
    from .store import store

    clean = [p for p in plan_ids if p and p.strip()]
    if not clean:
        return
    known = known_plan_ids()
    merged = list(dict.fromkeys(known + clean))
    if merged != known:
        store.set_setting(_KNOWN_PLANS_META, json.dumps(merged))
        added = [p for p in merged if p not in known]
        try:
            claim_round.request_round(f"新候选套餐入池: {added}")
        except Exception as err:  # noqa: BLE001 - 唤醒失败不阻断发现链路
            logs.warn("claim", f"领取轮唤醒失败（下轮兜底）: {err}")


def _forget_plan_id(plan_id: str) -> None:
    from .store import store

    known = known_plan_ids()
    if plan_id in known:
        store.set_setting(_KNOWN_PLANS_META, json.dumps([p for p in known if p != plan_id]))


def _probe_state() -> tuple[str, dict]:
    """(当日 key, 账号 id → 当日已试过的候选 plan_id)。跨重启持久化，免重复烧码。"""
    from .store import store

    day = _today_key()
    try:
        raw = json.loads(str(store.get_setting(_PROBE_META, "") or "{}"))
    except ValueError:
        return day, {}
    if not isinstance(raw, dict) or raw.get("date") != day:
        return day, {}
    tried = raw.get("tried")
    return day, tried if isinstance(tried, dict) else {}


def _record_probe(account_id: str, plan_id: str) -> None:
    day, tried = _probe_state()
    ids = list(tried.get(account_id) or [])
    if plan_id not in ids:
        ids.append(plan_id)
    tried[account_id] = ids
    from .store import store

    store.set_setting(_PROBE_META, json.dumps({"date": day, "tried": tried}))


def _held_plan_ids(account: Account) -> set[str]:
    """账号已持有的套餐 id（balance/current 的 plans[]），候选必须排除。"""
    out: set[str] = set()
    for p in account.plans or []:
        pid = str((p or {}).get("plan_id") or "").strip()
        if pid:
            out.add(pid)
    return out


def _date_plan_candidates(now: float | None = None) -> list[str]:
    """按活动 id 命名规律生成当日候选，不依赖任何账号已持有。

    实证：10-10/10-24/10-27/10-28/10-29/11-02 的活动 id 均为
    `zcode-v3-start-plan-{MMDD}`；周末活动带 `-wk`（10-30 周五启动）。
    preview 静默空窗、且池内没有任何账号已入账该套餐时，这是网关唯一能自己
    发现投放的渠道——否则每次都要主人先在官方客户端手动领一个，id 才会被
    balance 收割进候选池（2026-11-02 实证）。

    2026-11-09 增补 `-global` 变体：当日主 id 上游响应 1001（不存在）、
    而 `…-1109-global` 出现 429（有响应/名额/限流）——命名规律出现了新后缀，
    猜测器必须覆盖，否则白天投放窗口全漏。探测有每日预算收口，多出的候选
    只在预算内多打几个 1001 探测（无验证码消耗），代价可控。
    """
    ts = time.time() if now is None else now
    local = time.localtime(ts)
    today = time.strftime("%m%d", local)
    base = f"zcode-v3-start-plan-{today}"
    out = [base]
    if local.tm_wday in (4, 5, 6):  # 周五~周日：周末活动
        # 后缀形态（…-1030-wk）为 10 月起现行；前缀（…-wk-0905）为 9 月实证
        # 旧形——两种都试，官方改回也不漏（feed 上线后这只是廉价保险）。
        out.append(f"{base}-wk")
        out.append(f"zcode-v3-start-plan-wk-{today}")
    # 2026-11-09 实测新后缀：主 id 1001 而 …-global 429 → 上游在用 global 位
    out.append(f"{base}-global")
    return list(dict.fromkeys(out))


async def _probe_plan_code(account: Account, plan_id: str) -> int:
    """不带验证码探测套餐状态（零码池成本）。

    上游 claim 的校验顺序是「套餐存在 → 是否已领 → 验证码」：2026-11-02
    实测不存在的 id 返回 1001、已持有返回 1003；存在且可领返回 3007
    （缺验证码）。只有 3007 才值得解一次码，其余码一律跳过（不烧验证码）。
    """
    from .quota import _auth_headers

    body = await _billing_request(
        account, "POST", "/billing/claim",
        headers=_auth_headers(account), json={"plan_id": plan_id},
    )
    return _business_code(body)


def _candidate_plan_ids(account: Account) -> list[str]:
    """兜底候选：日期推导 id（当日活动，无需任何账号已持有）+ 历史收割 id。

    日期推导排在最前：池内没有任何账号领到时，它是唯一的发现渠道；已持有的
    id 直接剔除（必定 1003，白打一次）。
    """
    held = _held_plan_ids(account)
    # 收割到的事实优先于命名规律猜测：预算只有 4 个，日期候选（今日 3~4 个）
    # 若排在前面会把真正的投放 id 挤出当日窗口（2026-11-12 实证）。
    out = [pid for pid in known_plan_ids() + _date_plan_candidates()
           if pid and pid not in held]
    return list(dict.fromkeys(out))


async def _probe_unlisted_plans(account: Account,
                               skip_plan_ids: set[str] | None = None,
                               priority_ids: set[str] | None = None,
                               *, allow_disabled: bool = False) -> list[dict]:
    """preview 空时按候选 plan_id 发现并领取（投放空窗兜底）。

    候选先做无验证码存在性探测：只有 3007（缺验证码=存在且未持有）才走真实
    领取，1001/1003/1002/1005 等直接跳过——不存在的猜测不消耗验证码预算。
    幂等安全：1001 说明 id 已下线，从记住的候选里剔除。
    skip_plan_ids：1005 名额等待期跳过的 id（与 preview 主路同语义），不消耗预算。
    priority_ids：官方渠道（横幅 feed）报过名的 id，探测/领取不占每日预算、
    排在猜测候选之前——官方答案不该被命名规律猜测挤出预算窗口。
    """
    tried = _probe_state()[1]
    tried_ids = set(tried.get(account.id) or [])

    skip = skip_plan_ids or set()
    held = _held_plan_ids(account)
    priority = {pid for pid in (priority_ids or set())
                if pid and pid not in held and pid not in skip}
    candidates = [pid for pid in _candidate_plan_ids(account)
                  if pid not in tried_ids and pid not in skip
                  and pid not in priority]
    # 预算按「真没试过的候选」数收口：held 命中不消耗每日额度（它们必定 1003，
    # 记账只会把后面的新候选顶掉，白等一天）
    candidates = candidates[:max(0, _PROBE_DAILY_BUDGET - len(tried_ids))]
    if not candidates and not priority:
        return []

    logs.info("claim", f"账号 {account.name} preview 空，探测候选套餐: "
                       f"{sorted(priority | set(candidates))}")
    verified: list[str] = []
    for pid in sorted(priority) + candidates:
        if pid not in priority:
            # 先记账后打：异常/超时同样消耗预算，避免重启后反复重打同一候选
            _record_probe(account.id, pid)
        try:
            code = await _probe_plan_code(account, pid)
        except ClaimError as err:
            logs.info("claim", f"账号 {account.name} 候选 {pid} 探测失败: {err}")
            continue
        except Exception as err:  # noqa: BLE001 - 兜底路径绝不外溢
            logs.warn("claim", f"账号 {account.name} 候选 {pid} 探测异常: {err}")
            continue
        if code == 3007:
            verified.append(pid)
            continue
        if code == 1001:
            _forget_plan_id(pid)
        logs.info("claim", f"账号 {account.name} 候选 {pid} 不可领"
                          f"（业务码 {code}），跳过")

    outcomes: list[dict] = []
    for pid in verified:
        try:
            result = await claim(account, pid, allow_disabled=allow_disabled)
        except ClaimError as err:
            if err.code == 1001:
                _forget_plan_id(pid)
            logs.info("claim", f"账号 {account.name} 兜底领取 {pid} 未成功: {err}")
            continue
        except Exception as err:  # noqa: BLE001 - 兜底路径绝不外溢
            logs.warn("claim", f"账号 {account.name} 兜底领取 {pid} 异常: {err}")
            continue
        outcomes.append({"account_id": account.id, "account_name": account.name,
                         "ok": True, **result, "probe": True})
        logs.ok("claim", f"账号 {account.name} 兜底领取成功: "
                         f"{result.get('plan_name') or pid}（preview 未列此套餐）")
    return outcomes


def _today_key() -> str:
    """北京时区当日键（2.7.68 口径统一）：激活日门此前用宿主本地时区，与领取
    窗口的 gmtime+8 口径分裂——host 换 UTC 后日门翻转点从北京 0 点漂成 08:00，
    「每天 19:00 放量」的资格信号（app_daily_active）会在窗口中途才重置。"""
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + 8 * 3600))


# 同账号领取互斥（2.7.68）：手动端点 / 领取轮 / 入池任务三条链可对同一账号
# 并发跑 auto_claim_all_plans / claim——双链并发会 captcha 池争抢 + billing
# 双写重复打上游，且另一条链的 rebalance 插入「求解完成 → claim POST」窗口
# 时，token 的签发 IP 与请求 IP 漂移（上游判 unusual activity）。三条链的
# 账号段全部经此锁串行（锁只包账号段，跨账号天然并行）。
_ACCOUNT_CLAIM_LOCKS: dict[str, asyncio.Lock] = {}


def account_claim_lock(account_id: str) -> asyncio.Lock:
    return _ACCOUNT_CLAIM_LOCKS.setdefault(account_id, asyncio.Lock())


# ── 营销横幅 feed（官方活动第二投放列表，magpie #503 / cpa touch.go 同构）────
_TOUCH_CLAIM_ACTION = "claim_zcode_plan"
_TOUCH_WALK_DEPTH = 12


def _walk_touch(node, depth: int, ids: list[str], names: list[str],
                metas: dict) -> None:
    """递归 walk 一个投放：找 claim_zcode_plan action 与 zcode_plan 元数据。

    刻意 walk 而非钉 schema：横幅按钮挪位置/换层级仍能找到（cpa 同容忍）。
    """
    if node is None or depth > _TOUCH_WALK_DEPTH:
        return
    if isinstance(node, list):
        for item in node:
            _walk_touch(item, depth + 1, ids, names, metas)
        return
    if not isinstance(node, dict):
        return
    if node.get("type") == _TOUCH_CLAIM_ACTION:
        args = node.get("args")
        if isinstance(args, dict):
            pid = str(args.get("plan_id") or "").strip()
            if pid and pid not in ids:
                ids.append(pid)
    zp = node.get("zcode_plan")
    if isinstance(zp, dict):
        name = str(zp.get("name") or "").strip()
        if name and not names:
            names.append(name)
        mpid = str(zp.get("plan_id") or "").strip()
        if mpid and mpid not in metas:
            metas[mpid] = zp
    for child in node.values():
        _walk_touch(child, depth + 1, ids, names, metas)


def parse_touch_plans(body) -> list[dict]:
    """从 touch feed 响应提取候选 [{"plan_id", "name"}]（读不出 → [] 由调用方定语义）。

    命名回退按投放走（cpa touchPlansFromDelivery 同形）：先匹配同 id 的
    zcode_plan 元数据，缺省用**本投放**首个横幅名，最后退回 id 本身——
    跨投放的首名不该串到别的横幅上。
    """
    if not isinstance(body, dict):
        return []
    data = body.get("data")
    deliveries = data.get("deliveries") if isinstance(data, dict) else None
    if not isinstance(deliveries, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for dv in deliveries:
        ids: list[str] = []
        names: list[str] = []
        metas: dict = {}
        _walk_touch(dv, 0, ids, names, metas)
        for pid in ids:
            if pid in seen:
                continue
            seen.add(pid)
            meta = metas.get(pid) or {}
            name = (str(meta.get("name") or "").strip()
                    or (names[0] if names else "") or pid)
            out.append({"plan_id": pid, "name": name})
    return out


async def fetch_touch_plans(account: Account) -> list[dict]:
    """拉营销横幅 feed，返回横幅活动候选。

    404 = feed 未部署（无横幅活动的空态，不是错）；body 读不出 = 错误
    （「未知」绝不冒充「无套餐」，cpa 同语义）；401/403 = 凭证失效（同
    billing 门）。
    """
    global _TOUCH_SEQ
    from .quota import _auth_headers

    headers = _auth_headers(account)
    try:
        async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
            res = await client.get(settings.ZCODE_MARKETING_TOUCH_URL,
                                   headers=headers, params={"seq": str(_TOUCH_SEQ)})
    except httpx.HTTPError as err:
        raise ClaimError(f"营销 feed 网络错误: {err}") from err
    _TOUCH_SEQ += 1
    if res.status_code == 404:
        return []
    if res.status_code in (401, 403):
        text = (res.text or "").lower()
        if "captcha" not in text and "verify" not in text:
            _mark_auth_failure(account)
            raise ClaimError(AUTH_EXPIRED_MESSAGE)
    try:
        body = res.json()
    except ValueError:
        raise ClaimError(f"营销 feed 响应非 JSON HTTP {res.status_code}") from None
    return parse_touch_plans(body)


async def _banner_candidate(account: Account) -> list[str]:
    """横幅 feed 候选 id；发现即入候选池（remember 内含领取轮唤醒）。

    任何失败只记日志返回 []——feed 是增益渠道，绝不阻断既有 preview/兜底链。
    """
    try:
        banner = await fetch_touch_plans(account)
    except ClaimError as err:
        logs.info("claim", f"账号 {account.name} 营销 feed 读取失败: {err}")
        return []
    except Exception as err:  # noqa: BLE001 - feed 异常不阻断领取链
        logs.warn("claim", f"账号 {account.name} 营销 feed 读取异常: {err}")
        return []
    if not banner:
        return []
    ids = [p["plan_id"] for p in banner]
    remember_plan_ids(ids)
    logs.info("claim", f"账号 {account.name} 横幅发现候选套餐: "
                       f"{'、'.join(p['name'] for p in banner)}")
    return ids


async def auto_claim_all_plans(account: Account, *, skip_plan_ids: set[str] | None = None) -> list[dict]:
    """新账号入池自动领取：每日一次激活上报（preview 之前）→ preview → 逐个领取。

    入池链路的 fire-and-forget 收尾：任何失败只记日志/返回 outcome，绝不抛出
    （入池流程不受影响）。重复执行安全（上游 1003 已领取过幂等）。
    skip_plan_ids：本轮跳过领取的套餐 id（1005 名额等待期由 ClaimRoundManager
    记忆传入）；preview 仍照常执行，新套餐发现不受影响。

    自动路径一律 allow_disabled=True：停用（enabled=False）但票据仍有效的账号
    也要领包（2026-11-03 实证：停用导致 1103 投放漏领）；废票/冷却/风控封禁
    仍由 billing_block_reason 拦住，不会对它们发 billing 写请求。

    激活上报时序（2.6.6 根因修复）：上报必须先于 preview——活动投放疑似以
    当日激活信号为资格（constants.ACTIVATION_ELEMENTS 注释、09-06 实证），
    「先 preview 后补上报」会在新的一天形成 preview 恒空 → 永不激活的死锁。
    频率由每日去重门控制（每号每天 1 次，官方节奏），不再每轮空发。
    """
    if not (account.mode == "jwt" and account.jwt_token):
        return []
    # 同账号三条领取链互斥（2.7.68）：锁只包账号段，跨账号天然并行
    async with account_claim_lock(account.id):
        return await _auto_claim_all_plans_locked(account, skip_plan_ids=skip_plan_ids)


async def _auto_claim_all_plans_locked(account: Account, *,
                                       skip_plan_ids: set[str] | None = None) -> list[dict]:
    """auto_claim_all_plans 的锁内实现（同号互斥语义见外层函数）。"""
    outcomes: list[dict] = []

    if _ACTIVATION_REPORTED.get(account.id) != _today_key():
        try:
            err = await report_activation_events(account)
            if err:
                logs.warn("claim", f"账号 {account.name} 激活上报失败: {err}")
            else:
                _ACTIVATION_REPORTED[account.id] = _today_key()
        except Exception as err:  # noqa: BLE001 - 激活失败不阻断领取
            logs.warn("claim", f"账号 {account.name} 激活上报异常: {err}")

    try:
        plans = await preview_plans(account, allow_disabled=True)
    except ClaimError as err:
        logs.info("claim", f"账号 {account.name} 无可领套餐（{err}），转兜底候选")
        outcomes.extend(await _probe_unlisted_plans(
            account, skip_plan_ids, priority_ids=set(await _banner_candidate(account)),
            allow_disabled=True))
        return outcomes
    except Exception as err:  # noqa: BLE001
        logs.warn("claim", f"账号 {account.name} preview 异常: {err}")
        return outcomes

    if not plans:
        banner_ids = await _banner_candidate(account)
        probed = await _probe_unlisted_plans(account, skip_plan_ids,
                                             priority_ids=set(banner_ids),
                                             allow_disabled=True)
        if probed:
            outcomes.extend(probed)
            return outcomes
        hint = _stale_suffix()
        logs.info("claim", f"账号 {account.name} 上游无投放套餐，跳过领取{hint}")
        return outcomes

    skip = skip_plan_ids or set()
    skipped = 0
    for plan in plans:
        if plan["plan_id"] in skip:
            skipped += 1
            continue
        try:
            result = await claim_with_egress_retry(account, plan["plan_id"])
            outcomes.append({"account_id": account.id, "account_name": account.name,
                             "ok": True, **result})
            logs.ok("claim", f"账号 {account.name} 自动领取成功: "
                             f"{result.get('plan_name') or plan['plan_id']}")
        except ClaimError as err:
            if err.code == 1003:
                # 「已经领取过」= 幂等成功：上游投放会重复列出同一 plan，
                # 把 1003 记成失败会让每轮都报错（2.7.33）
                outcomes.append({"account_id": account.id, "account_name": account.name,
                                 "ok": True, "already_claimed": True,
                                 "plan_id": plan["plan_id"], "message": str(err)})
                logs.info("claim", f"账号 {account.name} 套餐 {plan['plan_id']} 已领取过，跳过")
                continue
            outcome = {"account_id": account.id, "account_name": account.name,
                       "ok": False, "plan_id": plan["plan_id"], "message": str(err)}
            if err.code != -1:
                outcome["code"] = err.code
            if err.next_at:
                outcome["next_at"] = err.next_at
            outcomes.append(outcome)
            logs.warn("claim", f"账号 {account.name} 自动领取 {plan['plan_id']} 失败: {err}")
            if err.code in (3007, 403):
                # 验证码被拒（3007）/ 上游风控 403（unusual activity 文案实锤，
                # _billing_request 只在命中 RISK_CONTROL_MARKERS 时才发 code=403）
                # 都是出口 IP 风险信号，403 的证据强度不亚于 3007——本账号本轮
                # 不再打第二个套餐（2.7.68 把 403 补进止损集：此前多套餐投放期
                # 403 后循环继续，同坏出口每个套餐都白烧一枚码）。
                break
        except Exception as err:  # noqa: BLE001
            logs.warn("claim", f"账号 {account.name} 自动领取异常: {err}")
            outcomes.append({"account_id": account.id, "account_name": account.name,
                             "ok": False, "plan_id": plan["plan_id"], "message": str(err)})
            # 求解失败/换绑后仍信誉不过（含 wrapper 换绑止损路径）：本账号本轮
            # 不再打第二个套餐——同一坏出口上每个套餐都要再烧一枚码（2.7.50
            # review：与 3007 break 同口径）
            if isinstance(err, CaptchaSolveError):
                break
    if skipped:
        logs.info("claim", f"账号 {account.name} {skipped} 个套餐名额等待期，本轮跳过领取")
    return outcomes


async def preview_plans(account: Account, *, allow_disabled: bool = False) -> list[dict]:
    """拉取账号当前可领取套餐，按优先级降序。

    双通道发现：
    1. /billing/preview（常规预览端点）
    2. /api/v1/marketing/touch（活动投放端点，2026-11 官方实证）
    两者套餐按 plan_id 去重合并并按优先级降序排列。
    allow_disabled：手动停用但票据仍有效的账号（自动领取路径放行）。
    """
    blocked = billing_block_reason(account, action="上游查询", allow_disabled=allow_disabled)
    if blocked:
        raise ClaimError(blocked)
    from .fingerprint import profile_for
    from .quota import _auth_headers

    body = await _billing_request(
        account, "GET", "/billing/preview",
        headers=_auth_headers(account),
        # platform 跟账号档案走（官方 TH() = process.platform-arch）；
        # 实测 client/configs 才拒 platform 参数，preview 宽容。
        params={"app_version": constants.BILLING_APP_VERSION,
                "platform": profile_for(account).platform_full},
    )
    code = _business_code(body)
    if code != 0:
        raise _fail_error(code, body)
    data = body.get("data")
    if not isinstance(data, dict):
        # 上游投放空窗的真实形态（实测 code=0 且无 data）。旧实现静默成「无可领」
        # 且不留痕迹，导致「领不到」与「上游真没发」在日志里不可区分。
        logs.info("claim", f"账号 {account.name} preview 无 data 字段（上游投放空窗形态）")
        return []
    raw_plans = data.get("plans") or []
    plans = [parsed for parsed in (parse_plan(p) for p in raw_plans) if parsed]

    # 合并 marketing/touch 活动投放通道（官方客户端实证：活动套餐由此下发）
    try:
        marketing_plans = await fetch_marketing_touch_plans(account)
        seen_ids = {p["plan_id"] for p in plans}
        for mp in marketing_plans:
            if mp["plan_id"] not in seen_ids:
                plans.append(mp)
                seen_ids.add(mp["plan_id"])
    except Exception as err:
        logs.warn("claim", f"账号 {account.name} marketing/touch 套餐合并异常: {err}")

    plans.sort(key=lambda p: (-p["priority"], p["plan_id"]))
    # preview 列出的 id 是最可信的候选源，入库供空窗兜底复用
    remember_plan_ids([p["plan_id"] for p in plans])
    return plans


async def _auto_pick_plan(account: Account, plan_id: str | None,
                         *, allow_disabled: bool = False) -> tuple[str, str, list]:
    """plan_id 为空时 preview 自动选优先级最高套餐。返回 (plan_id, plan_name, grants)。

    preview 空时降级到收割候选（preview 历史列出过 / 池内任一号已入账的 id）——
    后台面板的「手动领取」走的正是这条路，不降级就只能报「没有待领取的套餐」，
    而同一时刻带明确 id 打 claim 是能成功的（2026-10-31 实证）。
    """
    if plan_id:
        return plan_id, "", []
    plans = await preview_plans(account, allow_disabled=allow_disabled)
    if not plans:
        # 横幅 feed 先行：官方活动第二投放列表（挂横幅期间 preview 看不到）。
        # 候选同样先做免码探测：直接返回第一个 id 会把「不存在」的猜测交给
        # 带码请求（面板手动领取一次 = 一次验证码 + 一次 1001 失败）。
        for pid in await _banner_candidate(account):
            if pid in _held_plan_ids(account):
                continue
            try:
                code = await _probe_plan_code(account, pid)
            except ClaimError as err:
                logs.info("claim", f"账号 {account.name} 候选 {pid} 探测失败: {err}")
                continue
            except Exception as err:  # noqa: BLE001 - 探测异常不阻断选择
                logs.warn("claim", f"账号 {account.name} 候选 {pid} 探测异常: {err}")
                continue
            if code == 3007:
                logs.info("claim", f"账号 {account.name} preview 空，改用横幅候选 {pid}")
                return pid, "", []
            if code == 1001:
                _forget_plan_id(pid)
        for pid in _candidate_plan_ids(account):
            try:
                code = await _probe_plan_code(account, pid)
            except ClaimError as err:
                logs.info("claim", f"账号 {account.name} 候选 {pid} 探测失败: {err}")
                continue
            except Exception as err:  # noqa: BLE001 - 探测异常不阻断选择
                logs.warn("claim", f"账号 {account.name} 候选 {pid} 探测异常: {err}")
                continue
            if code == 3007:
                logs.info("claim", f"账号 {account.name} preview 空，改用候选 {pid}")
                return pid, "", []
            if code == 1001:
                _forget_plan_id(pid)
        raise ClaimError("没有待领取的套餐" + _stale_suffix())
    best = plans[0]
    return best["plan_id"], str(best.get("name") or "") or best["plan_id"], best.get("grants") or []


def _claim_headers(account: Account, verify_param: str, region: str | None) -> dict:
    """billing/claim 客户端请求头形态（asar claimManualPlan）。

    实测缺版本/平台头时即使验证码有效也 3007；X-Device-Mid 由 _auth_headers 提供。
    """
    from .quota import _auth_headers

    headers = _auth_headers(account)
    headers[constants.CAPTCHA_HEADER] = verify_param
    if region and region.strip():
        headers["X-Aliyun-Captcha-Verify-Region"] = region.strip()
    # 实测缺版本/平台头时即使验证码有效也 3007（_auth_headers 已带，此处显式
    # 兜底防止基座头漂移）。平台必须跟账号档案走，禁止再盖成全局 darwin-arm64。
    headers["X-ZCode-App-Version"] = constants.BILLING_APP_VERSION
    return headers


async def _post_claim(account: Account, headers: dict, plan_id: str) -> dict:
    """提交 billing/claim 并翻译业务码。"""
    body = await _billing_request(
        account, "POST", "/billing/claim",
        headers=headers, json={"plan_id": plan_id},
    )
    code = _business_code(body)
    if code != 0:
        raise _fail_error(code, body)
    return body


def _claim_outcome(body: dict, plan_id: str, plan_name: str, grants: list) -> dict:
    """领取成功返回集；server_time/starts_at/ends_at 为上游秒值 → 毫秒
    （zcode-switch 3.11.2 领取语义：服务端时钟随成功载荷下发，供前端
    区分本机时钟漂移）。缺失字段保持 None，不造数。"""
    data = body.get("data") or {}
    plan = data.get("plan") or {}

    def _ms(key: str) -> int | None:
        val = plan.get(key)
        return int(val * 1000) if isinstance(val, (int, float)) and val > 0 else None

    server_time = data.get("server_time")
    resp_name = str(plan.get("name") or "").strip()
    parsed_grants = grants or (parse_plan(plan) or {}).get("grants", [])
    return {
        "plan_id": plan_id,
        "plan_name": resp_name or plan_name or plan_id,
        "grants": parsed_grants,
        "starts_at": _ms("starts_at"),
        "ends_at": _ms("ends_at"),
        "server_time": int(server_time * 1000) if isinstance(server_time, (int, float)) and server_time > 0 else None,
    }


def _clear_cooling_after_claim(account: Account) -> None:
    """领取成功后清空该账号全部模型级冷却并持久化（2026-10-28 整改）。

    模型级冷却的唯一语义是「该模型无资源包/套餐」（1113/1309/无余额）；领取
    成功即资源状态整体刷新。冷却若保留（指数退避上限 6h），新包到账后的产能
    会在 is_selectable(model) 被白挡——夜间领包场景损失小时级可用窗口。清全部
    而非猜测 plan→model 映射：误清代价只是下一次请求多一次真实探测，远小于
    误留。只在真实成功（code==0）路径调用；1003 已领取过走失败分支不清，
    避免「包已耗尽 + 旧冷却」被误洗后空打上游。收尾失败只记日志，不回滚领取。
    """
    from .store import store

    try:
        live = store.find(account.provider, account.id)
        target = live if live is not None else account
        if not (target.model_cooling or target.model_fail_count):
            return
        target.clear_all_model_cooling()
        if live is not None:
            store.update_account(live)
        logs.ok("claim", f"账号 {account.name} 领取成功，已清空模型级冷却并持久化")
    except Exception as err:  # noqa: BLE001 - 领取成功语义不受收尾失败影响
        logs.warn("claim", f"账号 {account.name} 清模型冷却失败（不影响领取结果）: {err}")


async def claim_with_captcha(
    account: Account,
    verify_param: str,
    region: str | None,
    plan_id: str | None = None,
    *,
    allow_disabled: bool = False,
) -> dict:
    """手动领取：verify_param 由用户浏览器内阿里 SDK 滑块产生，本端只做转发。

    plan_id 缺省时先 preview 自动选优先级最高套餐（无需验证码）。
    """
    if not (account.mode == "jwt" and account.jwt_token):
        raise ClaimError("仅 Coding Plan (JWT) 账号支持领取")
    blocked = billing_block_reason(account, allow_disabled=allow_disabled)
    if blocked:
        raise ClaimError(blocked)
    if not (verify_param or "").strip():
        raise ClaimError("缺少验证码参数，请先完成人机验证")

    plan_id, plan_name, grants = await _auto_pick_plan(account, plan_id or None,
                                                      allow_disabled=allow_disabled)
    headers = _claim_headers(account, verify_param.strip(), region)
    body = await _post_claim(account, headers, plan_id)
    outcome = _claim_outcome(body, plan_id, plan_name, grants)
    _clear_cooling_after_claim(account)
    return outcome


async def claim(account: Account, plan_id: str | None = None, *,
                allow_disabled: bool = False) -> dict:
    """领取套餐。plan_id 缺省时自动选优先级最高的可领套餐。

    返回 _claim_outcome 形态（含 server_time/starts_at/ends_at，可能为 None）；
    1005 携带 next_at（名额恢复时间）。验证码被拒（3007）单次即止（2.6.5 整改）：
    拒码本身就是上游风险信号，换码连打只会加剧——拒码出池，下一轮自然重试。
    """
    if not (account.mode == "jwt" and account.jwt_token):
        raise ClaimError("仅 Coding Plan (JWT) 账号支持领取")
    blocked = billing_block_reason(account, allow_disabled=allow_disabled)
    if blocked:
        raise ClaimError(blocked)

    plan_id, plan_name, grants = await _auto_pick_plan(account, plan_id,
                                                      allow_disabled=allow_disabled)
    # claim 的 billing 请求出口见 _billing_request（BILLING_EGRESS，2.7.32 起
    # 可配 direct/proxy）；验证码必须同出口解，否则 token 的出口与请求出口
    # 不一致，上游判 unusual activity。fork 语义：billing/claim 链路默认直连，
    # claim_proxy 仅在 PER_ACCOUNT_EGRESS 开启且账号绑定出口时非 None
    # （2.7.36 per-account 语义经 egress.account_proxy 接入）。
    claim_egress = egress.egress_tag_for(account, settings.BILLING_EGRESS)
    claim_proxy = egress.account_proxy(account)
    _captcha_kw = {"proxy_url": claim_proxy} if claim_proxy else {}
    verify_param, verify_region = await captcha_manager.get_verify_param(
        egress_tag=claim_egress, **_captcha_kw,
        # 领取链求解预算解耦（2.7.68）：单号最坏 4×240s 会把整晚窗口吃穿，
        # claim 路径缺省 2 次起（settings.CAPTCHA_CLAIM_RETRIES 可调）
        retries=settings.CAPTCHA_CLAIM_SOLVE_RETRIES,
    )
    config = await captcha_manager.fetch_config(claim_egress, **_captcha_kw)
    headers = _claim_headers(account, verify_param, verify_region or config.get("region"))

    body = await _billing_request(
        account, "POST", "/billing/claim",
        headers=headers, json={"plan_id": plan_id},
    )
    code = _business_code(body)
    if code == 0:
        outcome = _claim_outcome(body, plan_id, plan_name, grants)
        _clear_cooling_after_claim(account)
        return outcome
    if code == 3007:
        captcha_manager.invalidate(claim_egress)  # 被拒的码出池，避免下轮复用
    raise _fail_error(code, body)


# IP 信誉驱动重试（2026-11-16 活动套餐实弹定案）：claim 成败强跟随出口节点——
# 同一坏节点上重试 4 次验证码也是同一个 IP，白烧求解与 claim 写流量。三类
# 失败归因 IP 信誉：①HTTP 403 风控（unusual activity 文案）；②验证码求解
# 失败（solver 是最灵敏的 IP 探针——F001 全拒=节点在阿里云侧已烂）；③3007
# 换码仍拒。命中即 rebalance 换绑（ban 旧节点带 billing 证据），换绑后用
# 「验证码现解成功」作真实验收（rebalance 自带的 Plan 探针只验 messages 通
# 道，不验 billing/阿里云维度），验收过才重打 claim。
# env 解析收编 settings（2026-11-17 深度迭代）：import 崩溃风险与全仓口径统一。
CLAIM_EGRESS_RETRIES = settings.CLAIM_EGRESS_RETRIES
_EGRESS_RETRY_DELAY_S = settings.CLAIM_EGRESS_RETRY_DELAY


def _is_egress_reputation_failure(err: Exception) -> bool:
    """判定是否 IP 信誉型失败（值得换绑重试，而非套餐/参数/凭证问题）。"""
    if isinstance(err, ClaimError):
        if err.code == 3007:
            return True
        # 403 必须「风控文案实锤」才换绑（2.7.50 review P1）：_billing_request
        # 只在命中 RISK_CONTROL_MARKERS 时才发 code=403，但防御纵深不可省——
        # 若上游文案漂移/新增 403 语义混进来，盲目换绑会白烧 2 轮 rebalance +
        # 4 次浏览器求解（每号每轮 10+ 分钟）而换 IP 根本无解。3001/3002 是
        # 频率/参数维度（3002 频率风控也非 IP 信誉，换 IP 无用）。
        if err.code == 403:
            text = str(err).lower()
            return any(m.lower() in text for m in constants.RISK_CONTROL_MARKERS)
        return False
    # captcha_manager.get_verify_param 抛的求解失败：出口不通/节点被阿里云拒
    return isinstance(err, CaptchaSolveError)


async def _rebalance_for_claim(account: Account, reason: str) -> bool:
    """为 claim 换绑出口节点；换绑后用验证码现解做真实验收。

    Returns True = 已换到新节点且验证码求解通过（IP 信誉已恢复）；
    False = 无 per-account 出口可换 / rebalance 失败 / 验收仍不过 /
    换绑环节自身异常（吞掉并记日志——不掩盖原始 claim 错误上抛，2.7.50
    review P2：本函数在 wrapper 的 except 块内被 await，穿出的异常会替换
    原 ClaimError，归因链断裂）。"""
    from .egress_pool import egress_pool

    claim_proxy = egress.account_proxy(account)
    if not claim_proxy:
        return False  # 直连/共享出口：无节点可换
    old_tag = egress.egress_tag_for(account, settings.BILLING_EGRESS)
    old_node = account.egress_node
    captcha_manager.invalidate(old_tag)  # 旧 IP 池内的码全部作废（换绑失败时
    #  池已污染；换绑成功后 egress_pool 还会对同一 tag 再 invalidate 一次，
    #  双清无害——两处清的都是「旧底层 IP 签发的码」）

    try:
        # TP ban reason 是闭合枚举（edge_405_html | unusual_activity_3012 |
        # rate_limited_429_burst，见 TetherPool API 需求 §ban），自由文本 422。
        # ⚠️ unusual_activity_3012 被 TP 服务端强制 quarantine_hours=0（只记录
        # 不隔离）——2026-11-17 实弹实证：当晚 8 条「ban 已隔离」日志实际全部
        # 未隔离，烂节点持续被 force_rebalance 分回，claim 换绑死循环的帮凶。
        # captcha 信誉失败（403 unusual activity / 求解 F001 / 3007）是 IP 级
        # 封禁信号，必须传唯一真隔离的 edge_405_html；细粒度归因放 evidence
        # （rebalance 自带）。
        ok = await egress_pool.rebalance(
            account, "edge_405_html", ban=True
        )
        if not ok or account.egress_node == old_node:
            logs.warn("claim", f"账号 {account.name} 换绑未生效（{old_node or '?'}），放弃本轮重试")
            return False

        # 验收：新节点上现解一枚验证码。这一步同时完成①IP 连通性②阿里云对该
        # 新 IP 的信誉评分。失败 = 新节点同样烂，不重打。预算走领取链口径
        # （2.7.68）：验收是决策探针不是可用性兜底，跑满 4 次只会拖长换绑链。
        new_tag = egress.egress_tag_for(account, settings.BILLING_EGRESS)
        await captcha_manager.get_verify_param(egress_tag=new_tag,
                                               proxy=egress.account_proxy(account),
                                               retries=settings.CAPTCHA_CLAIM_SOLVE_RETRIES)
        logs.ok("claim", f"账号 {account.name} 新出口 {account.egress_node} 验证码验收通过")
        return True
    except CaptchaSolveError as err:
        logs.warn("claim", f"账号 {account.name} 新出口 {account.egress_node} "
                           f"验证码验收失败: {err}——节点信誉不足，不再重打")
        return False
    except Exception as err:  # noqa: BLE001 - 换绑环节异常不掩盖原始 claim 失败
        logs.warn("claim", f"账号 {account.name} 换绑环节异常: "
                           f"{type(err).__name__}: {str(err)[:120]}——放弃本轮重试")
        return False


async def claim_with_egress_retry(account: Account, plan_id: str | None = None) -> dict:
    """claim 的 IP 信誉驱动包装：信誉型失败 → 换绑 → 验收 → 重打。

    非 IP 信誉型失败（1001-1005/3001/3002/凭证失效）原样上抛，行为与裸
    claim() 完全一致；换绑重试上限 ZCODE_CLAIM_EGRESS_RETRIES（缺省 2），
    每次重试间隔 ZCODE_CLAIM_EGRESS_RETRY_DELAY（缺省 20s，给 TP ban/新
    allocation 生效留缓冲）。"""
    attempts = CLAIM_EGRESS_RETRIES + 1
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await claim(account, plan_id)
        except Exception as err:  # noqa: BLE001 - 统一归因判定后分类处理
            last_err = err
            if attempt >= attempts or not _is_egress_reputation_failure(err):
                raise
            logs.warn("claim",
                      f"账号 {account.name} 第 {attempt}/{attempts} 次 claim 遇 IP 信誉型失败"
                      f"（{type(err).__name__}: {str(err)[:120]}）→ 换绑出口重试")
            try:
                rebalanced = await _rebalance_for_claim(account, str(err)[:80])
            except Exception as rb_err:  # noqa: BLE001 - 换绑异常不掩盖原始失败
                logs.warn("claim", f"账号 {account.name} 换绑调度异常: "
                                   f"{type(rb_err).__name__}: {str(rb_err)[:120]}"
                                   f"——上抛原始 claim 错误")
                raise err from err  # 显式上抛原始 ClaimError/CaptchaSolveError，非换绑异常
            if not rebalanced:
                raise
            await asyncio.sleep(_EGRESS_RETRY_DELAY_S)
    raise last_err  # pragma: no cover - 循环必经 return/raise


class ClaimRoundManager:
    """后台周期自动领取轮（默认 1 小时，2.6.5 整改；对齐 zcode-switch 用户触发语义）。

    每轮对全部可领取的 JWT 账号执行 auto_claim_all_plans（每日一次激活
    上报先于 preview + 逐个领取可领套餐）；冷却/失效/风控封禁账号由
    allows_claim() 先行过滤（停用的好票仍参与领取），保证「冷却期零上游
    billing 流量」不变量。
    1005（今日名额用完）按服务端 next_at 退避：等待期该套餐不打 claim
    （省 captcha 求解与上游写流量），preview 照常保留新套餐发现
    （docs/development/05「按服务端 next window 退避」语义落地）。
    轮间隔与**领取窗口**（claim_round_hours，北京时间小时）运行期读 meta 设置
    （interval 0 = 关闭，仍周期回看便于随时启用；窗口外只睡不跑、到点准时醒来；
    空窗口 = 全天）。轮内相邻账号间可加 0～stagger 秒随机延迟
    （claim_round_stagger，打散「同轮全池同拍打上游 billing」的行为聚类；
    0 = 关闭，首个账号不等）。新候选套餐入池（remember_plan_ids 发现真新 id）
    经 request_round 提前唤醒，不等满窗口/interval。
    单账号异常不中断整轮，后台任务异常全部自兜。
    """

    # 停服截止：轮内长尾（验证码求解重试可达分钟级）超时即 cancel，
    # uvicorn lifespan 关闭保持有界，不被半途的求解卡成 SIGKILL 脏停机
    STOP_GRACE_SECONDS = 10
    # 启动错峰（避开安装序 / 入池自动领取的首次流量高峰）；类常量便于测试提速
    START_DELAY_SECONDS = 30

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # 新候选套餐入池的提前唤醒（request_round 置位，_loop 消费后清除）
        self._wake = asyncio.Event()
        # 1005 退避记忆：落库持久化（上游 2026-11-22 review P2）
        self._claim_holds: dict[str, dict[str, int]] = self._load_holds()
        self._holds_persisted: dict[str, dict[str, int]] = {}
        # 账号 → 上次被预算跳过的 monotonic 时刻（轮转公平排序键）
        self._starved_at: dict[str, float] = {}
        # 无可服务账号的告警只在状态变化时发（持续故障期不逐轮刷屏）
        self._empty_warned = False
        # 错峰标记：本轮已处理过至少一个账号（首个账号不等，只错峰后续）
        self._round_visited = False

    # ── 1005 退避表持久化（2026-11-22 review P2）───────────────────────────
    _HOLDS_META_KEY = "claim_holds_1005"

    def _load_holds(self) -> dict[str, dict[str, int]]:
        """启动回读 1005 退避表（per-account meta 键）；损坏/缺失按空表处理
        （幂等重探一次可接受）。"""
        try:
            from .store import store
            out: dict[str, dict[str, int]] = {}
            for key, val in store.all_settings().items():
                if not key.startswith(f"{self._HOLDS_META_KEY}:"):
                    continue
                aid = key.split(":", 1)[1]
                if not val:
                    continue
                try:
                    pl = json.loads(str(val))
                except ValueError:
                    continue
                if isinstance(pl, dict):
                    out[aid] = {str(k): int(v) for k, v in pl.items()
                                if isinstance(v, (int, float))}
            return out
        except Exception:  # noqa: BLE001 - 持久层故障不阻断启动
            return {}

    def _persist_holds(self, holds: dict[str, dict[str, int]]) -> None:
        """增量落库：仅写与影子不同的账号条目（每轮通常 ≤N 条小 JSON）；
        已 pop 的账号同步清盘（2026-11-22 迭代二 F4：否则 meta 键只进不出，
        重启回读会复活陈旧 holds、键随历史账号单调累积）。"""
        try:
            from .store import store
            for aid in self._holds_persisted.keys() - holds.keys():
                store.set_setting(f"{self._HOLDS_META_KEY}:{aid}", "")
                self._holds_persisted.pop(aid, None)
            for aid, pl in holds.items():
                if self._holds_persisted.get(aid) != pl:
                    store.set_setting(f"{self._HOLDS_META_KEY}:{aid}",
                                      json.dumps(pl, separators=(",", ":")) if pl else "")
                    self._holds_persisted[aid] = dict(pl)
        except Exception as err:  # noqa: BLE001 - 落库失败退化为旧行为（内存态）
            logs.warn("claim", f"1005 退避表落库失败（本轮内存态继续）: {err}")

    async def _round(self) -> None:
        # 领到新套餐后让额度监控立即重算「最早生效时刻」，不必等下一次节拍；
        # 唤醒本身不打 billing（领包方下面已自行 refresh 一次）。
        from .quota import monitor, refresh_accounts
        from .store import store

        accounts = [
            a for a in store.list_accounts("zai")
            if a.mode == "jwt" and a.allows_claim()
        ]
        if not accounts:
            if not self._empty_warned:
                logs.warn("claim", "无可领取账号（全部冷却/失效/封禁），本轮跳过")
                self._empty_warned = True
            return
        self._empty_warned = False
        # 轮次执行前先轻量核验官方最新版本（若有新版本且开启 auto_sync，自动对齐指纹）
        try:
            from .version_tracker import version_tracker

            await version_tracker.check_and_sync()
        except Exception as err:  # noqa: BLE001
            logs.info("claim", f"轮次前官方版本核验跳过（{err}）")

        now_ms = time.time() * 1000
        # 错峰标记按轮重置：每轮首个账号不等，只错峰后续相邻账号
        self._round_visited = False
        # 退避表先清过期项（next_at 已过的恢复本轮重试）
        holds = {
            aid: {pid: at for pid, at in pl.items() if at > now_ms}
            for aid, pl in self._claim_holds.items()
        }
        self._claim_holds = holds
        # 轮级 wall-clock 预算（2.7.68）：单号最坏 ≈90 分钟（求解长尾 × 换绑链），
        # 11 号串行会把 19-23 点窗口吃穿（三晚 0 成功的结构性放大器）。预算到点
        # 后剩余账号本轮跳过（下拍续领），保证轮在窗口内收尾。
        budget = float(settings.CLAIM_ROUND_BUDGET or 0)
        deadline = time.monotonic() + budget if budget > 0 else None
        concurrency = max(1, int(settings.CLAIM_ROUND_CONCURRENCY))
        skipped_by_budget = 0
        # 轮转公平起点（2026-11-22 review P2 根修；迭代二勘误）：并发槽
        # Semaphore(concurrency) FIFO 下排前的账号先拿槽，预算耗尽时同一批尾部
        # 账号每轮都被跳过——缺省 19 点单小时窗口 + interval 3600s 下窗口内恰好
        # 只跑一轮 = 尾部号结构性饥饿 23h。分区排序把上轮预算跳过的账号排到
        # 队首（饿号组内按被跳过时刻先后，未饿号保持原序）；账号本轮实际被
        # 服务（含执行失败）即清除标记 = 「饿一次补一次」，饥饿不跨轮累积。
        # 迭代二 F1 实证：原先 sort(key=_starved_at.get(a.id, 0.0)) 升序把
        # 饿号（monotonic 恒正）排到未饿号（0.0）之后，方向恰反 = 修复无效。
        accounts.sort(key=lambda a: (a.id not in self._starved_at,))

        async def _claim_one(acc: Account, acc_holds: dict[str, int]) -> bool:
            """单账号领取段：预算内执行 + 退避表更新 + 入账刷新。返回是否入账。"""
            nonlocal skipped_by_budget
            if deadline is not None and time.monotonic() >= deadline:
                skipped_by_budget += 1
                self._starved_at[acc.id] = time.monotonic()
                logs.info("claim",
                          f"账号 {acc.name} 本轮预算耗尽跳过（轮级预算 "
                          f"{int(budget)}s 已用完，下拍续领·下轮队首优先）")
                return False

            # fork 保留轮内错峰（原串行轮的 claim_round_stagger）
            stagger = store.claim_round_stagger()
            if self._round_visited and stagger > 0:
                delay = secrets.randbelow(stagger + 1)
                if delay:
                    logs.info("claim", f"账号 {acc.name} 错峰等待 {delay}s")
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                        return False  # 等待期间收到停服信号，本轮不再领取
                    except TimeoutError:
                        pass
            self._round_visited = True
            # 本号本轮实际被服务，饥饿标记即消化（上游 2026-11-22 F5）
            self._starved_at.pop(acc.id, None)
            if acc_holds:
                soon = max(0, (min(acc_holds.values()) - now_ms) / 1000)
                logs.info("claim", f"账号 {acc.name} {len(acc_holds)} 个套餐名额等待中"
                                   f"（约 {int(soon)}s 后恢复重试）")
            # 单号硬截止 = 轮级剩余预算（求解长尾被 wait_for 兜住，不再拖垮整轮）
            left = (deadline - time.monotonic()) if deadline is not None else None
            try:
                outcomes = await asyncio.wait_for(
                    auto_claim_all_plans(acc, skip_plan_ids=set(acc_holds)),
                    timeout=left,
                )
            except TimeoutError:
                logs.warn("claim",
                          f"账号 {acc.name} 本轮领取超轮级预算被截断（下拍续领）")
                # 超时截断同属预算饥饿（2026-11-22 迭代二 F1）：下轮同样排队首
                self._starved_at[acc.id] = time.monotonic()
                return False
            except Exception as err:  # noqa: BLE001 - 单账号异常不中断整轮
                logs.warn("claim", f"轮次领取 账号 {acc.name} 异常: {err}")
                return False
            # 按本轮实绩更新退避表：1005+next_at 记忆；成功/其余失败清除
            # （被跳过的套餐无 outcome，等待自然延续到 next_at）
            updated = dict(acc_holds)
            for o in outcomes:
                pid = o.get("plan_id")
                if not pid:
                    continue
                next_at = o.get("next_at") if o.get("code") == 1005 else None
                if next_at:
                    updated[pid] = int(next_at)
                else:
                    updated.pop(pid, None)
            if updated:
                holds[acc.id] = updated
            else:
                holds.pop(acc.id, None)
            if not any(o.get("ok") for o in outcomes):
                return False
            live = store.find("zai", acc.id)
            if live is not None:
                try:
                    await refresh_accounts([live])  # 领到额度立即反映到 UI
                except Exception as err:  # noqa: BLE001
                    logs.warn("claim", f"轮次领取 账号 {acc.name} 额度刷新失败: {err}")
            return True

        # 并发调度（2.7.68）：求解进程级上限与 TP rebalance 互斥都有自己的全局
        # 闸，这里的并发只是调度并行度，不放大浏览器/上游压力。
        claimed = 0
        sem = asyncio.Semaphore(concurrency)

        async def _worker(acc: Account) -> None:
            nonlocal claimed
            async with sem:
                if await _claim_one(acc, holds.get(acc.id, {})):
                    claimed += 1

        await asyncio.gather(*[_worker(acc) for acc in accounts], return_exceptions=True)
        if skipped_by_budget:
            logs.info("claim",
                      f"本轮 {skipped_by_budget} 个账号因轮级预算耗尽未执行"
                      f"（下拍续领·下轮队首优先）")
        if claimed:
            monitor.request_refresh(f"自动领取轮新入账 {claimed} 个账号")
            logs.ok("claim", f"自动领取轮完成: {claimed} 个账号有新套餐入账")
        # 退避表增量落库（2026-11-22 review P2；迭代二 F5 补取消路径）：
        # 重启不再失忆，等待期套餐不因发版/SIGKILL 重启白烧求解+claim 流量。
        # finally 收口——优雅重启落在 19 点窗口内时 gather 被 cancel，此前
        # 本轮已更新的 1005 next_at 全部不落库 = 修复初衷恰好失效。
        try:
            self._persist_holds(holds)
        except asyncio.CancelledError:
            self._persist_holds(holds)
            raise

    async def _wait_stop_or_wake(self, timeout: float) -> bool:
        """等待「停服 / 提前唤醒 / 到点」，返回是否因唤醒返回（停服/超时 False）。"""
        stop_task = asyncio.ensure_future(self._stop.wait())
        wake_task = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({stop_task, wake_task}, timeout=timeout,
                               return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (stop_task, wake_task):
                t.cancel()
        if self._stop.is_set():
            return False
        return self._wake.is_set()

    def request_round(self, reason: str) -> None:
        """外部发现新候选套餐时提前唤醒领取轮（轮未运行时仅置位，无害）。

        唤醒不越过 interval<=0 的关闭语义：_loop 醒来后仍按设置决定是否跑轮。
        """
        if self._wake.is_set():
            return
        logs.info("claim", f"领取轮提前唤醒: {reason}")
        self._wake.set()

    async def _loop(self) -> None:
        from .store import store

        # 启动先等一段（避开启动安装序 / 入池自动领取的首次流量高峰）
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=self.START_DELAY_SECONDS)
            return
        except TimeoutError:
            pass

        while not self._stop.is_set():
            interval = store.claim_round_interval()  # 实时读取设置，改后即生效
            hours = store.claim_round_hours()
            if self._should_run(interval, hours):
                try:
                    await self._round()
                except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
                    logs.err("claim", f"自动领取轮出错: {err}")
            # 等待三件事任一：停服 / 新候选入池唤醒 / 下一次节拍到点
            # （节拍由 _next_wait 给出：关闭态 30s 回看、窗口外睡到窗口开启）。
            if await self._wait_stop_or_wake(self._next_wait(interval, hours)):
                # 新候选入池唤醒：清标记后立即进入下一轮（关闭态只回看设置）
                self._wake.clear()
                if interval > 0:
                    logs.info("claim", "新候选套餐入池，提前触发一轮领取")
            if self._stop.is_set():
                return

    @classmethod
    def _should_run(cls, interval: int, hours: list[int], now: float | None = None) -> bool:
        """本轮是否执行：轮次启用（interval>0）且（无窗口限制 或 当前在窗口内）。"""
        return interval > 0 and cls._in_window(hours, now)

    # ── 领取窗口调度（北京时间）─────────────────────────────────────────────
    # 上游套餐按天投放，用户侧实证每天 19:00 放量。窗口锚定北京时钟而非 host
    # 时区（host 换成 UTC 也不漂）；中国无夏令时，固定 +8 即精确，无 tzdata 依赖。

    @staticmethod
    def _beijing_hour_frac(now: float | None = None) -> float:
        """当前北京时间的小时（含小数，19.5 = 19:30）。"""
        lt = time.gmtime((now if now is not None else time.time()) + 8 * 3600)
        return lt.tm_hour + lt.tm_min / 60 + lt.tm_sec / 3600

    @classmethod
    def _in_window(cls, hours: list[int], now: float | None = None) -> bool:
        """当前北京时间小时是否在领取窗口内（空窗口 = 全天不限时）。"""
        if not hours:
            return True
        return int(cls._beijing_hour_frac(now)) in hours

    @classmethod
    def _seconds_until_window(cls, hours: list[int], now: float | None = None) -> float:
        """距下一个窗口起点（北京时间整点）的秒数；已在窗口内返回 0。"""
        frac = cls._beijing_hour_frac(now)
        if int(frac) in hours:
            return 0.0  # 恰逢整点（frac == hour）也属于窗口内，不能落进「今天已过」
        for hour in sorted(hours):
            if frac < hour:
                return (hour - frac) * 3600
        return (24 - frac + min(hours)) * 3600  # 今天窗口已过 → 明天第一个窗口

    @classmethod
    def _next_wait(cls, interval: int, hours: list[int], now: float | None = None) -> float:
        """下一次唤醒秒数。

        interval<=0：关闭态 30s 回看设置（随时可启用）；
        无窗口：interval 节拍；有窗口：窗口内 interval 节拍，窗口外最迟 30min
        回看一次设置、并在窗口开始时准时醒来（19:00 放量即领，不空转一天）。
        """
        if interval <= 0:
            return 30.0
        if not hours:
            return float(interval)
        now = now if now is not None else time.time()
        if cls._in_window(hours, now):
            return float(interval)
        return max(30.0, min(1800.0, cls._seconds_until_window(hours, now)))

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止后台轮：事件置位后有截止地等待；超时即 cancel 轮任务。

        轮内正在跑的验证码求解/上游请求长尾（求解重试可达分钟级）不允许
        卡住 lifespan 关闭——到期 cancel，收尾即返回。
        """
        self._stop.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=self.STOP_GRACE_SECONDS)
        except TimeoutError:
            pass  # wait_for 已在截止时 cancel 并等待收尾
        except Exception as err:  # noqa: BLE001 - _loop 自兜；此处留痕防停服冒泡
            logs.err("claim", f"领取轮任务异常退出: {err}")
        self._task = None


claim_round = ClaimRoundManager()
