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

from . import constants, freshness, logs, settings
from .captcha import captcha_manager
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
        if ent.get("meter") != "model_usage" or ent.get("unit_type") != "token":
            continue
        name = str(ent.get("show_name") or ent.get("showName") or "").strip()
        if not name:
            continue
        units = ent.get("grant_units", ent.get("grantUnits")) or 0
        grants.append({
            "name": name,
            "units": float(units),
            "period": ent.get("period") or "one_time",
        })
    return {
        "plan_id": plan_id,
        "name": str(raw.get("name") or "").strip(),
        "description": str(raw.get("description") or "").strip(),
        "priority": raw.get("priority") or 0,
        "grants": grants,
    }


async def _billing_request(account: Account, method: str, path: str, **kwargs) -> dict:
    headers = dict(kwargs.pop("headers"))
    try:
        # trust_env=False：billing 全家桶直连。实测「解码直连 + claim 直连」才能过
        # 上游风控（claim 走 US 出口被拒「unusual activity」，直连 code:0）；
        # messages 端点与此相反（直连曾被 3012 风控），仍走 env 的 US 代理。
        async with httpx.AsyncClient(timeout=25, trust_env=False) as client:
            res = await client.request(
                method, f"{settings.ZCODE_BILLING_BASE}{path}",
                headers=headers, **kwargs,
            )
    except httpx.HTTPError as err:
        # 连接/超时等网络故障统一转业务错误：路由层只需面对 ClaimError 一种失败
        raise ClaimError(f"上游网络错误: {err}") from err
    if res.status_code in (401, 403):
        text = (res.text or "").lower()
        if "captcha" not in text and "verify" not in text:
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
            await post_activation_event(profile, user_id, element)
        except (httpx.HTTPError, RuntimeError) as err:
            return f"激活事件 {element} 上报失败: {err}"
    return None


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
    """
    ts = time.time() if now is None else now
    local = time.localtime(ts)
    today = time.strftime("%m%d", local)
    out = [f"zcode-v3-start-plan-{today}"]
    if local.tm_wday in (4, 5, 6):  # 周五~周日：周末活动
        # 后缀形态（…-1030-wk）为 10 月起现行；前缀（…-wk-0905）为 9 月实证
        # 旧形——两种都试，官方改回也不漏（feed 上线后这只是廉价保险）。
        out.append(f"zcode-v3-start-plan-{today}-wk")
        out.append(f"zcode-v3-start-plan-wk-{today}")
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
    out = [pid for pid in _date_plan_candidates() + known_plan_ids()
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
    return time.strftime("%Y-%m-%d")


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
            result = await claim(account, plan["plan_id"], allow_disabled=True)
            outcomes.append({"account_id": account.id, "account_name": account.name,
                             "ok": True, **result})
            logs.ok("claim", f"账号 {account.name} 自动领取成功: "
                             f"{result.get('plan_name') or plan['plan_id']}")
        except ClaimError as err:
            outcome = {"account_id": account.id, "account_name": account.name,
                       "ok": False, "plan_id": plan["plan_id"], "message": str(err)}
            if err.code != -1:
                outcome["code"] = err.code
            if err.next_at:
                outcome["next_at"] = err.next_at
            outcomes.append(outcome)
            logs.warn("claim", f"账号 {account.name} 自动领取 {plan['plan_id']} 失败: {err}")
        except Exception as err:  # noqa: BLE001
            logs.warn("claim", f"账号 {account.name} 自动领取异常: {err}")
            outcomes.append({"account_id": account.id, "account_name": account.name,
                             "ok": False, "plan_id": plan["plan_id"], "message": str(err)})
    if skipped:
        logs.info("claim", f"账号 {account.name} {skipped} 个套餐名额等待期，本轮跳过领取")
    return outcomes


async def preview_plans(account: Account, *, allow_disabled: bool = False) -> list[dict]:
    """拉取账号当前可领取套餐，按优先级降序。"""
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
    return best["plan_id"], best["name"] or best["plan_id"], best["grants"]


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
    return {
        "plan_id": plan_id,
        "plan_name": plan_name,
        "grants": grants,
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
    verify_param, verify_region = await captcha_manager.get_verify_param()
    config = await captcha_manager.fetch_config()
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
        captcha_manager.invalidate()  # 被拒的码出池，避免下轮复用
    raise _fail_error(code, body)


class ClaimRoundManager:
    """后台周期自动领取轮（默认 1 小时，2.6.5 整改；对齐 zcode-switch 用户触发语义）。

    每轮对全部可领取的 JWT 账号执行 auto_claim_all_plans（每日一次激活
    上报先于 preview + 逐个领取可领套餐）；冷却/失效/风控封禁账号由
    allows_claim() 先行过滤（停用的好票仍参与领取），保证「冷却期零上游
    billing 流量」不变量。
    1005（今日名额用完）按服务端 next_at 退避：等待期该套餐不打 claim
    （省 captcha 求解与上游写流量），preview 照常保留新套餐发现
    （docs/development/05「按服务端 next window 退避」语义落地）。
    轮间隔运行期读 meta 设置（0 = 关闭，仍周期回看便于随时启用）；
    轮内相邻账号间可加 0～stagger 秒随机延迟（claim_round_stagger，打散
    「同轮全池同拍打上游 billing」的行为聚类；0 = 关闭，首个账号不等）。
    新候选套餐入池（remember_plan_ids 发现真新 id）经 request_round 提前
    唤醒，不等满 interval。
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
        # 账号 → {plan_id: 名额恢复时间(ms)}：1005 退避记忆（仅轮次路径使用）
        self._claim_holds: dict[str, dict[str, int]] = {}
        # 无可服务账号的告警只在状态变化时发（持续故障期不逐轮刷屏）
        self._empty_warned = False
        # 错峰标记：本轮已处理过至少一个账号（首个账号不等，只错峰后续）
        self._round_visited = False

    async def _round(self) -> None:
        from .quota import refresh_accounts
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
        now_ms = time.time() * 1000
        # 错峰标记按轮重置：每轮首个账号不等，只错峰后续相邻账号
        self._round_visited = False
        # 退避表先清过期项（next_at 已过的恢复本轮重试）
        holds = {
            aid: {pid: at for pid, at in pl.items() if at > now_ms}
            for aid, pl in self._claim_holds.items()
        }
        self._claim_holds = holds
        claimed = 0
        stagger = store.claim_round_stagger()  # 轮内相邻账号错峰上限（秒）
        for acc in accounts:
            if self._round_visited and stagger > 0:
                delay = secrets.randbelow(stagger + 1)  # 0～stagger 均匀分布，含端点
                logs.info("claim", f"账号错峰等待 {delay}s 后处理 {acc.name}")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    return  # 等待期间收到停服信号，直接收尾
                except TimeoutError:
                    pass  # 正常路径：延迟到点，继续处理本账号
            self._round_visited = True
            acc_holds = holds.get(acc.id, {})
            if acc_holds:
                soon = max(0, (min(acc_holds.values()) - now_ms) / 1000)
                logs.info("claim", f"账号 {acc.name} {len(acc_holds)} 个套餐名额等待中"
                                   f"（约 {int(soon)}s 后恢复重试）")
            try:
                outcomes = await auto_claim_all_plans(acc, skip_plan_ids=set(acc_holds))
            except Exception as err:  # noqa: BLE001 - 单账号异常不中断整轮
                logs.warn("claim", f"轮次领取 账号 {acc.name} 异常: {err}")
                continue
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
                continue
            claimed += 1
            live = store.find("zai", acc.id)
            if live is not None:
                try:
                    await refresh_accounts([live])  # 领到额度立即反映到 UI
                except Exception as err:  # noqa: BLE001
                    logs.warn("claim", f"轮次领取 账号 {acc.name} 额度刷新失败: {err}")
        if claimed:
            logs.ok("claim", f"自动领取轮完成: {claimed} 个账号有新套餐入账")

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
            if interval > 0:
                try:
                    await self._round()
                except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
                    logs.err("claim", f"自动领取轮出错: {err}")
            # interval<=0 视为关闭：仍周期性回看设置，便于随时启用
            wait = interval if interval > 0 else 30
            if await self._wait_stop_or_wake(wait):
                # 新候选入池唤醒：清标记后立即进入下一轮（关闭态只回看设置）
                self._wake.clear()
                if interval > 0:
                    logs.info("claim", "新候选套餐入池，提前触发一轮领取")
                continue
            if self._stop.is_set():
                return

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
