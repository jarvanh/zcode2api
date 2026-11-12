"""ZCode 额度 / 余额 / 用量查询，以及账号状态判定。

在查询基础上提供「额度用完自动标记 exhausted」的监控能力。
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx

from . import business_error, constants, logs, settings
from .models import Account, Status, expires_epoch, window_expired, window_usable
from .models import _plan_bonus_active as _bonus_active
from .quota_windows import pending_plan_start
from .store import store

_DEVICE_MID: str | None = None


def device_mid() -> str:
    """本机设备 ID（ZCode 客户端 telemetry 同款语义）。

    billing 全家桶（current/balance/usage/preview/claim）必需 X-Device-Mid，
    缺失时上游返回 code=3001 parameter error。首次生成后持久化到 data 目录。
    """
    global _DEVICE_MID
    if _DEVICE_MID:
        return _DEVICE_MID
    path = settings.DATA_DIR / "device_mid"
    try:
        _DEVICE_MID = path.read_text().strip()
        if _DEVICE_MID:
            return _DEVICE_MID
    except OSError:
        pass
    _DEVICE_MID = str(uuid.uuid4())
    try:
        os.makedirs(settings.DATA_DIR, exist_ok=True)
        with open(path, "x") as f:
            f.write(_DEVICE_MID)
    except OSError as err:  # noqa: BLE001 - 生成失败不阻断查询，仅进程内复用
        logs.warn("quota", f"device_mid 持久化失败（仅进程内生效）: {err}")
    return _DEVICE_MID


def _auth_headers(account: Account) -> dict:
    """billing 族请求头（对齐 zcode-switch zai_billing_headers 实证形态）。

    UA/版本走 BILLING_APP_VERSION（官方桌面端现行版，与 messages 指纹刻意
    分离）；平台/os/语言/时区/设备 ID 按账号指纹档案出值（每账号独立设备，
    见 fingerprint.py）；每请求全新 x-request-id。
    """
    from .fingerprint import profile_for

    profile = profile_for(account)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": f"ZCode/{constants.BILLING_APP_VERSION}",
        "HTTP-Referer": constants.ZCODE_ORIGIN,
        "X-Title": constants.BILLING_TITLE,
        "X-ZCode-App-Version": constants.BILLING_APP_VERSION,
        "X-Platform": profile.platform_full,
        "X-Release-Channel": constants.BILLING_RELEASE_CHANNEL,
        "X-Client-Language": profile.language,
        "X-Client-Timezone": profile.timezone,
        "X-Os-Category": profile.os_category,
        "X-Os-Version": profile.os_version,
        "X-Device-Mid": profile.device_mid,
        "x-request-id": str(uuid.uuid4()),
    }
    if account.mode == "jwt" and account.jwt_token:
        headers["Authorization"] = f"Bearer {account.jwt_token}"
    elif account.api_key:
        headers["x-api-key"] = account.api_key
    return headers


def quota_recoverable(account: Account, quota_map: dict, now: float) -> tuple[bool, bool]:
    """共享真源：账号此刻是否有可用额度，返回 (has_daily, has_bonus)。

    - has_daily：传入的窗口快照里存在「余额 > 0 且未过期」的窗口；
    - has_bonus：plan / plans 里存在**已到 effective_at 且未过 ends_at** 的
      one_time 赠送授权（balance 不含这类额度，故必须单独判）。

    两处调用方共用，防止口径漂移：
      ① `fetch_quota` 用本次拉到的 quota_map 判「耗尽 / 恢复」；
      ② `QuotaMonitor._activate_due` 用账号里的旧 quota_map 做**到点本地激活**
         （零上游流量：effective_at 是上游已落库的承诺，到点即可信）。
    2026-11-04 实证：旧窗口全部过期但 entitlement effective_at=23:00，
    23:00:00 起 has_bonus 即变 True，无需先打 billing。
    """
    has_daily = any(window_usable(w, now) for w in (quota_map or {}).values())
    has_bonus = _bonus_active(account.plan, now) or any(
        _bonus_active(p, now) for p in (account.plans or [])
    )
    return has_daily, has_bonus


async def fetch_quota(account: Account) -> dict:
    """拉取单个账号的 方案 / 余额 / 用量，写回账号状态并持久化。

    返回结构: {"billing":..., "balance":..., "usage":..., "error":...}
    """
    live = store.find(account.provider, account.id)
    if live is None:
        return {"error": "账号已删除"}
    if live is not account:
        account = live
    if (not account.allows_billing() and not account.is_cooling()
            and not account.allows_claim()):
        # 与 admin/monitor 同一扇门。冷却账号仍允许查询（测试与兜底约定：
        # 拿额度不得提前解除冷却）；废 JWT / 风控封禁 / 非 JWT 一律不打 billing。
        # 手动停用的好票放行：领取成功后要立即把新包反映到面板（allows_claim）。
        from .claim import AUTH_EXPIRED_MESSAGE, billing_block_reason

        return {
            "error": account.last_error
            or billing_block_reason(account, action="上游查询")
            or AUTH_EXPIRED_MESSAGE
        }
    headers = _auth_headers(account)
    base = settings.ZCODE_BILLING_BASE
    result: dict = {}

    async with httpx.AsyncClient(timeout=20) as client:
        async def _get(path: str):
            try:
                return await client.get(f"{base}{path}", headers=headers)
            except httpx.HTTPError:
                return None

        billing_res, balance_res, usage_res = await asyncio.gather(
            _get("/billing/current"),
            _get("/billing/balance"),
            _get("/usage"),
        )

    live = store.find(account.provider, account.id)
    if live is None:
        return {"error": "账号已删除"}
    if live is not account:
        account = live

    now = time.time()
    account.last_checked_at = now

    # 鉴权失败 → 标记 invalid
    if billing_res is not None and billing_res.status_code in (401, 403):
        body = (billing_res.text or "").lower()
        if "captcha" not in body and "verify" not in body:
            from .claim import AUTH_EXPIRED_MESSAGE

            account.status = Status.INVALID
            account.last_error = AUTH_EXPIRED_MESSAGE
            store.update_account(account)
            return {"error": account.last_error}

    # HTTP 200 承载的业务错误（Plan 通道实测：额度/风控类拒绝常是 200 +
    # {"code":1005,...}）。三条 billing 请求各自判：命中业务错误的**不得写库**，
    # 否则会把 quota 快照 / plans 洗空，看起来像「上游没投放、额度未知」。
    errors: dict[str, int] = {}
    billing_plans: list | None = None  # 本次 billing/current 实际解析到的 plans

    # plans[] 归并（plan_id 去重）：current 在投放空窗会返回 code=0 且**无 data**
    # （plans 恒空），而 balance 一直带着已生效套餐。旧实现只认 current 且直接
    # 赋值，等于每次刷新都把 account.plans 清成 []，_bonus_active 从此永不命中
    # ——赠送池账号被误标 exhausted、面板停在旧窗口读数（2026-10-31 实证）。
    plans_by_id: dict[str, dict] = {}
    plans_source_ok = False

    def _collect_plans(payload: dict) -> bool:
        # 返回「本次响应带 plans 字段」（即权威 plans 源）。缺 data、或 data 里
        # 没有 plans 键的响应都不得当作「plans 为空」的依据：上游投放空窗就是
        # code=0 无 data（官方客户端对此直接抛 manual_claim_preview_missing_data，
        # 口径对齐）；balance 的常态响应只有 balances[]、没有 plans 键，若把它
        # 当成权威源，billing/current 回 200+1005 时就会把历史 plans 洗成 []
        #（2026-10-31 实证：额度拒绝被写成「上游没投放套餐」）。
        data = payload.get("data")
        if not isinstance(data, dict) or "plans" not in data:
            return False
        for p in data.get("plans") or []:
            pid = str(p.get("plan_id") or "").strip()
            if pid and pid not in plans_by_id:
                plans_by_id[pid] = p
        return True

    if billing_res is not None and billing_res.status_code == 200:
        try:
            data = billing_res.json()
            result["billing"] = data
            code = business_error.business_code(data)
            if code is not None:
                errors["current"] = code
            else:
                billing_plans = (data.get("data") or {}).get("plans") or []
                # 归并进 plans_by_id（不在此直接写 account.plans）：current 与
                # balance 的 plans 按 plan_id 合并后再统一赋值，避免投放空窗时
                # current 的空列表把 balance 已生效套餐洗掉（2026-10-31 实证）。
                plans_source_ok = _collect_plans(data) or plans_source_ok
        except (ValueError, KeyError):
            pass

    quota_map: dict = {}
    if balance_res is not None and balance_res.status_code == 200:
        try:
            data = balance_res.json()
            result["balance"] = data
            code = business_error.business_code(data)
            if code is not None:
                errors["balance"] = code
            # balance 也带 plans[]（含 plan_id）：`/billing/current` 偶发失败或
            # 投放空窗时，这里就是套餐类型的唯一来源——少了它 trial 号会退化
            # 成「未知套餐」，拿不到体验档的并发收紧（见 models.Account.plan_kind）。
            # 归并由下方 plans_source_ok 统一做，此处不直接赋值（否则会与 current
            # 的归并结果互相覆盖）。业务错误时下方 two 处都按 code 短路。
            # 注意：quota_verified_at **不在这里**打点。balance 200 但 balances
            # 为空（新号 billing 全空形态，见下方注释）时什么也没确认——此时
            # 打点会把旧的过期快照误升格为「过期后实查确认」，正好把 2.7.8
            # 要救的号从另一扇门判死。打点在下方 quota_map 非空分支。
            for bal in [b for b in (data.get("data") or {}).get("balances") or []
                        if code is None]:
                name = bal.get("show_name") or bal.get("model") or "model"
                window = {
                    "total": bal.get("total_units"),
                    "used": bal.get("used_units"),
                    "remaining": bal.get("remaining_units"),
                    "expires_at": bal.get("expires_at"),
                }
                prev = quota_map.get(name)
                if prev:
                    # 防御：同模型多窗口（如日窗 + 一次性）合并，避免后者覆盖前者
                    for k in ("total", "used", "remaining"):
                        prev[k] = (prev.get(k) or 0) + (window.get(k) or 0)
                    # expires_at 保留**更晚**的那个。该字段线上是 epoch 秒、
                    # 快照里也出现过 ISO8601 字符串，直接 max() 会按字典序比
                    # （str 与 int 混型甚至 TypeError），必须先归一成 epoch 再挑。
                    prev_end = expires_epoch(prev.get("expires_at"))
                    new_end = expires_epoch(window.get("expires_at"))
                    if new_end is not None and (prev_end is None or new_end > prev_end):
                        prev["expires_at"] = window.get("expires_at")
                    logs.warn("quota", f"balance 同名窗口 {name} 已合并")
                else:
                    quota_map[name] = window
            plans_source_ok = _collect_plans(data) or plans_source_ok
        except (ValueError, KeyError):
            pass

    if plans_source_ok:
        # 全量保留（多套餐时 entitlements 不丢）；plan 兼容保留首个。
        # 两侧都拿不到 plans 数据时保持原值，不用空列表覆盖历史事实。
        account.plans = list(plans_by_id.values())
        account.plan = account.plans[0] if account.plans else {}
        # 投放 id 收割：上游会绕过 preview 直接把套餐入账（10-30 周末套餐实证），
        # 这些 id 只能从已入账号的 balance/current 里看到。入池后自动领取轮的
        # 空窗兜底（_probe_unlisted_plans）靠它给其它号领取同一活动。
        if plans_by_id:
            from .claim import remember_plan_ids

            remember_plan_ids(list(plans_by_id))

    if usage_res is not None and usage_res.status_code == 200:
        try:
            data = usage_res.json()
            code = business_error.business_code(data)
            if code is not None:
                errors["usage"] = code
            else:
                account.usage = data.get("data") or {}
                result["usage"] = account.usage
        except (ValueError, KeyError):
            pass

    if quota_map:
        account.quota = quota_map
        # 快照真实写库的时刻 = 「过期后实查」的证据点（见 models.Account.quota_expired）。
        # 绑在 quota_map 非空上：只有真的取到窗口，才谈得上「确认」它们的状态。
        account.quota_verified_at = now
        # 额度耗尽判定：逐窗口算「有效余量」（remaining > 0 且未过期）。
        # 原实现只看 remaining <= 0，把**已过期窗口**当可用 —— 2026-10-31 实证：
        # 老号 GLM-5.3-Flash 本地 remaining=1e8、expires_at 在 28 天前，上游对
        # 每个 messages 一律回 {"code":1005}。只看余额会让这类号长期占着轮询位。
        # 注意 balance 不含一次性赠送池：赠送池有效时不判耗尽。
        now = time.time()
        all_expired = all(window_expired(w, now) for w in quota_map.values())
        # 与「到点本地激活」共用同一谓词，避免两处口径漂移（2026-11-04）
        has_daily, has_bonus = quota_recoverable(account, quota_map, now)
        # 风控禁用 / 废 JWT：billing 数据照写，但**绝不**用额度数字改状态；
        # 否则 DISABLED→EXHAUSTED→（额度恢复）→ACTIVE 会绕过人工/风控封禁。
        if account.status in (Status.INVALID, Status.DISABLED):
            pass
        elif not account.enabled:
            pass  # 手动停用语义优先于额度语义，面板才能读出停用原因
        elif not has_daily and not has_bonus:
            account.status = Status.EXHAUSTED
            account.last_error = "额度已过期" if all_expired else "额度已用完"
        elif account.status in (Status.EXHAUSTED, Status.COOLING) and (
            has_daily or has_bonus
        ):
            # 额度恢复（窗口重置 / 赠送池生效）→ 重新激活。冷却期内不提前解除；
            # 废 JWT / 风控禁用绝不能因额度数字复活 Plan 通道。
            if not account.is_cooling():
                account.status = Status.ACTIVE
                account.last_error = None
                account.cooling_until = None
    elif errors:
        # billing 回了业务错误、但没拿到任何窗口：**不据此判死**。
        # 上游对「体验套餐/新号」的 billing 支持并不一致（老实证：billing 全空
        # 仍有 100% 可用的试用额度），拿不到账面数据 ≠ 没额度。真正的判定只有
        # 两处：balance 窗口全部失效（上面分支）、messages 真实回 1005（gateway）。
        # 这里只留痕，让一次真实请求去定生死——否则新号会被永久挡在门外。
        detail = "，".join(f"{k}=code{v}" for k, v in errors.items())
        logs.warn("quota", f"账号 {account.name} billing 业务错误但保留可选（{detail}）")

    # 新号体验额度：上游对新注册账号可能 **billing 全空/全拒**，但 messages 仍能
    # 正常服务（2026-09-06 实测：空 billing 的 oauth-login 100% 成功服务请求；
    # 2026-11-01 复核：billing/preview、balance 均空/1005，新号照样有体验套餐）。
    # 只凭 billing 就判死会把真能用的号挡在门外。这里**只给 ACTIVE 且零成功请求
    # 的号**一条提示，绝不复活已判死的号——否则「首探 1005 → 判死 → 下次刷新又
    # 拉活 → 再撞 1005」会无限循环，每圈烧一次真实请求（比误判更贵）。
    elif not (quota_map or billing_plans or result.get("usage")):
        if account.use_count == 0 and account.status == Status.ACTIVE:
            account.last_error = "billing 为空，待首次请求验证（疑似新号体验额度）"

    store.update_account(account)
    # 「三路全业务错误」才整体报失败（调用方据此区分「上游拒绝」与「上游确实
    # 什么都没有」）。一路成功就照常返回，错误只记日志——避免把「部分可用」
    # 误报成刷新失败。
    if errors and "current" in errors and not quota_map and not result.get("usage"):
        detail = "，".join(f"{k}=code{v}" for k, v in errors.items())
        logs.warn("quota", f"账号 {account.name} billing 全业务错误（{detail}）")
        return {"error": f"上游业务错误（{detail}）"}
    return result or {"error": "无法获取额度数据"}


def _normalize_limits(data: dict) -> list[dict]:
    """`data.limits[]` → 紧凑快照（TOKENS_LIMIT / TIME_LIMIT 两类窗口混排）。"""
    out: list[dict] = []
    for item in (data.get("data") or {}).get("limits") or []:
        if not isinstance(item, dict):
            continue
        out.append({
            "type": item.get("type"),
            "unit": item.get("unit"),
            "number": item.get("number"),
            "usage": item.get("usage"),
            "remaining": item.get("remaining"),
            "percentage": item.get("percentage"),
            "next_reset_time": item.get("next_reset_time"),
        })
    return out


async def fetch_coding_limits(account: Account) -> dict:
    """拉取 Coding 平面额度（api.z.ai monitor 端点）。

    与 `fetch_quota` 是**两套独立账本**：Plan 通道额度来自 zcode.z.ai 的
    billing 族；Coding 平面（API Key 直连 api.z.ai）的额度完全不在那套账里。
    体验套餐号在 billing 侧常全空/全拒，但这里能给出真实上限表——作为第二
    额度源补盲，只读、不写账号状态（避免与 billing 判定打架）。

    返回 {"limits": [...], "raw": {...}} 或 {"error": ...}
    """
    api_key = (account.api_key or "").strip()
    if not api_key:
        return {"error": "该账号没有 API Key，无法查询 Coding 平面额度"}
    headers = {"authorization": api_key, "accept": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(constants.CODING_LIMIT_URL, headers=headers)
    except httpx.HTTPError as err:
        return {"error": f"请求上游失败: {err}"}
    try:
        data = resp.json()
    except ValueError:
        return {"error": f"上游返回非 JSON（HTTP {resp.status_code}）"}
    if not isinstance(data, dict):
        return {"error": "上游返回结构异常"}
    code = business_error.business_code(data)
    if code is not None:
        return {"error": str(data.get("msg") or f"业务错误 code={code}"), "code": code}
    if resp.status_code >= 400:
        return {"error": f"HTTP {resp.status_code}"}
    return {"limits": _normalize_limits(data), "raw": data}


async def refresh_accounts(accounts: list[Account]) -> dict:
    """并发刷新一批账号，返回汇总。"""
    if not accounts:
        return {"ok": 0, "fail": 0}
    sem = asyncio.Semaphore(8)

    async def _one(acc: Account) -> bool:
        async with sem:
            res = await fetch_quota(acc)
            return "error" not in res

    results = await asyncio.gather(*[_one(a) for a in accounts], return_exceptions=True)
    ok = sum(1 for r in results if r is True)
    return {"ok": ok, "fail": len(accounts) - ok}


class QuotaMonitor:
    """后台周期性刷新额度 + **待生效套餐到点自动生效**（2026-11-04）。

    两层：
      ① 准点本地激活：`effective_at` 是已落库的上游承诺，到点零上游流量即可
         把 EXHAUSTED 号翻回 ACTIVE，不必等 30min 轮询 / 不必先打 billing。
      ② 准点后一次确认刷新：写回真实额度给看板；常规 1800s 节拍不变。
    等待取 `min(常规节拍, 最早 pending_plan_start)`；过点后 pending_plan_start
    自动返回 None → 天然一次性，不增加常态 billing 频率。
    """

    START_DELAY = 5            # 启动避让
    DISABLED_POLL = 30         # interval<=0 时回看设置的间隔
    CONFIRM_GRACE = 10         # 到点后给上游落地的宽限（秒）
    CONFIRM_RETRIES = 3        # 确认刷新有界重试次数
    CONFIRM_RETRY_WAIT = 30    # 重试间隔（秒）

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()

    # ── 外部提前唤醒：领包成功后立即重算 due（不额外打 billing）──
    def request_refresh(self, reason: str = "") -> None:
        if self._wake.is_set():
            return
        logs.info("quota", f"额度刷新提前唤醒: {reason}")
        self._wake.set()

    async def _wait_stop_or_wake(self, timeout: float) -> bool:
        """等「停服 / 提前唤醒 / 到点」；返回是否因提前唤醒（停服/超时 False）。"""
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

    @staticmethod
    def _wake_plan(now: float, next_regular_at: float, due: float | None):
        """选下一个唤醒点：常规节拍 vs 最早待生效时刻（纯函数，可单测）。

        due 为 None（无待生效包 / 已过时刻）或晚于常规节拍 → 走 regular。
        """
        if due is not None and due < next_regular_at:
            return due, "due"
        return next_regular_at, "regular"

    def _activate_due(self, now: float) -> int:
        """准点本地激活（零上游流量），返回激活账号数。

        只认 entitlement `effective_at` 已跨过且未失效的 one_time 授权
        （`quota_recoverable` 的 has_bonus）——这是权威时刻；`has_daily` 来自
        可能过期的本地快照，零流量复活会引入「402→翻活→再402」抖动，那类
        恢复交给常规 billing 刷新（fetch_quota 同一谓词）处理。
        冷却 / 停用 / 凭证失效 / 非 JWT 一律不动，保住既有不变量。
        """
        changed = 0
        for a in store.list_accounts("zai"):
            if a.mode != "jwt" or not a.enabled or a.status != Status.EXHAUSTED:
                continue
            if a.is_cooling(now):
                continue
            _, has_bonus = quota_recoverable(a, a.quota, now)
            if not has_bonus:
                continue
            a.status = Status.ACTIVE
            a.last_error = None
            a.cooling_until = None
            if store.update_account(a):
                changed += 1
        return changed

    async def _regular_refresh(self) -> None:
        try:
            accounts = [a for a in store.list_accounts("zai")
                        if a.mode == "jwt" and a.allows_billing()]
            if accounts:
                await refresh_accounts(accounts)
        except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
            logs.err("quota", f"后台刷新出错: {err}")

    async def _sleep_or_stop(self, secs: float) -> bool:
        """等 secs 秒；返回 True 表示期间收到停服（调用方应立即退出）。"""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, secs))
            return True
        except TimeoutError:
            return False

    async def _confirm_refresh(self) -> None:
        """准点确认刷新：写回真实额度；失败按 CONFIRM_RETRY_WAIT 有界重试。

        上限固定（3 次）且只在到点发生 —— 常态节拍仍是 1800s，不会回退到
        「把间隔调小」那类风控放大器（2026-10-24 实证 60s 轮询是 3012 主信号）。
        """
        for attempt in range(self.CONFIRM_RETRIES):
            try:
                accounts = [a for a in store.list_accounts("zai")
                            if a.mode == "jwt" and a.allows_billing()]
                if not accounts:
                    return
                res = await refresh_accounts(accounts)
            except Exception as err:  # noqa: BLE001
                logs.err("quota", f"到点确认刷新出错: {err}")
                res = {"ok": 0, "fail": 1}
            if not res.get("fail"):
                return
            if attempt + 1 < self.CONFIRM_RETRIES:
                if await self._sleep_or_stop(self.CONFIRM_RETRY_WAIT):
                    return
        logs.warn("quota", "到点确认刷新多次失败，回落常规轮询")

    async def _loop(self) -> None:
        # 启动后先等几秒，避免与服务启动争抢
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=self.START_DELAY)
            return
        except TimeoutError:
            pass

        # 保留原语义：启动后立即常规刷新一次（历史 22:38:32 首轮）
        if store.quota_refresh_interval() > 0:
            await self._regular_refresh()

        last_interval = store.quota_refresh_interval()
        next_regular_at = time.time() + (
            last_interval if last_interval > 0 else self.DISABLED_POLL)

        while not self._stop.is_set():
            interval = store.quota_refresh_interval()  # 实时读取，改后即生效
            now = time.time()
            if interval != last_interval:
                last_interval = interval
                next_regular_at = now + (
                    interval if interval > 0 else self.DISABLED_POLL)

            due = pending_plan_start(store.list_accounts("zai"), now)
            wake_at, reason = self._wake_plan(now, next_regular_at, due)
            # 下限 1s：wake_at 已过期（如改设置后立即重算）时不能退化成忙轮询
            wait = max(1.0, wake_at - now)

            woken = await self._wait_stop_or_wake(wait)
            if self._stop.is_set():
                return
            if woken:
                # 领包/外部唤醒：领包方已自行 refresh，这里只重算 due，不额外打点
                self._wake.clear()
                continue

            now = time.time()
            if reason == "due" and now >= due:
                changed = self._activate_due(now)
                if changed:
                    logs.ok("quota", f"待生效套餐到点，本地激活 {changed} 个账号（零上游请求）")
                # 常规节拍不受 due 影响（next_regular_at 保持不变）；
                # interval<=0 = 用户关了后台轮询 → 只做本地激活，不打 billing。
                if interval > 0:
                    if await self._sleep_or_stop(self.CONFIRM_GRACE):
                        return
                    await self._confirm_refresh()
                continue

            # 常规节拍刷新
            if interval > 0:
                await self._regular_refresh()
            next_regular_at = time.time() + (
                interval if interval > 0 else self.DISABLED_POLL)

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._wake.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()  # 立即打断等待
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


monitor = QuotaMonitor()


class ModelProbeMonitor:
    """定时探测「模型级冷却中」的账号模型（2026-10-28 领包回血探测轮）。

    背景：模型冷却指数退避上限 6h，语义是「该模型无资源包/套餐」；资源包
    不定期到账，若只能等下一次真实请求才探测，新包产能会被白挡数小时。
    本轮定期挑一个可探测组合发最小真实请求，成功即清该账号该模型冷却
    （与请求路径的 clear_model_cooling 同语义）；失败仅记录，不重置退避。
    节流：每轮最多 1 个探测，且仅当冷却剩余 < probe_window 秒时才打上游
    ——冷却刚建立（还剩几小时）时不浪费上游写流量，冷却接近尾段时
    主动把「也许已回血」变成确定态。interval=0 关闭。
    """

    PROBE_MAX_TOKENS = 8

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def _probe_window(self) -> int:
        return min(120, max(30, settings.MODEL_PROBE_INTERVAL))

    async def _probe_one(self, acc, model: str) -> bool:
        from .store import store

        url = f"{constants.BIGMODEL_ORIGIN}/api/paas/v4/chat/completions"
        headers = {"Authorization": f"Bearer {acc.api_key}", "Content-Type": "application/json"}
        body = {"model": model, "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": self.PROBE_MAX_TOKENS, "stream": False}
        try:
            async with httpx.AsyncClient(timeout=30, **acc.proxy_kwargs()) as client:
                resp = await client.post(url, headers=headers, json=body)
        except httpx.HTTPError as err:
            logs.warn("probe", f"账号 {acc.name} 探测 {model} 连接失败（保持冷却）: {err}")
            return False
        if resp.status_code == 200:
            acc.clear_model_cooling(model)
            store.update_account(acc)
            logs.ok("probe", f"账号 {acc.name} 探测 {model} 200 —— 包已到账，冷却已清")
            return True
        if resp.status_code == 429 and ("1113" in resp.text or "insufficient balance" in resp.text.lower()):
            logs.info("probe", f"账号 {acc.name} 探测 {model} 仍无资源包（保持冷却）")
            return False
        logs.info("probe", f"账号 {acc.name} 探测 {model} HTTP {resp.status_code}（保持冷却，不打扰退避）")
        return False

    async def _round(self) -> int:
        from .store import store

        now = time.time()
        window = self._probe_window()
        candidates = []
        for acc in store.list_accounts("bigmodel"):
            if not acc.enabled or not (acc.api_key or "").strip() or acc.is_cooling():
                continue  # 整号冷却/停用/无 Key 不探测（is_cooling 全通道门禁不变量）
            for model, until in (acc.model_cooling or {}).items():
                if until > now and (until - now) <= window:
                    candidates.append((until, acc, model))
        if not candidates:
            return 0
        candidates.sort()  # 最早到期的先探
        _, acc, model = candidates[0]
        return 1 if await self._probe_one(acc, model) else 0

    async def _loop(self) -> None:

        try:
            await asyncio.wait_for(self._stop.wait(), timeout=30)
            return
        except TimeoutError:
            pass
        while not self._stop.is_set():
            interval = settings.MODEL_PROBE_INTERVAL  # 运行期读环境快照；改后重启生效
            if interval > 0:
                try:
                    await self._round()
                except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
                    logs.err("probe", f"探测轮异常: {err}")
            wait = interval if interval > 0 else 60
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except TimeoutError:
                continue

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


model_probe = ModelProbeMonitor()


class RiskLaneProbeMonitor:
    """plan-lane 风控冷却探测轮（2026-10-29 lane-scoped 配套，修 P5）。

    lane 长冷（累计 4 次 → 6h）若只等到期，恢复带宽被压到最差；本轮在冷却
    尾段主动发一个最小 messages 请求验证 plan 通道是否已解封——3012 封锁与
    grant/billing 无关，billing 200 证明不了对话腿恢复，必须用对话请求本身
    验证。成功即清该 lane；失败不追加惩罚、不清退避（探测流量不得延长封锁）。

    节流（抄 ModelProbeMonitor 形状）：每轮最多 1 个探测；仅探剩余
    < probe_window 的 lane（刚建立的冷却不浪费上游写流量）；只探 plan 腿；
    跳过人工停用/失效账号。RISK_LANE_PROBE=False 或 MODEL_PROBE_INTERVAL=0
    关闭。探测轮是「冷却期零上游流量」不变量的唯一例外（仅限本 lane 尾段）。
    """

    PROBE_MAX_TOKENS = 8

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def _probe_window(self) -> int:
        return min(300, max(30, settings.MODEL_PROBE_INTERVAL // 2))

    @staticmethod
    def _risk_signal(status_code: int, text: str) -> bool:
        """复刻 gateway._is_risk_control 判定（不 import 防循环依赖）。"""
        if status_code in constants.RISK_CONTROL_HTTP_STATUSES:
            return True
        return any(marker in text for marker in constants.RISK_CONTROL_MARKERS)

    async def _probe_one(self, acc: Account, lane: str) -> bool:
        from .agent import build_request
        from .captcha import captcha_manager

        model = lane.split(":", 1)[1]
        body = {"model": model or "GLM-5.2", "stream": False,
                "max_tokens": self.PROBE_MAX_TOKENS,
                "messages": [{"role": "user", "content": "hi"}]}
        try:
            verify_param, _ = await captcha_manager.get_verify_param(settings.PORT)
        except Exception:  # noqa: BLE001 - 验证码池不可用不阻塞探测
            verify_param = None
        try:
            # 渠道梯语义（v2.9.40）：探测轮要测的是 plan 腿（zcode-plan JWT 通道）
            # 通了没，必须显式指定 zai 通道顶过自动路由 —— 但上游 401 已改为
            # 下沉而非判死（build_request 不再抛「缺 JWT」，此处异常仅剩配置错）。
            url, headers, payload = build_request(acc, body, verify_param, upstream_key="zai")
        except Exception as err:  # noqa: BLE001
            logs.warn("risk-probe", f"账号 {acc.name} 构建 plan 探测请求失败: {err}")
            return False
        try:
            async with httpx.AsyncClient(timeout=30, **acc.proxy_kwargs()) as client:
                resp = await client.post(url, headers=headers, content=payload)
        except httpx.HTTPError as err:
            logs.info("risk-probe", f"账号 {acc.name} plan 探测连接失败（保持冷却）: {err}")
            return False
        text = resp.text or ""
        if resp.status_code == 200 and not self._risk_signal(resp.status_code, text):
            was_downgrade = (acc.status == Status.COOLING
                             and (acc.last_error or "").startswith("命中风控"))
            acc.clear_lane_cooling(lane)
            if was_downgrade and acc.worst_lane_until() is None:
                # 整号 COOLING 若是全腿封锁的退化形态（「命中风控」可证），lane
                # 全清后恢复 ACTIVE；真 5xx 冷却（last_error 不同形）不动。
                acc.status = Status.ACTIVE
                acc.cooling_until = None
                acc.last_error = None
            store.update_account(acc)
            logs.ok("risk-probe", f"账号 {acc.name} plan 探测 200 —— lane {lane} 已解封")
            return True
        logs.info("risk-probe",
                  f"账号 {acc.name} plan 探测 HTTP {resp.status_code}（保持冷却，不追加惩罚）")
        return False

    async def _round(self) -> int:
        now = time.time()
        window = self._probe_window()
        candidates: list[tuple[float, Account, str]] = []
        for acc in store.list_accounts("zai"):
            if not acc.enabled or acc.status in (Status.INVALID, Status.DISABLED):
                continue  # 人工停用/失效不探测
            for lane, until in (acc.risk_lane_cooling or {}).items():
                if not lane.startswith("plan:"):
                    continue  # 只探 plan 腿
                if not acc.is_lane_cooling(lane, now):
                    continue
                if (until - now) <= window:
                    candidates.append((until, acc, lane))
        if not candidates:
            return 0
        candidates.sort()  # 最早到期的先探
        _, acc, lane = candidates[0]
        return 1 if await self._probe_one(acc, lane) else 0

    async def _loop(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=30)
            return
        except TimeoutError:
            pass
        while not self._stop.is_set():
            interval = settings.MODEL_PROBE_INTERVAL if settings.RISK_LANE_PROBE else 0
            if interval > 0:
                try:
                    await self._round()
                except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
                    logs.err("risk-probe", f"plan-lane 探测轮异常: {err}")
            wait = interval if interval > 0 else 60
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except TimeoutError:
                continue

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


risk_lane_probe = RiskLaneProbeMonitor()
