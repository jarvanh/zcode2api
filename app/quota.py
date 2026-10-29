"""ZCode 额度 / 余额 / 用量查询，以及账号状态判定。

在查询基础上提供「额度用完自动标记 exhausted」的监控能力。
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx

from . import constants, logs, settings
from .models import Account, Status
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


def _bonus_active(plan: dict, now: float) -> bool:
    """plan 是否带有已生效的一次性赠送授权（balance 不含这类额度）。

    用于额度耗尽判定：日窗口用完但赠送池有效时，账号仍有真实可用额度。
    """
    for e in (plan or {}).get("entitlements") or []:
        if e.get("period") != "one_time":
            continue
        eff = e.get("effective_at") or 0
        ends = e.get("ends_at") or e.get("expires_at") or 0
        if eff and eff <= now and (not ends or now <= ends):
            return True
    return False


async def fetch_quota(account: Account) -> dict:
    """拉取单个账号的 方案 / 余额 / 用量，写回账号状态并持久化。

    返回结构: {"billing":..., "balance":..., "usage":..., "error":...}
    """
    live = store.find(account.provider, account.id)
    if live is None:
        return {"error": "账号已删除"}
    if live is not account:
        account = live
    if not account.allows_billing() and not account.is_cooling():
        # 与 admin/monitor 同一扇门。冷却账号仍允许查询（测试与兜底约定：
        # 拿额度不得提前解除冷却）；废 JWT / 风控 / 手动停用一律不打 billing。
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

    if billing_res is not None and billing_res.status_code == 200:
        try:
            data = billing_res.json()
            result["billing"] = data
            plans = (data.get("data") or {}).get("plans") or []
            # 全量保留（多套餐时 entitlements 不丢）；plan 兼容保留首个
            account.plans = plans
            account.plan = plans[0] if plans else {}
        except (ValueError, KeyError):
            pass

    quota_map: dict = {}
    if balance_res is not None and balance_res.status_code == 200:
        try:
            data = balance_res.json()
            result["balance"] = data
            for bal in (data.get("data") or {}).get("balances") or []:
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
                    prev["expires_at"] = max(prev.get("expires_at") or 0, window.get("expires_at") or 0)
                    logs.warn("quota", f"balance 同名窗口 {name} 已合并")
                else:
                    quota_map[name] = window
        except (ValueError, KeyError):
            pass

    if usage_res is not None and usage_res.status_code == 200:
        try:
            account.usage = usage_res.json().get("data") or {}
            result["usage"] = account.usage
        except (ValueError, KeyError):
            pass

    if quota_map:
        account.quota = quota_map
        # 额度耗尽判定：所有日窗口剩余 <= 0。注意 balance 不含一次性赠送池——
        # 赠送池有效时不判耗尽，否则账号会被路由跳过而实际仍有 3 亿级可用额度
        now = time.time()
        remainings = [
            q.get("remaining") for q in quota_map.values() if q.get("remaining") is not None
        ]
        daily_exhausted = bool(remainings) and all((r or 0) <= 0 for r in remainings)
        has_bonus = _bonus_active(account.plan, now) or any(
            _bonus_active(p, now) for p in account.plans
        )
        has_daily = bool(remainings) and any((r or 0) > 0 for r in remainings)
        if daily_exhausted and not has_bonus:
            account.status = Status.EXHAUSTED
            account.last_error = "额度已用完"
        elif account.status in (Status.EXHAUSTED, Status.COOLING) and (
            has_daily or has_bonus
        ):
            # 额度恢复（窗口重置 / 赠送池生效）→ 重新激活。冷却期内不提前解除；
            # 废 JWT / 风控禁用绝不能因额度数字复活 Plan 通道。
            if not account.is_cooling():
                account.status = Status.ACTIVE
                account.last_error = None
                account.cooling_until = None

    store.update_account(account)
    return result or {"error": "无法获取额度数据"}


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
    """后台周期性刷新可管理账号的额度，实现实时用量监控。"""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _loop(self) -> None:
        # 启动后先等几秒，避免与服务启动争抢
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=5)
            return
        except TimeoutError:
            pass

        while not self._stop.is_set():
            interval = store.quota_refresh_interval()  # 实时读取设置，改后即生效
            if interval > 0:
                try:
                    accounts = [
                        a for a in store.list_accounts("zai")
                        if a.mode == "jwt" and a.allows_billing()
                    ]
                    if accounts:
                        await refresh_accounts(accounts)
                except Exception as err:  # noqa: BLE001 - 后台任务需吞掉异常继续运行
                    logs.err("quota", f"后台刷新出错: {err}")
            # interval<=0 视为关闭：仍周期性回看设置，便于随时启用
            wait = interval if interval > 0 else 30
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
        from .store import store

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
            url, headers, payload = build_request(acc, body, verify_param, force_plan=True)
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
