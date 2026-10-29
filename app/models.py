"""账号数据模型与状态枚举。"""

from __future__ import annotations

import secrets
import time
from dataclasses import asdict, dataclass, field

from . import settings


def _risk_lane_enabled() -> bool:
    """通道级风控总开关（运行期读，便于测试 monkeypatch / 灰度回退）。"""
    return bool(getattr(settings, "RISK_LANE_SCOPED", False))

PROVIDERS = ("zai", "bigmodel")


class Status:
    """账号运行状态。"""

    ACTIVE = "active"        # 正常，可参与轮询
    EXHAUSTED = "exhausted"  # 额度用完
    COOLING = "cooling"      # 临时限流（冷却中）
    INVALID = "invalid"      # 凭证失效 / 鉴权失败
    DISABLED = "disabled"    # 手动禁用


def _account_id(name: str) -> str:
    safe = "".join(c if c.isalnum() else "-" for c in (name or "account").lower())
    safe = safe.strip("-")[:32] or "account"
    return f"{safe}-{secrets.token_hex(4)}"


@dataclass
class Account:
    """单个可轮询的账号凭证 + 运行时状态。"""

    id: str
    name: str
    provider: str
    mode: str  # "jwt" | "apiKey"
    jwt_token: str | None = None
    api_key: str | None = None
    enabled: bool = True
    status: str = Status.ACTIVE

    # 额度快照：{ model_show_name: {total, used, remaining, expires_at} }
    quota: dict = field(default_factory=dict)
    plan: dict = field(default_factory=dict)        # 当前激活方案（billing/current plans[0]，兼容保留）
    plans: list = field(default_factory=list)       # 全部方案（上游 plans 数组；多套餐时 entitlements 不丢）
    # 按模型粒度的「无资源包」冷却：{model_key: until_epoch}
    # 1113 是「账号 + 模型」组合的属性，不是整号故障；冷却到期后由下一次
    # 请求探测，成功才清除，失败则指数退避延长（资源包补发/续期自动恢复）。
    model_cooling: dict = field(default_factory=dict)
    model_fail_count: dict = field(default_factory=dict)
    usage: dict = field(default_factory=dict)       # 近期用量原始数据

    use_count: int = 0
    fail_count: int = 0
    risk_strikes: int = 0  # 当前风控退避周期内累计命中次数；成功或跨衰减窗口即清零
    last_risk_at: float | None = None  # 最近一次风控命中时间（strikes 衰减窗口基准）
    # ── 通道级风控冷却（2026-10-29 lane-scoped 整改，开关 settings.RISK_LANE_SCOPED）──
    # 依据：zai-org/feedback #716 与自家实测都表明 3012/unusual activity 是
    # 「通道级」封锁（同一账号付费 Coding Plan 腿 0 拦截，只有 zcode-plan
    # grant 腿被拦；同号同刻 billing 全正常）。旧实现按整号计数并置 DISABLED，
    # 连带把健康的回退腿、billing/claim、额度刷新一起杀死。现按 lane 冷却。
    # lane_key = "plan:<model>" | "apikey:***"（见 lane_key）。
    risk_lane_cooling: dict = field(default_factory=dict)   # lane_key -> until_epoch
    risk_lane_strikes: dict = field(default_factory=dict)   # lane_key -> 窗口内命中次数
    risk_lane_last_at: dict = field(default_factory=dict)   # lane_key -> 上次命中 epoch（衰减基准）
    risk_last_lane: str | None = None                       # 最近命中的 lane（日志/后台展示）
    # 设备指纹换发频控（修「禁用→换机→启用→一发即封→再换机」的身份抖动循环）
    rotate_count: int = 0
    last_rotate_at: float | None = None
    rate_strikes: int = 0  # 连续 429 重试梯耗尽次数（自动熔断）；成功即清零
    # ── 连接失败独立计数（2026-10-30，与上游风控分开）──
    # 网络层失败发生在本地/出口链路，请求未达上游、对风控评分零影响，故
    # 不进 risk_strikes/lane 冷却（那会像旧版那样被死节点烧到禁用）；走
    # 独立短退避（见 net_fail_penalty），成功即清零。
    net_strikes: int = 0
    net_last_at: float | None = None  # 最近一次连接失败 epoch（衰减窗口基准）
    recent_results: list = field(default_factory=list)  # 最近请求结果 tick（True 成功/False 失败），最新在末尾
    last_used_at: float | None = None
    last_checked_at: float | None = None
    cooling_until: float | None = None
    last_error: str | None = None
    created_at: float = field(default_factory=time.time)
    # 每账号客户端指纹（fingerprint.DeviceProfile；dataclass 存 dict，取用时还原）
    fingerprint: dict | object | None = None
    # 安装身份：入池分配的稳定安装令牌（hub 内部，跨账号不重复，导出时剥离）
    install_id: str | None = None
    installed_at: float | None = None  # 按账号安装序完成时间；None = 未安装
    # 账号级出口代理：仅作用于 chat 网关上游（每请求新建 client 时注入）。
    # billing/claim/额度/验证码链路刻意直连（见 claim._billing_request 注释），
    # 不得引用。None = 直连；socks5 需 socksio（requirements 已含）。
    proxy_url: str | None = None

    @staticmethod
    def create(provider: str, name: str, secret: str) -> Account:
        secret = (secret or "").strip()
        is_jwt = secret.count(".") == 2 and provider == "zai"
        return Account(
            id=_account_id(name),
            name=name or f"{provider}-account",
            provider=provider,
            mode="jwt" if is_jwt else "apiKey",
            jwt_token=secret if is_jwt else None,
            api_key=None if is_jwt else secret,
        )

    @property
    def secret(self) -> str | None:
        return self.jwt_token if self.mode == "jwt" else self.api_key

    def uid(self) -> str | None:
        """账号的上游真实身份标识（用于区分同名 / 命名混乱的账号）。

        - jwt：解 JWT payload 的 user_id（sub 兜底）
        - apiKey：取 key 的点分前缀（key_id，上游按此识别调用方）

        仅展示用，不参与任何鉴权与调度逻辑。
        """
        if self.mode == "jwt":
            from .body_transform import jwt_user_id  # 局部导入：避免与调用方形成环

            return jwt_user_id(self.jwt_token)
        key = (self.api_key or "").strip()
        if "." in key:
            return key.split(".", 1)[0] or None
        return None

    def risk_penalty(self, base: float, cap: float, ban_strikes: int,
                     decay_seconds: float) -> None:
        """命中风控（3012/405「unusual activity」）：指数退避冷却，累计
        ban_strikes 次才升级为禁用（人工确认恢复）。

        实测（2026-10-01）上游对 messages 端点的 3012 是频道级瞬时频控：
        同号同刻 billing 全部正常、数小时自愈——硬禁用会让健康的 billing/claim
        陪葬（领取轮两度因此停摆）。冷却自动恢复且冷却期零上游流量
        （is_cooling 全通道门禁），既避免对真封禁账号持续施压，也不拖死领取轮。

        strikes 仅在 decay_seconds 窗口内累计：距上次命中超过窗口视为无关的
        新事件，重新起算——避免跨月偶发命中累积成禁用。Plan 通道成功（gateway
        成功分支）或窗口衰减都会清零。
        """
        now = time.time()
        if self.last_risk_at and now - self.last_risk_at > decay_seconds:
            self.risk_strikes = 0
        self.risk_strikes += 1
        self.last_risk_at = now
        if self.risk_strikes >= ban_strikes:
            self.status = Status.DISABLED
            self.cooling_until = None
            return
        self.status = Status.COOLING
        self.cooling_until = now + min(base * (2 ** (self.risk_strikes - 1)), cap)

    def has_apikey_fallback(self) -> bool:
        """同账号是否持有可走 api.z.ai 的 API Key（JWT 死后的对话回退）。

        仅 JWT 账号的附加 Key 算回退；纯 apiKey 账号的主键不是 fallback，
        风控/失效后不得靠这把 Key 继续被选中。
        """
        return self.mode == "jwt" and bool((self.api_key or "").strip())

    def uses_plan_channel(self) -> bool:
        """当前是否允许走 Coding Plan JWT 通道（messages + billing）。

        invalid / 风控 disabled / 手动停用 都视为 JWT 不可用；有 Key 时对话
        走回退通道，但 billing/claim 仍必须停（Key 通道没有套餐领取）。
        """
        if self.mode != "jwt" or not (self.jwt_token or "").strip():
            return False
        if not self.enabled:
            return False
        return self.status not in (Status.INVALID, Status.DISABLED)

    def allows_billing(self, now: float | None = None) -> bool:
        """是否允许打 billing 全家桶（preview/claim/current/balance/usage）。

        通道级风控冷却**不**在此门禁内：#716 与自家实测均证实 3012 期间
        billing 完全正常（有额度但对话腿被拦），冷却对话腿不该顺带禁掉
        领包/刷额度——旧实现靠整号 DISABLED 连带停了自动领包轮与额度刷新
        （领不到新包、后台「账面」停在冻结快照）。仍受整号 COOLING 约束。
        """
        return self.uses_plan_channel() and not self.is_cooling(now)

    # ── 通道级风控（lane-scoped penalty）─────────────────────────────────────
    @staticmethod
    def lane_key(channel: str, model: str) -> str:
        """风控通道键：channel(plan|apikey) + 归一模型名；无模型用 "-" 占位。"""
        ch = (channel or "plan").strip().lower()
        return f"{ch}:{Account.model_key(model) or '-'}"

    def is_lane_cooling(self, lane: str, now: float | None = None) -> bool:
        """该通道是否在风控冷却期。到期自动失效，无需人工。

        总开关 RISK_LANE_SCOPED 关闭时一律视为未封锁：运行期改回旧曲线后，
        存量 lane 记录自动失去效力（不删数据，回开即恢复语义）。
        """
        if not lane or not _risk_lane_enabled():
            return False
        until = float(self.risk_lane_cooling.get(lane) or 0)
        return until > (now or time.time())

    def lane_cooling_until(self, lane: str) -> float:
        return float(self.risk_lane_cooling.get(lane) or 0)

    def next_lane_uncooldown_at(self, channel: str | None = None) -> float | None:
        """本账号最早解除风控冷却的 epoch（channel 为空 = 全部通道）。"""
        now = time.time()
        soonest: float | None = None
        for lane, until in (self.risk_lane_cooling or {}).items():
            if channel and lane.split(":", 1)[0] != channel:
                continue
            u = float(until or 0)
            if u > now and (soonest is None or u < soonest):
                soonest = u
        return soonest

    def risk_lane_penalty(self, lane: str, cooldown: float, cap: float,
                          ban_strikes: int, ban_cooldown: float,
                          decay_seconds: float) -> dict:
        """通道级风控打击：只冷却命中的那条 lane，不动整号 status。

        返回 {lane, strikes, seconds, until, banned, all_blocked}。衰减按 lane
        独立计时（距该 lane 上次命中超 decay_seconds 重新起算）——旧实现按整号
        计数且禁用不清零，一个坏夜晚就能把号永久钉死在 DISABLED。
        """
        now = time.time()
        n = int(self.risk_lane_strikes.get(lane) or 0)
        last = float(self.risk_lane_last_at.get(lane) or 0)
        if last and now - last > decay_seconds:
            n = 0
        n += 1
        self.risk_lane_strikes[lane] = n
        self.risk_lane_last_at[lane] = now
        self.risk_last_lane = lane
        banned = n >= ban_strikes
        if banned:
            seconds = float(ban_cooldown)
        elif cooldown > 0:
            seconds = min(float(cooldown) * (2 ** (n - 1)), float(cap))
        else:
            seconds = float(cap)
        seconds = max(1.0, seconds)
        until = now + seconds
        self.risk_lane_cooling[lane] = until
        # 整号计数保留只做展示/审计（不再驱动状态位；DISABLED 回归「人工停用」
        # 单一语义），并同步刷新衰减窗口基准供后台显示。
        self.risk_strikes = int(self.risk_strikes or 0) + 1
        self.last_risk_at = now
        return {
            "lane": lane,
            "strikes": n,
            "seconds": seconds,
            "until": until,
            "banned": banned,
            "all_blocked": self.lanes_all_blocked(now),
        }

    def net_fail_penalty(self, base: float, cap: float,
                         decay_seconds: float) -> float:
        """连接失败（网络层）独立退避：返回 cooling_until epoch。

        与 risk_lane_penalty 平行但完全独立：只动 net_strikes/net_last_at，
        不碰 risk_strikes/lane 冷却/状态位（置 COOLING 与 cooling_until 由
        调用方负责）。失败发生在本地/出口链路、请求未达上游，零风控代价，
        故起步短（base）、×2 递进只针对持续失联的坏节点（cap 封顶）——节点
        被 watchdog 自愈或瞬时抖动过去后快速回池；衰减窗口内无新失败则
        重新起算。
        """
        now = time.time()
        if self.net_last_at and now - self.net_last_at > decay_seconds:
            self.net_strikes = 0
        self.net_strikes += 1
        self.net_last_at = now
        return now + max(1.0, min(base * (2 ** (self.net_strikes - 1)), cap))

    def lanes_all_blocked(self, now: float | None = None) -> bool:
        """该号全部可用服务通道是否都在风控冷却中。

        只看实际存在的通道：纯 Key 号唯一服务通道是 apikey；JWT 号看 plan，
        有回退 Key 时需两条腿都冷却才算。
        """
        now = now or time.time()
        channels: list[str] = []
        if self.uses_plan_channel():
            channels.append("plan")
        if self.mode == "apiKey" or (self.uses_plan_channel() and self.has_apikey_fallback()):
            channels.append("apikey")
        if not channels:
            return False
        return all(self.channel_blocked(c, now) for c in channels)

    def has_lane_cooling(self, now: float | None = None) -> bool:
        """是否有任何风控 lane 冷却在场（不管哪条通道）。

        供 5xx/连接失败等瞬态处置判断：场上有风控退避时不得用通用冷却覆盖
        last_error，否则运维看到的是「5xx 冷却」而真因（风控 lane）被洗掉。
        """
        now = now or time.time()
        return any(
            float(until or 0) > now
            for until in (self.risk_lane_cooling or {}).values()
        )

    def worst_lane_until(self, now: float | None = None) -> float | None:
        """当前生效中最迟解除的风控冷却 epoch。"""
        now = now or time.time()
        vals = [float(u or 0) for u in (self.risk_lane_cooling or {}).values()
                if float(u or 0) > now]
        return max(vals) if vals else None

    def lane_unblock_at(self, model: str, now: float | None = None) -> float | None:
        """该模型因风控封锁时、最早可重新被选中的 epoch（未封锁返回 None）。

        两条腿都在冷却时要等**较晚**那条解除才能服务（max）；只有一条腿时等
        它自己到期。供 Retry-After 与探测轮选型。
        """
        now = now or time.time()
        if self.mode == "apiKey" and not self.uses_plan_channel():
            # 纯 Key 号：唯一服务通道就是 apikey，封锁看它而非 plan
            if not self.channel_blocked("apikey", now):
                return None
            return max(
                (float(u or 0) for lane, u in (self.risk_lane_cooling or {}).items()
                 if lane.startswith("apikey:") and float(u or 0) > now),
                default=0.0,
            ) or None
        if not self.channel_blocked("plan", now):
            return None
        plan_until = max(
            (float(u or 0) for lane, u in (self.risk_lane_cooling or {}).items()
             if lane.startswith("plan:") and float(u or 0) > now), default=0.0)
        if not self.has_apikey_fallback():
            return plan_until or None
        if not self.channel_blocked("apikey", now):
            return None
        key_until = max(
            (float(u or 0) for lane, u in (self.risk_lane_cooling or {}).items()
             if lane.startswith("apikey:") and float(u or 0) > now), default=0.0)
        return max(plan_until, key_until) or None

    def migrate_legacy_risk_ban(self, ban_cooldown: float, floor_seconds: float,
                                decay_seconds: float) -> bool:
        """旧「风控升级禁用」→ 通道级有界冷却（一次性迁移，返回是否改动）。

        存量库里那批 status=disabled + enabled=True + 「风控封禁…」的号，是被
        旧语义错定为终态的通道级封锁（修 P1/P4/P5）。迁移后：整号回 ACTIVE，
        封锁记在 plan 通道上，冷却到点自动回轮询（不再等人工启用，也避开
        「启用→一发即封→再换机」循环）。

        冷却长度：以 RISK_LANE_BAN_COOLDOWN 从**上次命中**起算，扣去已经过的
        时间，并夹在 [floor_seconds, ban_cooldown] 内——既不拿陈年命中当新伤，
        也不把几分钟前刚被拦的号直接放行。
        """
        if self.status != Status.DISABLED or not self.enabled:
            return False
        if not (self.last_error or "").startswith("风控封禁"):
            return False
        now = time.time()
        last = float(self.last_risk_at or 0) or now
        if now - last > decay_seconds:
            remaining = float(floor_seconds)
        else:
            remaining = float(ban_cooldown) - (now - last)
        remaining = min(max(remaining, floor_seconds), ban_cooldown)
        lane = self.lane_key("plan", "")
        self.risk_lane_cooling[lane] = now + remaining
        self.risk_lane_strikes[lane] = max(1, int(self.risk_strikes or 0))
        self.risk_lane_last_at[lane] = last
        self.risk_last_lane = lane
        self.status = Status.ACTIVE
        self.cooling_until = None
        self.last_error = None
        # 整号计数保留作历史审计（不再驱动状态），不归零——归零会把「这个号
        # 被打过」的信息一并抹掉。
        return True

    def clear_lane_cooling(self, lane: str) -> None:
        """该通道已恢复（真实成功或探测 200）：清冷却与命中计数。"""
        self.risk_lane_cooling.pop(lane, None)
        self.risk_lane_strikes.pop(lane, None)
        self.risk_lane_last_at.pop(lane, None)

    def clear_channel_cooling(self, channel: str) -> None:
        """该通道真实成功：清掉 channel 下全部 lane。

        封锁按通道聚合，解封也按通道聚合；逐模型分别等到期会把一次故障拆成
        N 条惩罚尾巴。
        """
        pref = f"{(channel or 'plan').strip().lower()}:"
        for lane in [l for l in list(self.risk_lane_cooling or {}) if l.startswith(pref)]:
            self.clear_lane_cooling(lane)

    def clear_all_risk_lanes(self) -> None:
        """人工确认恢复：清掉全部通道级风控记录（与清 strikes 同语义）。"""
        self.risk_lane_cooling.clear()
        self.risk_lane_strikes.clear()
        self.risk_lane_last_at.clear()
        self.risk_last_lane = None

    def channel_blocked(self, channel: str, now: float | None = None) -> bool:
        """该通道（plan|apikey）是否有任何 lane 处于风控冷却中。"""
        now = now or time.time()
        pref = f"{(channel or 'plan').strip().lower()}:"
        return any(
            lane.startswith(pref) and float(until or 0) > now
            for lane, until in (self.risk_lane_cooling or {}).items()
        )

    def lane_blocked_for_model(self, model: str, now: float | None = None) -> bool:
        """该模型的服务通道是否全被风控冷却（选号门禁）。

        封锁**按通道聚合**：3012 是通道级封锁（#716：同账号同机器付费 Plan 腿
        0 拦截、zcode-plan grant 腿 100% 拦），与具体模型无关。因此任一
        plan:* lane 在冷却即视为 plan 通道不可用；否则换另一个模型重试会把
        同一封锁拆成多个 lane、逐个重踩一次。
        """
        now = now or time.time()
        if not self.uses_plan_channel():
            # 纯 Key 号：唯一服务通道就是 apikey，封锁看它而非 plan
            return self.mode == "apiKey" and self.channel_blocked("apikey", now)
        if not self.channel_blocked("plan", now):
            return False
        if not self.has_apikey_fallback():
            return True
        return self.channel_blocked("apikey", now)

    def plan_lane_available(self, model: str, now: float | None = None) -> bool:
        """该模型的 Plan 腿是否可用（路由选择的唯一判据）。

        选号能选中≠ Plan 腿可打：该腿可能正在风控冷却而靠 Key 腿服务。
        build_request 与 needs_captcha 共用本判定，避免两处推导不一致。
        无 Key 回退时仍返回 True（不能因封锁丢掉唯一凭证），交给上游定成败。
        """
        if not self.uses_plan_channel():
            return False
        if not self.channel_blocked("plan", now):
            return True
        # plan 被封且无可用回退腿：仍维持 plan（否则本号直接无通道）
        return not self.has_apikey_fallback()

    def record_result(self, ok: bool, detail: str = "", keep: int = 20) -> None:
        """记录单次请求结果明细（后台「最近请求」tick 悬停展示），只保留最近 keep 条。

        条目形如 {"ok": bool, "at": epoch 秒, "detail": 文案}；历史遗留的纯布尔条目
        由前端兼容渲染。
        """
        entry = {"ok": bool(ok), "at": time.time(), "detail": str(detail or ("成功" if ok else "失败"))}
        self.recent_results = (self.recent_results + [entry])[-keep:]

    @staticmethod
    def model_key(model: str) -> str:
        """模型冷却键：统一小写，避免 GLM-5.3-Flash / glm-5.3-flash 分家。"""
        return (model or "").strip().lower()

    def is_model_cooling(self, model: str, now: float | None = None) -> bool:
        """该账号是否因「该模型无资源包」处于冷却期。"""
        key = self.model_key(model)
        if not key:
            return False
        until = float(self.model_cooling.get(key) or 0)
        return until > (now or time.time())

    def cool_model(self, model: str, base: float, cap: float) -> float:
        """记录一次「该模型无额度」并按指数退避冷却，返回截止 epoch。"""
        key = self.model_key(model)
        if not key:
            return 0.0
        n = int(self.model_fail_count.get(key) or 0) + 1
        self.model_fail_count[key] = n
        seconds = min(float(base) * (2 ** (n - 1)), float(cap)) if base > 0 else float(cap)
        until = time.time() + max(1.0, seconds)
        self.model_cooling[key] = until
        return until

    def clear_model_cooling(self, model: str) -> None:
        """探测成功：清除该模型的冷却与失败计数。"""
        key = self.model_key(model)
        if not key:
            return
        self.model_cooling.pop(key, None)
        self.model_fail_count.pop(key, None)

    def clear_all_model_cooling(self) -> None:
        """清空全部模型级冷却与失败计数（领取成功 = 资源状态整体刷新）。"""
        self.model_cooling.clear()
        self.model_fail_count.clear()

    def is_selectable(self, now: float | None = None, model: str | None = None) -> bool:
        """是否可被轮询选中。

        JWT 失效 / 风控禁用后，若同账号有 API Key，仍可选中并走回退通道；
        手动 enabled=False 永远不选。model 非空时同时检查该模型的冷却。
        """
        if not self.enabled:
            return False
        if model and self.is_model_cooling(model, now):
            return False
        if model and self.lane_blocked_for_model(model, now):
            # 通道级风控：该模型的服务通道全在冷却中 → 跳过该号（其余模型、其余
            # 账号仍可用；不再因一条 grant 腿被拦而整号退出轮询）
            return False
        if self.status == Status.EXHAUSTED:
            return False
        if self.status in (Status.INVALID, Status.DISABLED):
            return self.has_apikey_fallback()
        if self.status == Status.COOLING:
            now = now or time.time()
            return bool(self.cooling_until and now >= self.cooling_until)
        return True

    def is_cooling(self, now: float | None = None) -> bool:
        """冷却是否仍在生效（含风控指数退避）。冷却期内不应产生任何上游流量。"""
        if self.status != Status.COOLING:
            return False
        now = now or time.time()
        return bool(self.cooling_until and now < self.cooling_until)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> Account:
        known = {f for f in Account.__dataclass_fields__}  # type: ignore[attr-defined]
        return Account(**{k: v for k, v in data.items() if k in known})

    @staticmethod
    def normalize_proxy(url: str | None) -> str | None:
        """规整账号级代理 URL（后台写入口校验）：空白 → None，非法协议抛 ValueError。"""
        text = (url or "").strip()
        if not text:
            return None
        scheme = text.split("://", 1)[0].lower()
        if scheme not in ("http", "https", "socks5", "socks5h"):
            raise ValueError(f"不支持的代理协议: {scheme or '(缺协议)'}（支持 http/https/socks5）")
        return text

    def proxy_kwargs(self) -> dict:
        """httpx.AsyncClient 出口代理参数；未配置/非法配置均返回 {}（直连兜底）。

        写入口已校验协议，此处仅防御历史脏数据，绝不抛异常（网关热路径）。
        """
        text = (self.proxy_url or "").strip()
        if not text:
            return {}
        scheme = text.split("://", 1)[0].lower()
        if scheme in ("http", "https", "socks5", "socks5h"):
            return {"proxy": text}
        return {}

    def proxy_view(self) -> str | None:
        """代理的对外形态（前端展示）：剥离 userinfo 凭据（user:pass@ → user:***@）。"""
        text = (self.proxy_url or "").strip()
        if not text:
            return None
        if "://" in text:
            scheme, rest = text.split("://", 1)
        else:
            scheme, rest = "", text
        if "@" in rest:
            cred, _, host = rest.rpartition("@")
            user = cred.split(":", 1)[0]
            prefix = f"{scheme}://" if scheme else ""
            return f"{prefix}{user}:***@{host}"
        return text

    def fingerprint_view(self) -> dict | None:
        """指纹的对外形态（dict）；未分配/半初始化返回 None。"""
        from .fingerprint import DeviceProfile

        fp = self.fingerprint
        if isinstance(fp, DeviceProfile):
            return {
                "platform": fp.platform, "arch": fp.arch,
                "os_version": fp.os_version, "language": fp.language,
                "timezone": fp.timezone, "screen": fp.screen,
                "device_mid": fp.device_mid,
            }
        if isinstance(fp, dict) and fp.get("device_mid"):
            return fp
        return None

    def public_view(self) -> dict:
        """返回给前端的视图（脱敏 token）。"""
        secret = self.secret or ""
        masked = secret if len(secret) <= 16 else f"{secret[:8]}…{secret[-6:]}"
        return {
            "id": self.id,
            "name": self.name,
            "uid": self.uid(),
            "provider": self.provider,
            "mode": self.mode,
            "token_masked": masked,
            "proxy": self.proxy_view(),
            "enabled": self.enabled,
            "status": self.effective_status(),
            "quota": self.quota,
            "plan": self.plan,
            "plans": self.plans,
            "model_cooling": self.model_cooling,
            "model_fail_count": self.model_fail_count,
            "use_count": self.use_count,
            "fail_count": self.fail_count,
            "risk_strikes": self.risk_strikes,
            "rate_strikes": self.rate_strikes,
            "net_strikes": self.net_strikes,
            "net_last_at": self.net_last_at,
            "recent_results": self.recent_results,
            "last_used_at": self.last_used_at,
            "last_checked_at": self.last_checked_at,
            "cooling_until": self.cooling_until,
            "last_error": self.last_error,
            "created_at": self.created_at,
            "fingerprint": self.fingerprint_view(),
            "install_id": self.install_id,
            "installed_at": self.installed_at,
        }

    def effective_status(self, now: float | None = None) -> str:
        """考虑冷却到期后的实时状态。"""
        if self.status == Status.COOLING:
            now = now or time.time()
            if self.cooling_until and now >= self.cooling_until:
                return Status.ACTIVE
        return self.status
