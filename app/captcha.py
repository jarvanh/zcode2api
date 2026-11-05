"""验证码求解 + 预解 token 池。

主求解器 solver_pw.js：真浏览器（puppeteer-core + 系统 Chromium）运行阿里云
官方无痕 SDK 求 verifyParam（X-Aliyun-Captcha-Verify-Param）——阿里云无痕
验证信任真实浏览器环境，happy-dom 伪造路线 2026-09 起被风控「unusual
activity」全拒（solver.js 仅作 ZCODE_CAPTCHA_SOLVER=legacy 回滚）。

架构对齐 zapi captcha.ts 预解池 + captcha-pool.ts 韧性机制：
- 热路径永不等待：请求到来时直接从池里取一枚已解好的 token（亚毫秒），
  后台任务持续补充库存（目标 min，上限 max）。
- token 时效：verifyParam 实际 TTL ~2 分钟，池内按 FIFO + 年龄淘汰，
  超过 token_ttl 的直接丢弃重解。
- certifyId 去重：已签发登记表 + 池内查重，杜绝同一 certifyId 二次
  消费（上游 F008 拒绝），对齐 zapi CertifyIdRegistry。
- 池空竞速：并行 EMPTY_TAKE_RACE 路求解，首个成功即回、其余入库，
  总死线 RACE_DEADLINE；无胜者再给后台补货 TAKE_GRACE 秒宽限
  （铸码失败成簇出现，宽限把多数 500 变成慢成功），对齐 zapi
  solveRaced + take-grace。
- 挑战失效：上游返回挑战时 invalidate() 清空整池（该批指纹可能已被
  风控盯上，继续复用只会连环 3007）。
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import signal
import time

import httpx

from . import captcha_ledger, constants, egress, logs, settings, vitals
from .store import store

# 池参数：真浏览器求解重（10–40s/枚），默认 min1/max2 见 settings 注释
POOL_MIN = settings.CAPTCHA_POOL_MIN
POOL_MAX = settings.CAPTCHA_POOL_MAX
TOKEN_TTL_MS = settings.CAPTCHA_TOKEN_TTL  # 单枚 token 的最大可用时长（ms）


def _now_ms() -> float:
    """墙上时间（ms）。配置缓存用，不能换成 monotonic（重启后要与 TTL 比对）。"""
    return time.time() * 1000


class CaptchaSolveError(Exception):
    """验证码求解最终失败（重试耗尽/求解器不可用）。

    独立于 ClaimError 体系（验证码先于领域层使用），路由层按兜底回执处理。
    """


def _proc_children() -> dict[int, list[int]]:
    """读 /proc 建立 ppid -> [pid] 映射（一次遍历，供进程树递归使用）。"""
    tree: dict[int, list[int]] = {}
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat", "rb") as fh:
                    data = fh.read()
            except OSError:
                continue
            # comm 字段可能含空格/括号，stat 末尾 "...) PPID ..." 才是可靠切分点
            rp = data.rfind(b")")
            if rp < 0:
                continue
            fields = data[rp + 2:].split()
            if len(fields) < 2:
                continue
            try:
                ppid = int(fields[1])
            except ValueError:
                continue
            tree.setdefault(ppid, []).append(int(entry))
    except OSError:
        pass
    return tree


def _collect_descendants(root: int, tree: dict[int, list[int]]) -> list[int]:
    """按 ppid 递归收集 root 的全部后代（深度优先）。

    ⚠️ 关键：Chromium 启动时会自行 setsid()，脱离 solver 的进程组 ——
    实测 solver pgid=732645 / chrome pgid=732741，两者不同组，因此
    killpg(solver组) 根本打不到浏览器。但 chrome 的 ppid 仍是 solver，
    按 /proc 遍历 ppid 树才能抓住它。这是 2026-10-06 孤儿泄漏的根因。
    """
    out: list[int] = []
    stack = list(tree.get(root, []))
    seen = {root}
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
        stack.extend(tree.get(pid, []))
    return out


def _kill_solver_tree(proc: asyncio.subprocess.Process) -> None:
    """强杀 solver 及其整棵子进程树（真 Chromium）。

    solver 用 start_new_session=True 起在独立进程组里，这里直接向整个组发
    SIGKILL，确保 puppeteer 拉起的浏览器不会在父进程死后被 init/systemd
    收养成永生孤儿。只杀 proc 本身会让 Chromium 遗留：孤儿累积到几十个即
    可吃光内存与 swap（2026-10-05 实测 92 个 / 27.6GB），最终新请求连
    Chromium 都起不动，Cloudflare 侧表现为 524。

    2026-10-06 强化：Chromium 自行 setsid() 逃出进程组，killpg 打不到它，
    故先按 /proc 递归收割全部后代（含 setsid 的 chrome 与其 crashpad），
    再 killpg / kill 兜底。三层递进，任一层失败都不影响下一层。
    """
    root = proc.pid
    victims: list[int] = []
    try:
        victims = _collect_descendants(root, _proc_children())
    except Exception:
        victims = []

    # 1) 优先收割浏览器等子进程：先 TERM 给它一次优雅退出的机会
    for pid in victims:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    # 2) 进程组收割（覆盖同组但未出现在 ppid 树里的进程）
    try:
        os.killpg(os.getpgid(root), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass

    # 3) 兜底：对仍在的后代补 SIGKILL（setsid 逃逸者只能靠这一步）
    for pid in victims:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    # 4) 最后杀 solver 自身
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass


def _stderr_tail(stderr: bytes | None, limit: int = 300) -> str | None:
    """取 solver stderr 末段作失败诊断（pe 失速/guest 错误摘要都在这里）。"""
    if not stderr:
        return None
    text = stderr.decode("utf-8", "ignore").strip()
    if not text:
        return None
    tail = text[-limit:]
    return tail.replace("\n", " | ")


def _cool_all_accounts(seconds: int, reason: str, persist: bool = True) -> int:
    """让全部可走 Plan 通道的账号同步冷却（验证码风暴时减少对上游的施压）。

    只动 active 账号（exhausted/invalid/disabled 保持原状态，冷却不洗掉
    已判定的语义）；冷却到期由 is_selectable 的 cooling_until 逻辑自动恢复。
    persist=False 仅改内存对象不落库（单测替身用），防止测试账号写进真库。
    返回被冷却的账号数。
    """
    from .models import Status

    n = 0
    for acc in store.list_accounts("zai"):
        if acc.status == Status.ACTIVE and acc.enabled:
            acc.status = Status.COOLING
            acc.cooling_until = time.time() + seconds
            acc.last_error = reason
            if persist:
                store.update_account(acc)
            n += 1
    return n


def _parse_certify_id(param: str) -> str | None:
    """从 base64-JSON verifyParam 解出 certifyId（对齐 zapi parseCertifyId）。"""
    try:
        decoded = json.loads(base64.b64decode(param).decode("utf-8"))
    except Exception:  # noqa: BLE001 - 非 base64/JSON 的残缺参数按无 ID 处理
        return None
    if not isinstance(decoded, dict):
        return None
    cid = decoded.get("certifyId")
    return cid if isinstance(cid, str) and cid else None


class _Token:
    __slots__ = ("param", "region", "born_at", "certify_id", "egress")

    def __init__(self, param: str, region: str | None, egress_tag: str = egress.DIRECT) -> None:
        self.param = param
        self.region = region
        self.born_at = time.monotonic()
        self.certify_id = _parse_certify_id(param)
        self.egress = egress_tag

    def expired(self) -> bool:
        return (time.monotonic() - self.born_at) * 1000 >= TOKEN_TTL_MS


class CaptchaManager:
    def __init__(self) -> None:
        # 按出口分池：token 只能用在签发它的那个出口上，混池会让请求与
        # token 的 IP 对不上，上游判 unusual activity（见 app/egress.py）。
        self._pools: dict[str, asyncio.Queue[_Token]] = {}
        self._pool_sizes: dict[str, int] = {}
        self._refill_task: asyncio.Task | None = None
        self._refilling = False
        self._config_lock = asyncio.Lock()
        # 配置按出口缓存：上游按出口 IP 给不同的 captcha 配置（实测直连 region=cn、
        # 走 US 代理 region=sgp），混用会把错地区的 token 配到请求上。
        self._config_cache: dict[str, dict] = {}
        self._config_cache_at: dict[str, float] = {}
        self._last_error: str | None = None
        self._last_solver_stderr: str | None = None
        # 已签发 certifyId → 签发时刻（monotonic s），TTL 同 token：过期后上游
        # 也不可能再接受该 ID，登记即失效
        self._issued: dict[str, float] = {}
        # 铸码失败风暴检测（对齐 zapi maybeFireMintStormReset 的信号语义）：
        # 滑动窗口记录成功/失败时刻。连续多枚铸码失败且零成功 ≈ 出口 IP 被阿里
        # 云风控盯上（"too many captcha requests"族）——此时继续高频求解只会
        # 加剧风控，应让账号提前冷却，等 IP 侧解除后再恢复。
        self._mint_failures: list[float] = []
        self._mint_successes: list[float] = []
        self._storm_cool_until: float = 0.0   # 风暴冷却截止（monotonic s）
        self._last_storm_at: float = 0.0      # 上次触发时刻（去重窗口）
        # 闲置停解（对齐 zapi deep-idle）：最后一次取用时刻（monotonic s）。
        # 0 = 从未取用（视为闲置，后台不预热——起服后第一个请求才唤醒）。
        self._last_take_at: float = 0.0
        # 热路径触发的补货任务强引用（事件循环只持弱引用，裸 create_task 会被 GC）
        self._bg_tasks: set[asyncio.Task] = set()
        # 风控波次助推（2026-11-01，B 档）：波次暂停确认时由 gateway 调
        # notify_wave_pause——暂停到期前后正是试探/恢复期，验证码消耗最集中，
        # 预热目标临时抬升 CAPTCHA_WAVE_BOOST，闲置停解同时被压制（波次是
        # 显式信号，优先级高于 deep-idle）。到期自动回落，无需显式清除。
        self._wave_boost_until = 0.0

    # ── 出口 ─────────────────────────────────────────────────────────────────
    def hot_egress(self) -> str:
        """预热哪个出口的池：默认跟随 messages 上游的实际出口。

        只有 messages 是热路径（每次请求都要验证码）；claim 一小时一轮，
        按需现解即可，不必占第二个池的内存。
        """
        forced = settings.CAPTCHA_POOL_EGRESS
        return forced or egress.egress_for(settings.UPSTREAM["zai"])

    def _pool_for(self, tag: str) -> asyncio.Queue[_Token]:
        pool = self._pools.get(tag)
        if pool is None:
            pool = asyncio.Queue(maxsize=POOL_MAX)
            self._pools[tag] = pool
            self._pool_sizes[tag] = 0
        return pool

    def _size(self, tag: str) -> int:
        return self._pool_sizes.get(tag, 0)

    # ── 配置 ─────────────────────────────────────────────────────────────────
    async def fetch_config(self, egress_tag: str | None = None) -> dict:
        """取 captcha 配置（按出口缓存）。`egress_tag` 决定拉取走哪条出口。"""
        tag = egress_tag or self.hot_egress()
        ttl = settings.CAPTCHA_CONFIG_CACHE_TTL
        if self._config_cache.get(tag) and _now_ms() - self._config_cache_at.get(tag, 0) < ttl:
            return self._config_cache[tag]
        async with self._config_lock:
            # 双检：等锁期间可能已被其他请求填充
            if self._config_cache.get(tag) and _now_ms() - self._config_cache_at.get(tag, 0) < ttl:
                return self._config_cache[tag]
            err: Exception | None = None

            async def _try_fetch(use_proxy: bool) -> dict | None:
                """单次拉取（trust_env 按 use_proxy）；成功返回 captcha 配置。"""
                try:
                    async with httpx.AsyncClient(timeout=30, trust_env=use_proxy) as client:
                        res = await client.get(
                            f"{constants.CLIENT_CONFIGS_URL}?{constants.CLIENT_CONFIGS_QUERY}"
                        )
                    res.raise_for_status()
                    return ((res.json().get("data") or {}).get("configs") or {}).get("captcha")
                except (httpx.HTTPError, ValueError) as exc:
                    nonlocal err
                    err = exc
                    return None

            captcha: dict | None = None
            for _ in range(2):  # 15s 走 US 代理偶发 ReadTimeout，同出口重试一次
                captcha = await _try_fetch(tag == egress.PROXY)
                if captcha:
                    break
            if not captcha:
                # 主出口两连败 → 另一条出口兜底（2026-11-04 线上实锤：proxy 整晚
                # 超时而 direct 稳定；配置拿不到会误开「模型请求带码」→ 3012）
                alt = egress.DIRECT if tag == egress.PROXY else egress.PROXY
                captcha = await _try_fetch(alt == egress.PROXY)
                if captcha:
                    logs.info("captcha", f"出口 {tag} 拉取失败，已用 {alt} 兜底")
            if captcha:
                self._config_cache[tag] = captcha
                self._config_cache_at[tag] = _now_ms()
                return captcha
            logs.warn(
                "captcha",
                f"获取配置失败（出口={tag}，含兜底），使用默认: "
                f"{type(err).__name__ if err else '无响应'}: {err}",
            )
            return dict(constants.CAPTCHA_DEFAULTS)

    async def model_requests_need_captcha(self, egress_tag: str | None = None) -> bool:
        """模型请求是否该带验证码——官方 `requiresRequestVerification` 的等价物。

        bundle 原文：`mode==="start-plan" && enabled!==false && skipModelRequest!==true`。
        线上配置 `skip_model_request:true` ⇒ 官方客户端在模型请求上**不带**
        `X-Aliyun-Captcha-Verify-Param`；hub 长期无条件带，这正是「官方不会干的
        事」，也是 3012 的结构性嫌疑。无配置时官方仍要求验证码，故默认 False。
        """
        config = await self.fetch_config(egress_tag)
        return config.get("enabled") is not False and config.get("skip_model_request") is not True

    # ── 预解池 ───────────────────────────────────────────────────────────────
    def start(self) -> None:
        """启动后台补充循环（main.py lifespan 调用）。"""
        if self._refill_task is None or self._refill_task.done():
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def close(self) -> None:
        if self._refill_task and not self._refill_task.done():
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass
        self._refill_task = None

    def _gate_open(self) -> bool:
        """是否允许**预热**：存在可走 Plan 对话的 jwt 账号才预热。

        预热本身就是上游流量，因此只跟着「能服务对话」的号走：apiKey 回退 /
        废 JWT / 风控禁用 / 额度用完 / 冷却 都不预热，重启后全冷却期间保持关门。
        注意这只管预热——领取（billing 链）不受此门约束：冷却/停放号要领取时，
        `get_verify_param` 池空会现解（不依赖预热池，见 2026-11-04 两条链口径）。
        """
        return any(
            a.allows_billing() and a.is_selectable() and not a.channel_blocked("plan")
            for a in store.list_accounts("zai")
        )

    # ── 铸码失败风暴检测 ─────────────────────────────────────────────────────
    def _prune_mint_windows(self, now: float) -> None:
        f_cut, s_cut = now - 300.0, now - 180.0
        self._mint_failures = [t for t in self._mint_failures if t > f_cut]
        self._mint_successes = [t for t in self._mint_successes if t > s_cut]

    def _note_mint_failure(self) -> None:
        """solver 一轮完整重试（_solve_one）失败记账，并检测失败风暴。"""
        now = time.monotonic()
        self._mint_failures.append(now)
        self._prune_mint_windows(now)
        self._maybe_fire_storm(now)

    def _note_mint_success(self) -> None:
        now = time.monotonic()
        self._mint_successes.append(now)
        self._prune_mint_windows(now)
        if self._storm_cool_until and now >= self._storm_cool_until:
            # 冷却到期后的首次成功：IP 侧已解除，出口恢复
            self._storm_cool_until = 0.0
            logs.ok("captcha", "铸码风暴冷却解除（恢复成功），恢复常规求解节奏")

    def _maybe_fire_storm(self, now: float) -> None:
        """风暴判定（对齐 zapi）：5 分钟内 ≥8 次铸码失败且 3 分钟内零成功。

        触发动作：让全部可服务账号进入同步冷却 STORM_COOL_SECONDS（长于上游
        风控窗口的经验值），期间补货门关闭、热路径改走宽限等待，不再对被
        盯上的出口 IP 施压。12 分钟去重窗口防止连续触发。
        """
        if now - self._last_storm_at < settings.CAPTCHA_STORM_DEDUPE:
            return
        fails = len([t for t in self._mint_failures if t > now - 300.0])
        succs = len([t for t in self._mint_successes if t > now - 180.0])
        if fails < settings.CAPTCHA_STORM_THRESHOLD or succs > 0:
            return
        self._last_storm_at = now
        self._storm_cool_until = now + settings.CAPTCHA_STORM_COOL
        cool_min = settings.CAPTCHA_STORM_COOL // 60
        logs.warn(
            "captcha",
            f"铸码失败风暴：{fails} 次失败/5min 且 0 成功/3min，疑似出口 IP 被阿里云风控；"
            f"全部账号同步冷却 {cool_min} 分钟（停止对被盯上的出口施压，到期自动恢复）",
        )
        _cool_all_accounts(cool_min * 60, reason=f"验证码风暴：铸码连续失败 {fails} 次")

    def _storm_gate_closed(self) -> bool:
        """风暴冷却期间禁止主动铸码（热路径仍可消费池内余量/宽限等待）。"""
        return bool(self._storm_cool_until and time.monotonic() < self._storm_cool_until)

    # ── 闲置停解（对齐 zapi deep-idle）───────────────────────────────────────
    def _idle(self, now: float | None = None) -> bool:
        """零流量时段判定：无取用超过 CAPTCHA_IDLE_AFTER 秒即闲置。

        从未取用（_last_take_at=0）按闲置算——起服后不为第一个可能永远不来
        的请求空烧真浏览器。
        """
        if self._wave_boost_active():
            return False  # 波次助推期：显式信号压过 deep-idle（试探/恢复期消耗最集中）
        if settings.CAPTCHA_IDLE_AFTER <= 0:
            return False
        now = time.monotonic() if now is None else now
        return self._last_take_at <= 0 or (now - self._last_take_at) >= settings.CAPTCHA_IDLE_AFTER

    def _wave_boost_active(self) -> bool:
        return time.monotonic() < self._wave_boost_until

    def _pool_target(self) -> int:
        """当前预热目标：POOL_MIN + 波次助推加成（助推期外回落常规水位）。"""
        if self._wave_boost_active():
            return POOL_MIN + max(0, settings.CAPTCHA_WAVE_BOOST)
        return POOL_MIN

    def notify_wave_pause(self, seconds: float) -> None:
        """波次暂停确认 → 临时抬升预热目标并压制闲置停解（gateway 波次点调用）。

        到期自动回落（monotonic 比较），无需显式清除；重复确认取更晚到期。
        """
        until = time.monotonic() + max(0.0, float(seconds))
        if until > self._wave_boost_until:
            self._wave_boost_until = until
        logs.info("captcha", f"波次助推 {max(0, int(seconds))}s：预热目标 "
                             f"{self._pool_target()}（常规 {POOL_MIN}），闲置停解暂时压制")

    async def _refill_loop(self) -> None:
        while True:
            try:
                tag = self.hot_egress()
                # 风暴冷却期间停止主动铸码（不产生上游流量，等 IP 侧解除）
                if self._storm_gate_closed():
                    await self._evict_expired(tag)
                    await asyncio.sleep(5)
                    continue
                # 闲置停解：零流量时段只淘汰过期 token（让池自然蒸发清空），
                # 不铸新码；任意取用即恢复（get_verify_param 里打点）
                if self._idle():
                    await self._evict_expired(tag)
                    await asyncio.sleep(3)
                    continue
                # 无可服务账号（全冷却/禁用/无号）：只淘汰过期 token，不解新码（不产生上游流量）
                if not self._gate_open():
                    await self._evict_expired(tag)
                    await asyncio.sleep(3)
                    continue
                # 上游对模型请求免验证码（skip_model_request）时池无人取用，
                # 继续预热只是白烧阿里云求解流量与 chrome 内存
                if not await self.model_requests_need_captcha(tag):
                    await self._evict_expired(tag)
                    await asyncio.sleep(30)
                    continue
                need = self._pool_target() - self._size(tag)
                if need > 0:
                    await self._refill_batch(need, tag)
                else:
                    await self._evict_expired(tag)
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - 后台循环永不退出
                self._last_error = str(err)
                logs.warn("captcha", f"补充循环异常: {err}")
                await asyncio.sleep(5)

    async def _refill_batch(self, need: int, tag: str | None = None) -> None:
        """串行补充（求解有 CPU 开销，避免并发爆 Node 进程）。

        闲置（无取用超阈值/从未取用）直接返回——门禁在此兜底而不仅靠
        _refill_loop 的分支顺序，热路径误触发的补货同样被拦。
        """
        if self._idle() or self._refilling:
            return
        self._refilling = True
        tag = tag or self.hot_egress()
        try:
            config = await self.fetch_config(tag)
            for _ in range(need):
                if self._size(tag) >= POOL_MAX:
                    break
                token = await self._solve_one(config, tag)
                if token is None:
                    break
                self._put(token, tag)
        finally:
            self._refilling = False

    # ── certifyId 登记（防 F008 重复消费）────────────────────────────────────
    def _prune_issued(self) -> None:
        cutoff = time.monotonic() - TOKEN_TTL_MS / 1000
        for cid, at in list(self._issued.items()):
            if at < cutoff:
                del self._issued[cid]

    def _is_issued(self, certify_id: str) -> bool:
        self._prune_issued()
        return certify_id in self._issued

    def _mark_issued(self, token: _Token) -> None:
        self._prune_issued()
        if token.certify_id:
            self._issued[token.certify_id] = time.monotonic()

    def _pool_has_certify_id(self, certify_id: str) -> bool:
        return any(
            t.certify_id == certify_id
            for pool in list(self._pools.values())
            for t in list(pool._queue)
        )

    def _put(self, token: _Token, tag: str | None = None) -> None:
        # 已签发（=已消费，_pop_fresh 出口登记）/池内已有的 certifyId 不再
        # 入库（对齐 zapi pushToken：只查重、不登记——登记必须发生在真正
        # 交给客户端的出口，否则池内 token 会被自己的账本拦成不可用）。
        # 查重跨出口：同一 certifyId 在任一出池内都算重复，上游只认证 ID。
        if token.certify_id and (self._is_issued(token.certify_id) or self._pool_has_certify_id(token.certify_id)):
            return
        tag = tag or token.egress
        try:
            self._pool_for(tag).put_nowait(token)
            self._pool_sizes[tag] = self._size(tag) + 1
        except asyncio.QueueFull:
            pass

    def _pop_fresh(self, tag: str | None = None) -> _Token | None:
        """取一枚可用 token（指定出口池）：跳过过期、跨出口与已签发（F008 风险），取出即登记。"""
        tag = tag or self.hot_egress()
        pool = self._pool_for(tag)
        while self._size(tag) > 0:
            try:
                token = pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pool_sizes[tag] = max(0, self._size(tag) - 1)
            if token.expired() or token.egress != tag:
                # 出口不符的 token 不能将就用（请求 IP 与 token 不一致 → 3012）
                continue
            if token.certify_id and self._is_issued(token.certify_id):
                continue
            self._mark_issued(token)
            return token
        return None

    def _trigger_refill(self, need: int, tag: str | None = None) -> None:
        # fire-and-forget；_refilling 防重入，强引用防 GC
        task = asyncio.create_task(self._refill_batch(need, tag))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _evict_expired(self, tag: str | None = None) -> None:
        tags = [tag] if tag else list(self._pools)
        for one in tags:
            pool = self._pools.get(one)
            if pool is None:
                continue
            kept: list[_Token] = []
            while True:
                try:
                    token = pool.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._pool_sizes[one] = max(0, self._size(one) - 1)
                if not token.expired() and len(kept) < POOL_MAX:
                    kept.append(token)
            for token in kept:
                self._put(token, one)

    async def get_verify_param(self, egress_tag: str | None = None) -> tuple[str, str | None]:
        """取一枚可用 token：池内直取 → 池空竞速现解 → 宽限窗口等补货。

        `egress_tag` 必须是本次请求真实使用的出口（`proxy` / `direct`），
        否则拿到的 token 与请求 IP 不一致，上游会判 unusual activity。
        只有热路径出口有池；其它出口现解。

        返回 (verify_param, region)。region 可为 None（旧求解器无 region 概念）。
        取用即打点 _last_take_at——闲置停解由任意请求唤醒（对齐 zapi wake）。
        """
        self._last_take_at = time.monotonic()
        tag = egress_tag or self.hot_egress()
        pooled = tag == self.hot_egress()

        # 1) 池内直取（仅热路径出口有池；亚毫秒，取出即触发后台补货）
        if pooled:
            token = self._pop_fresh(tag)
            if token is not None:
                self._trigger_refill(1, tag)
                return token.param, token.region

        # 2) 池空/全过期/非热路径出口：竞速现解（对齐 zapi takeToken：
        #    空池到达本身即扩容信号）
        config = await self.fetch_config(tag)
        token = await self._solve_raced(config, tag)
        if token is not None:
            self._mark_issued(token)
            self._trigger_refill(1, tag)
            return token.param, token.region

        # 3) 竞速无果：宽限窗口内等后台落库（竞速败者/超时者的迟到成功也会入库；
        #    全冷却期间补货门关闭则直接放弃，不空等）
        if self._gate_open():
            grace_deadline = time.monotonic() + settings.CAPTCHA_TAKE_GRACE
            while time.monotonic() < grace_deadline:
                await asyncio.sleep(0.4)
                token = self._pop_fresh(tag)
                if token is not None:
                    self._trigger_refill(1, tag)
                    return token.param, token.region
        raise CaptchaSolveError(f"验证码求解失败: {self._last_error or '多次重试无结果'}")

    def try_take_now(self, egress_tag: str | None = None) -> tuple[str, str | None] | None:
        """热路径同步取码（C 档 CAPTCHA_ATTACH_PLAN 用）：仅池内直取，绝不现解。

        与 get_verify_param 的差别：池空返回 None（请求按无验证码形态发出，
        fail-open），绝不阻塞热路径去起浏览器；取用同样打点 _last_take_at
        （C 档开启时每请求都是流量信号，池水位由后台循环维持）。
        """
        self._last_take_at = time.monotonic()
        token = self._pop_fresh(egress_tag or self.hot_egress())
        if token is None:
            return None
        self._trigger_refill(1, token.egress)
        return token.param, token.region

    # ── 求解 ─────────────────────────────────────────────────────────────────
    async def _solve_raced(self, config: dict, tag: str | None = None) -> _Token | None:
        """空池竞速：并行 EMPTY_TAKE_RACE 路求解，首个成功即回、其余入库。

        全部失败或总死线 RACE_DEADLINE 内无胜者 → None（错误已记 _last_error）。
        超时后竞速任务继续在后台跑（_bg_tasks 持引用）：竞速内部对「服务」
        与「入库」区分对待——死线超时返回 None 后，迟到成功不再有 served
        窗口，全部经 _put 入库（等价 zapi 背景波收尾，也是闲置唤醒后的
        冷启动路径：请求超时离开，token 落池给下一次请求亚毫秒直取）。
        """
        racers = max(1, settings.CAPTCHA_EMPTY_TAKE_RACE)
        loop = asyncio.get_running_loop()
        win: asyncio.Future[_Token | None] = loop.create_future()
        served = False
        failed = 0

        async def racer() -> None:
            nonlocal served, failed
            try:
                token = await self._solve_one(config, tag)
            except Exception as err:  # noqa: BLE001 - 单路失败不拖垮竞速
                self._last_error = str(err)
                token = None
            if token is None:
                failed += 1
                if failed >= racers and not win.done():
                    win.set_result(None)
                return
            if served or win.done():
                # 已有胜者，或调用方已按死线超时离开：铸出有效 token 一律
                # 入库（此前 win.done() 分支会把迟到成功整个丢弃）
                self._put(token, tag)
                return
            served = True  # 事件循环单线程，查改之间无 await，此判原子
            win.set_result(token)

        for _ in range(racers):
            task = asyncio.create_task(racer())
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        try:
            return await asyncio.wait_for(win, timeout=settings.CAPTCHA_RACE_DEADLINE)
        except TimeoutError:
            self._last_error = (
                f"竞速 {settings.CAPTCHA_RACE_DEADLINE}s 无胜者"
                f" (最后诊断: {self._last_solver_stderr or self._last_error or '无'})"
            )
            return None

    async def _solve_one(self, config: dict, tag: str = egress.DIRECT) -> _Token | None:
        scene = config.get("sceneId") or constants.CAPTCHA_DEFAULTS["sceneId"]
        region = config.get("region") or constants.CAPTCHA_DEFAULTS["region"]
        prefix = config.get("prefix") or constants.CAPTCHA_DEFAULTS["prefix"]

        last_err: str | None = None
        for attempt in range(1, settings.CAPTCHA_SOLVE_RETRIES + 1):
            try:
                param = await self._run_solver(scene, region, prefix, tag)
            except Exception as err:  # noqa: BLE001
                last_err = str(err)
                param = None
            if param:
                certify_id = _parse_certify_id(param)
                if certify_id and (self._is_issued(certify_id) or self._pool_has_certify_id(certify_id)):
                    # 重复 certifyId 上游必 F008：弃用重解，不当有效结果放行
                    last_err = f"重复 certifyId {certify_id}（F008 风险），弃用重解"
                    self._last_error = last_err
                    logs.warn("captcha", last_err)
                    continue
                if attempt > 1:
                    logs.ok("captcha", f"求解成功（第 {attempt} 次尝试，出口={tag}）")
                self._note_mint_success()
                return _Token(param, region, tag)
            if last_err is None:
                # 求解器退出但无 param：用 stderr 末段代替沉默的"无结果"
                last_err = f"求解器无输出 (exit 诊断: {self._last_solver_stderr or 'stderr 为空'})"
            self._last_error = last_err
            logs.warn("captcha", f"第 {attempt}/{settings.CAPTCHA_SOLVE_RETRIES} 次求解未果: {last_err}")

        logs.warn("captcha", f"求解失败: {last_err or '多次重试无结果'}")
        self._note_mint_failure()
        return None

    async def _run_solver(
        self, scene: str, region: str, prefix: str, tag: str = egress.DIRECT
    ) -> str | None:
        solver = settings.CAPTCHA_SOLVER_JS
        runtime = os.path.basename(str(settings.NODE_PATH))
        if not solver.exists():
            captcha_ledger.record(route=solver.name, runtime=runtime, ok=False,
                                  dur_ms=0, region=region, err=f"求解器缺失: {solver.name}")
            raise RuntimeError(
                f"未找到求解器 {solver}，请先在 captcha_node 下执行 npm install"
            )
        # 出口必须与取用该 token 的请求一致（见 app/egress.py）：
        #   direct → 剥掉代理变量，chrome 不带 --proxy-server
        #   proxy  → 注入代理变量，solver 用它作 --proxy-server
        solver_env = {**os.environ, "ZCODE_CHROMIUM_PATH": str(settings.CHROMIUM_PATH)}
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                    "ALL_PROXY", "all_proxy"):
            solver_env.pop(key, None)
        t0 = time.monotonic()
        if tag == egress.PROXY:
            proxy = egress.proxy_url()
            if not proxy:
                raise RuntimeError("请求走代理但 env 未配置代理地址，无法对齐验证码出口")
            solver_env["HTTP_PROXY"] = proxy
            solver_env["HTTPS_PROXY"] = proxy
        proc = await asyncio.create_subprocess_exec(
            settings.NODE_PATH, str(solver), scene, region, prefix,
            cwd=str(settings.CAPTCHA_SOLVER_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,  # 本地保留 stderr 捕获（失败诊断用）
            env=solver_env,
            # 独立进程组：solver 会拉起真 Chromium 子进程，超时强杀时必须
            # 按组收割，否则只杀 node 会留下永生孤儿浏览器（2026-10-05 实测
            # 累积 92 个、吃掉 27.6GB RSS，把内存和 swap 榨干后新请求连
            # Chromium 都起不来，Cloudflare 侧表现为 524 超时）。
            start_new_session=True,
        )
        # 体征登记：solver 及其 Chromium 后代的堆积形态（2026-10-05 实测 92 个
        # 孤儿浏览器吃 27.6GB RSS）由 app/vitals 每秒观测；只登记不收割——
        # 回收权属于 asyncio 子进程 watcher，抢 wait 会让停机挂死。
        vitals.register_child(proc.pid)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=settings.CAPTCHA_SOLVE_TIMEOUT)
        except TimeoutError:
            _kill_solver_tree(proc)
            self._last_solver_stderr = None  # 强杀拿不到 stderr
            captcha_ledger.record(route=solver.name, runtime=runtime, ok=False,
                                  dur_ms=int((time.monotonic() - t0) * 1000), region=region,
                                  err=f"超时强杀（>{settings.CAPTCHA_SOLVE_TIMEOUT}s）")
            return None
        except FileNotFoundError as err:
            captcha_ledger.record(route=solver.name, runtime=runtime, ok=False,
                                  dur_ms=int((time.monotonic() - t0) * 1000), region=region,
                                  err=f"无法启动 Node: {err}")
            raise RuntimeError(f"无法启动 Node（{settings.NODE_PATH}）: {err}") from err

        # 失败诊断：solver 把根因（pe 失速/guest 错误摘要）写 stderr，保留末段
        self._last_solver_stderr = _stderr_tail(stderr)
        param = None
        for line in stdout.decode("utf-8", "ignore").splitlines():
            if line.startswith("VERIFY_PARAM="):
                param = line[len("VERIFY_PARAM="):].strip()
        # 铸码台账：一次 solver 子进程尝试一条（含重试与竞速败者），按路线可统计成功率
        captcha_ledger.record(
            route=solver.name, runtime=runtime, ok=bool(param),
            dur_ms=int((time.monotonic() - t0) * 1000), region=region,
            err=None if param else (self._last_solver_stderr or "求解器无输出"),
        )
        return param

    # ── 失效 ─────────────────────────────────────────────────────────────────
    def invalidate(self, tag: str | None = None) -> None:
        """上游返回验证码挑战时清空该出口的池（该批 token/指纹已不可信）。

        默认只清热路径出口：只按出口清，避免把另一条通道（如 claim 的直连池）
        一起废掉。传 `tag=None` 且需要全清时用 `invalidate_all()`。
        """
        self._drain(tag or self.hot_egress())

    def invalidate_all(self) -> None:
        for tag in list(self._pools):
            self._drain(tag)

    def _drain(self, tag: str) -> None:
        pool = self._pools.get(tag)
        if pool is None:
            return
        drained = 0
        while True:
            try:
                pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pool_sizes[tag] = max(0, self._size(tag) - 1)
            drained += 1
        self._issued.clear()
        if drained:
            logs.warn("captcha", f"验证码失效（{tag} 出口），清空池 {drained} 枚")


captcha_manager = CaptchaManager()
