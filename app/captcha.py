"""验证码求解 + 预解 token 池。

通过 Bun/Node 子进程在 happy-dom 模拟浏览器环境中运行阿里云无痕 SDK，
求得 verifyParam（X-Aliyun-Captcha-Verify-Param）。

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
import time

import httpx

from . import constants, logs, settings
from .store import store

# 池参数（对齐 zapi：min 20-40 / max 120 过重，单账号网关用小池足矣）
POOL_MIN = settings.CAPTCHA_POOL_MIN
POOL_MAX = settings.CAPTCHA_POOL_MAX
TOKEN_TTL_MS = settings.CAPTCHA_TOKEN_TTL  # 单枚 token 的最大可用时长（ms）


class CaptchaSolveError(Exception):
    """验证码求解最终失败（重试耗尽/求解器不可用）。

    独立于 ClaimError 体系（验证码先于领域层使用），路由层按兜底回执处理。
    """


def _stderr_tail(stderr: bytes | None, limit: int = 300) -> str | None:
    """取 solver stderr 末段作失败诊断（pe 失速/guest 错误摘要都在这里）。"""
    if not stderr:
        return None
    text = stderr.decode("utf-8", "ignore").strip()
    if not text:
        return None
    tail = text[-limit:]
    return tail.replace("\n", " | ")


def _cool_all_accounts(seconds: int, reason: str) -> int:
    """让全部可走 Plan 通道的账号同步冷却（验证码风暴时减少对上游的施压）。

    只动 active 账号（exhausted/invalid/disabled 保持原状态，冷却不洗掉
    已判定的语义）；冷却到期由 is_selectable 的 cooling_until 逻辑自动恢复。
    返回被冷却的账号数。
    """
    from .models import Status

    n = 0
    for acc in store.list_accounts("zai"):
        if acc.status == Status.ACTIVE and acc.enabled:
            acc.status = Status.COOLING
            acc.cooling_until = time.time() + seconds
            acc.last_error = reason
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
    __slots__ = ("param", "region", "born_at", "certify_id")

    def __init__(self, param: str, region: str | None) -> None:
        self.param = param
        self.region = region
        self.born_at = time.monotonic()
        self.certify_id = _parse_certify_id(param)

    def expired(self) -> bool:
        return (time.monotonic() - self.born_at) * 1000 >= TOKEN_TTL_MS


class CaptchaManager:
    def __init__(self) -> None:
        self._pool: asyncio.Queue[_Token] = asyncio.Queue(maxsize=POOL_MAX)
        self._pool_size = 0          # Queue 无可信 len，自行维护
        self._refill_task: asyncio.Task | None = None
        self._refilling = False
        self._config_lock = asyncio.Lock()
        self._config_cache: dict | None = None
        self._config_cache_at: float = 0.0
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
        # 热路径触发的补货任务强引用（事件循环只持弱引用，裸 create_task 会被 GC）
        self._bg_tasks: set[asyncio.Task] = set()

    # ── 配置 ─────────────────────────────────────────────────────────────────
    async def fetch_config(self) -> dict:
        now = time.time() * 1000
        if self._config_cache and now - self._config_cache_at < settings.CAPTCHA_CONFIG_CACHE_TTL:
            return self._config_cache
        async with self._config_lock:
            # 双检：等锁期间可能已被其他请求填充
            if self._config_cache and time.time() * 1000 - self._config_cache_at < settings.CAPTCHA_CONFIG_CACHE_TTL:
                return self._config_cache
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    res = await client.get(
                        f"{constants.CLIENT_CONFIGS_URL}?{constants.CLIENT_CONFIGS_QUERY}"
                    )
                res.raise_for_status()
                captcha = ((res.json().get("data") or {}).get("configs") or {}).get("captcha")
                if captcha:
                    self._config_cache = captcha
                    self._config_cache_at = time.time() * 1000
                    return captcha
            except (httpx.HTTPError, ValueError) as err:
                logs.warn("captcha", f"获取配置失败，使用默认: {err}")
            return dict(constants.CAPTCHA_DEFAULTS)

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
        """是否允许预热：存在可走 Plan 对话的 jwt 账号才预热。

        apiKey 回退 / 废 JWT / 风控禁用 / 额度用完 / 冷却 都不需要验证码。
        账号冷却状态随 store 落库，重启后全冷却期间此门保持关闭。
        """
        return any(
            a.allows_billing() and a.is_selectable()
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

    async def _refill_loop(self) -> None:
        while True:
            try:
                # 风暴冷却期间停止主动铸码（不产生上游流量，等 IP 侧解除）
                if self._storm_gate_closed():
                    await self._evict_expired()
                    await asyncio.sleep(5)
                    continue
                # 无可服务账号（全冷却/禁用/无号）：只淘汰过期 token，不解新码（不产生上游流量）
                if not self._gate_open():
                    await self._evict_expired()
                    await asyncio.sleep(3)
                    continue
                need = POOL_MIN - self._pool_size
                if need > 0:
                    await self._refill_batch(need)
                else:
                    await self._evict_expired()
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - 后台循环永不退出
                self._last_error = str(err)
                logs.warn("captcha", f"补充循环异常: {err}")
                await asyncio.sleep(5)

    async def _refill_batch(self, need: int) -> None:
        """串行补充（求解有 CPU 开销，避免并发爆 Node 进程）。"""
        if self._refilling:
            return
        self._refilling = True
        try:
            config = await self.fetch_config()
            for _ in range(need):
                if self._pool_size >= POOL_MAX:
                    break
                token = await self._solve_one(config)
                if token is None:
                    break
                self._put(token)
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
        return any(t.certify_id == certify_id for t in list(self._pool._queue))

    def _put(self, token: _Token) -> None:
        # 已签发/池内已有的 certifyId 不再入库（对齐 zapi pushToken 查重）
        if token.certify_id and (self._is_issued(token.certify_id) or self._pool_has_certify_id(token.certify_id)):
            return
        try:
            self._pool.put_nowait(token)
            self._pool_size += 1
        except asyncio.QueueFull:
            pass

    def _pop_fresh(self) -> _Token | None:
        """取一枚可用 token：跳过过期与已签发（F008 风险），取出即登记。"""
        while self._pool_size > 0:
            try:
                token = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pool_size = max(0, self._pool_size - 1)
            if token.expired():
                continue
            if token.certify_id and self._is_issued(token.certify_id):
                continue
            self._mark_issued(token)
            return token
        return None

    def _trigger_refill(self, need: int) -> None:
        # fire-and-forget；_refilling 防重入，强引用防 GC
        task = asyncio.create_task(self._refill_batch(need))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _evict_expired(self) -> None:
        kept: list[_Token] = []
        while True:
            try:
                token = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pool_size = max(0, self._pool_size - 1)
            if not token.expired() and len(kept) < POOL_MAX:
                kept.append(token)
        for token in kept:
            self._put(token)

    async def get_verify_param(self, port: int | None = None) -> tuple[str, str | None]:
        """取一枚可用 token：池内直取 → 池空竞速现解 → 宽限窗口等补货。

        返回 (verify_param, region)。region 可为 None（旧求解器无 region 概念）。
        """
        # 1) 池内直取（亚毫秒；取出即触发后台补货）
        token = self._pop_fresh()
        if token is not None:
            self._trigger_refill(1)
            return token.param, token.region

        # 2) 池空/全过期：竞速现解（对齐 zapi takeToken：空池到达本身即扩容信号）
        config = await self.fetch_config()
        token = await self._solve_raced(config)
        if token is not None:
            self._mark_issued(token)
            self._trigger_refill(1)
            return token.param, token.region

        # 3) 竞速无果：宽限窗口内等后台落库（竞速败者/超时者的迟到成功也会入库；
        #    全冷却期间补货门关闭则直接放弃，不空等）
        if self._gate_open():
            grace_deadline = time.monotonic() + settings.CAPTCHA_TAKE_GRACE
            while time.monotonic() < grace_deadline:
                await asyncio.sleep(0.4)
                token = self._pop_fresh()
                if token is not None:
                    self._trigger_refill(1)
                    return token.param, token.region
        raise CaptchaSolveError(f"验证码求解失败: {self._last_error or '多次重试无结果'}")

    # ── 求解 ─────────────────────────────────────────────────────────────────
    async def _solve_raced(self, config: dict) -> _Token | None:
        """空池竞速：并行 EMPTY_TAKE_RACE 路求解，首个成功即回、其余入库。

        全部失败或总死线 RACE_DEADLINE 内无胜者 → None（错误已记 _last_error）。
        超时后竞速任务继续在后台跑（_bg_tasks 持引用），迟到成功经 _put 入库。
        """
        racers = max(1, settings.CAPTCHA_EMPTY_TAKE_RACE)
        loop = asyncio.get_running_loop()
        win: asyncio.Future[_Token | None] = loop.create_future()
        served = False
        failed = 0

        async def racer() -> None:
            nonlocal served, failed
            try:
                token = await self._solve_one(config)
            except Exception as err:  # noqa: BLE001 - 单路失败不拖垮竞速
                self._last_error = str(err)
                token = None
            if token is None:
                failed += 1
                if failed >= racers and not win.done():
                    win.set_result(None)
                return
            if served:
                self._put(token)  # 输了竞速但铸出有效 token：入库不浪费
                return
            served = True  # 事件循环单线程，查改之间无 await，此判原子
            if not win.done():
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

    async def _solve_one(self, config: dict) -> _Token | None:
        scene = config.get("sceneId") or constants.CAPTCHA_DEFAULTS["sceneId"]
        region = config.get("region") or constants.CAPTCHA_DEFAULTS["region"]
        prefix = config.get("prefix") or constants.CAPTCHA_DEFAULTS["prefix"]

        last_err: str | None = None
        for attempt in range(1, settings.CAPTCHA_SOLVE_RETRIES + 1):
            try:
                param = await self._run_solver(scene, region, prefix)
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
                    logs.ok("captcha", f"求解成功（第 {attempt} 次尝试）")
                self._note_mint_success()
                return _Token(param, region)
            if last_err is None:
                # 求解器退出但无 param：用 stderr 末段代替沉默的"无结果"
                last_err = f"求解器无输出 (exit 诊断: {self._last_solver_stderr or 'stderr 为空'})"
            self._last_error = last_err
            logs.warn("captcha", f"第 {attempt}/{settings.CAPTCHA_SOLVE_RETRIES} 次求解未果: {last_err}")

        logs.warn("captcha", f"求解失败: {last_err or '多次重试无结果'}")
        self._note_mint_failure()
        return None

    async def _run_solver(self, scene: str, region: str, prefix: str) -> str | None:
        solver = settings.CAPTCHA_SOLVER_JS
        if not solver.exists():
            raise RuntimeError(
                f"未找到求解器 {solver}，请先在 captcha_node 下执行 npm install"
            )
        proc = await asyncio.create_subprocess_exec(
            settings.NODE_PATH, str(solver), scene, region, prefix,
            cwd=str(settings.CAPTCHA_SOLVER_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=settings.CAPTCHA_SOLVE_TIMEOUT)
        except TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            self._last_solver_stderr = None  # 强杀拿不到 stderr
            return None
        except FileNotFoundError as err:
            raise RuntimeError(f"无法启动 Node（{settings.NODE_PATH}）: {err}") from err

        # 失败诊断：solver 把根因（pe 失速/guest 错误摘要）写 stderr，保留末段
        self._last_solver_stderr = _stderr_tail(stderr)
        param = None
        for line in stdout.decode("utf-8", "ignore").splitlines():
            if line.startswith("VERIFY_PARAM="):
                param = line[len("VERIFY_PARAM="):].strip()
        return param

    # ── 失效 ─────────────────────────────────────────────────────────────────
    def invalidate(self) -> None:
        """上游返回验证码挑战时清空整池（该批 token/指纹已不可信）。"""
        drained = 0
        while True:
            try:
                self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pool_size = max(0, self._pool_size - 1)
            drained += 1
        self._issued.clear()
        if drained:
            logs.warn("captcha", f"验证码失效，清空池 {drained} 枚")


captcha_manager = CaptchaManager()
