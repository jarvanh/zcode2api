"""运行体征采样 —— 进程侧与池侧的秒级仪表（监控页「运行体征」面板数据源）。

动机（2026-11-04 主人定稿第三批）：reqlog 看的是「请求」，本模块看的是「承载
请求的这台进程」。memory 里那次「restart 等满 300s 被 SIGKILL」的根因是
solver/Chromium 子进程树与 SSE 排空，而这类事实以前只能事后翻 journal ——
能提前十分钟看见的形态（RSS 缓涨、solver 进程堆积、事件循环卡顿）没有任何
观测面。

设计约束：
- **零新依赖**：不引 psutil（requirements 没有它，部署目录重克隆后 pip 源不
  稳定）。RSS 走 /proc/self/statm，子进程存活走 os.kill(pid, 0) + /proc 状态
  位；都是 Linux 专属，非 Linux 返回 None（面板显示「未知」，不假装是 0）。
- **绝不 waitpid**：solver 由 asyncio.create_subprocess_exec 起，回收权属于
  asyncio 的子进程 watcher；这里抢先 wait 会让它的 returncode 永远等不到 →
  优雅停机挂死。观测只读状态，收割一概不碰。
- **纯观测**：任何采样异常吞掉并按 key 限频留痕；全部在后台心跳跑，请求热
  路径零开销。

口径：
  rss_mb        常驻内存（MB）：缓涨=泄漏嫌疑，骤增=大 body 缓冲；2026-10-05
                那次 92 个孤儿浏览器吃 27.6GB，在 RSS 趋势上早于一切日志可见
  solvers       登记在册且存活的 solver 进程数（不含僵尸）
  zombies       其中已退出、尚未被 asyncio watcher 回收的（持续 >0 = 回收链断了）
  browser_procs solver 的全部后代（真 Chromium 及其 renderer/GPU 子进程）——
                孤儿浏览器的直接指纹；kill 链断时这里会先于 RSS 起跳
  pool          验证码令牌池最深一档（按出口 tag）
  loop_ms       事件循环延迟：预期 1s 与实际 1s 之差。上游 read=None 的长流或
                同步 sqlite 调用阻塞 loop 时，第一个被压出来的就是它
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

from . import logs

_TICK = 1.0
_MAX_SAMPLES = 900          # 秒级尾巴 15 分钟
_LINUX = os.path.exists("/proc/self/statm")

_lock = threading.Lock()
_vitals: dict = {"rss_mb": None, "solvers": 0, "zombies": 0,
                 "browser_procs": 0, "pool": 0, "loop_ms": None, "ts": 0.0}
_samples: list[dict] = []
_children: set[int] = set()      # 本进程直接子进程 pid（由 captcha 登记）
_last_warn: dict[str, float] = {}
_task = None


def _warn_once(key: str, msg: str) -> None:
    """同类异常 5 分钟最多一条：心跳坏了不能变成日志刷屏源。"""
    now = time.time()
    if now - _last_warn.get(key, 0.0) > 300:
        _last_warn[key] = now
        logs.warn("vitals", msg)


def _rss_mb() -> int | None:
    """常驻内存（MB）：/proc/self/statm 第 2 字段 = resident pages。"""
    if not _LINUX:
        return None
    try:
        with open("/proc/self/statm", encoding="ascii") as f:
            resident = int(f.read().split()[1])
        return resident * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024)
    except (OSError, IndexError, ValueError) as err:
        _warn_once("rss", f"RSS 采样失败: {err}")
        return None


def register_child(pid: int) -> None:
    """子进程登记（captcha 起 solver 时调）：只登记不收割，见模块注释。

    登记表自愈：_child_states() 每轮把已从进程表消失的 pid 摘掉，所以调用方
    **不需要**配对的反向操作 —— solver 正常退出或被强杀，登记都会随下一次
    采样自然消失（曾写过 forget_child()，因无调用方成为死代码，已删）。
    """
    if pid and pid > 0:
        _children.add(int(pid))


def _scan_procs() -> dict[int, tuple[int, str]]:
    """一次 /proc 扫描：pid → (ppid, state)。非 Linux 返回空表。

    1s 一次的开销可忽略（几百个目录名 + 小文件读），换来 solver → Chromium
    整棵子树的可见性；comm 可含空格/括号，字段一律从最后一个 ')' 之后取。
    """
    out: dict[int, tuple[int, str]] = {}
    if not _LINUX:
        return out
    try:
        pids = os.listdir("/proc")
    except OSError as err:
        _warn_once("proc", f"/proc 扫描失败: {err}")
        return out
    for name in pids:
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            with open(f"/proc/{pid}/stat", encoding="ascii", errors="ignore") as f:
                rest = f.read().rpartition(")")[2].split()
        except OSError:
            continue      # 扫描窗口内进程消失，属正常竞态
        if len(rest) >= 2:
            out[pid] = (int(rest[1]), rest[0])   # rest[0]=state, rest[1]=ppid
    return out


def _descendants(procs: dict[int, tuple[int, str]], roots: set[int]) -> int:
    """roots 的全部后代数（BFS）：真 Chromium 与其 renderer/GPU 子进程。

    孤儿浏览器不在本口径内（它们已重父到 init，无法归因到某次 solve）；
    kill 链断的当场特征是「solver 活着但后代持续膨胀」，RSS 趋势则接住
    重父之后的那一段（2026-10-05 形态：92 进程 / 27.6GB → 先撞 RSS）。
    """
    kids: dict[int, list[int]] = {}
    for pid, (ppid, _state) in procs.items():
        kids.setdefault(ppid, []).append(pid)
    count = 0
    frontier = list(roots)
    while frontier:
        pid = frontier.pop()
        for kid in kids.get(pid, ()):
            if kid != pid:
                count += 1
                frontier.append(kid)
    return count


def _child_states() -> tuple[int, int, int]:
    """(存活 solver 数, 其中僵尸数, solver 后代总数)。僵尸 = 已退出但尚未被
    asyncio watcher 回收。两个口径分开：语义完全不同（僵尸堆积 = 回收链断了，
    正是拖住优雅停机的形态）。"""
    procs = _scan_procs()
    alive = zombies = 0
    live_roots: set[int] = set()
    for pid in list(_children):
        info = procs.get(pid)
        if info is None:
            # /proc 里没有且 kill(pid,0) 也报错 = 彻底消失；还在 = 竞态，保留
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                _children.discard(pid)
                continue
            except PermissionError:
                pass
            alive += 1
            live_roots.add(pid)
            continue
        _ppid, state = info
        if state == "Z":
            zombies += 1
        else:
            alive += 1
            live_roots.add(pid)
    return alive, zombies, _descendants(procs, live_roots)


def _pool_depth() -> int:
    """令牌池最深一档：只读 qsize，不碰池的锁语义与补充逻辑。"""
    try:
        from .captcha import captcha_manager

        pools = getattr(captcha_manager, "_pools", None) or {}
        sizes = [p.qsize() for p in pools.values() if hasattr(p, "qsize")]
        return max(sizes) if sizes else 0
    except Exception as err:  # noqa: BLE001 - 池未初始化/结构变化都不该炸心跳
        _warn_once("pool", f"令牌池采样失败: {err}")
        return 0


def sample(loop_lag_ms: float | None = None) -> dict:
    """采一次并入尾巴（心跳调；单测可直接调本函数）。"""
    solvers, zombies, browsers = _child_states()
    row = {"ts": time.time(), "rss_mb": _rss_mb(), "solvers": solvers,
           "zombies": zombies, "browser_procs": browsers, "pool": _pool_depth(),
           "loop_ms": loop_lag_ms}
    with _lock:
        _vitals.clear()
        _vitals.update(row)
        _samples.append(row)
        if len(_samples) > _MAX_SAMPLES:
            del _samples[:len(_samples) - _MAX_SAMPLES]
    return row


def snapshot(minutes: int = 10) -> dict:
    """管理端数据源：当前值 + 最近 N 分钟秒级尾巴（前端画 sparkline）。"""
    with _lock:
        now = dict(_vitals)
        cutoff = time.time() - minutes * 60
        series = [dict(s) for s in _samples if s["ts"] >= cutoff]
    return {"now": now, "series": series, "tick": _TICK, "linux": _LINUX,
            "pid": os.getpid(), "retention_minutes": minutes,
            "tracked_children": len(_children)}


async def _loop() -> None:
    """1s 心跳：loop_lag = 实际间隔 − 预期（越大说明 loop 被同步块拖住）。"""
    expected = time.monotonic() + _TICK
    while True:
        await asyncio.sleep(_TICK)
        lag_ms = round((time.monotonic() - expected) * 1000, 1)
        expected += _TICK
        if lag_ms < 0:          # 被提前唤醒：重锚，防累积假延迟
            expected = time.monotonic() + _TICK
            lag_ms = 0.0
        try:
            sample(lag_ms)
        except Exception as err:  # noqa: BLE001 - 心跳异常不许掉线
            _warn_once("loop", f"体征采样异常（继续）: {err}")


def start():
    """启动心跳（lifespan 调）；无 running loop 返回 None（单测/create_app 路径）。"""
    global _task
    try:
        _task = asyncio.create_task(_loop())
        return _task
    except RuntimeError:
        return None


async def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        except Exception as err:  # noqa: BLE001 - 停机尽力而为
            logs.warn("vitals", f"心跳停止异常: {err}")
        _task = None


def _reset_for_tests() -> None:
    with _lock:
        _samples.clear()
        _children.clear()
        _vitals.update({"rss_mb": None, "solvers": 0, "zombies": 0,
                        "browser_procs": 0, "pool": 0, "loop_ms": None, "ts": 0.0})
    _last_warn.clear()
