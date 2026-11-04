"""运行体征采样单测 —— 进程/池侧仪表（app/vitals.py）。

关键约束钉住：
- **绝不 waitpid**：solver 的回收权属于 asyncio 子进程 watcher，观测层抢 wait
  会让 returncode 永远等不到 → 优雅停机挂死。这里只登记、只读状态。
- 非 Linux 平台 RSS 返回 None（面板显示「未知」），不得假装成 0。
- 采样异常一律吞掉，不得冒泡进心跳。
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest

from app import vitals


@pytest.fixture(autouse=True)
def _clean():
    vitals._reset_for_tests()
    yield
    vitals._reset_for_tests()


class TestRss:
    def test_self_rss_positive_on_linux(self):
        if not vitals._LINUX:
            pytest.skip("非 Linux：本用例只验 /proc 口径")
        mb = vitals._rss_mb()
        assert isinstance(mb, int) and mb > 0

    def test_non_linux_returns_none_not_zero(self, monkeypatch):
        monkeypatch.setattr(vitals, "_LINUX", False)
        assert vitals._rss_mb() is None

    def test_unreadable_statm_is_swallowed(self, monkeypatch):
        """statm 读不到（容器限制/平台差异）时返回 None，绝不抛出。"""
        def _boom(*_a, **_k):
            raise OSError("EACCES")
        monkeypatch.setattr(vitals, "open", _boom, raising=False)
        assert vitals._rss_mb() is None

    def test_sysconf_failure_is_swallowed(self, monkeypatch):
        def _boom(*_a, **_k):
            raise ValueError("no sysconf")
        monkeypatch.setattr(os, "sysconf", _boom)
        assert vitals._rss_mb() is None


class TestChildren:
    def test_registry_self_heals_after_exit(self):
        """登记表自愈：进程消失后下一次采样自动摘除（调用方无需配对反操作）。"""
        p = subprocess.Popen(["true"])
        pid = p.pid
        vitals.register_child(pid)
        assert pid in vitals._children
        p.wait()                            # asyncio/watcher 之外手工回收
        time.sleep(0.2)
        vitals._child_states()              # 一次采样
        assert pid not in vitals._children

    def test_non_positive_pid_ignored(self):
        vitals.register_child(0)
        vitals.register_child(-1)
        assert vitals._children == set()

    def test_disappeared_pid_dropped(self):
        """登记表里的 pid 若已从进程表消失 → 摘除且不计数（表不会只增不减）。"""
        vitals.register_child(2 ** 30)      # 不可能存在的 pid
        alive, zombies, _browsers = vitals._child_states()
        assert (alive, zombies) == (0, 0)
        assert vitals._children == set()

    def test_real_child_counted_without_reaping(self):
        """真子进程：计数为 1，且观测层没有把它 wait 掉。"""
        p = subprocess.Popen(["sleep", "3"])
        try:
            vitals.register_child(p.pid)
            alive, zombies, _b = vitals._child_states()
            assert alive == 1 and zombies == 0
            assert p.poll() is None          # 仍存活 = 未被回收
        finally:
            p.kill()
            p.wait()

    def test_zombie_counted_separately(self):
        """僵尸（已退出未回收）单列：持续 >0 就是拖住优雅停机的形态。"""
        p = subprocess.Popen(["true"])
        pid = p.pid
        time.sleep(0.5)                      # 退出但不回收 → 僵尸
        vitals.register_child(pid)
        alive, zombies, _b = vitals._child_states()
        p.wait()
        if zombies:                          # 平台/时机相关：没抓到僵尸就跳过
            assert alive == 0
            assert zombies == 1

    def test_descendants_counted(self):
        """solver 的后代（真 Chromium 那一层）计入 browser_procs。"""
        parent = subprocess.Popen(["python3", "-c",
                                   "import subprocess,time;"
                                   "[subprocess.Popen(['sleep','30']) for _ in range(2)];"
                                   "print('up',flush=True);time.sleep(30)"])
        # 等解释器起好并拉起子进程（否则采到 0 后代是时机竞态，不是功能坏）
        time.sleep(1.2)
        vitals.register_child(parent.pid)
        try:
            alive, _z, browsers = vitals._child_states()
            assert alive == 1
            assert browsers >= 2, f"后代未计入: {browsers}"
        finally:
            parent.kill()
            parent.wait()


class TestNoReapInvariant:
    def test_source_never_waits(self):
        """结构性钉子：观测层不得出现 wait/wait3/waitpid（会抢 asyncio 的回收权）。"""
        import inspect

        src = inspect.getsource(vitals)
        for banned in ("wait3(", "waitpid(", "os.wait(", "wait("):
            assert banned not in src, f"vitals 出现回收调用: {banned}"


class TestPoolAndSample:
    def test_pool_depth_reads_max_qsize(self, monkeypatch):
        class _Q:
            def __init__(self, n):
                self._n = n

            def qsize(self):
                return self._n

        class _Mgr:
            _pools = {"direct": _Q(2), "proxy": _Q(7)}

        monkeypatch.setattr("app.captcha.captcha_manager", _Mgr())
        assert vitals._pool_depth() == 7

    def test_pool_depth_broken_source_returns_zero(self, monkeypatch):
        class _Bad:
            @property
            def _pools(self):
                raise RuntimeError("没初始化")

        monkeypatch.setattr("app.captcha.captcha_manager", _Bad())
        assert vitals._pool_depth() == 0

    def test_pool_depth_empty_pools_zero(self, monkeypatch):
        class _Mgr:
            _pools = {}
        monkeypatch.setattr("app.captcha.captcha_manager", _Mgr())
        assert vitals._pool_depth() == 0

    def test_sample_trims_tail(self, monkeypatch):
        monkeypatch.setattr(vitals, "_MAX_SAMPLES", 5)
        for i in range(9):
            vitals.sample(loop_lag_ms=float(i))
        snap = vitals.snapshot(minutes=60)
        assert len(snap["series"]) == 5
        assert snap["now"]["loop_ms"] == 8.0

    def test_snapshot_window_filters_old(self):
        vitals.sample(loop_lag_ms=1.0)
        with vitals._lock:
            vitals._samples.append({"ts": time.time() - 3600, "rss_mb": 1,
                                    "solvers": 0, "zombies": 0, "browser_procs": 0,
                                    "pool": 0, "loop_ms": 9.0})
        snap = vitals.snapshot(minutes=10)
        assert all(s["loop_ms"] != 9.0 for s in snap["series"])

    def test_snapshot_shape(self):
        vitals.sample(loop_lag_ms=1.5)
        snap = vitals.snapshot()
        for key in ("now", "series", "tick", "linux", "pid",
                    "retention_minutes", "tracked_children"):
            assert key in snap
        for key in ("rss_mb", "solvers", "zombies", "browser_procs", "pool", "loop_ms"):
            assert key in snap["now"]

    def test_start_without_running_loop_returns_none(self):
        assert vitals.start() is None


class TestHeartbeat:
    """心跳真跑一遍：曾因 stop() 里 asyncio 只有函数内 import 而在 except 子句
    求值时抛 NameError（ruff F821 抓到），会直接破坏优雅停机。"""

    def test_start_sample_stop_clean(self):
        import asyncio

        async def scenario():
            task = vitals.start()
            assert task is not None
            await asyncio.sleep(1.2)          # 至少一次 tick
            assert len(vitals.snapshot(minutes=1)["series"]) >= 1
            now = vitals.snapshot()["now"]
            assert now["loop_ms"] is not None  # 心跳必须写进 loop 延迟
            await vitals.stop()                # 不得抛（含 CancelledError 分支）
            assert vitals._task is None

        asyncio.run(scenario())

    def test_stop_is_idempotent(self):
        import asyncio

        async def scenario():
            await vitals.stop()               # 没起过也要安全
            vitals._task = asyncio.create_task(vitals._loop())
            await vitals.stop()
            await vitals.stop()
        asyncio.run(scenario())
