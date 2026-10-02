"""验证码池闲置停解（deep-idle，对齐 zapi 语义）+ 竞速迟到成功入库。

真浏览器求解重（单枚 10–40s + 数百 MB Chromium），零流量时段后台循环
若持续补货就是纯浪费。这里锁四件事：
1. 闲置判定：从未取用 / 超过 idle_after 无取用 = 闲置；取用即唤醒；
2. 停解门控：闲置期间 _refill_loop 只淘汰不铸码（不产生上游求解流量）；
3. 保底供给：闲置唤醒后热路径照常走 竞速→宽限，首个请求自己解自己的码；
4. 迟到入库：竞速死线超时后调用方已离开，迟到成功必须落池（曾整个丢弃），
   这是闲置唤醒后的冷启动关键路径——本请求超时，下一个请求亚毫秒直取。
"""

from __future__ import annotations

import asyncio
import time

import pytest

from app import settings
from app.captcha import CaptchaManager, _Token


def _token(param: str = "eyJjZXJ0aWZ5SWQiOiJ0ZXN0In0=") -> _Token:
    return _Token(param, "cn")


@pytest.fixture()
def mgr(monkeypatch):
    """干净的管理器 + 关闭风暴检测干扰；不启动真实后台循环。

    _trigger_refill 必须隔离：它 fire-and-forget 地跑 _refill_batch →
    _run_solver 会 spawn 真 Node 子进程，事件循环收摊时 transport 清理
    挂死（曾让整个测试会话 Terminated）。
    """
    m = CaptchaManager()
    monkeypatch.setattr(m, "_note_mint_failure", lambda: None)
    monkeypatch.setattr(m, "_note_mint_success", lambda: None)
    monkeypatch.setattr(m, "_trigger_refill", lambda need: None)
    return m


# ── 闲置判定 ─────────────────────────────────────────────────────────────────
class TestIdleDetection:
    def test_never_used_is_idle(self, mgr, monkeypatch):
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 600)
        assert mgr._idle() is True  # 从未取用 → 闲置，起服不预热

    def test_fresh_take_not_idle(self, mgr, monkeypatch):
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 600)
        mgr._last_take_at = time.monotonic()
        assert mgr._idle() is False

    def test_stale_take_is_idle(self, mgr, monkeypatch):
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 600)
        mgr._last_take_at = time.monotonic() - 601
        assert mgr._idle() is True

    def test_disabled_by_zero(self, mgr, monkeypatch):
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 0)
        mgr._last_take_at = 0.0
        assert mgr._idle() is False  # 0 = 不停解


# ── 取用唤醒 ─────────────────────────────────────────────────────────────────
class TestWakeOnTake:
    async def test_get_verify_param_marks_take(self, mgr, monkeypatch):
        """取用（无论池空与否）都必须打点，闲置随即解除。"""
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 600)
        assert mgr._idle() is True

        async def raced(config):
            mgr._put(_token())
            return mgr._pop_fresh()

        monkeypatch.setattr(mgr, "_solve_raced", raced)
        param, region = await mgr.get_verify_param()
        assert param
        assert mgr._idle() is False


# ── 停解门控 ─────────────────────────────────────────────────────────────────
class TestRefillGating:
    async def test_idle_loop_does_not_mint(self, mgr, monkeypatch):
        """闲置期间补充循环只淘汰过期，绝不触达求解器。"""
        monkeypatch.setattr(settings, "CAPTCHA_IDLE_AFTER", 600)
        assert mgr._idle() is True
        calls = []

        async def boom(config):
            calls.append(config)
            return _token()

        monkeypatch.setattr(mgr, "_solve_one", boom)
        # 直接跑一轮循环体逻辑（不起新任务）：复用 _refill_batch 的门控语义
        await mgr._refill_batch(1)
        assert calls == []  # 闲置 → _refill_loop 的 _idle 分支根本不会进来；
        # 防御性再验：即便误调用 _refill_batch，求解也不发生


# ── 竞速迟到成功入库 ─────────────────────────────────────────────────────────
class TestRaceLateArrival:
    async def test_late_success_beyond_deadline_is_banked(self, mgr, monkeypatch):
        """竞速死线超时后调用方已离开：迟到成功必须入池，不得丢弃。"""
        monkeypatch.setattr(settings, "CAPTCHA_RACE_DEADLINE", 0.2)

        release = asyncio.Event()

        async def slow_solve(config):
            await asyncio.wait_for(release.wait(), timeout=5)
            return _token()

        async def solve_one(config):
            return await slow_solve(config)

        monkeypatch.setattr(mgr, "_solve_one", solve_one)
        got = await mgr._solve_raced({})
        assert got is None  # 调用方按死线拿到 None（本请求失败）

        # 竞速任务仍在后台跑：放行后迟到成功应落池
        release.set()
        await asyncio.sleep(0.3)
        assert mgr._pool_size == 1  # 此前该分支会把 token 整个丢弃
        # 落池的 token 可被下一次请求亚毫秒直取
        token = mgr._pop_fresh()
        assert token is not None

    async def test_winner_still_served_before_deadline(self, mgr, monkeypatch):
        """死线内成功走正常返回路径。三路同 certifyId 同帧完成时，胜者
        直接返回，T2 入池（pushToken 语义：只查重已签发=已消费/池内，
        胜者此时尚未消费故不拦），T3 被池内查重拦截 → 池=1（对齐上游）。"""
        monkeypatch.setattr(settings, "CAPTCHA_RACE_DEADLINE", 5)

        async def fast_solve(config):
            return _token()

        monkeypatch.setattr(mgr, "_solve_one", fast_solve)
        got = await mgr._solve_raced({})
        assert got is not None
        assert mgr._pool_size == 1

    async def test_distinct_loser_tokens_are_banked(self, mgr, monkeypatch):
        """死线内败者（不同 certifyId 的有效 token）入库不浪费。"""
        monkeypatch.setattr(settings, "CAPTCHA_RACE_DEADLINE", 5)
        counter = {"n": 0}

        async def fast_solve(config):
            counter["n"] += 1
            import base64, json as _json

            cid = f"cid-{counter['n']}"
            return _token(base64.b64encode(_json.dumps({"certifyId": cid}).encode()).decode())

        monkeypatch.setattr(mgr, "_solve_one", fast_solve)
        got = await mgr._solve_raced({})
        assert got is not None
        assert mgr._pool_size == 2  # 胜者直接返回，两个败者入库
