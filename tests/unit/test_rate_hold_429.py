"""429 频控扣号（magpie plugins#503 移植）+ 线上 3008 形态回归。

回归目标（2026-11-06 23:15–23:43 故障窗口）：普通 429 后账号秒回池子，
换号梯子把 4 个号全敲一遍（reqlog att=4），同 IP 多账号高频拉长风控波次。
扣号后其余请求选不到号，走排队/快速 503+客户端重试，上游压力降一个数量级。
另：2026-11-07 线上 429 主力是 {"code":3008,"msg":"user concurrency limit
exceeded"}，扣号豁免只认 3009，3008 必须扣号。
"""

from __future__ import annotations

import time

from app import settings
from app.models import Account, Status


def _acct(name: str) -> Account:
    return Account(id=name, provider="zai", name=name, mode="jwt")


def test_hold_rate_blocks_selectable_until_expiry():
    a = _acct("a")
    assert a.is_selectable(model="GLM-5.3-Flash")
    until = a.hold_rate("GLM-5.3-Flash", 30)
    assert until > time.time()
    assert a.is_rate_held("GLM-5.3-Flash")
    assert not a.is_selectable(model="GLM-5.3-Flash")
    # 模型名大小写归一；别的模型不受影响
    assert a.is_rate_held("glm-5.3-flash")
    assert a.is_selectable(model="GLM-5.2")
    # 到期自动放回
    a.rate_hold["glm-5.3-flash"] = time.time() - 1
    assert not a.is_rate_held("GLM-5.3-Flash")
    assert a.is_selectable(model="GLM-5.3-Flash")


def test_hold_rate_only_extends_and_clear_on_success():
    a = _acct("a")
    first = a.hold_rate("m", 10)
    # 只延长不缩短：并发扣号不会互相改短等待期
    second = a.hold_rate("m", 1)
    assert second >= first
    a.clear_rate_hold("m")
    assert not a.is_rate_held("m")


def test_hold_rate_zero_seconds_is_noop():
    a = _acct("a")
    assert a.hold_rate("m", 0) == 0.0
    assert not a.rate_hold
    assert a.hold_rate("", 30) == 0.0


def test_store_select_skips_rate_held_account(fresh_app):
    """核心回归：一个号被扣住后，store.select 只选另一个号（梯子不再打全池）。"""
    a = fresh_app.add_account("zai", "a", "ka")
    b = fresh_app.add_account("zai", "b", "kb")
    a.hold_rate("GLM-5.3-Flash", 60)
    assert not a.is_selectable(model="GLM-5.3-Flash")
    picked = fresh_app.select("zai", model="GLM-5.3-Flash")
    assert picked is not None and picked.id == b.id
    # 两个号都扣住 → 无号可选（上层排队等 Retry-After，而不是再打上游）
    b.hold_rate("GLM-5.3-Flash", 60)
    assert fresh_app.select("zai", model="GLM-5.3-Flash") is None


def test_hold_settings_defaults():
    assert settings.RETRY_429_HOLD_DEFAULT > 0
    assert 0 < settings.RETRY_429_HOLD_DEFAULT <= settings.RETRY_429_HOLD_MAX


def test_concurrency_classification_for_hold():
    """线上回归：3008 账号级频控必须扣号；只豁免 3009 模型并发槽。"""
    from app.routes.gateway import _is_model_concurrency_hit, _is_concurrency_hit

    rate_3008 = '{"code":3008,"msg":"user concurrency limit exceeded"}'
    rate_3009 = '{"code":3009,"msg":"model concurrency limit exceeded"}'
    assert _is_concurrency_hit(rate_3008) is True   # 短退避策略：都算并发争用
    assert _is_concurrency_hit(rate_3009) is True
    assert _is_model_concurrency_hit(rate_3009) is True
    assert _is_model_concurrency_hit(rate_3008) is False  # 但 3008 必须扣号
    assert _is_model_concurrency_hit('{"error":{"message":"rate limited"}}') is False
