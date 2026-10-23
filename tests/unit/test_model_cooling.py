"""按模型粒度冷却：1113/1309 只影响该模型，冷却到期允许探测，成功才清除。"""

from __future__ import annotations

import time

from app.models import Account


def test_model_cooling_is_per_model():
    acc = Account.create("bigmodel", "t", "key")
    assert acc.is_selectable(model="glm-4.5-air")
    until = acc.cool_model("glm-4.5-air", 60, 3600)
    assert until > time.time()
    assert not acc.is_selectable(model="glm-4.5-air")
    assert acc.is_selectable(model="glm-5.3-flashx")
    # 冷却到期 → 允许一次探测
    acc.model_cooling["glm-4.5-air"] = time.time() - 1
    assert acc.is_selectable(model="glm-4.5-air")
    # 探测成功 → 清除
    acc.clear_model_cooling("glm-4.5-air")
    assert acc.is_selectable(model="glm-4.5-air")
    assert acc.model_fail_count == {}


def test_model_cooling_backoff_caps():
    acc = Account.create("bigmodel", "t", "key")
    for _ in range(10):
        acc.cool_model("m", 10, 40)
    assert acc.model_fail_count["m"] == 10
    assert 0 < acc.model_cooling["m"] - time.time() <= 40.1


def test_store_select_skips_model_cooled(fresh_app):
    a = fresh_app.add_account("bigmodel", "a", "key-a")
    b = fresh_app.add_account("bigmodel", "b", "key-b")
    a.cool_model("m", 60, 3600)
    fresh_app.update_account(a)
    picked = {fresh_app.select("bigmodel", model="m").id for _ in range(4)}
    assert a.id not in picked
    assert b.id in picked
