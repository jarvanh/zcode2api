"""mihomo 节点自动轮换（app/proxy_rotate.py）单元测试。

控制器不可达 / 组不存在 / 全封 / 冷却中 → 一律 None（退回换号语义）。
"""

from __future__ import annotations

import pytest

from app import proxy_rotate, settings
from app.models import Account


def _account(name: str = "zai-131") -> Account:
    return Account(id=f"test-{name}", name=name, provider="zai", mode="jwt",
                   jwt_token="a.b.c")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(settings, "PROXY_ROTATE_ENABLED", True)
    monkeypatch.setattr(settings, "PROXY_ROTATE_COOLDOWN", 0)
    proxy_rotate.reset_state()
    yield
    proxy_rotate.reset_state()


def test_group_for_follows_naming_convention():
    assert proxy_rotate.group_for("zai-131") == "eg-zai-131"


async def test_disabled_returns_none(monkeypatch):
    monkeypatch.setattr(settings, "PROXY_ROTATE_ENABLED", False)

    async def fail_get(*_a, **_k):  # pragma: no cover - 不应被调用
        raise AssertionError("disabled 时不应访问 controller")

    monkeypatch.setattr(proxy_rotate, "_api_get", fail_get)
    assert await proxy_rotate.rotate("r1", _account()) is None


async def test_controller_unreachable_returns_none(monkeypatch):
    async def unreachable(path):
        return None

    monkeypatch.setattr(proxy_rotate, "_api_get", unreachable)
    assert await proxy_rotate.rotate("r1", _account()) is None


async def test_rotates_to_first_unblocked_node(monkeypatch):
    group = "eg-zai-131"

    async def fake_get(path):
        assert path == f"/proxies/{group}"
        return {"now": "node-a", "all": ["node-a", "node-b", "node-c", "AUTO"]}

    puts: list[tuple[str, dict]] = []

    async def fake_put(path, body):
        puts.append((path, body))
        return True

    monkeypatch.setattr(proxy_rotate, "_api_get", fake_get)
    monkeypatch.setattr(proxy_rotate, "_api_put", fake_put)

    node = await proxy_rotate.rotate("r1", _account())
    assert node == "node-b"
    assert puts == [(f"/proxies/{group}", {"name": "node-b"})]
    # 当前节点进黑名单
    assert "node-a" in proxy_rotate._blocked[group]
    assert proxy_rotate.snapshot()["rotations"] == 1


async def test_second_rotation_skips_blocked_and_respects_cooldown(monkeypatch):
    async def fake_get(path):
        return {"now": "node-a", "all": ["node-a", "node-b", "node-c"]}

    async def fake_put(path, body):
        return True

    monkeypatch.setattr(proxy_rotate, "_api_get", fake_get)
    monkeypatch.setattr(proxy_rotate, "_api_put", fake_put)

    assert await proxy_rotate.rotate("r1", _account()) == "node-b"
    # 冷却内再切 → None
    monkeypatch.setattr(settings, "PROXY_ROTATE_COOLDOWN", 600)
    assert await proxy_rotate.rotate("r2", _account()) is None
    # 冷却归零：node-b 是当前节点，node-a 已封 → 选 node-c
    monkeypatch.setattr(settings, "PROXY_ROTATE_COOLDOWN", 0)

    async def fake_get2(path):
        return {"now": "node-b", "all": ["node-a", "node-b", "node-c"]}

    monkeypatch.setattr(proxy_rotate, "_api_get", fake_get2)
    assert await proxy_rotate.rotate("r3", _account()) == "node-c"


async def test_all_blocked_returns_none(monkeypatch):
    async def fake_get(path):
        return {"now": "node-a", "all": ["node-a", "node-b"]}

    monkeypatch.setattr(proxy_rotate, "_api_get", fake_get)
    assert await proxy_rotate.rotate("r1", _account()) is None


async def test_put_failure_returns_none(monkeypatch):
    async def fake_get(path):
        return {"now": "node-a", "all": ["node-a", "node-b"]}

    async def fake_put(path, body):
        return False

    monkeypatch.setattr(proxy_rotate, "_api_get", fake_get)
    monkeypatch.setattr(proxy_rotate, "_api_put", fake_put)
    assert await proxy_rotate.rotate("r1", _account()) is None


async def test_non_zai_account_skipped(monkeypatch):
    acc = Account(id="test-bm-1", name="bm-1", provider="bigmodel", mode="apiKey",
                  api_key="sk-x")

    async def fail_get(*_a, **_k):  # pragma: no cover
        raise AssertionError("非 zai 不应访问 controller")

    monkeypatch.setattr(proxy_rotate, "_api_get", fail_get)
    assert await proxy_rotate.rotate("r1", acc) is None
