"""最终 503 细分原因（app/routes/gateway._no_account_kind）单元测试。"""

from __future__ import annotations

from types import SimpleNamespace

from app.routes import gateway


def _acc(aid: str, selectable: bool = True):
    return SimpleNamespace(id=aid, is_selectable=lambda now, model=None: selectable)


def test_concurrency_full(monkeypatch):
    monkeypatch.setattr(gateway.store, "list_accounts", lambda p: [_acc("a"), _acc("b")])
    monkeypatch.setattr(gateway, "_inflight", {"a": 4, "b": 4})
    assert gateway._no_account_kind("zai", "m", 4) == "concurrency_full"


def test_one_slot_free_is_not_concurrency_full(monkeypatch):
    monkeypatch.setattr(gateway.store, "list_accounts", lambda p: [_acc("a"), _acc("b")])
    monkeypatch.setattr(gateway, "_inflight", {"a": 1, "b": 4})
    assert gateway._no_account_kind("zai", "m", 4) == "no_available_account"


def test_no_selectable_account(monkeypatch):
    monkeypatch.setattr(gateway.store, "list_accounts", lambda p: [_acc("a", selectable=False)])
    monkeypatch.setattr(gateway, "_inflight", {})
    assert gateway._no_account_kind("zai", "m", 4) == "no_available_account"


def test_unlimited_concurrency_never_full(monkeypatch):
    monkeypatch.setattr(gateway.store, "list_accounts", lambda p: [_acc("a")])
    monkeypatch.setattr(gateway, "_inflight", {"a": 999})
    assert gateway._no_account_kind("zai", "m", 0) == "no_available_account"
