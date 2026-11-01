"""端点映射跟随（app/endpoint_routing.py）单元测试。

fail-open 铁律：快照缺失/过期/表无此条/任何异常 → 原 URL 原样放行；
表校验违例整快照拒绝（退回旧快照），绝不部分采纳。
"""

from __future__ import annotations

import pytest

from app import endpoint_routing as er, settings


@pytest.fixture(autouse=True)
def _routing_on(monkeypatch):
    monkeypatch.setattr(settings, "ENDPOINT_ROUTING", True)
    er._snapshot = None
    er._retry_after = 0.0
    yield
    er._snapshot = None
    er._retry_after = 0.0


def _set_snapshot(mapping: dict, expires_in: float = 300.0):
    import time

    er._snapshot = {"expires_at": time.monotonic() + expires_in, "mapping": mapping}


# ── 表构建与校验 ──────────────────────────────────────────────────────────────

def test_build_mapping_ok():
    m = er._build_mapping({
        "proxyEndpoint": {"mapping": [
            {"from": "https://api.z.ai/api/anthropic/v1/messages",
             "to": "https://zcode.z.ai/api/v1/ultra/anthropic/v1/messages"},
        ]},
    })
    assert "https://api.z.ai:443/api/anthropic/v1/messages" in m


def test_build_mapping_missing_block_is_empty():
    assert er._build_mapping({}) == {}
    assert er._build_mapping({"proxyEndpoint": {}}) == {}


@pytest.mark.parametrize("entries", [
    [{"from": "http://api.z.ai/x", "to": "https://zcode.z.ai/y"}],          # 非 https
    [{"from": "https://api.z.ai/x?query=1", "to": "https://zcode.z.ai/y"}],  # 带 query
    [{"from": "https://u:p@api.z.ai/x", "to": "https://zcode.z.ai/y"}],      # 带凭据
    [{"from": "https://api.z.ai/x", "to": "https://evil.com/y"}],            # to 任意 host（允许，映射本身合法）
])
def test_build_mapping_url_validation(entries):
    if entries[0]["to"].startswith("https://evil.com"):
        # to 无限制（表驱动）；仅 from/to 形状受校验
        m = er._build_mapping({"proxyEndpoint": {"mapping": entries}})
        assert m
    else:
        with pytest.raises(ValueError):
            er._build_mapping({"proxyEndpoint": {"mapping": entries}})


def test_build_mapping_duplicate_from_rejected():
    with pytest.raises(ValueError):
        er._build_mapping({"proxyEndpoint": {"mapping": [
            {"from": "https://api.z.ai/x", "to": "https://zcode.z.ai/y"},
            {"from": "https://api.z.ai/x/", "to": "https://zcode.z.ai/z"},  # 规范化后同键
        ]}})


def test_build_mapping_too_many_rejected():
    entries = [{"from": f"https://h{i}.z.ai/x", "to": "https://zcode.z.ai/y"} for i in range(er.MAX_MAPPING_ENTRIES + 1)]
    with pytest.raises(ValueError):
        er._build_mapping({"proxyEndpoint": {"mapping": entries}})


def test_build_mapping_auth_flag_safety():
    # 标记值必须是头名自身 + to 必须在官方 ultra 路径下
    ok = er._build_mapping({"proxyEndpoint": {"mapping": [
        {"from": "https://api.z.ai/x", "to": "https://zcode.z.ai/api/v1/ultra/y",
         "zcodeAuthorizationHeader": "X-ZCode-Authorization"},
    ]}})
    assert ok["https://api.z.ai:443/x"].include_zcode_auth is True

    with pytest.raises(ValueError):
        er._build_mapping({"proxyEndpoint": {"mapping": [
            {"from": "https://api.z.ai/x", "to": "https://zcode.z.ai/api/v1/ultra/y",
             "zcodeAuthorizationHeader": "Bearer evil"},
        ]}})
    with pytest.raises(ValueError):
        er._build_mapping({"proxyEndpoint": {"mapping": [
            {"from": "https://api.z.ai/x", "to": "https://evil.com/api/v1/ultra/y",
             "zcodeAuthorizationHeader": "x-zcode-authorization"},
        ]}})


# ── 热路径 apply ──────────────────────────────────────────────────────────────

URL = "https://api.z.ai/api/anthropic/v1/messages?beta=true"

def test_apply_no_snapshot_fail_open():
    assert er.apply(URL) == (URL, {})


def test_apply_expired_snapshot_fail_open():
    _set_snapshot({}, expires_in=-1.0)
    assert er.apply(URL) == (URL, {})


def test_apply_rewrites_and_keeps_query():
    _set_snapshot({"https://api.z.ai:443/api/anthropic/v1/messages": er._Target(
        url="https://zcode.z.ai:443/api/v1/ultra/anthropic/v1/messages", include_zcode_auth=False)})
    new_url, extra = er.apply(URL)
    assert new_url == "https://zcode.z.ai:443/api/v1/ultra/anthropic/v1/messages?beta=true"
    assert extra == {}


def test_apply_auth_flag_with_and_without_jwt():
    _set_snapshot({"https://api.z.ai:443/api/anthropic/v1/messages": er._Target(
        url="https://zcode.z.ai:443/api/v1/ultra/anthropic/v1/messages", include_zcode_auth=True)})
    _, extra = er.apply(URL, zcode_jwt="jwt.token.x")
    assert extra == {"X-ZCode-Authorization": "Bearer jwt.token.x"}
    # fail-open：本侧无 JWT 照发，只是不带头
    new_url, extra = er.apply(URL, zcode_jwt=None)
    assert "ultra" in new_url
    assert extra == {}


def test_apply_disabled_bypass(monkeypatch):
    monkeypatch.setattr(settings, "ENDPOINT_ROUTING", False)
    _set_snapshot({"https://api.z.ai:443/api/anthropic/v1/messages": er._Target(
        url="https://zcode.z.ai:443/y", include_zcode_auth=False)})
    assert er.apply(URL) == (URL, {})


def test_apply_garbage_url_never_raises():
    _set_snapshot({})
    assert er.apply("not a url") == ("not a url", {})
