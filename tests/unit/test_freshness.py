"""system sidecar 文本新鲜度状态机（2026-10-31 文本校验波次防复发）。

锁三件事：_meta 缺失/版本不齐/超龄任一命中即 stale；全对齐 + 未超龄 =
fresh；巡检结果落 meta 表供 admin API / healthcheck 读取。
"""

from __future__ import annotations

import json

import pytest

from app import freshness
from app.store import store


@pytest.fixture(autouse=True)
def _isolated_sections(monkeypatch):
    """每个用例注入独立的 sidecar _meta，不污染真实模块状态。"""
    holder: dict = {"meta": {}}

    def fake_load():
        return {"_meta": holder["meta"], "cliPrefix": "x"}

    monkeypatch.setattr(freshness, "_load_sections", fake_load)
    yield holder


def _meta(version="3.17.1", captured="2026-10-31"):
    return {"client_version": version, "captured_at": captured}


def test_missing_meta_is_stale(_isolated_sections):
    st = freshness.status()
    assert st["stale"] and st["problems"]


def test_version_mismatch_with_gateway_is_stale(_isolated_sections):
    _isolated_sections["meta"] = _meta(version="3.16.3")
    st = freshness.status()
    assert st["stale"]
    assert any("≠ 网关身份版本" in p for p in st["problems"])


def test_fresh_when_aligned_and_recent(_isolated_sections):
    import datetime
    today = datetime.date.today().isoformat()
    _isolated_sections["meta"] = _meta(captured=today)
    st = freshness.status()
    assert not st["stale"]
    assert st["age_days"] == 0


def test_overage_is_stale(_isolated_sections):
    import datetime
    old = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    _isolated_sections["meta"] = _meta(captured=old)
    st = freshness.status()
    assert st["stale"]
    assert any("天未跟进" in p for p in st["problems"])


def test_check_and_record_persists_to_meta_table(_isolated_sections):
    _isolated_sections["meta"] = _meta()
    st = freshness.check_and_record()
    raw = store.get_setting("sections_freshness")
    assert raw and json.loads(raw)["sidecar_client_version"] == st["sidecar_client_version"]
    # stale_hint 读同一份缓存
    assert freshness.stale_hint() is None
    _isolated_sections["meta"] = _meta(version="3.16.3")
    freshness.check_and_record()
    hint = freshness.stale_hint()
    assert hint and "≠ 网关身份版本" in hint
