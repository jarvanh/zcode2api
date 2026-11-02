"""system sidecar 文本新鲜度状态机（2026-10-31 文本校验波次防复发）。

锁三件事：_meta 缺失/版本不齐/超龄任一命中即 stale；全对齐 + 未超龄 =
fresh；巡检结果落 meta 表供 admin API / healthcheck 读取。
"""

from __future__ import annotations

import json

import pytest

from app import constants, freshness
from app.store import store


@pytest.fixture(autouse=True)
def _isolated_sections(monkeypatch):
    """每个用例注入独立的 sidecar _meta，不污染真实模块状态。

    本机官方客户端版本同样必须钉住：部署机装了 ZCode 时读的是真实
    current.json，官方一自更新（2026-11-02 实测 3.17.1 → 3.17.2），「对齐」
    用例就会因宿主状态变红——那是部署机的漂移告警，不该是单测结论。
    漂移本身由 freshness 巡检落 meta 表供 healthcheck 读，路径不变。
    """
    holder: dict = {"meta": {}}

    def fake_load():
        return {"_meta": holder["meta"], "cliPrefix": "x"}

    monkeypatch.setattr(freshness, "_load_sections", fake_load)
    monkeypatch.setattr(freshness.hostinfo, "local_client_version",
                        lambda: constants.CLIENT_APP_VERSION)
    yield holder


def _meta(version="3.17.2", captured="2026-11-03"):
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


def test_refresh_recomputes_and_writes_back(_isolated_sections):
    """refresh() 现算并回写 meta 表：读路径消官方升版后的巡检盲窗（2026-11-02
    实测：启动巡检 7 分钟后本机客户端自更新，控制台亮绿到下一跳）。"""
    _isolated_sections["meta"] = _meta()
    st = freshness.refresh()
    raw = store.get_setting("sections_freshness")
    assert raw and json.loads(raw)["sidecar_client_version"] == constants.CLIENT_APP_VERSION
    assert st["stale"] is False


def test_refresh_logs_only_on_transition(_isolated_sections, monkeypatch):
    """日志只在状态翻转/问题集变化时打——否则 30s 控制台轮询刷屏。"""
    calls: list[str] = []
    monkeypatch.setattr(freshness.logs, "info", lambda *a, **k: calls.append("info"))
    monkeypatch.setattr(freshness.logs, "warn", lambda *a, **k: calls.append("warn"))
    # 先钉住已知起点（store 跨用例共享，前序用例可能留下任意缓存状态）
    _isolated_sections["meta"] = _meta(version="3.16.3")
    freshness.refresh()
    calls.clear()
    _isolated_sections["meta"] = _meta()
    freshness.refresh()  # stale→fresh：info 一次
    freshness.refresh()  # 状态未变：不重复打
    assert calls == ["info"]
    _isolated_sections["meta"] = _meta(version="3.16.3")
    freshness.refresh()  # fresh→stale：warn 一次
    assert calls == ["info", "warn"]
