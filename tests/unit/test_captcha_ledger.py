"""铸码台账（captcha_ledger）：按 solver 路线记录每次求解尝试并聚合成功率。

口径锁定（与模块 docstring 一致）：
- record() 双层落账：内存 deque + data/captchalog-<北京日期>.jsonl
- history() 服务端聚合：按 (route, runtime) 分组，回成功率/平均耗时/逐日分布
- err 截断到 _ERR_LIMIT，且空白归一为 None，不落 token/certifyId 物料
"""

from __future__ import annotations

import json

from app import captcha_ledger as ledger
from app import settings


def _isolated(tmp_path, monkeypatch):
    """隔离：DATA_DIR 指向 tmp，清空共享内存 deque。"""
    monkeypatch.setattr(settings, "DATA_DIR", tmp_path)
    ledger._entries.clear()


def test_record_persists_and_snapshots(tmp_path, monkeypatch):
    _isolated(tmp_path, monkeypatch)
    ledger.record(route="solver-bun.ts", runtime="bun", ok=True, dur_ms=4000, region="sgp")
    ledger.record(route="solver_pw.js", runtime="bun", ok=False, dur_ms=25000, err="boom")

    assert len(ledger.snapshot()) == 2
    files = list(tmp_path.glob("captchalog-*.jsonl"))
    assert len(files) == 1
    rows = [json.loads(x) for x in files[0].read_text(encoding="utf-8").splitlines()]
    assert [r["route"] for r in rows] == ["solver-bun.ts", "solver_pw.js"]
    assert rows[0]["ok"] is True and rows[1]["ok"] is False


def test_history_aggregates_by_route(tmp_path, monkeypatch):
    _isolated(tmp_path, monkeypatch)
    ledger.record(route="solver-bun.ts", runtime="bun", ok=True, dur_ms=4000)
    ledger.record(route="solver-bun.ts", runtime="bun", ok=False, dur_ms=25000)
    ledger.record(route="solver_pw.js", runtime="bun", ok=True, dur_ms=6000)

    h = ledger.history(days=7)
    by_key = {(r["route"], r["runtime"]): r for r in h["routes"]}
    bun = by_key[("solver-bun.ts", "bun")]
    assert bun["attempts"] == 2 and bun["ok"] == 1 and bun["fail"] == 1
    assert bun["success_rate"] == 0.5
    assert bun["avg_ms"] == 14500
    pw = by_key[("solver_pw.js", "bun")]
    assert pw["attempts"] == 1 and pw["success_rate"] == 1.0
    total = sum(r["attempts"] for day in h["by_day"].values() for r in day.values())
    assert total == 3


def test_err_truncated_and_normalized(tmp_path, monkeypatch):
    _isolated(tmp_path, monkeypatch)
    ledger.record(route="a.ts", runtime="bun", ok=False, dur_ms=1, err="x" * 500)
    ledger.record(route="a.ts", runtime="bun", ok=True, dur_ms=1, err="   ")
    snap = ledger.snapshot()
    assert len(snap[0]["err"]) == ledger._ERR_LIMIT
    assert snap[1]["err"] is None  # 空白归一为 None
