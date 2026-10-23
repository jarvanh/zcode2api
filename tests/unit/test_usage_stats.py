"""用量统计聚合的单元测试。"""

from __future__ import annotations

import json
from datetime import date, timedelta

from app import usage_stats


def _write(tmp_path, day: date, rows: list[dict]):
    path = tmp_path / f"usage-{day.isoformat()}.jsonl"
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    lines.append("not-json")  # 半行/坏行必须跳过
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_summarize_periods_and_probe(tmp_path):
    today = date(2026, 10, 23)  # 周五
    week_start = today - timedelta(days=today.weekday())  # 10/19
    month_start = today.replace(day=1)  # 10/1

    files = [
        (today, _write(tmp_path, today, [
            {"in": 10, "out": 5, "probe": False, "credit": 1.5},
            {"in": 100, "out": 50, "probe": True},
            {"in": None, "out": None},
        ])),
        (today - timedelta(days=1), _write(tmp_path, today - timedelta(days=1), [
            {"in": 20, "out": 10},
        ])),
        (week_start - timedelta(days=1), _write(tmp_path, week_start - timedelta(days=1), [
            {"in": 30, "out": 15},
        ])),
        (month_start - timedelta(days=1), _write(tmp_path, month_start - timedelta(days=1), [
            {"in": 40, "out": 20},
        ])),
    ]

    data = usage_stats._summarize(files, today)

    # 今日：10+5 + 0+0（None 行算一次请求，token 0）；probe 行完全不计
    assert data["today"] == {
        "requests": 2, "in": 10, "out": 5, "total": 15, "credit": 1.5,
    }
    # 本周：今日 + 昨天（10/22）
    assert data["week"]["requests"] == 3
    assert data["week"]["total"] == 45
    # 本月：今日 + 昨天 + 10/18（周一前一日，但仍在 10 月）
    assert data["month"]["requests"] == 4
    assert data["month"]["total"] == 90
    # 历史：全部（含 9/30）
    assert data["all"]["requests"] == 5
    assert data["all"]["total"] == 150
    assert data["range"] == {
        "from": "2026-09-30", "to": "2026-10-23", "days": 4,
    }


def test_summarize_empty():
    data = usage_stats._summarize([], date(2026, 10, 23))
    assert data["today"]["requests"] == 0
    assert data["all"]["total"] == 0
    assert data["range"] == {"from": None, "to": None, "days": 0}
