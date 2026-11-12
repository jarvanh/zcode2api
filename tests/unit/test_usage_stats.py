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

    files = [
        (today, _write(tmp_path, today, [
            {"in": 10, "out": 5, "probe": False, "credit": 1.5},
            {"in": 100, "out": 50, "probe": True},
            {"in": None, "out": None},
        ])),
        (today - timedelta(days=1), _write(tmp_path, today - timedelta(days=1), [
            {"in": 20, "out": 10},
        ])),
        # 9 天前：在近 7 天窗外、近 30 天窗内
        (today - timedelta(days=9), _write(tmp_path, today - timedelta(days=9), [
            {"in": 30, "out": 15},
        ])),
        # 40 天前：两个滚动窗都窗外，仅「历史」
        (today - timedelta(days=40), _write(tmp_path, today - timedelta(days=40), [
            {"in": 40, "out": 20},
        ])),
    ]

    data = usage_stats._summarize(files, today)

    # 今日：10+5 + 0+0（None 行算一次请求，token 0）；probe 行完全不计
    assert data["today"] == {
        "requests": 2, "in": 10, "out": 5, "total": 15, "credit": 1.5,
    }
    # 近 7 天：今日 + 昨天（9 天前在窗外）
    assert data["day7"]["requests"] == 3
    assert data["day7"]["total"] == 45
    # 近 30 天：今日 + 昨天 + 9 天前（40 天前在窗外）
    assert data["day30"]["requests"] == 4
    assert data["day30"]["total"] == 90
    # 历史：全部（含 40 天前那份）
    assert data["all"]["requests"] == 5
    assert data["all"]["total"] == 150
    assert data["range"] == {
        "from": "2026-09-13", "to": "2026-10-23", "days": 4,
    }


def test_summarize_rolling_window_boundary(tmp_path):
    """滚动窗边界：第 7 天（含）与第 8 天（不含）——与自然周口径的关键差异。"""
    today = date(2026, 10, 23)
    files = [
        (today - timedelta(days=6), _write(tmp_path, today - timedelta(days=6), [
            {"in": 7, "out": 0},
        ])),
        (today - timedelta(days=7), _write(tmp_path, today - timedelta(days=7), [
            {"in": 8, "out": 0},
        ])),
    ]
    data = usage_stats._summarize(files, today)
    assert data["day7"]["requests"] == 1  # 只含 6 天前那份，7 天前不在窗内
    assert data["day7"]["in"] == 7


def test_summarize_empty():
    data = usage_stats._summarize([], date(2026, 10, 23))
    assert data["today"]["requests"] == 0
    assert data["all"]["total"] == 0
    assert data["range"] == {"from": None, "to": None, "days": 0}
