"""用量统计：按北京日期聚合 data/usage-*.jsonl。

后台「使用统计」卡片的数据源。只统计成功请求落盘的 token 用量；
`probe=true` 的是额度探测请求，默认不计入真实使用量。

保留期与 gateway.USAGE_HISTORY_DAYS 对齐（30 天）；「历史」即本地现有明细，
不是服务启动以来的全量账本（超出保留期的文件已被清理）。
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import settings

_BJ = timezone(timedelta(hours=8))
_PREFIX = "usage-"
_CACHE_TTL = 30.0

_lock = threading.Lock()
_cache: dict = {"key": None, "at": 0.0, "data": None}


def _empty() -> dict:
    return {"requests": 0, "in": 0, "out": 0, "total": 0, "credit": 0.0}


def _as_int(value) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def _add(bucket: dict, row: dict) -> None:
    tin = _as_int(row.get("in"))
    tout = _as_int(row.get("out"))
    bucket["requests"] += 1
    bucket["in"] += tin
    bucket["out"] += tout
    bucket["total"] += tin + tout
    credit = row.get("credit")
    if isinstance(credit, (int, float)) and not isinstance(credit, bool):
        bucket["credit"] += float(credit)


def _day_files() -> list[tuple[date, Path]]:
    """扫描 data/usage-YYYY-MM-DD.jsonl，按日期升序。"""
    out: list[tuple[date, Path]] = []
    for path in sorted(Path(settings.DATA_DIR).glob(f"{_PREFIX}*.jsonl")):
        raw = path.name[len(_PREFIX):-len(".jsonl")]
        try:
            out.append((date.fromisoformat(raw), path))
        except ValueError:
            continue  # 非日期文件（手工放错/临时文件）直接跳过
    out.sort(key=lambda item: item[0])
    return out


def _signature(files: list[tuple[date, Path]]) -> tuple:
    """缓存键：日期 + mtime。文件追加/清理都会让缓存失效。"""
    items = []
    for day, path in files:
        try:
            items.append((day.isoformat(), path.stat().st_mtime_ns))
        except OSError:
            continue
    return tuple(items)


def _summarize(files: list[tuple[date, Path]], today: date) -> dict:
    """把文件列表聚合成今日/本周/本月/历史四个桶。

    周从周一算起，月从 1 号算起，均按北京日期；探测请求不计入。
    """
    files = sorted(files, key=lambda item: item[0])
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    buckets = {
        "today": _empty(),
        "week": _empty(),
        "month": _empty(),
        "all": _empty(),
    }

    for day, path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue  # 半行写入（进程被杀）直接跳过
            if not isinstance(row, dict) or row.get("probe"):
                continue
            _add(buckets["all"], row)
            if day == today:
                _add(buckets["today"], row)
            if day >= week_start:
                _add(buckets["week"], row)
            if day >= month_start:
                _add(buckets["month"], row)

    return {
        "today": buckets["today"],
        "week": buckets["week"],
        "month": buckets["month"],
        "all": buckets["all"],
        "range": {
            "from": files[0][0].isoformat() if files else None,
            "to": files[-1][0].isoformat() if files else None,
            "days": len(files),
        },
        "generated_at": time.time(),
        "note": "不含探测请求；历史为本地保留期内的用量明细",
    }


def summary() -> dict:
    """返回今日/本周/本月/历史用量（30 秒缓存，按文件 mtime 失效）。"""
    files = _day_files()
    today = datetime.now(_BJ).date()
    key = (today.isoformat(), _signature(files))
    now = time.time()

    with _lock:
        cached = _cache["data"]
        if cached is not None and _cache["key"] == key and now - _cache["at"] < _CACHE_TTL:
            return cached

    data = _summarize(files, today)

    with _lock:
        _cache.update(key=key, at=now, data=data)
    return data
