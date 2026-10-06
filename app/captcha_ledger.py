"""验证码铸码台账 —— 按 solver 路线记录每次求解尝试，供成功率统计。

背景：验证码存在多条求解路线（happy-dom 模拟 / 真浏览器 Chromium），由
ZCODE_CAPTCHA_SOLVER_JS / ZCODE_CAPTCHA_SOLVER 切换。切换后哪条路线
成功率更高只能靠数据回答，故在 solver 调用层记台账。

设计要点（对齐 reqlog.py 的记账纪律）：
- 粒度：一次 solver 子进程尝试一条（含 _solve_one 的重试与竞速败者；
  迟到成功也入账 —— 它证明该路线能铸出 token）
- 双层：内存 deque（近期秒级观测，重启清零）+ 落盘
  data/captchalog-<北京日期>.jsonl（按天分文件，保留 HISTORY_DAYS 天）
- best-effort：落盘失败只记日志，绝不影响求解路径
- 隐私：只记路线级诊断（stderr 末段再截断），不落 token/certifyId 物料
- 口径：ok = 该次尝试是否产出合法形态的 param（certifyId 去重属下游池
  状态问题，不影响本次 solver 层成败判定）
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import logs, settings

KEEP = 200
# 磁盘历史保留天数（与 reqlog 对齐；超出按天文件整体删除）
HISTORY_DAYS = 30
# 单条 err 截断上限（stderr 末段已截 300，这里再收紧）
_ERR_LIMIT = 160

_BJ = timezone(timedelta(hours=8))
_PREFIX = "captchalog-"

_lock = threading.Lock()
_entries: deque = deque(maxlen=KEEP)
_last_prune_day = ""


def _data_dir() -> Path:
    return Path(settings.DATA_DIR)


def record(*, route: str, runtime: str, ok: bool, dur_ms: int,
           attempt: int = 1, region: str | None = None,
           err: str | None = None) -> None:
    """记一次求解尝试。route=solver 文件名，runtime=解释器名（bun/node）。"""
    entry = {
        "ts": time.time(),
        "route": route,
        "runtime": runtime,
        "ok": bool(ok),
        "attempt": int(attempt),
        "dur_ms": int(dur_ms),
        "region": region,
        "err": ((err or "").strip()[:_ERR_LIMIT]) or None,
    }
    with _lock:
        _entries.append(entry)
        _append_locked(entry)


def snapshot() -> list[dict]:
    """内存近期明细（重启清零）。"""
    with _lock:
        return list(_entries)


def history(days: int = 7) -> dict:
    """按路线聚合近 days 天：尝试数/成功/成功率/平均耗时 + 逐日分布。

    服务端聚合（与 reqlog.history 同思路），调用方拿统计不拿明细。
    """
    days = max(1, min(days, HISTORY_DAYS))
    data_dir = _data_dir()
    now = datetime.now(_BJ)

    routes: dict[tuple[str, str], dict] = {}
    by_day: dict[str, dict[str, dict]] = {}

    for i in range(days - 1, -1, -1):
        day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        path = data_dir / f"{_PREFIX}{day}.jsonl"
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as err:
            logs.warn("captchalog", f"读取失败 {path.name}: {err}")
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            route = str(row.get("route") or "?")
            runtime = str(row.get("runtime") or "?")
            ok = bool(row.get("ok"))
            dur = row.get("dur_ms") if isinstance(row.get("dur_ms"), (int, float)) else 0
            agg = routes.setdefault((route, runtime), {
                "route": route, "runtime": runtime, "attempts": 0, "ok": 0,
                "dur_sum": 0, "last_ts": 0.0,
            })
            agg["attempts"] += 1
            agg["ok"] += 1 if ok else 0
            agg["dur_sum"] += dur
            agg["last_ts"] = max(agg["last_ts"], float(row.get("ts") or 0))
            day_agg = by_day.setdefault(day, {}).setdefault(route, {"attempts": 0, "ok": 0})
            day_agg["attempts"] += 1
            day_agg["ok"] += 1 if ok else 0

    out_routes = []
    for agg in routes.values():
        attempts = agg["attempts"]
        out_routes.append({
            "route": agg["route"],
            "runtime": agg["runtime"],
            "attempts": attempts,
            "ok": agg["ok"],
            "fail": attempts - agg["ok"],
            "success_rate": round(agg["ok"] / attempts, 4) if attempts else 0.0,
            "avg_ms": round(agg["dur_sum"] / attempts) if attempts else 0,
            "last_ts": agg["last_ts"] or None,
        })
    out_routes.sort(key=lambda r: -r["attempts"])
    return {"days": days, "routes": out_routes, "by_day": by_day}


def _append_locked(entry: dict) -> None:
    global _last_prune_day
    try:
        data_dir = _data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        day = datetime.now(_BJ).strftime("%Y-%m-%d")
        path = data_dir / f"{_PREFIX}{day}.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if _last_prune_day != day:
            _prune(data_dir, day)
            _last_prune_day = day
    except OSError as err:
        logs.warn("captchalog", f"落盘失败: {err}")


def _prune(data_dir: Path, today: str) -> None:
    """删除超过保留期的按天文件（best-effort）。"""
    try:
        cutoff = (datetime.now(_BJ) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
        for path in data_dir.glob(f"{_PREFIX}*.jsonl"):
            day = path.name[len(_PREFIX):-len(".jsonl")]
            if day < cutoff:
                path.unlink(missing_ok=True)
    except OSError as err:
        logs.warn("captchalog", f"清理失败: {err}")
