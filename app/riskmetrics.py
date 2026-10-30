"""风控指标 —— 风控/限流/连接失败/波次事件的按天磁盘统计（后台 API 数据源）。

设计要点（与 reqlog 同构）：
- 事件层：风控命中（HTTP 405 / 业务码 3012）、429 限流换号、连接失败
  （独立 net 计数，2026-10-30）、频道级波次确认，各自 best-effort 追加一行
  JSONL 到 data/risk-<北京日期>.jsonl
- 按天分文件、保留 HISTORY_DAYS 天自动清理
- 纯观测：不触碰账号状态与调度逻辑；落盘失败只记日志，绝不影响请求
- 用途：回答「405 占多少 / 哪个账号被风控最多 / 波次多频繁」这类运营问题，
  不再依赖 grep 日志
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import logs, settings

HISTORY_DAYS = 30
MAX_DAYS = 90            # API 查询上限（与保留天数对齐）
_BJ = timezone(timedelta(hours=8))
_PREFIX = "risk-"

_hist_lock = threading.Lock()
_last_prune_day = ""

_KINDS = ("risk", "rate", "net", "wave")


def record(kind: str, account: str = "", **fields) -> None:
    """记一条风控相关事件（best-effort，失败不影响请求）。

    kind: risk（上游风控命中）/ rate（429 限流换号）/ net（连接失败）/
    wave（频道级波次确认）。fields 随事件类型不同：signal/lane/strikes、
    model、err、provider 等；值一律截断防刷盘。
    """
    if kind not in _KINDS:
        return
    row = {"ts": round(time.time(), 3), "kind": kind, "account": str(account or "")}
    for k, v in fields.items():
        if v is None:
            continue
        row[k] = str(v)[:120]
    _persist(row)


def aggregate(days: int = 7) -> dict:
    """按天聚合近 days 天的风控指标（升序，末位为今天）。"""
    try:
        days = int(days)
    except (TypeError, ValueError):
        days = 7
    days = max(1, min(days, MAX_DAYS))
    out: list[dict] = []
    for day in _recent_days(days):
        buckets: dict[str, Counter] = {k: Counter() for k in _KINDS}
        by_signal: Counter = Counter()
        by_account: Counter = Counter()
        path = _day_path(day)
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        try:
                            row = json.loads(line)
                        except ValueError:
                            continue
                        kind = row.get("kind")
                        if kind in buckets:
                            buckets[kind][row.get("account") or ""] += 1
                        if kind == "risk":
                            by_signal[str(row.get("signal") or "?")] += 1
                        if kind in ("risk", "net"):
                            by_account[row.get("account") or ""] += 1
            except OSError as err:
                logs.warn("riskmetrics", f"读取失败（忽略）: {path.name} {err}")
        out.append({
            "date": day,
            "risk": sum(buckets["risk"].values()),
            "rate": sum(buckets["rate"].values()),
            "net": sum(buckets["net"].values()),
            "wave": sum(buckets["wave"].values()),
            "risk_by_signal": dict(by_signal.most_common()),
            "risk_net_by_account": dict(by_account.most_common(20)),
        })
    return {"days": out, "history_days": HISTORY_DAYS,
            "generated_at": round(time.time(), 3)}


# ── 磁盘层（与 reqlog 同构）───────────────────────────────────────────────
def _day_path(day: str) -> Path:
    return Path(settings.DATA_DIR) / f"{_PREFIX}{day}.jsonl"


def _recent_days(days: int) -> list[str]:
    now = datetime.now(_BJ)
    return [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _maybe_prune(data_dir: Path) -> None:
    global _last_prune_day
    today = datetime.now(_BJ).strftime("%Y-%m-%d")
    if _last_prune_day == today:
        return
    _last_prune_day = today
    cutoff = (datetime.now(_BJ) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    for path in data_dir.glob(f"{_PREFIX}*.jsonl"):
        day = path.name[len(_PREFIX):-len(".jsonl")]
        if day < cutoff:
            try:
                path.unlink()
            except OSError as err:
                logs.warn("riskmetrics", f"历史文件清理失败（忽略）: {path.name} {err}")


def _persist(row: dict) -> None:
    try:
        data_dir = Path(settings.DATA_DIR)
        data_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        day = datetime.now(_BJ).strftime("%Y-%m-%d")
        with _hist_lock:
            _maybe_prune(data_dir)
            with open(data_dir / f"{_PREFIX}{day}.jsonl", "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as err:  # noqa: BLE001 - 记账失败不能影响请求
        try:
            logs.warn("riskmetrics", f"落盘失败（不影响请求）: {err}")
        except Exception:
            pass
