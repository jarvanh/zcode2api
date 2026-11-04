"""值守告警台账 —— 按北京日期落盘 `data/alertlog-*.jsonl`。

为什么需要：主机侧 cron 脚本的去重标记在 /tmp（重启即丢），告警本身也只在
TG 消息与 cron 日志里，事后想答「最近告警是不是变多了 / 哪类规则最吵」
无从查起。本模块按天记一条「已发送的告警」，并提供 7/30 天聚合：
  - 逐日告警条数与规则分布（哪类在恶化）
  - 规则 TOP、最近告警时刻、静默时长
  - 与 conc/vitals 曲线同源：重启不清零，随 HISTORY_DAYS 清理

写入方：`POST /admin/api/monitoring/alerts`（值守脚本发送成功后调）。
刻意不记告警正文里的账号明细/数值（那是瞬时状态，且可能含敏感量），
只记规则 ID 集合 + 条数 + 时间戳；明细看 TG 消息与 cron 日志即可。

纯观测：任何落盘失败都吞掉并留痕，绝不影响值守脚本的发送结果。
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import settings

_BJ = timezone(timedelta(hours=8))
_PREFIX = "alertlog-"
_HISTORY_DAYS = 30

_lock = threading.Lock()


def _data_dir() -> Path:
    return Path(settings.DATA_DIR)


def _recent_days(days: int) -> list[str]:
    now = datetime.now(_BJ)
    return [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _prune(data_dir: Path) -> None:
    cutoff = (datetime.now(_BJ) - timedelta(days=_HISTORY_DAYS)).strftime("%Y-%m-%d")
    for f in data_dir.glob(f"{_PREFIX}*.jsonl"):
        d = f.name[len(_PREFIX):-len(".jsonl")]
        if d < cutoff:
            try:
                f.unlink()
            except OSError:
                pass


def record(rules: list[str] | None = None, *, dry_run: bool = False,
           delivered: bool = True) -> dict:
    """记一条已发送（或 dry-run）的告警。返回落盘行，供调用方核对。

    rules = 触发的规则 ID 集合（与值守脚本去重键同源，如 RISK/UP5XX/POOL）。
    dry_run 只用于本地自检，**不落盘**（不污染真实告警曲线）。
    """
    if dry_run:
        return {"recorded": False, "reason": "dry-run 不落盘"}
    now = time.time()
    day = datetime.fromtimestamp(now, _BJ).strftime("%Y-%m-%d")
    row = {"ts": round(now, 3), "day": day,
           "rules": sorted({str(r).strip() for r in (rules or []) if str(r).strip()}),
           "n": len(set(str(r).strip() for r in (rules or []) if str(r).strip())),
           "delivered": bool(delivered)}
    try:
        data_dir = _data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        with _lock:
            _prune(data_dir)
            with open(data_dir / f"{_PREFIX}{day}.jsonl", "a", encoding="utf-8") as f:
                f.write(line)
        return {"recorded": True, **row}
    except Exception as err:  # noqa: BLE001 - 台账失败绝不改变告警发送结果
        from . import logs
        logs.warn("alertlog", f"告警台账落盘失败（忽略）: {err}")
        return {"recorded": False, "reason": str(err)[:120]}


def history(days: int = 7) -> dict:
    """近 days 天告警聚合：逐日条数 + 规则分布 + TOP + 静默时长。"""
    days = max(1, min(int(days or 1), _HISTORY_DAYS))
    day_list = _recent_days(days)
    daily: dict[str, dict] = {d: {"name": d, "alerts": 0, "rules": {}} for d in day_list}
    rule_counts: dict[str, int] = {}
    total = 0
    last_ts: float | None = None
    for day in day_list:
        f = _data_dir() / f"{_PREFIX}{day}.jsonl"
        try:
            lines = f.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            b = daily[day]
            b["alerts"] += 1
            total += 1
            ts = r.get("ts")
            if isinstance(ts, (int, float)) and (last_ts is None or ts > last_ts):
                last_ts = float(ts)
            for k in r.get("rules") or []:
                name = str(k)
                b["rules"][name] = b["rules"].get(name, 0) + 1
                rule_counts[name] = rule_counts.get(name, 0) + 1
    rows = [daily[d] for d in day_list]
    return {
        "days": days, "total": total,
        "daily": [{"name": r["name"], "alerts": r["alerts"],
                   "rules": [{"id": k, "count": n} for k, n in
                             sorted(r["rules"].items(), key=lambda kv: -kv[1])[:5]]}
                  for r in rows],
        "rules": [{"id": k, "count": n} for k, n in
                  sorted(rule_counts.items(), key=lambda kv: -kv[1])[:8]],
        "last_alert_at": last_ts,
        "silent_seconds": round(time.time() - last_ts) if last_ts else None,
        "note": "只记已发送告警的规则集合，不记正文/数值；dry-run 不落盘",
    }
