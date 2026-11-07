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

_KINDS = ("risk", "rate", "net", "wave", "edge", "busy")

# 业务码/文样 → 归因标签（控制台「风控归因」用）：ip = 出口地址问题（波次/
# WAF 封禁）；account = 账号身份问题（3012 风控）；busy = 并发槽争用
# （3008/3009/3010，天然恢复，不 penalty）；rate = 真频控（1302/1303）。
_ATTR_CARRIER = {"risk": "account", "wave": "ip", "edge": "ip", "busy": "busy", "rate": "rate", "net": "net"}


def record(kind: str, account: str = "", **fields) -> None:
    """记一条风控相关事件（best-effort，失败不影响请求）。

    kind: risk（账号级上游风控）/ edge（IP/出口级 WAF 拦截）/
    rate（429 真频控）/ busy（3008/3009/3010 并发槽饱和、BUSY_HOLD 全池刹车）/
    net（连接失败）/ wave（IP 级波次确认）。
    fields 随事件类型不同：signal/lane/strikes、carrier、egress、
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
    """按天聚合近 days 天的风控指标（升序，末位为今天）。

    除裸计数外保留归因维度（控制台「风控归因」区块数据源）：
    - carrier_by_kind：每类事件的载体分布（ip/account/busy/rate）
    - rate_by_code：429 失败的 HTTP 状态分布（结合 kind 区分并发饱和/真频控）
    - recent：当天最后 8 条非 rate 事件（含 ts/账号/归因字段，控制台直接展示）
    """
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
        carrier_by_kind: dict[str, Counter] = {}
        rate_by_code: Counter = Counter()
        recent: list[dict] = []
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
                        if kind not in buckets:
                            continue
                        buckets[kind][row.get("account") or ""] += 1
                        if kind == "risk":
                            by_signal[str(row.get("signal") or "?")] += 1
                        if kind in ("risk", "net"):
                            by_account[row.get("account") or ""] += 1
                        # 归因载体：事件自带 carrier 优先，否则按 kind 推导
                        carrier = str(row.get("carrier") or _ATTR_CARRIER.get(kind, ""))
                        carrier_by_kind.setdefault(kind, Counter())[carrier] += 1
                        # 最近事件（rate 量大且无归因价值，不占位）
                        if kind != "rate":
                            recent.append({
                                "ts": row.get("ts"), "kind": kind,
                                "account": row.get("account") or "",
                                "carrier": carrier,
                                "signal": row.get("signal") or "",
                                "level": row.get("level") or "",
                                "pause": row.get("pause") or "",
                                "status": row.get("status") or "",
                                "egress": row.get("egress") or "",
                                "model": row.get("model") or "",
                            })
                        if kind == "rate":
                            rate_by_code[str(row.get("status") or "429")] += 1
            except OSError as err:
                logs.warn("riskmetrics", f"读取失败（忽略）: {path.name} {err}")
        out.append({
            "date": day,
            "risk": sum(buckets["risk"].values()),
            "rate": sum(buckets["rate"].values()),
            "net": sum(buckets["net"].values()),
            "wave": sum(buckets["wave"].values()),
            "edge": sum(buckets["edge"].values()),
            "busy": sum(buckets["busy"].values()),
            "risk_by_signal": dict(by_signal.most_common()),
            "risk_net_by_account": dict(by_account.most_common(20)),
            "carrier_by_kind": {k: dict(v) for k, v in carrier_by_kind.items()},
            "rate_by_code": dict(rate_by_code.most_common()),
            "recent": recent[-8:],
        })
    return {"days": out, "history_days": HISTORY_DAYS,
            "generated_at": round(time.time(), 3)}


def recent_events(hours: float = 6.0, limit: int = 50) -> list[dict]:
    """滑窗读取最近 hours 小时的事件（跨北京日期文件，新→旧）。

    控制台「风控归因」用：北京日历日在午夜后会清零，滑窗才能回答
    「刚才/今晚到底发生了什么」。rate 量大且无归因价值，默认剔除。
    """
    cutoff = time.time() - max(0.0, float(hours)) * 3600
    out: list[dict] = []
    for day in _recent_days(2):
        path = _day_path(day)
        if not path.exists():
            continue
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    ts = float(row.get("ts") or 0)
                    if ts < cutoff or row.get("kind") == "rate":
                        continue
                    out.append(row)
        except OSError as err:
            logs.warn("riskmetrics", f"滑窗读取失败（忽略）: {path.name} {err}")
    out.sort(key=lambda r: float(r.get("ts") or 0), reverse=True)
    return out[:max(1, int(limit))]


def window_counts(hours: float = 6.0) -> dict:
    """滑窗按 kind/carrier 计数（与 recent_events 同源，供归因层用）。"""
    counts: Counter = Counter()
    for row in recent_events(hours=hours, limit=2000):
        kind = str(row.get("kind") or "?")
        counts[kind] += 1
        carrier = str(row.get("carrier") or _ATTR_CARRIER.get(kind, ""))
        if carrier:
            counts[f"{kind}:{carrier}"] += 1
    return dict(counts)



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
