"""请求监控 —— 网关请求的实时环形日志 + 磁盘历史（管理端「请求监控」页数据源）。

设计要点：
- 实时层：内存 deque 上限 500 条（含在途），重启即清零，供「实时」页秒级观测
- 历史层：请求收口（成功/失败）时 best-effort 追加一行 JSONL 到
  data/reqlog-<北京日期>.jsonl，按天分文件、保留 HISTORY_DAYS 天自动清理，
  供「7 天 / 30 天」视图聚合 —— 补上内存日志「重启即清零、看不了趋势」的缺口
- 只落终态条目：在途条目（ok=None）不落盘，避免进程被杀留下永久幽灵行
- 粒度：一次客户端请求一条；多账号重试不分裂，账号字段记录最终归宿
- ok 三态：None=在途/未知，True=成功，False=失败（含客户端断开 status=499）
- 纯观测：不触碰账号状态与调度逻辑；落盘失败只记日志，绝不影响请求
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import logs, settings

KEEP = 500
# 磁盘历史保留天数（与前端「30 天」视图上限对齐，超出按天文件整体删除）
HISTORY_DAYS = 30
# 聚合分组回传条数上限（账号/模型可能很多，超过按调用量截断）
TOP_GROUPS = 50
TOP_ERRORS = 8

_BJ = timezone(timedelta(hours=8))
_PREFIX = "reqlog-"

_lock = threading.Lock()
_entries: deque = deque(maxlen=KEEP)
_inflight: dict[str, dict] = {}

_hist_lock = threading.Lock()
_last_prune_day = ""


def begin(req_id: str, endpoint: str, model: str, stream: bool, preview: str = "") -> None:
    """请求进入网关（鉴权通过、body 解析成功后）。"""
    entry = {
        "req_id": req_id,
        "ts": time.time(),
        "endpoint": endpoint,
        "model": model,
        "stream": bool(stream),
        "preview": (preview or "")[:80],
        "account": "",
        "mode": "",
        "ok": None,
        "status": None,
        "error": "",
        "t_first": None,
        "t_total": None,
        "input_tokens": None,
        "output_tokens": None,
    }
    with _lock:
        _entries.append(entry)
        _inflight[req_id] = entry


def mark_account(req_id: str, account_name: str, mode: str) -> None:
    """记录实际服务该请求的账号（多账号重试时最后一次生效）。"""
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["account"] = account_name
            entry["mode"] = mode


def finish_ok(req_id: str, t_first: float | None = None,
              input_tokens: int | None = None, output_tokens: int | None = None,
              status: int | None = None) -> None:
    with _lock:
        entry = _inflight.pop(req_id, None)
        if entry is None:
            return
        entry["ok"] = True
        entry["status"] = status or 200
        entry["t_first"] = t_first
        entry["t_total"] = time.time() - entry["ts"]
        entry["input_tokens"] = input_tokens
        entry["output_tokens"] = output_tokens
        row = dict(entry)
    _persist(row)


def finish_error(req_id: str, error: str, status: int | None = None,
                 t_first: float | None = None) -> None:
    with _lock:
        entry = _inflight.pop(req_id, None)
        if entry is None:
            return
        entry["ok"] = False
        entry["status"] = status
        entry["error"] = (error or "")[:200]
        entry["t_first"] = t_first
        entry["t_total"] = time.time() - entry["ts"]
        row = dict(entry)
    _persist(row)


def snapshot() -> list[dict]:
    """全部条目，最新在前（含在途）。"""
    with _lock:
        return [dict(e) for e in reversed(_entries)]


def clear() -> None:
    """清空内存实时层（磁盘历史不受影响）。"""
    with _lock:
        _entries.clear()
        _inflight.clear()


# ── 磁盘历史 ─────────────────────────────────────────────────────────────────
def _hist_dir() -> Path:
    return Path(settings.DATA_DIR)


def _recent_days(days: int) -> list[str]:
    """近 days 天的北京日期列表（升序，末位为今天）。"""
    now = datetime.now(_BJ)
    return [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _maybe_prune(data_dir: Path) -> None:
    """按天清理过期历史文件（每个自然日最多一次）。"""
    global _last_prune_day
    today = datetime.now(_BJ).strftime("%Y-%m-%d")
    if _last_prune_day == today:
        return
    _last_prune_day = today
    cutoff = (datetime.now(_BJ) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    for path in data_dir.glob(f"{_PREFIX}*.jsonl"):
        day = path.name[len(_PREFIX):-len(".jsonl")]
        if day < cutoff:  # 日期字符串字典序 == 时间序
            try:
                path.unlink()
            except OSError as err:
                logs.warn("reqlog", f"历史文件清理失败（忽略）: {path.name} {err}")


def _persist(entry: dict) -> None:
    """终态条目落盘（best-effort）。"""
    try:
        data_dir = _hist_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        row = {
            "ts": round(entry.get("ts") or 0, 3),
            "ok": entry.get("ok"),
            "status": entry.get("status"),
            "account": entry.get("account") or "",
            "mode": entry.get("mode") or "",
            "model": entry.get("model") or "",
            "endpoint": entry.get("endpoint") or "",
            "stream": bool(entry.get("stream")),
            "t_first": entry.get("t_first"),
            "t_total": entry.get("t_total"),
            "in": entry.get("input_tokens"),
            "out": entry.get("output_tokens"),
            # 历史只留错误摘要（明细 preview 含用户内容，不进磁盘）
            "err": (entry.get("error") or "")[:160],
        }
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        day = datetime.now(_BJ).strftime("%Y-%m-%d")
        with _hist_lock:
            _maybe_prune(data_dir)
            with open(data_dir / f"{_PREFIX}{day}.jsonl", "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as err:  # noqa: BLE001 - 记账失败不能影响请求
        try:
            logs.warn("reqlog", f"请求历史落盘失败（不影响请求）: {err}")
        except Exception:
            pass


def _new_bucket(key: str = "") -> dict:
    return {"name": key, "calls": 0, "ok": 0, "fail": 0, "in": 0, "out": 0,
            "ttfb_sum": 0.0, "ttfb_n": 0, "last": 0.0}


def _acc(bucket: dict, row: dict) -> None:
    bucket["calls"] += 1
    ok = row.get("ok")
    if ok is True:
        bucket["ok"] += 1
    elif ok is False:
        bucket["fail"] += 1
    bucket["in"] += int(row.get("in") or 0)
    bucket["out"] += int(row.get("out") or 0)
    tf = row.get("t_first")
    if isinstance(tf, (int, float)):
        bucket["ttfb_sum"] += float(tf)
        bucket["ttfb_n"] += 1
    ts = row.get("ts") or 0
    if ts > bucket["last"]:
        bucket["last"] = ts


def _finalize(bucket: dict) -> dict:
    done = bucket["ok"] + bucket["fail"]
    return {
        "name": bucket["name"],
        "calls": bucket["calls"],
        "ok": bucket["ok"],
        "fail": bucket["fail"],
        "rate": round(bucket["ok"] / done * 100, 1) if done else None,
        "in": bucket["in"],
        "out": bucket["out"],
        "ttfb": round(bucket["ttfb_sum"] / bucket["ttfb_n"], 3) if bucket["ttfb_n"] else None,
        "ttfb_n": bucket["ttfb_n"],
        "last": bucket["last"] or None,
    }


def _top(buckets: dict[str, dict]) -> list[dict]:
    rows = [_finalize(b) for b in buckets.values()]
    rows.sort(key=lambda r: (-r["calls"], r["name"]))
    return rows[:TOP_GROUPS]


def history(days: int = 7) -> dict:
    """磁盘历史聚合（近 days 天，含今天）。

    只回传聚合结果：30 天可达数万条明细，全量回传既拖慢页面也无额外信息
    （明细由实时页覆盖）。文件按天读，缺失的天补零 —— 保证曲线连续。
    """
    days = max(1, min(int(days or 1), HISTORY_DAYS))
    day_list = _recent_days(days)
    data_dir = _hist_dir()

    daily: dict[str, dict] = {d: _new_bucket(d) for d in day_list}
    accounts: dict[str, dict] = {}
    models: dict[str, dict] = {}
    errors: dict[str, int] = {}
    total = _new_bucket()

    for day in day_list:
        path = data_dir / f"{_PREFIX}{day}.jsonl"
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
                continue  # 半行写入（进程被杀）直接跳过，不拖垮整页
            _acc(total, row)
            _acc(daily[day], row)
            acc_key = row.get("account") or "—"
            mdl_key = row.get("model") or "—"
            accounts.setdefault(acc_key, _new_bucket(acc_key))
            models.setdefault(mdl_key, _new_bucket(mdl_key))
            _acc(accounts[acc_key], row)
            _acc(models[mdl_key], row)
            if row.get("ok") is False:
                err = (row.get("err") or f"HTTP {row.get('status') or '未知'}")[:80] or "失败"
                errors[err] = errors.get(err, 0) + 1

    totals = _finalize(total)
    return {
        "days": days,
        "from": day_list[0],
        "to": day_list[-1],
        "totals": totals,
        "daily": [_finalize(daily[d]) for d in day_list],
        "accounts": _top(accounts),
        "models": _top(models),
        "errors": [{"error": e, "count": c} for e, c in
                   sorted(errors.items(), key=lambda kv: -kv[1])[:TOP_ERRORS]],
    }
