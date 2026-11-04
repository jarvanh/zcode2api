"""额度节奏外推 —— 「几点打光 / 几点回血」的诚实口径。

为什么不能简单用 `remaining / 每秒消耗` 给一个「耗尽时刻」：

1. **两套账**（2026-10-31 线上实证）：billing/balance 窗口的 `remaining` 与
   messages 实际可发额度不是一回事 —— 账号可以 `remaining=2.1 亿` 却已被标
   `exhausted` 并回 1005。拿 remaining 做分子等于假精度。
2. **`expires_at` 不可信**：实测它早于真正重置时刻（北京 23:00 日窗），
   不能当回血时间用；日窗重置由 `_quota_reset_at` 的运营兜底给。
3. **usage 明细无 credit**：只有 token 数，反推不出额度消耗速率。

因此本模块只做三件能被证据支撑的事：
  - **回血时刻**（可信）：日窗 23:00 北京 + plans 里待生效套餐的 starts_at，
    取最早；这是运营事实，不是预测。
  - **消耗速率**（有条件的真值）：对同一账号的 `quota` 快照做时间序列差分
    （`used` 增量 / 两次 `quota_verified_at` 间隔）。要求 ≥2 个快照、同窗口、
    used 递增、间隔合理；不满足就 `insufficient_data`，不猜。
  - **耗尽外推**：仅当「速率有效 + 快照新鲜 + 账本一致（未被 exhausted 矛盾
    打脸）」三者同时成立才给 ETA；否则明确标 `unreliable` 并附原因。

快照怎么来的：quota 刷新时 `app/quota.py` 会更新 `quota_verified_at`；本模块
在每次被查询时把当前 (verified_at, used, remaining, expires_at) 记入
`data/quota-history-<北京日>.jsonl`（一行一快照，按 (账号, 模型, verified_at)
去重），随 HISTORY_DAYS 清理。纯观测：不触碰账号状态、不触发额外 billing 请求。
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import settings

_BJ = timezone(timedelta(hours=8))
_PREFIX = "quota-history-"
_HISTORY_DAYS = 30
_FRESH_SEC = 900.0         # 快照超过 15min 视为陈旧，不给 ETA
_MIN_GAP = 60.0            # 两次快照至少隔 1min，否则抖动被放大成速率

_lock = threading.Lock()
_hist_lock = threading.Lock()


def _data_dir() -> Path:
    return Path(settings.DATA_DIR)


def _day_str(ts: float | None = None) -> str:
    return (datetime.now(_BJ) if ts is None
            else datetime.fromtimestamp(ts, _BJ)).strftime("%Y-%m-%d")


def _recent_days(days: int) -> list[str]:
    now = datetime.now(_BJ)
    return [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _prune(data_dir: Path) -> None:
    cutoff = (datetime.now(_BJ) - timedelta(days=_HISTORY_DAYS)).strftime("%Y-%m-%d")
    for f in data_dir.glob(f"{_PREFIX}*.jsonl"):
        day = f.name[len(_PREFIX):-len(".jsonl")]
        if day < cutoff:
            try:
                f.unlink()
            except OSError:
                pass


def observe(accounts: list) -> int:
    """登记当前 quota 快照（管理端点每次查询调一次）。返回新增行数。

    去重键 = (账号名, 模型名, quota_verified_at)：同一次刷新被多个端点/多次
    查询看到时只记一行，不然速率会被重复采样压歪。
    """
    if not accounts:
        return 0
    day = _day_str()
    path = _data_dir() / f"{_PREFIX}{day}.jsonl"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        seen: set[tuple] = set()
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                seen.add((r.get("acc"), r.get("model"), r.get("v")))
        except OSError:
            pass
        rows = []
        for a in accounts:
            verified = a.quota_verified_at
            if not isinstance(verified, (int, float)) or verified <= 0:
                continue
            for model, w in (a.quota or {}).items():
                if not isinstance(w, dict):
                    continue
                key = (a.name, str(model), round(float(verified), 3))
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"acc": a.name, "model": str(model), "v": round(float(verified), 3),
                             "used": w.get("used"), "rem": w.get("remaining"),
                             "exp": w.get("expires_at"), "status": str(a.status)})
        if not rows:
            return 0
        with _hist_lock:
            _prune(path.parent)
            with open(path, "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
        return len(rows)
    except Exception:  # noqa: BLE001 - 纯观测绝不影响管理端点
        return 0


def _read_rows(days: int) -> list[dict]:
    out: list[dict] = []
    for day in _recent_days(days):
        f = _data_dir() / f"{_PREFIX}{day}.jsonl"
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
        except OSError:
            continue
    return out


def _refill_points(accounts: list, now: float) -> list[dict]:
    """回血候选时刻：复用网关同一套口径（app/quota_windows.py），不自己算时区。

    日窗 23:00 与待生效套餐两个来源都给出来（面板要能看出「为什么是这个
    时刻」），按时间升序；最终承诺时刻 = 两者取最早，由 quota_windows 判定。
    """
    from .quota_windows import bj_today_at, pending_plan_start, quota_reset_at

    points = [{"at": bj_today_at(23, 0, now), "kind": "day_reset",
               "label": "北京 23:00 日窗重置"}]
    plan_at = pending_plan_start(accounts, now)
    if plan_at is not None:
        points.append({"at": plan_at, "kind": "plan_starts",
                       "label": "已领待生效套餐（早于日窗则以此为准）"})
    points.sort(key=lambda x: x["at"])
    committed = quota_reset_at(accounts, now)
    for p in points:
        p["committed"] = abs(p["at"] - committed) < 1
    return points


def _rate(rows: list[dict], now: float) -> dict:
    """同 (账号,模型) 的 used 差分速率；证据不足时明确标注原因。"""
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        if not isinstance(r.get("used"), (int, float)) or not isinstance(r.get("v"), (int, float)):
            continue
        groups.setdefault((r.get("acc"), r.get("model")), []).append(r)
    rates = []
    for (acc, model), items in groups.items():
        items.sort(key=lambda x: x["v"])
        if len(items) < 2:
            rates.append({"account": acc, "model": model, "rate": None,
                          "reason": "快照不足（需≥2 次刷新）"})
            continue
        # 不能死取最后两帧：forecast() 每次查询都会 observe() 追加当前快照，
        # 而 quota_verified_at 只在真正刷新时才变 —— 同一帧可能被重复记录，
        # 末两帧 used 相等就把有效历史速率误判成「间隔过短/未递增」。
        # 从最新往前扫，取第一组满足 gap≥_MIN_GAP 且 used 递增的连续快照。
        found = None
        for i in range(len(items) - 1, 0, -1):
            a, b = items[i - 1], items[i]
            gap = float(b["v"]) - float(a["v"])
            delta = float(b["used"]) - float(a["used"])
            if gap >= _MIN_GAP and delta > 0:
                found = (a, b, gap, delta)
                break
        if found is None:
            rates.append({"account": acc, "model": model, "rate": None,
                          "reason": "无有效差分（间隔过短或 used 未递增）"})
            continue
        _a, b, gap, delta = found
        rates.append({"account": acc, "model": model, "rate": round(delta / gap, 2),
                      "unit": "used/秒", "samples": len(items),
                      "last_v": b["v"], "last_rem": b.get("rem"),
                      "last_status": b.get("status")})
    return {"per_account": sorted(rates, key=lambda r: (-(r["rate"] or 0), r["account"]))}


def forecast(accounts: list, days: int = 3) -> dict:
    """额度节奏视图：回血时刻（可信）+ 消耗速率 + 有条件 ETA。

    `eta_reliable` 为 False 时 `eta` 必为 None —— 宁可不给数，也不拿两套账
    的 remaining 造一个看似精确、实则误导的「几点打光」。
    """
    now = time.time()
    days = max(1, min(int(days or 1), _HISTORY_DAYS))
    observe(accounts)                       # 每次查询顺带登记当前快照
    rows = _read_rows(days)
    rate = _rate(rows, now)
    refills = _refill_points(accounts, now)

    usable = [r for r in rate["per_account"] if r.get("rate")]
    fresh = [r for r in usable if now - float(r.get("last_v") or 0) <= _FRESH_SEC]
    # 账本一致性：任一 active 之外的号仍持有大量 remaining = 两套账矛盾
    contradictory = any(
        str(a.status) == "exhausted" and
        any(isinstance(w, dict) and (w.get("remaining") or 0) > 0 for w in (a.quota or {}).values())
        for a in accounts
    )
    eta_reliable = bool(fresh) and not contradictory
    eta = None
    eta_basis = None
    if eta_reliable:
        # 用「最热的号、最近 15min 内快照」的 remaining / 该号速率估 ETA
        cand = max(fresh, key=lambda r: r["rate"])
        rem = cand.get("last_rem")
        if isinstance(rem, (int, float)) and rem > 0 and cand["rate"] > 0:
            eta = now + float(rem) / cand["rate"]
            eta_basis = f"{cand['account']}/{cand['model']} · remaining/速率"
    reasons = []
    if not usable:
        reasons.append("尚无可信消耗速率（需至少 2 次 quota 刷新快照，间隔≥60s）")
    elif not fresh:
        reasons.append("最近快照已超过 15 分钟，速率不足以外推")
    if contradictory:
        reasons.append("账本不一致：存在 exhausted 账号仍显示 remaining>0（balance 与 grant 两套账），拒绝外推")

    return {
        "generated_at": now,
        "days": days,
        "snapshots": len(rows),
        "refills": [{**r, "in_seconds": round(r["at"] - now)} for r in refills],
        # 承诺时刻由 quota_windows 判定（= earliest），不是简单取列表第一项
        "next_refill": next((r for r in refills if r.get("committed")), refills[0] if refills else None),
        "rates": rate,
        "eta": eta,
        "eta_basis": eta_basis,
        "eta_reliable": eta_reliable,
        "reasons": reasons,
        "note": "回血时刻是运营事实；耗尽 ETA 仅在快照新鲜且账本一致时才给，否则明确不可靠",
    }
