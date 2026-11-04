"""额度窗口时刻的共享真源（北京时区）。

以前 `_quota_reset_at` / `_bj_today_at` 定义在 routes/gateway.py 里，任何
「几点回血」类观测想复用就得 import 网关（循环依赖）或自己抄一份 —— 抄
一份就会漏掉「starts_at 可能是毫秒」「当天 23:00 已过要取次日」这两个
线上实证过的坑。本模块把口径收口成两个纯函数，网关与 forecast 共同引用。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

_BJ = timezone(timedelta(hours=8))


def bj_today_at(hour: int, minute: int, now: float) -> float:
    """北京时区当天 hour:minute 的 epoch；该时刻已过则取次日同一时刻。"""
    local = datetime.fromtimestamp(now, _BJ)
    target = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target.timestamp() if target.timestamp() > now else target.timestamp() + 86400


def pending_plan_start(accounts: list, now: float) -> float | None:
    """plans[].entitlements[] 里最早的未来生效时刻（含毫秒归一），无则 None。

    上游秒/毫秒都可能下发（claim 载荷即做了 ms 归一），统一到秒；只看
    future（当前已生效的套餐不构成「回血时刻」）。
    """
    pending: list[float] = []
    for a in accounts:
        plans = ([a.plan] if isinstance(getattr(a, "plan", None), dict) and a.plan else []) \
            + list(getattr(a, "plans", None) or [])
        for plan in plans:
            for ent in (plan or {}).get("entitlements") or []:
                start = ent.get("starts_at") or ent.get("effective_at") or 0
                try:
                    start = float(start)
                except (TypeError, ValueError):
                    continue
                start = start / 1000 if start > 1e12 else start
                if start > now:
                    pending.append(start)
    return min(pending) if pending else None


def quota_reset_at(accounts: list, now: float) -> float:
    """额度耗尽后的承诺恢复时刻：默认当天 23:00 北京，待生效套餐更早则用之。"""
    pending = pending_plan_start(accounts, now)
    today_reset = bj_today_at(23, 0, now)
    return pending if pending is not None and pending < today_reset else today_reset
