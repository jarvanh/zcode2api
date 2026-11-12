"""v2.9.40 候选生成器命名变体覆盖（-global 后缀，2026-11-09 实证新增）。

背景：当日主 id 上游 1001（不存在）而 `…-1109-global` 出现 429（有响应）——
上游命名规律出现新后缀，猜测器不覆盖就漏掉整个白天投放窗口。
"""

from __future__ import annotations

import time

from app.claim import _date_plan_candidates


def _ts_for_wday(mon: int, day: int, year: int, hour: int = 12):
    """构造指定日期（本地时区）的 epoch，用于测 wday 分支。"""

    return time.mktime((year, mon, day, hour, 0, 0, 0, 0, -1))


def test_global_variant_always_present():
    """-global 变体是常规档：任何星期都在候选里。"""
    cands = _date_plan_candidates()
    today = time.strftime("%m%d")
    assert f"zcode-v3-start-plan-{today}" in cands
    assert f"zcode-v3-start-plan-{today}-global" in cands
    # 去重
    assert len(cands) == len(set(cands))


def test_weekday_no_wk_variants():
    """周一~周四：无 -wk 变体（周末档才有）。"""
    ts = _ts_for_wday(11, 9, 2026)  # 2026-11-09 是周一
    assert time.localtime(ts).tm_wday == 0
    cands = _date_plan_candidates(ts)
    assert cands == ["zcode-v3-start-plan-1109", "zcode-v3-start-plan-1109-global"]


def test_weekend_has_wk_variants():
    """周五~周日：-wk 前后缀变体 + -global 都在。"""
    ts = _ts_for_wday(11, 6, 2026)  # 2026-11-06 是周五
    assert time.localtime(ts).tm_wday == 4
    cands = _date_plan_candidates(ts)
    assert "zcode-v3-start-plan-1106" in cands
    assert "zcode-v3-start-plan-1106-wk" in cands
    assert "zcode-v3-start-plan-wk-1106" in cands
    assert "zcode-v3-start-plan-1106-global" in cands


def test_known_shape_regression():
    """历史实证形态不回归：11-02（周一）主 id + 11-08（周日）三件套。"""
    cands = _date_plan_candidates(_ts_for_wday(11, 2, 2026))
    assert "zcode-v3-start-plan-1102" in cands
    assert not any("-wk" in c for c in cands)
    cands = _date_plan_candidates(_ts_for_wday(11, 8, 2026))
    assert "zcode-v3-start-plan-1108-wk" in cands
    assert "zcode-v3-start-plan-wk-1108" in cands
