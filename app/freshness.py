"""system sidecar 文本新鲜度 —— 2026-10-31 WAF 文本校验波次的防复发哨兵。

背景：上游按当前官方版本逐字节校验 system 块1（cli_prefix）与块2（stable）
文本（见 body_transform 模块头）。sidecar 是手工跟进的镜像，官方升版后会
悄悄过时——405 在日志里只表现为风控波次，没人会第一时间怀疑到措辞
（2026-10-31 实弹定位：base 405 / 3.17.1 块1+2 + 3.16.3 块3 200）。

与既有防线的关系：
  - 措辞锁死单测（tests/unit/test_body_transform_system.py）挡「改错」；
  - main._warn_version_drift 挡「网关身份常量落后本机官方客户端」；
  - 本模块挡「sidecar 镜像落后官方」：版本比对 + captured_at 年龄 + 每日
    巡检落 meta 表（admin API / healthcheck 读），风控波次确认点调
    stale_hint() 叠加「高度疑似措辞触发」提示。

无远端「最新客户端版本」端点可查（configs 响应不含版本），故「过时」判定
用三类本地信号：sidecar 版本 ≠ 网关身份版本、本机官方客户端版本 ≠ sidecar
版本（检测得到才有此信号）、captured_at 年龄超阈值。任一命中即 stale。
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time

from . import constants, hostinfo, logs, settings
from .body_transform import _load_sections
from .store import store

_FRESHNESS_KEY = "sections_freshness"
_CHECK_INTERVAL = 6 * 3600  # 每日巡检（6h 步进取整日界，代价为零——纯本地读）

_task: asyncio.Task | None = None


def sections_meta() -> dict:
    """sidecar _meta（client_version / captured_at / source）；缺失返回 {}。"""
    sec = _load_sections() or {}
    meta = sec.get("_meta")
    return meta if isinstance(meta, dict) else {}


def status() -> dict:
    """计算当前新鲜度状态（纯本地读，零网络）。stale=True 表示存在任一问题。"""
    meta = sections_meta()
    sidecar_ver = str(meta.get("client_version") or "")
    captured_at = str(meta.get("captured_at") or "")
    local_ver = hostinfo.local_client_version() or ""
    now = datetime.datetime.now()

    problems: list[str] = []
    age_days: int | None = None
    if not sidecar_ver or not captured_at:
        problems.append("sidecar 缺 _meta(client_version/captured_at)，无法自证对齐版本")
    else:
        if sidecar_ver != constants.CLIENT_APP_VERSION:
            problems.append(
                f"sidecar 版本 {sidecar_ver} ≠ 网关身份版本 {constants.CLIENT_APP_VERSION}"
            )
        if local_ver and local_ver != sidecar_ver:
            problems.append(f"本机官方客户端 {local_ver} ≠ sidecar {sidecar_ver}，官方可能已升版")
    # billing 侧一致性（claim/billing 全链路用 BILLING_APP_VERSION）：只改一个
    # 常量的「半更新」正是 2026-10-31 那轮「领取失败/措辞过时」的组合病因
    if constants.BILLING_APP_VERSION != constants.CLIENT_APP_VERSION:
        problems.append(
            f"billing 版本 {constants.BILLING_APP_VERSION} ≠ 身份版本 "
            f"{constants.CLIENT_APP_VERSION}（两个常量须同步升）"
        )
    try:
        captured = datetime.datetime.strptime(captured_at, "%Y-%m-%d")
        age_days = max(0, (now - captured).days)
        if age_days > settings.SECTIONS_STALE_DAYS:
            problems.append(
                f"sidecar 已 {age_days} 天未跟进（阈值 {settings.SECTIONS_STALE_DAYS}）"
            )
    except ValueError:
        if captured_at:
            problems.append(f"sidecar captured_at 格式非法: {captured_at!r}")

    return {
        "sidecar_client_version": sidecar_ver,
        "captured_at": captured_at,
        "age_days": age_days,
        "gateway_app_version": constants.CLIENT_APP_VERSION,
        "billing_app_version": constants.BILLING_APP_VERSION,
        "local_client_version": local_ver,
        "stale": bool(problems),
        "problems": problems,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def check_and_record() -> dict:
    """巡检一次并落 meta 表（admin API / healthcheck 的数据源）。"""
    st = status()
    store.set_setting(_FRESHNESS_KEY, json.dumps(st, ensure_ascii=False))
    if st["stale"]:
        logs.warn("freshness", "system sidecar 文本疑似过时: " + "; ".join(st["problems"]))
    else:
        logs.info("freshness", f"sidecar 对齐官方 {st['sidecar_client_version']}"
                               f"（captured {st['captured_at']}）")
    return st


def refresh() -> dict:
    """现算一次并回写 meta 表，返回最新状态（admin 读路径调用）。

    与 check_and_record 的差别：status() 零网络纯本地读，现算代价为零，
    admin 轮询（30s 一轮）直接现算即可消掉「官方升版后最长 6h 巡检盲窗」
    （2026-11-02 实测：启动巡检 7 分钟后本机客户端自更新 3.17.1→3.17.2，
    控制台一直亮绿到下一跳）。回写让 stale_hint() 热路径与 healthcheck 同步
    吃到最新状态；日志只在状态翻转/问题集变化时打一次，否则 30s 轮询刷屏。
    """
    st = status()
    prev_raw = store.get_setting(_FRESHNESS_KEY)
    prev: dict | None = None
    if prev_raw:
        try:
            prev = json.loads(prev_raw)
        except (ValueError, TypeError):
            prev = None
    sig = (st["stale"], tuple(st["problems"]), st["sidecar_client_version"],
           st["local_client_version"])
    prev_sig = ((prev or {}).get("stale"), tuple((prev or {}).get("problems") or []),
                (prev or {}).get("sidecar_client_version"), (prev or {}).get("local_client_version"))
    if _strip_checked_at(st) != _strip_checked_at(prev):
        store.set_setting(_FRESHNESS_KEY, json.dumps(st, ensure_ascii=False))
    if prev is None or sig != prev_sig:
        if st["stale"]:
            logs.warn("freshness", "system sidecar 文本疑似过时: " + "; ".join(st["problems"]))
        else:
            logs.info("freshness", f"sidecar 对齐官方 {st['sidecar_client_version']}"
                                   f"（captured {st['captured_at']}）")
    return st


def _strip_checked_at(st: dict | None) -> dict | None:
    """去掉 checked_at 的副本（时间戳每秒都变，不能参与回写判定）。"""
    if not isinstance(st, dict):
        return None
    return {k: v for k, v in st.items() if k != "checked_at"}


def claim_hint() -> str | None:
    """领取/preview 失败时的版本新鲜度提示（claim.py 失败路径调用）。

    2026-10-30/31 实证：BILLING_APP_VERSION 停在旧版时，preview 空窗/领取
    拒绝在日志里只表现为「上游无投放套餐」——没人会第一时间怀疑版本。
    读缓存（每日巡检落 meta 表），无缓存现算一次。
    """
    try:
        raw = store.get_setting(_FRESHNESS_KEY)
        st = json.loads(raw) if raw else None
        if st is None:
            st = status()
    except (ValueError, TypeError):
        st = status()
    if not st.get("stale"):
        return None
    return (
        f"客户端版本/文本新鲜度告警（billing={st.get('billing_app_version')}, "
        f"sidecar={st.get('sidecar_client_version')}, "
        f"captured {st.get('captured_at')}）：{'；'.join(st.get('problems') or [])}"
    )


def stale_hint() -> str | None:
    """风控波次确认点的叠加提示：已过时返回问题摘要，否则 None。

    读缓存（meta 表）而非现算——波次在请求热路径上，绝不 here 做文件 IO。
    """
    try:
        raw = store.get_setting(_FRESHNESS_KEY)
        st = json.loads(raw) if raw else None
    except (ValueError, TypeError):
        st = None
    if not st or not st.get("stale"):
        return None
    return "; ".join(st.get("problems") or []) or "sidecar 状态未知"


async def _loop() -> None:
    while True:
        try:
            check_and_record()
        except Exception as err:  # noqa: BLE001 - 巡检失败只留痕，绝不影响服务
            logs.warn("freshness", f"巡检异常: {err}")
        await asyncio.sleep(_CHECK_INTERVAL)


def start() -> None:
    """启动每日巡检（main.lifespan 调用；先同步巡检一次再起循环）。"""
    global _task
    check_and_record()
    if _task is None or _task.done():
        _task = asyncio.create_task(_loop())


async def stop() -> None:
    global _task
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
    _task = None
