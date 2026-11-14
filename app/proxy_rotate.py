"""mihomo 节点自动轮换（出口 IP 被 WAF 405 后换节点）。

背景（2026-11-14）：账号按 1:1 绑定本地 mihomo listener 端口，每个端口后面
是一个 Fallback 组（5–6 个订阅节点）。组内 fallback 只对连接失败切换，
HTTP 405（阿里云 WAF 拦截页）不会触发 —— IP 被封后该出口只能干等
`EDGE_HOLD_SECONDS`。本模块让网关在 405 时主动调 mihomo controller API，
把该账号的组切到未封节点，立即恢复该账号产能。

策略（与主人确认）：
- 仅当请求尚未携带 captcha token 时才切 —— token 绑定签发出口 IP，切换后
  复用会被上游判 3012「unusual activity」，比 405 更糟；
- 已出 token 的请求直接换号，由下一个请求在新出口重新解 captcha；
- 命中 405 的当前节点记入组内黑名单（TTL 内不再选中），轮换选
  「未封且非当前」的第一个节点（保持 fallback 语义：排前者优先）；
- 全组节点都被封 → 返回 None，edge hold 保持，账号被调度跳过；
- 每账号 `PROXY_ROTATE_COOLDOWN` 秒最多轮换一次；组级锁保证并发请求
  只有一次真实切换，其余等锁后按冷却直接放弃（沿用切换后的新出口）。

组名约定：`eg-<账号名>`（listener `<账号>-egress` 的 proxy 字段，端口与
账号已 1:1 对齐，见 /dropbox/self-hosted/zcode2api/proxy/config.yaml）。
组不存在时 controller 返回非 2xx，本模块静默返回 None —— 未部署 mihomo
的环境自动退回「只换号」。
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import httpx

from . import logs, settings

if TYPE_CHECKING:  # pragma: no cover - 仅类型标注
    from .models import Account

# 账号 -> 上次轮换时刻（monotonic）
_cooldown: dict[str, float] = {}
# 组 -> {节点: 封禁时刻}（405 记忆，TTL 内不再选中）
_blocked: dict[str, dict[str, float]] = {}
# 组 -> 轮换锁（并发请求只做一次真实切换）
_locks: dict[str, asyncio.Lock] = {}


def group_for(account_name: str) -> str:
    """账号出口组名约定：listener `<账号>-egress` 的 proxy 即 `eg-<账号名>`。"""
    return f"eg-{account_name}"


def reset_state() -> None:
    """测试隔离钩子：清空冷却、黑名单与锁。"""
    _cooldown.clear()
    _blocked.clear()
    _locks.clear()


def _prune_blocked(group: str, now: float) -> None:
    """清掉超过 TTL 的黑名单节点（WAF 封禁通常 1–2h，过期节点可重试）。"""
    entries = _blocked.get(group)
    if not entries:
        return
    ttl = max(0, settings.PROXY_ROTATE_BLOCK_TTL)
    for node in [n for n, at in entries.items() if now - at > ttl]:
        entries.pop(node, None)


async def _api_get(path: str) -> dict | None:
    """GET controller API；不可达/非 200/非 JSON 一律 None（静默降级）。"""
    try:
        async with httpx.AsyncClient(timeout=settings.PROXY_ROTATE_TIMEOUT) as client:
            resp = await client.get(f"{settings.PROXY_CONTROLLER_URL}{path}")
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:  # noqa: BLE001 - controller 不可达时退回「只换号」
        return None


async def _api_put(path: str, body: dict) -> bool:
    """PUT controller API（选组内节点）；失败一律 False。"""
    try:
        async with httpx.AsyncClient(timeout=settings.PROXY_ROTATE_TIMEOUT) as client:
            resp = await client.put(f"{settings.PROXY_CONTROLLER_URL}{path}", json=body)
        return resp.status_code in (200, 204)
    except Exception:  # noqa: BLE001
        return False


async def rotate(req_id: str, account: Account) -> str | None:
    """把账号出口组切到未封节点；成功返回新节点名，失败返回 None。

    失败（未启用/非 zai/冷却中/组不存在/全封/PUT 失败）一律 None，调用方
    退回「换号」语义，不影响请求主链路。
    """
    if not settings.PROXY_ROTATE_ENABLED or account.provider != "zai":
        return None
    group = group_for(account.name)
    lock = _locks.setdefault(group, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        if now - _cooldown.get(account.id, 0.0) < max(0, settings.PROXY_ROTATE_COOLDOWN):
            logs.info(req_id, f"账号 {account.name} 节点轮换冷却中，跳过")
            return None

        data = await _api_get(f"/proxies/{group}")
        if data is None:
            return None
        nodes = [n for n in (data.get("all") or []) if isinstance(n, str)]
        current = data.get("now") if isinstance(data.get("now"), str) else None

        _prune_blocked(group, now)
        entries = _blocked.setdefault(group, {})
        if current:
            # 刚命中 405 的节点进黑名单（setdefault：保留更早的封禁时刻）
            entries.setdefault(current, now)

        candidates = [
            n for n in nodes
            if n != current and n != "AUTO" and n not in entries
        ]
        if not candidates:
            logs.warn(req_id, f"出口组 {group} 无可用候选节点（全封/组空），保持静默换号")
            return None

        target = candidates[0]
        if not await _api_put(f"/proxies/{group}", {"name": target}):
            logs.warn(req_id, f"出口组 {group} 切节点 {target} 失败（controller 拒绝），保持静默换号")
            return None

        _cooldown[account.id] = time.monotonic()
        logs.warn(
            req_id,
            f"账号 {account.name} 出口节点已轮换：{current or '?'} → {target}（组 {group}）",
        )
        return target
