"""出口拓扑快照：账号 → 出口 → 公网 IP + 地理位置（观测/看板共用）。

设计：
- 拓扑 = 账号 proxy_url（绑定出口）+ 直连兜底；proxy_url 为空的第一个
  启用账号按 assign_egress.py 的约定视为直连。
- IP 实测：并发 curl api.ipify.org（经各出口）；geo：ip-api.com 免费端点
  （本机 ≤5 个 IP，远低于 45/min 限额），失败回退离线判定（TS-AutoClaw
  = 北京阿里云；本机直连 = Azure）。
- 缓存 1h（拓扑/geo 都低频变化），进程内单例；best-effort，任何失败
  都降级为「出口形态 + 空 IP」，绝不影响请求路径。
- 看板小程序（quota-board）经 /admin/api/egress-topology 读同一份。
"""

from __future__ import annotations

import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from . import logs, settings

_CACHE: dict = {"at": 0.0, "data": None}
_TTL = 3600.0

# 离线 geo 回退（探测失败时的已知出口标注，2026-11-08 实测拓扑）
_KNOWN = {
    "47.93.224.81": ("中国 北京", "TS-AutoClaw（阿里云北京）"),
}


def _fetch(url: str, timeout: float = 12) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "zcode2api-topology/1"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception:  # noqa: BLE001 - best-effort
        return None


def _probe_ip(proxy: str | None) -> str:
    handler = urllib.request.ProxyHandler(
        {"http": proxy, "https": proxy} if proxy else {})
    opener = urllib.request.build_opener(handler)
    req = urllib.request.Request("https://api.ipify.org",
                                 headers={"User-Agent": "curl/8"})
    try:
        with opener.open(req, timeout=15) as r:
            return r.read().decode().strip()
    except Exception:  # noqa: BLE001 - HTTPS 抖动走 HTTP 兜底
        try:
            data = _fetch("http://ip-api.com/json/?fields=query")
            return (json.loads(data).get("query") or "") if data else ""
        except Exception:  # noqa: BLE001
            return ""


def _geo(ip: str) -> dict:
    if not ip:
        return {}
    try:
        data = _fetch(f"http://ip-api.com/json/{ip}?fields=status,country,city,isp,query")
        if data:
            d = json.loads(data)
            if d.get("status") == "success":
                return {"country": d.get("country") or "", "city": d.get("city") or "",
                        "isp": d.get("isp") or ""}
    except Exception:  # noqa: BLE001
        pass
    if ip in _KNOWN:
        return {"country": _KNOWN[ip][0], "city": _KNOWN[ip][1], "isp": ""}
    return {}


def topology(force: bool = False) -> dict:
    """账号 → {egress, ip, geo...}；缓存 1h。纯观测，失败不抛。"""
    now = time.time()
    if not force and _CACHE["data"] is not None and now - _CACHE["at"] < _TTL:
        return _CACHE["data"]
    try:
        from .store import store
        accounts = [a for a in store.list_accounts("zai") if a.enabled]
        if not accounts:
            data = {"generated_at": now, "direct_ip": "", "accounts": []}
            _CACHE.update(at=now, data=data)
            return data
        # 第一个无代理的启用账号 = 直连（assign_egress.py 约定）
        direct_first = next((a.name for a in accounts if not (a.proxy_url or "").strip()), None)
        targets: list[tuple[str, str | None]] = []
        for a in accounts:
            px = (a.proxy_url or "").strip() or None
            if px is None and a.name != direct_first:
                # 非首个且未绑定的账号：仍按直连探测（现状），标注 shared
                pass
            targets.append((a.name, px))
        with ThreadPoolExecutor(max_workers=8) as ex:
            ips = dict(zip([n for n, _ in targets],
                           ex.map(lambda t: _probe_ip(t[1]), targets)))
        uniq_ips = sorted({v for v in ips.values() if v})
        with ThreadPoolExecutor(max_workers=5) as ex:
            geos = dict(zip(uniq_ips, ex.map(_geo, uniq_ips)))
        direct_ip = ips.get(direct_first, "") if direct_first else ""
        out = []
        for name, px in targets:
            ip = ips.get(name, "")
            geo = geos.get(ip) or (_KNOWN.get(ip, ("",))[0] and
                                   {"country": _KNOWN[ip][0], "city": _KNOWN[ip][1], "isp": ""}) or {}
            loc = " ".join(x for x in (geo.get("country", ""), geo.get("city", "")) if x)
            out.append({
                "account": name,
                "egress": px or "direct",
                "ip": ip,
                "location": loc,
                "isp": geo.get("isp") or "",
                "shared": name != direct_first and not px,
            })
        data = {"generated_at": now, "direct_ip": direct_ip, "accounts": out}
        _CACHE.update(at=now, data=data)
        return data
    except Exception as err:  # noqa: BLE001 - 纯观测绝不抛
        logs.warn("egress-topology", f"拓扑采集失败（忽略）: {err}")
        return _CACHE["data"] or {"generated_at": now, "direct_ip": "", "accounts": []}
