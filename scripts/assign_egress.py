#!/usr/bin/env python3
"""按账号绑定 mihomo pinned 出口（zcode2api 账号池 → 每账号独立 IP）。

规则（主人 2026-11-08 拍板）：
  - 第 1 个启用账号直连（proxy_url 清空）；
  - 之后的账号按序绑 mihomo pinned listener（http://127.0.0.1:7891..789N）；
  - 绑定前实测每个出口的公网 IP，要求账号间出口 IP 互不相同
    （与直连 IP 也不得相同）；IP 相同/不可用的出口跳过不绑；
  - 可用出口 < 需求时：能绑多少绑多少，缺口发 Telegram 通知。

出口池实况（2026-11-08 实测，mihomo pinned listeners）：
  7891 → 38.28.193.1  7892 → 74.82.60.31
  7893 → 71.43.26.234 7894 → 65.49.68.18
  直连 → 52.165.59.130（Azure）

幂等：已符合目标绑定的账号不动；只写差异。写库即生效（调度层每次请求
现读 account.proxy_url），无需重启。
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from app.store import Store  # noqa: E402
from app.models import Account  # noqa: E402

BASE = os.path.dirname(os.path.abspath(__file__))
LISTENERS = [7891, 7892, 7893, 7894]  # mihomo pinned listeners（gen-config.py ACCT_SPEC）
IP_ECHO = "https://api.ipify.org"
TIMEOUT = 20


def notify_telegram(text: str) -> bool:
    """缺口告警：TELEGRAM_BOT_TOKEN(_ALERT) / TELEGRAM_CHAT_ID(_ALERT)。"""
    def env(k: str) -> str:
        p = os.path.join(os.path.dirname(BASE), ".env")
        # .env 简读（本脚本独立运行，不依赖 app.settings 的 dotenv 顺序）
        try:
            for line in open(p):
                if line.strip().startswith(f"{k}="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            pass
        return os.environ.get(k, "")
    token = env("TELEGRAM_BOT_TOKEN_ALERT") or env("TELEGRAM_BOT_TOKEN")
    chat = env("TELEGRAM_CHAT_ID_ALERT") or env("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print("[warn] 无 Telegram 凭据，缺口告警未发")
        return False
    body = urllib.parse.urlencode({"chat_id": chat, "text": text[:3800]}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=body, timeout=15).read()
        print("[alert] 已推送 Telegram")
        return True
    except Exception as err:  # noqa: BLE001
        print(f"[warn] Telegram 推送失败: {err}")
        return False


def probe_ip(proxy: str | None) -> str:
    """经代理（None=直连）测公网 IP；多源探测（https 优先、http 兜底）。

    单源会误杀：TS-AutoClaw 隧道对部分 HTTPS echo 有 SSL_ERROR_SYSCALL
    抖动，但代理本身可用（HTTP echo 正常）。返回空串 = 确认不可用。
    """
    for url in (IP_ECHO, "http://ip-api.com/json/?fields=query"):
        for _ in range(2):  # 每源重试一次（隧道冷启动抖动）
            try:
                handler = urllib.request.ProxyHandler(
                    {"http": proxy, "https": proxy} if proxy else {})
                opener = urllib.request.build_opener(handler)
                req = urllib.request.Request(url, headers={"User-Agent": "curl/8"})
                with opener.open(req, timeout=TIMEOUT) as r:
                    raw = r.read().decode().strip()
                if url.endswith("fields=query"):
                    raw = json.loads(raw).get("query", "")
                if raw:
                    return raw
            except Exception as err:  # noqa: BLE001
                print(f"[warn] 探测 {proxy or 'direct'} via {url.split('/')[2]}: {err}")
        time.sleep(1)
    return ""


def admin_key() -> str:
    for line in open(os.path.join(os.path.dirname(BASE), ".env")):
        if line.strip().startswith("ZCODE_ADMIN_KEY="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("ZCODE_ADMIN_KEY", "")


def api_set_proxy(account_id: str, proxy_url: str | None) -> bool:
    """经 admin API 改账号代理：写在服务进程内生效，不被内存副本整体 REPLACE 冲掉。

    直接写库会被服务后台任务（quota refresh/claim 用启动时的内存对象
    update_account）冲回旧值 —— 2026-11-08 实证。
    """
    key = admin_key()
    if not key:
        print("[error] 无 ZCODE_ADMIN_KEY，无法经 admin API 写入")
        return False
    body = json.dumps({"proxy_url": proxy_url or ""}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{os.environ.get('ZCODE_PORT', '8319')}"
        f"/admin/api/accounts/{account_id}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {key}"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except Exception as err:  # noqa: BLE001
        print(f"[error] API 写入失败 {account_id}: {err}")
        return False


def main() -> int:
    store = Store()
    accounts = [a for a in store.list_accounts("zai") if a.enabled]
    if not accounts:
        print("[error] 无启用账号")
        return 1

    direct_ip = probe_ip(None)
    print(f"[info] 直连出口 IP: {direct_ip or '(探测失败)'}")

    # 1) 探测出口池：{port: ip}，IP 为空/与直连同段的出口剔除
    pool: dict[int, str] = {}
    used_ips = {direct_ip} if direct_ip else set()
    for port in LISTENERS:
        proxy = f"http://127.0.0.1:{port}"
        ip = probe_ip(proxy)
        if not ip:
            print(f"[warn] 出口 {port} 不可用（无 IP），跳过")
            continue
        if ip in used_ips:
            print(f"[warn] 出口 {port} IP {ip} 与已用出口重复，跳过")
            continue
        pool[port] = ip
        used_ips.add(ip)
        print(f"[info] 出口 {port}: {ip} ✓")
    print(f"[info] 可用独立出口: {len(pool)} 个 + 直连 1 个")

    # 2) 分配：账号1直连；账号2+按序吃 pool
    plans: list[tuple[Account, str | None, str | None]] = []  # (account, target_url, target_ip)
    ports = sorted(pool)
    for i, acct in enumerate(accounts):
        if i == 0:
            plans.append((acct, None, None))
        elif i - 1 < len(ports):
            port = ports[i - 1]
            plans.append((acct, f"http://127.0.0.1:{port}", pool[port]))
        else:
            plans.append((acct, "SHORTAGE", None))  # 出口不足

    # 3) 差异写入（幂等）+ IP 互异终检
    changed = 0
    shortage: list[str] = []
    final_ips: dict[str, str] = {}
    if direct_ip:
        final_ips[accounts[0].name] = direct_ip
    for acct, target, ip in plans:
        if target == "SHORTAGE":
            shortage.append(acct.name)
            print(f"[warn] {acct.name}: 无可用独立出口，保持现状（需人工扩容出口池）")
            continue
        current = (acct.proxy_url or "").strip() or None
        if current == target:
            print(f"[skip] {acct.name}: 已是目标绑定（{'直连' if target is None else target}）")
        else:
            if not api_set_proxy(acct.id, target):
                print(f"[error] {acct.name}: 写入失败，中止")
                return 4
            changed += 1
            print(f"[set] {acct.name}: {'直连' if target is None else target}"
                  f"（原: {current or '直连'}）")
        if ip:
            if ip in final_ips:
                print(f"[error] {acct.name}: 出口 IP {ip} 与 {final_ips} 冲突！")
                return 2
            final_ips[acct.name] = ip

    # 4) 汇总 + 缺口通知
    print("\n=== 最终出口拓扑 ===")
    for name, ip in final_ips.items():
        print(f"  {name}: {ip}")
    if shortage:
        msg = ("⚠️ zcode2api 出口不足\n"
               f"启用账号 {len(accounts)} 个，可用独立出口仅 {len(ports) + 1} 个\n"
               f"未绑定账号: {', '.join(shortage)}\n"
               "这些账号暂保持现绑定；请在 mihomo 订阅补节点后重跑本脚本")
        notify_telegram(msg)
        print(f"\n[summary] 变更 {changed} 项；出口缺口 {len(shortage)} 个（已通知）")
        return 3
    print(f"\n[summary] 变更 {changed} 项；出口全覆盖、IP 互异 ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())
