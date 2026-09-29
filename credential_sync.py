#!/usr/bin/env python3
"""凭据自动跟随：解密 ~/.zcode/v2/credentials.json，把最新的 JWT / API Key
同步到本机 zcode2api 网关的账号池。

归属规则（2026-09-29 定稿）：槽位按「登录身份」划分，跟随与来源无关——
- jwt/<user_id>：JWT 自带身份（payload.user_id），账号归属看「它存量凭据
  解出的身份」。无论凭据是本脚本同步的还是人工在后台粘贴的，只要本机凭据
  文件里出现同身份的新值（客户端轮换/换登后换回）就跟随更新。
- bigmodel/<plan>-<账号ID>：凭据文件里任意账号的 key（正则动态发现）。
  key 无在带身份，按值精确匹配或上次推送记录（data/cred-sync-state.json，
  原子写）归属；本机从未出现过的 key 无轮换来源，SKIP 即正确。

红线：账号只能被更新为「它已持有身份」的凭据——不同账号不能相互覆盖，
跨身份改写在结构上不可能发生。同一身份的多个账号会一起跟随。

自动入池（plan_auto_adds）：本机凭据文件出现「新登录身份」（首次出现，
或消失过又出现=退出后重登）且池里无人持有 → 自动建号入池，命名
zai-<uid尾4> / bigmodel-<uid尾4>-<plan>。身份「持续在场」时池里删了也
不复活。升级首跑为播种轮：只登记在场身份、一律不添加。

用法：python3 credential_sync.py [--gateway URL] [--dry-run]
退出码：0=无需变更或同步成功，1=同步失败。
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

CREDENTIALS_FILE = Path.home() / ".zcode" / "v2" / "credentials.json"
DEFAULT_GATEWAY = "http://127.0.0.1:8319"
DEFAULT_DB = Path(__file__).parent / "data" / "accounts.db"
# 与 zcode-server.cjs credentialCipherProvider 相同的派生逻辑
SECRET_ENV = "ZCODE_CREDENTIAL_SECRET"

JWT_KEY = "zcodejwttoken"
# 凭据文件里的 bigmodel coding-plan key：plan（individual/team）+ 账号 ID 动态发现
BIGMODEL_KEY_RE = re.compile(
    r"^account-provider:coding-plan:account:bigmodel-(individual|team)-coding-plan:account:(\d+):api-key$"
)

STATE_FILENAME = "cred-sync-state.json"


def cipher_secret() -> str:
    env = os.environ.get(SECRET_ENV)
    if env:
        return env
    import getpass
    return f"zcode-credential-fallback:{sys.platform}:{Path.home()}:{getpass.getuser()}"


def decrypt_value(value: str) -> str:
    """解密 enc:v1:<iv b64url>.<tag b64url>.<ct b64url>（AES-256-GCM）。"""
    if not value.startswith("enc:v1:"):
        return value
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = hashlib.sha256(cipher_secret().encode()).digest()
    iv_raw, tag_raw, ct_raw = value[len("enc:v1:"):].split(".")
    iv = base64.urlsafe_b64decode(iv_raw + "==")
    tag = base64.urlsafe_b64decode(tag_raw + "==")
    ct = base64.urlsafe_b64decode(ct_raw + "==")
    return AESGCM(key).decrypt(iv, ct + tag, None).decode("utf-8")


def jwt_uid(token: str) -> str | None:
    """从 JWT 载荷解出登录身份（user_id，退回 sub）；解不出返回 None。"""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:  # noqa: BLE001 - 残缺 token 按 nullish 身份处理
        return None
    if not isinstance(payload, dict):
        return None
    uid = payload.get("user_id") or payload.get("sub")
    return str(uid) if uid else None


def load_local_slots() -> dict[str, str]:
    """本机凭证文件里的全部槽位：槽位名 → 当前凭据值。

    jwt/<user_id> 与 bigmodel/<plan>-<账号ID>；解不出身份的 JWT 落入
    jwt/unknown 兜底单槽（此时才可能退化为旧行为）。
    """
    raw = json.loads(CREDENTIALS_FILE.read_text())
    slots: dict[str, str] = {}
    try:
        jwt = decrypt_value(raw[JWT_KEY])
        if jwt:
            uid = jwt_uid(jwt)
            slots[f"jwt/{uid or 'unknown'}"] = jwt
    except Exception as e:  # noqa: BLE001 —— 单项失败不阻塞另一项
        print(f"WARN 解密 {JWT_KEY} 失败: {e}", file=sys.stderr)
    for key, value in raw.items():
        m = BIGMODEL_KEY_RE.match(key or "")
        if not m:
            continue
        try:
            slots[f"bigmodel/{m.group(1)}-{m.group(2)}"] = decrypt_value(value)
        except Exception as e:  # noqa: BLE001
            print(f"WARN 解密 {m.group(1)}-{m.group(2)} 失败: {e}", file=sys.stderr)
    return slots


def admin_request(gateway: str, admin_key: str, method: str, path: str, payload=None):
    req = urllib.request.Request(
        f"{gateway}/admin/api{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method,
        headers={
            "Authorization": f"Bearer {admin_key}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def stored_secrets(db_path: Path) -> dict:
    """只读查询账号池当前密钥（admin API 不回传密钥，对比走库）。键为账号 id。"""
    import sqlite3

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT id, data FROM accounts").fetchall()
    finally:
        con.close()
    secrets = {}
    for id_, data in rows:
        d = json.loads(data)
        secrets[id_] = d.get("jwt_token") or d.get("api_key") or ""
    return secrets


def load_state(db_path: Path) -> dict:
    """上次推送记录：槽位名 → 推送过的值。用于在身份轮换后仍能归属账号。"""
    path = db_path.parent / STATE_FILENAME
    try:
        return json.loads(path.read_text())
    except Exception:  # noqa: BLE001 - 首次运行/文件损坏都从空状态开始
        return {}


def save_state(db_path: Path, state: dict) -> None:
    path = db_path.parent / STATE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    tmp.replace(path)


def resolve_admin_key() -> str:
    env = os.environ.get("ZCODE_ADMIN_KEY")
    if env:
        return env
    env_path = Path(__file__).parent / ".env"
    for line in env_path.read_text().splitlines():
        if line.startswith("ZCODE_ADMIN_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("找不到 ZCODE_ADMIN_KEY（环境变量或 .env）")


def attribute_account(
    provider: str, mode: str, current: str, slots: dict[str, str], state: dict
) -> tuple[str | None, dict[str, str]]:
    """判定账号归属的槽位。返回 (owner, candidates)；owner=None 表示不跟进。

    归属红线：账号只能被更新为「它已持有身份」的凭据，跨身份覆盖在结构上
    不可能——
    - zai/jwt：JWT 自带身份（payload.user_id），按存量凭据解出的身份归属。
      人工粘贴的 JWT 同样适用：只要本机凭据文件里出现同身份的新值就跟随
      轮换；解不出身份时退回值精确匹配兜底。
    - bigmodel/apiKey：key 无在带身份，按值精确匹配或上次推送记录归属；
      本机凭据文件里从未出现过的 key 无轮换来源，SKIP 即正确行为。
    - 其余类型（如 zai/apiKey 回退账号）本脚本不管。
    """
    if provider == "zai" and mode == "jwt":
        candidates = {s: v for s, v in slots.items() if v and s.startswith("jwt/")}
        uid = jwt_uid(current) if current else None
        if uid and f"jwt/{uid}" in candidates:
            return f"jwt/{uid}", candidates
        owner = next((s for s, v in candidates.items() if v == current), None)
        return owner, candidates
    if provider == "bigmodel" and mode == "apiKey":
        candidates = {s: v for s, v in slots.items() if v and s.startswith("bigmodel/")}
        owner = next(
            (s for s, v in candidates.items()
             if v == current or current == state.get(s)),
            None,
        )
        return owner, candidates
    return None, {}


def slot_account_spec(slot: str) -> tuple[str | None, str | None]:
    """槽位 → (provider, 入池账号名)。jwt/unknown 解不出身份，不入池。"""
    if slot.startswith("jwt/"):
        uid = slot[len("jwt/"):]
        return ("zai", f"zai-{uid[-4:]}") if uid and uid != "unknown" else (None, None)
    if slot.startswith("bigmodel/"):
        plan, _, acc_id = slot[len("bigmodel/"):].partition("-")
        return ("bigmodel", f"bigmodel-{acc_id[-4:]}-{plan}") if acc_id else (None, None)
    return None, None


def _pool_held_identities(accounts: list, stored: dict, slots: dict[str, str]) -> set[str]:
    """池里当前有哪些身份的凭据：zai/jwt 解存量 JWT 的 uid；bigmodel 按值匹配槽位。"""
    held: set[str] = set()
    for acc in accounts:
        provider, mode = acc.get("provider"), acc.get("mode")
        current = stored.get(acc.get("id")) or ""
        if provider == "zai" and mode == "jwt":
            uid = jwt_uid(current)
            if uid:
                held.add(f"jwt/{uid}")
        elif provider == "bigmodel" and mode == "apiKey":
            for s, v in slots.items():
                if s.startswith("bigmodel/") and v == current:
                    held.add(s)
    return held


def plan_auto_adds(
    slots: dict[str, str], presence: dict, seeded: bool, held: set[str]
) -> tuple[list[tuple[str, str]], dict, bool]:
    """自动入池计划（纯函数）。返回 (待添加槽位, 新 presence, seeded)。

    在场语义：本机凭据文件里身份「持续在场」→ 池里删了也不复活；「消失过
    又出现」（退出登录后再登录，token 相同也算）→ 用户重新要用了 → 入池；
    「首次出现」→ 新登录 → 入池。升级首跑为播种轮：只登记在场身份、一律
    不添加（否则升级瞬间会把此前被删的账号全部复活）。
    """
    if not seeded:
        return [], {s: {"token": v, "absent": False} for s, v in slots.items()}, True
    for s in list(presence):
        if s not in slots and isinstance(presence[s], dict):
            presence[s]["absent"] = True
    adds: list[tuple[str, str]] = []
    for s, v in slots.items():
        p = presence.get(s)
        fresh = p is None or bool(p.get("absent"))
        presence[s] = {"token": v, "absent": False}
        if fresh and s not in held:
            adds.append((s, v))
    return adds, presence, True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    slots = load_local_slots()
    if not slots:
        print("ERROR 本地凭据解密全部失败，未做任何变更")
        return 1

    state = load_state(args.db)
    stored = stored_secrets(args.db)
    admin_key = resolve_admin_key()
    accounts = admin_request(args.gateway, admin_key, "GET", "/accounts")
    if isinstance(accounts, dict):
        accounts = accounts.get("accounts") or accounts.get("data") or []

    # ── 自动入池：本机新登录的身份（首次出现 / 消失后重现）建号入池 ──────────
    held = _pool_held_identities(accounts, stored, slots)
    adds, presence, seeded = plan_auto_adds(
        slots, state.get("presence") or {}, bool(state.get("presence_seeded")), held
    )
    state["presence"] = presence
    state["presence_seeded"] = seeded

    changed = 0
    added = 0

    for slot, value in adds:
        provider, name = slot_account_spec(slot)
        if not provider:
            print(f"SKIP {slot} 身份不明，不入池")
            continue
        print(f"ADD {slot}：本机新登录身份，自动入池（{name}）")
        if not args.dry_run:
            admin_request(args.gateway, admin_key, "POST", "/accounts",
                          {"provider": provider, "name": name, "tokens": [value]})
        added += 1

    for acc in accounts:
        acc_id, provider, mode = acc.get("id"), acc.get("provider"), acc.get("mode")
        current = stored.get(acc_id) or ""
        owner, candidates = attribute_account(provider, mode, current, slots, state)

        if owner is None:
            reason = "账号无密钥" if not current else "人工凭据，归属不明"
            print(f"SKIP {acc_id} ({provider}/{mode}) {reason}")
            continue

        new_secret = candidates[owner]
        if current == new_secret:
            print(f"SKIP {acc_id} ({provider}/{mode}) 已是最新（槽位 {owner}）")
        else:
            print(f"UPDATE {acc_id} ({provider}/{mode})：跟随槽位 {owner} 轮换")
            if not args.dry_run:
                admin_request(args.gateway, admin_key, "PUT",
                              f"/accounts/{acc_id}", {"token": new_secret})
            changed += 1
        state[owner] = new_secret

    if not args.dry_run:
        # presence 每轮都要落盘（在场/消失标记是自动入池的信号源）
        save_state(args.db, state)

    print(f"DONE 检查 {len(accounts)} 个账号，新增 {added} 个，更新 {changed} 个"
          + ("（dry-run）" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
