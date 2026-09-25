#!/usr/bin/env python3
"""凭据自动跟随：解密 ~/.zcode/v2/credentials.json，把最新的 JWT / API Key
同步到本机 zcode2api 网关的账号池。

用法：python3 credential_sync.py [--gateway URL] [--dry-run]
退出码：0=无需变更或同步成功，1=同步失败。
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
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
BIGMODEL_KEYS = [
    # 优先 individual（与当前账号池一致），退回 team
    "account-provider:coding-plan:account:bigmodel-individual-coding-plan:account:51681787803732714:api-key",
    "account-provider:coding-plan:account:bigmodel-team-coding-plan:account:51681787803732714:api-key",
]


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


def load_local_credentials() -> dict:
    raw = json.loads(CREDENTIALS_FILE.read_text())
    creds: dict = {"jwt": None, "bigmodel_key": None}
    try:
        creds["jwt"] = decrypt_value(raw[JWT_KEY])
    except Exception as e:  # noqa: BLE001 —— 单项失败不阻塞另一项
        print(f"WARN 解密 {JWT_KEY} 失败: {e}", file=sys.stderr)
    for k in BIGMODEL_KEYS:
        if k in raw:
            try:
                creds["bigmodel_key"] = decrypt_value(raw[k])
                break
            except Exception as e:  # noqa: BLE001
                print(f"WARN 解密 {k} 失败: {e}", file=sys.stderr)
    return creds


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
    """只读查询账号池当前密钥（admin API 不回传密钥，对比走库）。"""
    import sqlite3

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT id, provider, mode, data FROM accounts").fetchall()
    finally:
        con.close()
    secrets = {}
    for id_, provider, mode, data in rows:
        d = json.loads(data)
        secrets[(provider, mode)] = d.get("jwt_token") or d.get("api_key") or ""
    return secrets


def resolve_admin_key() -> str:
    env = os.environ.get("ZCODE_ADMIN_KEY")
    if env:
        return env
    env_path = Path(__file__).parent / ".env"
    for line in env_path.read_text().splitlines():
        if line.startswith("ZCODE_ADMIN_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("找不到 ZCODE_ADMIN_KEY（环境变量或 .env）")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    local = load_local_credentials()
    if not local["jwt"] and not local["bigmodel_key"]:
        print("ERROR 本地凭据解密全部失败，未做任何变更")
        return 1

    stored = stored_secrets(args.db)
    admin_key = resolve_admin_key()
    accounts = admin_request(args.gateway, admin_key, "GET", "/accounts")
    if isinstance(accounts, dict):
        accounts = accounts.get("accounts") or accounts.get("data") or []

    changed = 0
    for acc in accounts:
        provider, mode = acc.get("provider"), acc.get("mode")
        new_secret = local["jwt"] if (provider == "zai" and mode == "jwt") else (
            local["bigmodel_key"] if (provider == "bigmodel" and mode == "apiKey") else None)
        if not new_secret:
            continue
        if stored.get((provider, mode)) == new_secret:
            print(f"SKIP {acc.get('id')} ({provider}/{mode}) 已是最新")
            continue
        print(f"UPDATE {acc.get('id')} ({provider}/{mode})：凭据与本机不一致")
        if not args.dry_run:
            admin_request(args.gateway, admin_key, "PUT",
                          f"/accounts/{acc['id']}", {"token": new_secret})
        changed += 1

    print(f"DONE 检查 {len(accounts)} 个账号，更新 {changed} 个" + ("（dry-run）" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
