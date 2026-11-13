"""安全默认回归：凭证库权限、代理头信任、首启密钥与监听地址。

覆盖 Z1/Z3/Z4 的关键修复，防止后续改动把「可用默认」重新放回公网面。
"""

from __future__ import annotations

import ipaddress
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app import auth_admin, settings
from app.store import Store


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _make_store(tmp_path, monkeypatch, admin_key: str = "unit-admin-key") -> Store:
    data_dir = tmp_path / "data"
    monkeypatch.setattr(settings, "DATA_DIR", data_dir)
    monkeypatch.setattr(settings, "DB_PATH", data_dir / "accounts.db")
    monkeypatch.setattr(settings, "DEFAULT_ADMIN_KEY", admin_key)
    return Store()


def _request(peer: str, headers: tuple[tuple[str, str], ...] = ()) -> Request:
    return Request({
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": (peer, 1234),
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers],
    })


# ── Z1：SQLite 凭证库权限 ────────────────────────────────────────────────


def test_store_hardens_db_and_sidecar_permissions(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)
    os.chmod(data_dir, 0o755)
    db_path = data_dir / "accounts.db"
    db_path.touch()
    os.chmod(db_path, 0o644)

    store = _make_store(tmp_path, monkeypatch)

    # 已存在的宽松目录/文件在启动时收紧
    assert _mode(data_dir) == 0o700
    assert _mode(db_path) == 0o600

    # 打开连接并写入，强制创建 WAL/SHM，运行期新建文件同样 0600
    conn = store._connect()
    try:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('perm_probe', '1')")
        conn.commit()
        for suffix in ("-wal", "-shm"):
            sidecar = db_path.with_name(db_path.name + suffix)
            assert sidecar.exists(), f"{suffix} 未创建"
            assert _mode(sidecar) == 0o600
    finally:
        conn.close()


def test_umask_keeps_new_sensitive_files_private(tmp_path, monkeypatch):
    store = _make_store(tmp_path, monkeypatch)
    probe = settings.DATA_DIR / "new-file"
    probe.write_text("secret", encoding="utf-8")
    assert _mode(probe) == 0o600
    assert store.admin_key() == "unit-admin-key"


# ── Z3：TRUST_PROXY_IP / 伪造转发头 ──────────────────────────────────────


def test_forged_forwarded_header_ignored_when_trust_disabled(monkeypatch):
    monkeypatch.setattr(auth_admin, "TRUST_PROXY_IP", False)
    req = _request("127.0.0.1", (("cf-connecting-ip", "203.0.113.7"),))
    assert auth_admin._client_ip(req) == "127.0.0.1"


def test_forwarded_header_only_from_trusted_proxy(monkeypatch):
    monkeypatch.setattr(auth_admin, "TRUST_PROXY_IP", True)
    monkeypatch.setattr(
        auth_admin,
        "TRUSTED_PROXY_NETWORKS",
        (ipaddress.ip_network("10.0.0.0/24"),),
    )

    trusted = _request("10.0.0.5", (("cf-connecting-ip", "203.0.113.7"),))
    assert auth_admin._client_ip(trusted) == "203.0.113.7"

    # 直连来源不在受信代理网段：伪造头必须忽略
    direct = _request("203.0.113.9", (("cf-connecting-ip", "198.51.100.1"),))
    assert auth_admin._client_ip(direct) == "203.0.113.9"


def test_x_real_ip_fallback_and_malformed_header(monkeypatch):
    monkeypatch.setattr(auth_admin, "TRUST_PROXY_IP", True)
    monkeypatch.setattr(
        auth_admin,
        "TRUSTED_PROXY_NETWORKS",
        (ipaddress.ip_network("127.0.0.0/8"),),
    )
    assert auth_admin._client_ip(
        _request("127.0.0.1", (("x-real-ip", "203.0.113.8"),))
    ) == "203.0.113.8"
    # 非 IP 头值不可作为失败计数桶
    assert auth_admin._client_ip(
        _request("127.0.0.1", (("cf-connecting-ip", "not-an-ip"),))
    ) == "127.0.0.1"


def test_trust_proxy_ip_defaults_off(tmp_path):
    env = os.environ.copy()
    env.pop("ZCODE_TRUST_PROXY_IP", None)
    env["ZCODE_DATA_DIR"] = str(tmp_path / "subprocess-data")
    env["ZCODE_ADMIN_KEY"] = "subprocess-test-admin"
    env["ZCODE_ENV_FILE"] = "0"  # 不读仓库 .env（生产 .env 里有 ZCODE_HOST=0.0.0.0）
    code = "from app import auth_admin; print(auth_admin.TRUST_PROXY_IP)"
    out = subprocess.check_output(
        [sys.executable, "-c", code],
        cwd=str(Path(__file__).resolve().parents[2]),
        env=env,
        text=True,
    )
    assert out.strip() == "False"


# ── Z4：HOST / 空网关 Key / 后台默认口令 ────────────────────────────────


def test_host_defaults_to_loopback(tmp_path):
    env = os.environ.copy()
    env.pop("ZCODE_HOST", None)
    env["ZCODE_DATA_DIR"] = str(tmp_path / "subprocess-data")
    env["ZCODE_ADMIN_KEY"] = "subprocess-test-admin"
    env["ZCODE_ENV_FILE"] = "0"  # 不读仓库 .env（生产 .env 里 ZCODE_HOST=0.0.0.0）
    code = "from app import settings; print(settings.HOST)"
    out = subprocess.check_output(
        [sys.executable, "-c", code],
        cwd=str(Path(__file__).resolve().parents[2]),
        env=env,
        text=True,
    )
    assert out.strip() == "127.0.0.1"


def test_first_boot_generates_random_admin_key(tmp_path, monkeypatch):
    store = _make_store(tmp_path, monkeypatch, admin_key="")
    key = store.admin_key()
    assert key
    assert key != "zcode"
    assert len(key) >= 32


def test_admin_key_from_env_and_persisted(tmp_path, monkeypatch):
    store = _make_store(tmp_path, monkeypatch, admin_key="env-admin-secret")
    assert store.admin_key() == "env-admin-secret"

    # 二次启动换了环境变量也不改已落库的管理密钥
    monkeypatch.setattr(settings, "DEFAULT_ADMIN_KEY", "another-env-key")
    assert Store().admin_key() == "env-admin-secret"


def test_legacy_default_admin_key_is_replaced(tmp_path, monkeypatch):
    _make_store(tmp_path, monkeypatch, admin_key="zcode")
    monkeypatch.setattr(settings, "DEFAULT_ADMIN_KEY", "")
    upgraded = Store().admin_key()
    assert upgraded != "zcode"
    assert len(upgraded) >= 32


def test_empty_gateway_key_rejects_public_bind(fresh_app, monkeypatch):
    from app import main as main_module

    monkeypatch.setattr(settings, "HOST", "0.0.0.0")
    with pytest.raises(RuntimeError, match="API Key"):
        main_module._validate_security_defaults()


def test_empty_gateway_key_allows_loopback_bind(fresh_app, monkeypatch):
    from app import main as main_module

    monkeypatch.setattr(settings, "HOST", "127.0.0.1")
    main_module._validate_security_defaults()


def test_configured_gateway_key_allows_public_bind(fresh_app, monkeypatch):
    from app import main as main_module

    fresh_app.set_setting("gateway_key", "sk-test-gateway")
    monkeypatch.setattr(settings, "HOST", "0.0.0.0")
    main_module._validate_security_defaults()


async def test_empty_gateway_key_denies_non_loopback_request(fresh_app):
    with pytest.raises(HTTPException) as exc:
        await auth_admin.verify_gateway_key(_request("203.0.113.9"))
    assert exc.value.status_code == 401


async def test_empty_gateway_key_allows_loopback_request(fresh_app):
    await auth_admin.verify_gateway_key(_request("127.0.0.1"))
