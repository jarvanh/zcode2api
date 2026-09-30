"""credential_sync：uid→用户名学习（learn_aliases）与账号命名/补名。

- learn_aliases：凭据文件 oauth:*:user_info 解出 uid→username；坏条目、
  非数字 uid 一律忽略。
- slot_account_spec：自动入池命名用户名优先，学不到退回 uid 尾 4 位；
  jwt/unknown（访客/未登录）不入池。
- maybe_rename_account：只改 cred-sync 自动命名的账号（zai-<尾4> /
  bigmodel-<尾4>-<plan>），人工命名不碰；身份离场（归属不明）的也要补名。
"""

from __future__ import annotations

import base64
import hashlib
import json

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import credential_sync as cs

SECRET = "unit-test-secret"


def _enc(plain: str) -> str:
    key = hashlib.sha256(SECRET.encode()).digest()
    iv = b"0123456789abcdef"
    ct = AESGCM(key).encrypt(iv, plain.encode(), None)
    b64 = lambda raw: base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    return f"enc:v1:{b64(iv)}.{b64(ct[-16:])}.{b64(ct[:-16])}"


@pytest.fixture(autouse=True)
def _secret_env(monkeypatch):
    monkeypatch.setenv(cs.SECRET_ENV, SECRET)


def _write_credentials(tmp_path, raw: dict, monkeypatch):
    f = tmp_path / "credentials.json"
    f.write_text(json.dumps(raw))
    monkeypatch.setattr(cs, "CREDENTIALS_FILE", f)


def test_learn_aliases_parses_user_info(tmp_path, monkeypatch):
    info = _enc(json.dumps({"id": "4321790740758411", "username": "176gg",
                            "displayName": "176gg"}))
    _write_credentials(tmp_path, {"oauth:bigmodel:user_info": info}, monkeypatch)
    assert cs.learn_aliases() == {"4321790740758411": "176gg"}


def test_learn_aliases_skips_broken_and_guest(tmp_path, monkeypatch):
    _write_credentials(tmp_path, {
        "oauth:bigmodel:user_info": _enc("not-json"),
        "oauth:zai:user_info": _enc(json.dumps({"id": "72af271e-f069-4", "username": "guest"})),
        "oauth:other:user_info": _enc(json.dumps({"id": "51681787803732714", "username": ""})),
        "unrelated": _enc(json.dumps({"id": "1", "username": "x"})),
    }, monkeypatch)
    assert cs.learn_aliases() == {}


def test_account_label_username_first_and_fallback():
    assert cs.account_label("4321790740758411", {"4321790740758411": "176gg"}) == "176gg"
    assert cs.account_label("51681787803732714", {}) == "2714"
    assert cs.account_label("51681787803732714", {"51681787803732714": "  "}) == "2714"


def test_slot_account_spec_naming():
    aliases = {"4321790740758411": "176gg"}
    assert cs.slot_account_spec("jwt/4321790740758411", aliases) == ("zai", "zai-176gg")
    assert cs.slot_account_spec("jwt/4321790740758411") == ("zai", "zai-8411")
    assert cs.slot_account_spec(
        "bigmodel/individual-4321790740758411", aliases
    ) == ("bigmodel", "bigmodel-176gg-individual")
    assert cs.slot_account_spec("jwt/unknown") == (None, None)


def _jwt(uid: str) -> str:
    payload = base64.urlsafe_b64encode(
        json.dumps({"user_id": uid, "sub": uid}).encode()
    ).rstrip(b"=").decode()
    return f"h.{payload}.sig"


def test_rename_auto_named_jwt_with_alias():
    uid = "4321790740758411"
    aliases = {uid: "176gg"}
    acc = {"id": "zai-8411-x", "name": "zai-8411"}
    assert cs.maybe_rename_account(acc, "zai", "jwt", _jwt(uid), aliases, {}, {}) == "zai-176gg"


def test_rename_leaves_manual_and_unknown_and_guest():
    aliases = {"51681787803732714": "someuser"}
    # 人工命名（名字≠zai-<尾4>）不碰
    acc = {"id": "a", "name": "131"}
    assert cs.maybe_rename_account(acc, "zai", "jwt", _jwt("51681787803732714"), aliases, {}, {}) is None
    # 自动命名但无别名不碰
    acc = {"id": "b", "name": "zai-5247"}
    assert cs.maybe_rename_account(acc, "zai", "jwt", _jwt("84041790658495247"), aliases, {}, {}) is None
    # 访客 UUID 身份不碰
    acc = {"id": "c", "name": "175"}
    assert cs.maybe_rename_account(acc, "zai", "jwt", _jwt("72af271e-f069-4"), aliases, {}, {}) is None
    # 名字已是目标名不改
    acc = {"id": "d", "name": "zai-176gg"}
    assert cs.maybe_rename_account(acc, "zai", "jwt", _jwt("4321790740758411"),
                                   {"4321790740758411": "176gg"}, {}, {}) is None


def test_rename_bigmodel_by_slot_value_match():
    uid = "4321790740758411"
    key = "keyid.secret"
    aliases = {uid: "176gg"}
    state = {f"bigmodel/individual-{uid}": key}
    acc = {"id": "bm-1", "name": "bigmodel-8411-individual"}
    assert cs.maybe_rename_account(acc, "bigmodel", "apiKey", key, aliases, {}, state) \
        == "bigmodel-176gg-individual"
    # 本地槽位也认
    acc = {"id": "bm-2", "name": "bigmodel-8411-individual"}
    assert cs.maybe_rename_account(acc, "bigmodel", "apiKey", key, aliases,
                                   {f"bigmodel/individual-{uid}": key}, {}) \
        == "bigmodel-176gg-individual"
    # 人工命名（无 plan 后缀）不碰
    acc = {"id": "bm-3", "name": "bigmodel-175"}
    assert cs.maybe_rename_account(acc, "bigmodel", "apiKey", key, aliases, {}, state) is None
    # 同值但无别名不碰
    aliases2 = {"51681787803732714": "x"}
    assert cs.maybe_rename_account(acc, "bigmodel", "apiKey", key, aliases2, {}, state) is None


def test_rename_ignores_zai_apikey_and_empty():
    aliases = {"4321790740758411": "176gg"}
    acc = {"id": "z", "name": "zai-8411"}
    assert cs.maybe_rename_account(acc, "zai", "apiKey", "k.s", aliases, {}, {}) is None
    assert cs.maybe_rename_account(acc, "zai", "jwt", "", aliases, {}, {}) is None


def test_meta_alias_seeds(tmp_path):
    import sqlite3
    db = tmp_path / "accounts.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("alias/IGNORED", "x"),                       # 斜杠 key 不匹配 alias: 前缀
        ("alias:4321790740758411", "176gg"),
        ("alias:51681787803732714", " 131 "),
        ("alias:72af271e-f069-4", "guest"),           # UUID 身份不收
        ("alias:84041790658495247", ""),              # 空值不收
        ("gateway_key", "sk-x"),
    ])
    con.commit(); con.close()
    assert cs.meta_alias_seeds(db) == {
        "4321790740758411": "176gg",
        "51681787803732714": "131",
    }
    assert cs.meta_alias_seeds(tmp_path / "missing.db") == {}
