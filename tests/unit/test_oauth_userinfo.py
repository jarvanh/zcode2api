"""OAuth userinfo 抓取：载荷解析与 uid 一致性（2026-09-30 命名对齐用户名）。"""

from __future__ import annotations

import base64
import json

from app.oauth import parse_userinfo


def _jwt(uid: str) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"user_id": uid}).encode()).rstrip(b"=").decode()
    return f"h.{payload}.sig"


def test_parse_username_variants():
    uid = "4321790740758411"
    assert parse_userinfo({"id": uid, "username": "176gg"}, uid) == "176gg"
    assert parse_userinfo({"id": uid, "displayName": "176gg"}, uid) == "176gg"
    assert parse_userinfo({"sub": uid, "name": "nm"}, uid) == "nm"
    assert parse_userinfo({"sub": uid, "nickname": "nk"}, uid) == "nk"
    # 信封形态（data 包一层）由 userinfo() 解包，这里只测载荷本身


def test_parse_rejects_mismatched_uid_and_garbage():
    assert parse_userinfo({"id": "other", "username": "x"}, "4321790740758411") is None
    assert parse_userinfo(None) is None
    assert parse_userinfo("nope") is None
    assert parse_userinfo({"id": "4321790740758411"}) is None          # 无用户名字段
    assert parse_userinfo({"id": "4321790740758411", "username": "  "}) is None


def test_parse_sanitizes_and_caps():
    name = parse_userinfo({"id": "1", "username": "a/b c#d" * 10}, "1")
    assert name is not None and "/" not in name and len(name) <= 24


