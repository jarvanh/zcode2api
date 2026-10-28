"""Account 账号级出口代理单测：normalize / proxy_kwargs / proxy_view / 序列化回环。

不变量：
- 未配置（None/空白）→ 直连（proxy_kwargs 为 {}）
- 合法协议 http/https/socks5/socks5h → {"proxy": url}；其余协议写入口即拒
- proxy_view 剥离 userinfo 凭据（user:pass@ → user:***@），无凭据原样
- to_dict/from_dict 回环不丢 proxy_url（老数据无该字段也能加载）
"""

from __future__ import annotations

import pytest

from app.models import Account


def _acct(**kw) -> Account:
    # Account.create 负责生成必需字段 id；proxy_url 等由用例覆盖
    a = Account.create("zai", "t", "h1.eyJzdWIiOiJhIn0.sig")
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def test_normalize_blank_returns_none():
    assert Account.normalize_proxy(None) is None
    assert Account.normalize_proxy("") is None
    assert Account.normalize_proxy("   ") is None


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:8080",
    "https://proxy.example.com",
    "socks5://10.0.0.1:1080",
    "socks5://user:pass@10.0.0.1:1080",
])
def test_normalize_accepts_supported_schemes(url):
    assert Account.normalize_proxy(url) == url


@pytest.mark.parametrize("url", ["ftp://x", "://no-scheme", "random-string"])
def test_normalize_rejects_bad_scheme(url):
    with pytest.raises(ValueError):
        Account.normalize_proxy(url)


def test_proxy_kwargs_unset_is_direct():
    assert _acct().proxy_kwargs() == {}
    assert _acct(proxy_url="  ").proxy_kwargs() == {}


def test_proxy_kwargs_set():
    a = _acct(proxy_url="socks5://u:p@1.2.3.4:1080")
    assert a.proxy_kwargs() == {"proxy": "socks5://u:p@1.2.3.4:1080"}


def test_proxy_kwargs_garbage_falls_back_to_direct():
    """历史脏数据防御：网关热路径绝不抛异常，回落直连。"""
    a = _acct(proxy_url="ftp://bad")
    assert a.proxy_kwargs() == {}


def test_proxy_view_masks_credentials():
    assert _acct(proxy_url="socks5://alice:secret@1.2.3.4:1080").proxy_view() \
        == "socks5://alice:***@1.2.3.4:1080"
    assert _acct(proxy_url="http://1.2.3.4:8080").proxy_view() == "http://1.2.3.4:8080"
    assert _acct().proxy_view() is None


def test_proxy_roundtrip_and_legacy_dict():
    a = _acct(proxy_url="http://p:1")
    restored = Account.from_dict(a.to_dict())
    assert restored.proxy_url == "http://p:1"
    # 老数据无 proxy_url 字段：加载不报错且为 None
    legacy = _acct().to_dict()
    legacy.pop("proxy_url")
    assert Account.from_dict(legacy).proxy_url is None
