"""回归基线：zcode2api 原行为锁定（M0 出口标准）。

这些用例描述的是**底座既有行为**，不是新设计 —— 改动底座时它们必须保持绿色，
若有意变更行为，先改用例并在 PR 里说明（docs/development/06 §测试纪律）。
"""

from __future__ import annotations

import json
import time

import pytest

from app import identity as identity_module
from app import constants, settings as app_settings
from app.models import Account, Status
from app.routes.gateway import (
    _detect_provider,
    _is_captcha_error,
    _is_concurrency_hit,
    _is_exhausted,
    _normalize_body,
)


# ── 模型归一化 ────────────────────────────────────────────────────────────────
class TestNormalizeBody:
    def test_lowercase_alias_mapped(self):
        # glm-5.2 是**已退役**型号（上游 3006），别名表直接改写到 GLM-5.3
        body = {"model": "glm-5.2"}
        assert _normalize_body(body)["model"] == "GLM-5.3"

    def test_provider_prefix_stripped(self):
        # 前缀剥离后仍是退役型号 → 再改写到 GLM-5.3（两层改写都覆盖）
        body = {"model": "bigmodel/GLM-5.2"}
        assert _normalize_body(body)["model"] == "GLM-5.3"

    def test_provider_prefix_stripped_when_alias_unchanged(self):
        # 回归（v2.9.44）：前缀剥离后映射结果与裸名相同 → 旧实现不写回，
        # zai/ 前缀被原样透传，上游 3006。必须无条件写回剥前缀后的模型名。
        body = {"model": "zai/GLM-5.3-Flash"}
        assert _normalize_body(body)["model"] == "GLM-5.3-Flash"

    def test_provider_prefix_stripped_lowercase_alias(self):
        body = {"model": "zai/glm-5.3-flash"}
        assert _normalize_body(body)["model"] == "GLM-5.3-Flash"

    def test_unknown_model_passed_through(self):
        body = {"model": "glm-unknown"}
        assert _normalize_body(body)["model"] == "glm-unknown"

    def test_string_content_bridged_to_blocks(self):
        body = {"messages": [{"role": "user", "content": "hi"}]}
        assert _normalize_body(body)["messages"][0]["content"] == [{"type": "text", "text": "hi"}]

    def test_block_content_untouched(self):
        blocks = [{"type": "text", "text": "hi"}]
        body = {"messages": [{"role": "user", "content": blocks}]}
        assert _normalize_body(body)["messages"][0]["content"] is blocks


# ── provider 判定 ─────────────────────────────────────────────────────────────
class TestDetectProvider:
    def test_default_zai(self):
        assert _detect_provider({}, {}) == "zai"

    def test_model_prefix(self):
        assert _detect_provider({"model": "bigmodel/GLM-5.2"}, {}) == "bigmodel"

    def test_header(self):
        assert _detect_provider({}, {"x-provider": "bigmodel"}) == "bigmodel"


# ── 被拒信号判定（值以 constants 为唯一来源）───────────────────────────────────
class TestRejectionSignals:
    def test_402_is_exhausted(self):
        assert _is_exhausted(402, "")

    @pytest.mark.parametrize("text", ["insufficient balance", "quota exceeded", "余额不足", "额度不足"])
    def test_keyword_exhausted(self, text):
        assert _is_exhausted(400, text)

    def test_plain_400_not_exhausted(self):
        assert not _is_exhausted(400, "bad request")

    @pytest.mark.parametrize("text", ["captcha required", "verify token invalid", "Verify Failed"])
    def test_captcha_error(self, text):
        assert _is_captcha_error(text)

    def test_auth_401_not_captcha(self):
        assert not _is_captcha_error("invalid credentials")


# ── Account 状态机（models.py 原语义）─────────────────────────────────────────
class TestAccountStateMachine:
    def _acc(self, **kw) -> Account:
        return Account.create("zai", "t", "a.b.c", **kw)

    def test_jwt_detection(self):
        acc = self._acc()
        assert acc.mode == "jwt"

    def test_apikey_detection(self):
        acc = Account.create("zai", "t", "not-a-jwt")
        assert acc.mode == "apiKey"

    def test_selectable_default(self):
        assert self._acc().is_selectable()

    def test_exhausted_not_selectable(self):
        acc = self._acc()
        acc.status = Status.EXHAUSTED
        assert not acc.is_selectable()

    def test_cooling_until_expiry(self):
        acc = self._acc()
        acc.status = Status.COOLING
        acc.cooling_until = time.time() - 1
        assert acc.is_selectable()

    def test_cooling_within_window(self):
        acc = self._acc()
        acc.status = Status.COOLING
        acc.cooling_until = time.time() + 100
        assert not acc.is_selectable()

    def test_invalid_jwt_not_selectable_without_apikey(self):
        acc = self._acc()
        acc.status = Status.INVALID
        assert not acc.is_selectable()
        assert not acc.allows_billing()
        assert acc.uses_plan_channel() is False

    def test_invalid_jwt_selectable_via_apikey_fallback(self):
        acc = self._acc()
        acc.api_key = "sk-fallback"
        acc.status = Status.INVALID
        assert acc.is_selectable()
        assert not acc.allows_billing()
        assert acc.uses_plan_channel() is False

    def test_risk_cooling_jwt_not_selectable(self):
        """风控冷却期整号不可选（即使有 Key 回退；同请求回退由 force_fallback 保证）。"""
        acc = self._acc()
        acc.api_key = "sk-fallback"
        acc.risk_penalty(base=900.0, cap=86400.0, ban_strikes=4, decay_seconds=7 * 86400)
        assert acc.status == Status.COOLING
        assert acc.enabled is True
        assert not acc.is_selectable()
        assert not acc.allows_billing()

    def test_manual_disable_never_selectable(self):
        acc = self._acc()
        acc.api_key = "sk-fallback"
        acc.enabled = False
        acc.status = Status.DISABLED
        assert not acc.is_selectable()
        assert not acc.allows_billing()

    def test_exhausted_jwt_not_selectable_but_plan_alive(self):
        acc = self._acc()
        acc.status = Status.EXHAUSTED
        assert not acc.is_selectable()
        assert acc.uses_plan_channel() is True
        assert acc.allows_billing() is True

    def test_effective_status_after_cooldown(self):
        acc = self._acc()
        acc.status = Status.COOLING
        acc.cooling_until = time.time() - 1
        assert acc.effective_status() == Status.ACTIVE

    def test_normalize_expired_cooling(self):
        """到期冷却要把持久化状态洗回 ACTIVE，不能留给外部只读消费者。"""
        acc = self._acc()
        acc.status = Status.COOLING
        acc.cooling_until = time.time() - 1
        assert acc.normalize_expired_cooling() is True
        assert acc.status == Status.ACTIVE
        assert acc.cooling_until is None

    def test_normalize_keeps_active_cooling(self):
        acc = self._acc()
        acc.status = Status.COOLING
        acc.cooling_until = time.time() + 300
        assert acc.normalize_expired_cooling() is False
        assert acc.status == Status.COOLING

    def test_public_view_masks_token(self):
        acc = Account.create("zai", "t", "x" * 100)
        view = acc.public_view()
        assert "x" * 100 not in json.dumps(view)
        assert view["token_masked"].startswith("x")


# ── Store 轮询（store.py 原语义）──────────────────────────────────────────────
class TestStoreRotation:
    def test_round_robin_distribution(self, fresh_app):
        for i in range(3):
            fresh_app.add_account("zai", f"a{i}", f"jwt.token.{i}")
        # 粘性活跃集（magpie maxAccounts，2026-11-07）：cap=2 时只有前 2 个
        # 号轮换，第 3 个留作备用 —— 均分限制在活跃集内。
        cap = fresh_app.max_active_accounts()
        picks = [fresh_app.select("zai").id for _ in range(9)]
        if cap > 0:
            assert len(set(picks)) == cap
            assert all(picks.count(p) in (9 // cap, 9 // cap + 1) for p in set(picks))
        else:
            assert len(set(picks)) == 3
            assert all(picks.count(p) == 3 for p in set(picks))

    def test_skip_ids(self, fresh_app):
        a = fresh_app.add_account("zai", "a", "jwt.token.a")
        b = fresh_app.add_account("zai", "b", "jwt.token.b")
        pick = fresh_app.select("zai", skip_ids={a.id})
        assert pick.id == b.id

    def test_duplicate_secret_dedup(self, fresh_app):
        fresh_app.add_account("zai", "a", "jwt.token.same")
        again = fresh_app.add_account("zai", "b", "jwt.token.same")
        assert len(fresh_app.list_accounts("zai")) == 1
        assert again.name == "a"

    def test_disabled_skipped(self, fresh_app):
        a = fresh_app.add_account("zai", "a", "jwt.token.a")
        fresh_app.set_enabled("zai", a.id, False)
        assert fresh_app.select("zai") is None

    def test_persistence_roundtrip(self, fresh_app, tmp_path):
        fresh_app.add_account("zai", "a", "jwt.token.a")
        from app.store import Store
        reloaded = Store()
        assert [x.name for x in reloaded.list_accounts("zai")] == ["a"]

    def test_export_import(self, fresh_app):
        fresh_app.add_account("zai", "a", "jwt.token.a")
        data = fresh_app.export()
        fresh_app.remove_account("zai", "a")
        assert fresh_app.import_accounts(data) == 1


# ── 请求构建（agent.py 原语义）────────────────────────────────────────────────
class TestBuildRequest:
    def test_jwt_routes_to_plan_channel(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        url, headers, _payload = build_request(acc, {}, None)
        assert url == "https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages"
        assert headers["Authorization"] == "Bearer a.b.c"
        assert headers["anthropic-version"] == "2023-06-01"
        # JWT 通道带全量身份头 + 追踪头；桌面端账号走 electron/production
        assert headers["X-Title"] == "Z Code@electron"
        assert headers["X-Release-Channel"] == "production"
        # v2.9.41 头形状随上游 ebbc73e 对齐 Magpie packages/zcode startHeaders
        # （ZCode 3.17.2）：带 X-Device-Mid（与 attribution blob 同源同值）；
        # UA = USER_AGENT + SDK 后缀（build_identity_headers 统一拼接）
        assert headers["X-Device-Mid"]
        assert headers["User-Agent"] == (
            app_settings.USER_AGENT + " " + constants.ANTHROPIC_SDK_UA
        )
        assert "x-request-id" in headers and "x-zcode-trace-id" in headers

    def test_apikey_routes_to_fallback(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "plain-key")
        url, headers, _payload = build_request(acc, {}, None)
        assert url == "https://api.z.ai/api/anthropic/v1/messages"
        assert headers["x-api-key"] == "plain-key"

    def test_invalid_jwt_with_apikey_routes_to_fallback(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        acc.api_key = "sk-fallback"
        acc.status = Status.INVALID
        url, headers, _payload = build_request(acc, {}, None)
        assert url == "https://api.z.ai/api/anthropic/v1/messages"
        assert headers["x-api-key"] == "sk-fallback"
        assert "Authorization" not in headers

    def test_captcha_header_injected(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        _, headers, _payload = build_request(acc, {}, "vp-123")
        assert headers["X-Aliyun-Captcha-Verify-Param"] == "vp-123"

    def test_captcha_region_header_injected(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        _, headers, _payload = build_request(acc, {}, "vp-123", None, "cn")
        assert headers["X-Aliyun-Captcha-Verify-Region"] == "cn"

    def test_client_auth_headers_dropped(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        _, headers, _payload = build_request(acc, {}, None, {
            "authorization": "Bearer client", "x-api-key": "leak", "user-agent": "UA/1",
            "x-zcode-foo": "strip", "x-custom": "keep",
        })
        # 客户端的鉴权/身份头被丢弃，authorization 保留的是账号自身凭证
        assert headers["Authorization"] == "Bearer a.b.c"
        # Start Plan 请求不再下发 x-api-key（对齐 Magpie startHeaders：仅
        # Authorization: Bearer JWT），客户端透传的 "leak" 同理被丢弃
        assert "x-api-key" not in headers
        assert headers.get("x-zcode-foo") is None
        assert headers["User-Agent"] != "UA/1"
        assert headers.get("x-custom") == "keep"

    def test_missing_credential_raises(self):
        from app.agent import build_request
        acc = Account.create("zai", "t", "a.b.c")
        acc.jwt_token = None
        with pytest.raises(RuntimeError):
            build_request(acc, {}, None)


# ── billing 阻断文案 ──────────────────────────────────────────────────────────
class TestBillingBlockReason:
    def test_cooling(self):
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.status = Status.COOLING
        acc.cooling_until = time.time() + 100
        assert "冷却" in (billing_block_reason(acc) or "")

    def test_invalid_jwt(self):
        from app.claim import AUTH_EXPIRED_MESSAGE, billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.status = Status.INVALID
        assert billing_block_reason(acc) == AUTH_EXPIRED_MESSAGE

    def test_manual_disable(self):
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.enabled = False
        acc.status = Status.DISABLED
        msg = billing_block_reason(acc, action="上游刷新") or ""
        assert "停用" in msg
        assert "重新授权" not in msg

    def test_manual_disable_still_claimable(self):
        """手动停用的好票：对话轮询不选，但领取必须放行（2026-11-03 漏领教训）。"""
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.enabled = False
        acc.status = Status.DISABLED
        assert acc.allows_claim() is True
        assert acc.is_selectable() is False, "停用账号绝不参与对话轮询"
        assert billing_block_reason(acc, allow_disabled=True) is None

    def test_risk_banned_not_claimable_even_if_allowed(self):
        """风控 strikes 封禁（enabled=True）与手动停用同状态不同语义：不放行。"""
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        for _ in range(4):
            acc.risk_penalty(base=900.0, cap=86400.0, ban_strikes=4, decay_seconds=7 * 86400)
        assert acc.status == Status.DISABLED and acc.enabled
        assert acc.allows_claim() is False
        assert billing_block_reason(acc, allow_disabled=True)

    def test_invalid_never_claimable(self):
        from app.claim import AUTH_EXPIRED_MESSAGE, billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.enabled = False
        acc.status = Status.INVALID
        assert acc.allows_claim() is False
        assert billing_block_reason(acc, allow_disabled=True) == AUTH_EXPIRED_MESSAGE

    def test_risk_cooling(self):
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        acc.risk_penalty(base=900.0, cap=86400.0, ban_strikes=4, decay_seconds=7 * 86400)
        msg = billing_block_reason(acc) or ""
        assert "冷却" in msg
        assert "重新授权" not in msg

    def test_risk_disabled_after_strikes(self):
        from app.claim import billing_block_reason
        acc = Account.create("zai", "t", "a.b.c")
        for _ in range(4):
            acc.risk_penalty(base=900.0, cap=86400.0, ban_strikes=4, decay_seconds=7 * 86400)
        msg = billing_block_reason(acc) or ""
        assert "风控封禁" in msg
        assert "重新授权" not in msg


# ── 网关主流程（HTTP 层，走 Mock 上游；夹具见 conftest.py）────────────────────
@pytest.mark.integration
class TestGatewayHTTP:
    async def test_models_endpoint(self, gateway_client):
        client, _ = gateway_client
        res = await client.get("/v1/models")
        assert res.status_code == 200
        ids = [m["id"] for m in res.json()["data"]]
        # Magpie 命名空间前缀：`zai/` 与 `bigmodel/` 前缀供外部 Agent 选 provider
        assert ids == [
            "zai/GLM-5.3-Flash", "zai/GLM-5.3",
            "bigmodel/GLM-5.3-Flash", "bigmodel/GLM-5.3",
        ]

    async def test_messages_ok(self, gateway_client):
        client, upstream = gateway_client
        fresh_account("h1.eyJzdWIiOiJhIn0.sig")
        res = await client.post("/v1/messages", json={
            "model": "glm-5.2", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200
        assert res.json()["content"][0]["text"] == "Hello from mock upstream"
        # 上游收到归一化后的模型名与鉴权头
        method, path, headers, _ = upstream.state.calls[-1]
        assert path == "/api/v1/zcode-plan/anthropic/v1/messages"

    async def test_no_account_503(self, gateway_client):
        client, _ = gateway_client
        res = await client.post("/v1/messages", json={"model": "glm-5.2", "messages": []})
        assert res.status_code == 503
        assert res.json()["error"]["type"] == "no_available_account"


def fresh_account(secret: str) -> Account:
    """向当前 store 注入账号（gateway_client 用 fresh_app 的 store 单例）。"""
    from app.store import store
    return store.add_account("zai", "t", secret)


# ── 3009 并发识别（2026-11-03 review：禁止裸子串，logid 会撞车）───────────────
class TestConcurrencyHit:
    def test_business_code_3009(self):
        body = '{"code":3009,"msg":"model concurrency limit exceeded","logid":"202611030003009"}'
        assert _is_concurrency_hit(body) is True

    def test_logid_substring_alone_is_not_enough(self):
        """20 位随机 logid 含 "3009" 子串（真实 429 形态）不得误判为并发。"""
        body = '{"code":1302,"msg":"rate limited","logid":"202611030003009xxxx"}'
        assert _is_concurrency_hit(body) is False

    def test_text_fallback_for_non_json(self):
        assert _is_concurrency_hit("model concurrency limit exceeded") is True

    def test_other_codes_are_not_concurrency(self):
        assert _is_concurrency_hit('{"code":1113,"msg":"insufficient balance"}') is False
        assert _is_concurrency_hit("server busy") is False
