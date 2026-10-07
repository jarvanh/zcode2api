"""套餐外模型前置校验回归：秒回 400，而不是排队空等一个绝无可能的账号。

背景（2026-10-07 故障复盘）：客户端请求 glm-5.3，但账号池套餐只授权
glm-5.3-flash。旧路径里该请求会走完排队（等满预算）、换遍全部账号后
仍 503「无可用账号 / 额度均已耗尽」，错误信息误导成容量问题；实际上
这是授权问题，等再久都不会成功，应秒回 400 并列出可用模型。

边界：授权名单取不到（空池 / 账号无套餐信息）时不拦截 —— 静态
AVAILABLE_MODELS 只是 /v1/models 的展示回退而非授权事实，拿它拦截
会在冷启动误杀（回归基线 test_no_account_503 锁定空池 → 503 行为）。
"""

from __future__ import annotations

from app.models import Account
from app.routes.gateway import _reject_model_not_allowed


def _account_with_entitlements(
    secret: str = "jwt.token.a",
    name: str = "t",
    capabilities: list[str] | None = None,
) -> Account:
    """注入一个带套餐授权的 JWT 账号（gateway_client / fresh_app 共用 store）。"""
    from app.store import store

    acc = store.add_account("zai", name, secret)
    acc.plans = [{
        "plan_id": "test-plan",
        "entitlements": [{
            "entitlement_id": "test-plan",
            "capabilities": capabilities if capabilities is not None
            else ["model:glm-5.3-flash"],
        }],
    }]
    store.update_account(acc)
    return acc


class TestModelNotAllowedHTTP:
    async def test_unauthorized_model_rejected_with_400(self, gateway_client):
        """套餐外模型必须 400 + 明确报错 + 可用清单，绝不能走成 503。"""
        client, _ = gateway_client
        _account_with_entitlements()
        res = await client.post("/v1/messages", json={
            "model": "glm-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 400
        err = res.json()["error"]
        assert err["type"] == "model_not_allowed"
        assert "GLM-5.3-Flash" in err["allowed_models"]
        # 报错文案里是官方大小写的 canon 名
        assert "GLM-5.3-Flash" in err["message"]

    async def test_unauthorized_model_rejected_on_openai_route(self, gateway_client):
        """OpenAI 兼容端点同样受前置校验保护（两入口都要拦）。"""
        client, _ = gateway_client
        _account_with_entitlements()
        res = await client.post("/v1/chat/completions", json={
            "model": "glm-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 400
        assert res.json()["error"]["type"] == "model_not_allowed"

    async def test_empty_pool_not_intercepted(self, gateway_client):
        """空池时不得按静态展示名单误杀 —— 仍走调度出原有 503。"""
        client, _ = gateway_client
        # 不注入任何账号（无 entitlements → 授权名单为空）
        res = await client.post("/v1/messages", json={
            "model": "glm-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code != 400
        assert res.status_code == 503
        assert res.json()["error"]["type"] == "no_available_account"

    async def test_account_without_entitlements_not_intercepted(self, gateway_client):
        """账号存在但拿不到 capabilities → 同样放行（宁走原路径，不误杀）。"""
        client, _ = gateway_client
        from app.store import store
        store.add_account("zai", "no-plan", "jwt.token.noplan")  # 无 plans
        res = await client.post("/v1/messages", json={
            "model": "glm-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code != 400


class TestModelAllowedPassThrough:
    def test_authorized_model_passes(self, fresh_app):
        """授权内模型放行（None = 不拦截）。"""
        _account_with_entitlements()
        assert _reject_model_not_allowed("glm-5.3-flash") is None
        assert _reject_model_not_allowed("GLM-5.3-Flash") is None  # 标准化后官方名

    def test_case_insensitive(self, fresh_app):
        """大小写不敏感：奇怪大小写的授权内模型也放行。"""
        _account_with_entitlements()
        assert _reject_model_not_allowed("GLM-5.3-FLASH") is None

    def test_unauthorized_model_rejected_unit(self, fresh_app):
        """单元级：套餐外模型返回 400 响应体。"""
        _account_with_entitlements()
        resp = _reject_model_not_allowed("glm-5.3")
        assert resp is not None
        assert resp.status_code == 400

    def test_multiple_entitlements_union(self, fresh_app):
        """多账号 capabilities 取并集：任一账号授权即放行。"""
        # 注意：两个账号必须用不同 secret —— store 按 secret 去重，
        # 同 secret 会拿到同一个账号对象，第二个的 plans 会覆盖第一个
        _account_with_entitlements(
            capabilities=["model:glm-5.3-flash"], name="a", secret="jwt.token.a",
        )
        _account_with_entitlements(
            capabilities=["model:glm-5.3"], name="b", secret="jwt.token.b",
        )
        assert _reject_model_not_allowed("glm-5.3") is None
        assert _reject_model_not_allowed("glm-5.3-flash") is None
        assert _reject_model_not_allowed("glm-5.2") is not None
