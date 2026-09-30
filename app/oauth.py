"""Z.AI OAuth 登录流程。

主要供 CLI `login zai` 使用：发起 OAuth → 轮询 → 兑换 API Key。
"""

from __future__ import annotations

import re
import secrets

import httpx

from . import constants, settings


def parse_userinfo(info: dict | None, expected_uid: str | None = None) -> str | None:
    """从 userinfo 载荷提取用户名（账号命名的依据）。

    字段名按桌面端缓存形态与常见 OIDC 命名防御式解析（username/displayName/
    name/nickname）；uid 可解析时与 JWT 身份比对，对不上视为不可信返回 None。
    """
    if not isinstance(info, dict):
        return None
    uid = str(info.get("user_id") or info.get("id") or info.get("sub") or "")
    if expected_uid and uid and uid != expected_uid:
        return None
    for key in ("username", "displayName", "name", "nickname"):
        raw = info.get(key)
        if not isinstance(raw, str):
            continue
        name = re.sub(r"[\s/\\:#?\"'<>|%*`]+", "", raw.strip())[:24]
        if name:
            return name
    return None


class ZaiAuthFlow:
    """api_base / exchange_origin 可注入（测试指向 Mock 上游）；
    默认值来自 settings（其缺省又来自 constants 收口）。

    官方 CLI 规范（对齐 zcode.cjs createZaiCliOAuthClient）：
    - init 仅带 Authorization: Bearer <pollToken> 与 Content-Type: application/json
    - poll 仅带 Authorization: Bearer <pollToken>
    不携带额外伪装头，避免上游服务端对 OAuth 会话产生异常的设备/上下文绑定限制。
    """

    def __init__(self, api_base: str | None = None, exchange_origin: str | None = None) -> None:
        self.api_base = api_base or settings.OAUTH_API_BASE
        self.exchange_origin = exchange_origin or settings.ZAI_EXCHANGE_ORIGIN
        self.poll_token = secrets.token_hex(32)

    async def init(self) -> tuple[str, str]:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.post(
                f"{self.api_base}/oauth/cli/init",
                headers={
                    "Authorization": f"Bearer {self.poll_token}",
                    "Content-Type": "application/json",
                },
                json={"provider": "zai"},
            )
        res.raise_for_status()
        data = res.json().get("data") or {}
        flow_id, authorize_url = data.get("flow_id"), data.get("authorize_url")
        if not flow_id or not authorize_url:
            raise RuntimeError("返回的 OAuth 流程数据不完整")
        return flow_id, authorize_url

    async def poll(self, flow_id: str) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.get(
                f"{self.api_base}/oauth/cli/poll/{flow_id}",
                headers={"Authorization": f"Bearer {self.poll_token}"},
            )
        res.raise_for_status()
        return res.json().get("data") or {}

    async def userinfo(self, access_token: str) -> dict | None:
        """OAuth access_token → 用户身份（uid + 用户名）。

        只在授权成功的 poll 返回里有这个 token，userinfo 也只认它——池内存量
        zcode JWT / apiKey 打上游任何身份接口都被拒（2026-09-30 实测），所以
        用户名必须在 poll 当场抓取，错过不可补查。失败一律返回 None（命名/别名
        是锦上添花，绝不能阻塞入池主链路）。
        """
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(
                    constants.OAUTH_USERINFO_URL,
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            if res.status_code != 200:
                return None
            data = res.json()
        except (httpx.HTTPError, ValueError):  # noqa: BLE001 - 上游抖动/非 JSON
            return None
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        return data if isinstance(data, dict) else None

    async def exchange_api_key(self, access_token: str) -> str:
        """OAuth access_token → 业务 token → 机构/项目 → API Key。"""
        async with httpx.AsyncClient(timeout=30) as client:
            login = await client.post(
                f"{self.exchange_origin}/api/auth/z/login",
                headers={"Content-Type": "application/json"},
                json={"token": access_token},
            )
            login.raise_for_status()
            biz = (login.json().get("data") or {})
            biz_token = biz.get("access_token") or biz.get("accessToken")
            if not biz_token:
                raise RuntimeError("返回数据中不含业务凭证")

            info = await client.get(
                f"{self.exchange_origin}/api/biz/customer/getCustomerInfo",
                headers={"Authorization": f"Bearer {biz_token}"},
            )
            info.raise_for_status()
            orgs = (info.json().get("data") or {}).get("organizations") or []
            org = next((o for o in orgs if "默认机构" in (o.get("organizationName") or "")), None) or (orgs[0] if orgs else None)
            if not org:
                raise RuntimeError("找不到可用的机构")
            projects = org.get("projects") or []
            proj = next((p for p in projects if "默认项目" in (p.get("projectName") or "")), None) or (projects[0] if projects else None)
            if not proj:
                raise RuntimeError("找不到可用的项目")

            org_id, proj_id = org["organizationId"], proj["projectId"]
            key_url = (f"{self.exchange_origin}/api/biz/v1/organization/"
                       f"{org_id}/projects/{proj_id}/api_keys")

            keys_res = await client.get(key_url, headers={"Authorization": f"Bearer {biz_token}"})
            keys_res.raise_for_status()
            keys = keys_res.json().get("data") or []
            key_obj = next((k for k in keys if k.get("name") == "zcode-api-key"), None)
            if not key_obj:
                create = await client.post(
                    key_url,
                    headers={"Authorization": f"Bearer {biz_token}", "Content-Type": "application/json"},
                    json={"name": "zcode-api-key"},
                )
                create.raise_for_status()
                key_obj = create.json().get("data")

            api_key = (key_obj or {}).get("apiKey")
            if not api_key:
                raise RuntimeError("获取 API Key 失败")

            copy = await client.get(
                f"{key_url}/copy/{api_key}",
                headers={"Authorization": f"Bearer {biz_token}"},
            )
            copy.raise_for_status()
            secret_key = (copy.json().get("data") or {}).get("secretKey")
            if not secret_key:
                raise RuntimeError("未能解密 Secret Key")
        return f"{api_key}.{secret_key}"
