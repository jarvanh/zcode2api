"""请求体变换 —— 镜像官方 ZCode 3.17.2 ContextBuilder（bundle
assembleSystemMessages；3.17.2 起默认链 system 块仅剩 cli_prefix+identity，
env_info/model/current_date 迁 meta_user 附件 A3a、块3 默认为空——网关维持
3 块线缆形状不变，块3/context_prefix 不受校验（10-31 实弹））。历史镜像：
3.16.3/3.17.1 见 git 历史 zcode_sections_3163/3171.json。

上游 WAF 对 system 块的校验史（同为 HTTP 405 承载的 3012 "unusual activity"，
与账号/指纹/出口无关）：
  2026-10-12 起  按块「形状」拦截：独立 Environment 块 + powered-by 独立块、
                 2 块子代理形状均被拒，只认官方 3 块默认链（10-24 实弹修复）。
  2026-10-31 起  按「文本」拦截：块1 cli_prefix 与块2 stable 的措辞必须与
                 当前官方版本（3.17.1）一致——"an interactive coding agent"
                 （3.16.3）与块2 里的 Desktop Context 追加段均触发 405；
                 块3 dynamic 不校验（10-31 离线二分实弹：base 405 /
                 3.17.1 块1+2 + 3.16.3 块3 200，交叉混合逐块定位）。

官方桌面端默认装配（assembleSystemMessages）恰好 3 个线缆块：
  块1 cli_prefix 单独一块，无 cache_control（3.15.1 起不再带）；
  块2 全部 stable 段 \n\n 连接（3.17.1/3.17.2 同文：Agent Identity + Harness；
      CLI 面不携带 Desktop Context；3.17.2 custom_system_prompt 归 stable 族，
      但块2 文本被上游按版本校验，追加即破坏逐字节一致性——维持并入块3），ephemeral；
  块3 全部 dynamic 段 \n\n 连接并带 "\n\n" 前缀（Dynamic Behavior /
      Environment Info（powered-by 行并入本段末行）/ Context
      Management），ephemeral。
客户端自带 system（customSystemPrompt 语义）：官方 bundle 里本就是 stable
  段（Oda cacheHint:"stable"），但块2 文本已被上游按版本校验，任何追加都会
  破坏逐字节一致性——改并入块3（dynamic）尾部（Agent2API PR #343 同语义，
  10-31 实弹 200）。
另注入 meta_user context_prefix（currentDate 的 <system-reminder> 首条
user 消息，官方客户端始终携带；10-31 实弹证实不受校验）。

所有变换对畸形输入保持 no-op：解析失败返回原样，坏 body 永远不会被这里放大。
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
import os
import time
import uuid
from pathlib import Path

Sections = dict  # 侧车 zcode_sections_3172.json 的加载形态


def _load_sections() -> Sections | None:
    global _SECTIONS
    if _SECTIONS is None:
        try:
            _SECTIONS = json.loads(
                (Path(__file__).with_name("zcode_sections_3172.json")).read_text("utf-8")
            )
        except (OSError, ValueError):
            _SECTIONS = {}
    return _SECTIONS or None


_SECTIONS: Sections | None = None


def _is_plain_dict(v: object) -> bool:
    return isinstance(v, dict)


def _normalize_user_system(system: object) -> list[dict]:
    """把客户端原有 system 归一为 text block 列表（镜像 zapi normalizeUserSystem）。"""
    if system is None:
        return []
    if isinstance(system, str):
        text = system.strip()
        return [{"type": "text", "text": system}] if text else []
    if not isinstance(system, list):
        return []
    out: list[dict] = []
    for item in system:
        if isinstance(item, str):
            if item.strip():
                out.append({"type": "text", "text": item})
        elif isinstance(item, dict):
            if item.get("type") == "text" and isinstance(item.get("text"), str) and item["text"].strip():
                block = {"type": "text", "text": item["text"]}
                cc = item.get("cache_control")
                if isinstance(cc, dict):
                    block["cache_control"] = cc
                out.append(block)
    return out


def _custom_system_text(system: object) -> str | None:
    """客户端 system → customSystemPrompt 单字符串（bundle 语义 customSystemPrompt?.trim()）。

    字符串原样（trim 后空 = 无）；数组取 text 块 \n\n 连接。全部为空视为无
    custom（走默认三块身份路径，对齐 bundle !!o）。

    严格三块（上游 2026-11-02 实弹，与本实现的形状结论一致）：网关校验的是
    system 的**块结构**（cli_prefix 必须独立成首块），而非逐字内容——实测末块
    内容可任意（空串 / 仅前缀 / 无前缀均 200）。外来 system 文本只许并入末块
    内部；追加成第 4 块、或另起独立块，都会破坏形态 → 405 3012。
    """
    if system is None:
        return None
    if isinstance(system, str):
        text = system.strip()
        return text or None
    if not isinstance(system, list):
        return None
    parts: list[str] = []
    for item in system:
        if isinstance(item, str):
            if item.strip():
                parts.append(item)
        elif isinstance(item, dict):
            if item.get("type") == "text" and isinstance(item.get("text"), str) and item.get("text").strip():
                # cache_control 有意丢弃 —— 见 build_start_plan_system 文档
                parts.append(item["text"])
    return "\n\n".join(parts) if parts else None


def has_custom_system_prompt(system: object) -> bool:
    """客户端 system 是否携带 customSystemPrompt（现并入 stable 块，不再走独立形状）。"""
    return _custom_system_text(system) is not None


def build_environment_section(env: dict, model: str | None, provider: str | None) -> str:
    """Environment Info 段（bundle Eua）：真实运行时值 + powered-by 末行。"""
    e = _load_sections()["environment"]
    lines = [
        e["heading"],
        e["invokedLine"],
        f"- {e['cwdLabel']}: {env.get('cwd', 'unknown')}",
        f"- {e['gitLabel']}: {e['gitNo']}",
        f"- {e['platformLabel']}: {env.get('platform', 'unknown')}",
        f"- {e['shellLabel']}: {env.get('shell', 'unknown')}",
        f"- {e['osVersionLabel']}: {env.get('osVersion', 'unknown')}",
    ]
    model_id = (model or "").strip()
    if model_id and provider:
        lines.append(
            e["poweredByLine"].replace("{provider}", provider).replace("{model}", model_id)
        )
    return "\n".join(lines)


def build_start_plan_system(
    existing_system: object,
    model: str | None = None,
    provider: str | None = None,
    env: dict | None = None,
) -> list[dict]:
    """镜像官方 assembleSystemMessages 的 3 块默认装配；custom system 并入
    dynamic 合并块尾部（块2 须保持 3.17.2 逐字节官方文本——与 3.17.1 同文，
    见模块头注释）。

    env 段值必须与身份头同源（platform/osVersion 与 X-Platform/X-Os-Version
    一致），混合组合是真实客户端不产生的形状。
    """
    sec = _load_sections()
    if not sec:
        return []
    custom = _custom_system_text(existing_system)
    env = env or {}
    stable = "\n\n".join(sec["stableSections"])
    dynamic = "\n\n".join([
        sec["dynamicSections"]["beforeEnvironment"],
        build_environment_section(env, model, provider),
        sec["dynamicSections"]["afterEnvironment"],
    ])
    if custom is not None:
        # 2026-10-31 起 WAF 按版本校验块1/块2 文本：stable 块任何追加都会破坏
        # 逐字节一致性（"an interactive coding agent" 旧措辞与 Desktop Context
        # 追加段均 405）。custom（官方语义 stable 段）改并入不受校验的块3 尾部，
        # 线缆仍为 3 块官方形状（Agent2API PR #343 同语义，10-31 实弹 200）。
        dynamic = "\n\n".join([dynamic, custom])
    return [
        {"type": "text", "text": sec["cliPrefix"]},
        {"type": "text", "text": stable, "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": f"\n\n{dynamic}", "cache_control": {"type": "ephemeral"}},
    ]


def build_context_prefix_message(now: datetime.datetime | None = None) -> dict:
    """meta_user context_prefix（bundle tct+Vre+blt）：首条 user 消息，单 text 块
    为 <system-reminder> 包裹的 currentDate 段（官方客户端始终携带）。
    """
    sec = _load_sections()
    if not sec:
        return {}
    cp = sec["contextPrefix"]
    date = (now or datetime.datetime.now()).strftime("%Y-%m-%d")
    current_date = f"{cp['currentDateHeading']}\n{cp['currentDateLine'].replace('{date}', date)}"
    body = "\n".join([cp["intro"], current_date, "", cp["outro"]])
    sr = sec["systemReminder"]
    return {
        "role": "user",
        "content": [{"type": "text", "text": f"{sr['open']}{body}{sr['close']}"}],
    }


def resolve_env_info(account=None) -> dict:
    """Environment 段的真实运行时值。与身份头同源：有账号取其指纹档案
    （platform/osVersion 与 X-Platform/X-Os-Version 一致），无账号回退全局
    伪装常量。cwd/shell 取本机真实值（真实客户端 cwd 永不为 unknown）。
    """
    from . import constants

    if account is not None:
        from .fingerprint import profile_for

        profile = profile_for(account)
        platform = profile.platform
        os_version = profile.os_version
    else:
        platform = constants.CLIENT_PLATFORM.split("-")[0]
        os_version = constants.IDENTITY_OS_VERSION
    shell_raw = os.getenv("SHELL") or os.getenv("ComSpec") or ""
    return {
        "cwd": os.getcwd(),
        "platform": platform,
        "shell": os.path.basename(shell_raw) if shell_raw else "unknown",
        "osVersion": os_version,
    }


def apply_start_plan_system(body: dict, model: str | None = None, account=None) -> bool:
    """重写 system 为官方 3.16.3 线缆形状（默认 3 块 / custom 2 块）。

    幂等：首块已是官方 cli_prefix 且块数匹配新形状则跳过（重复变换安全，
    验证码重试时用同一 body 重建请求）。
    """
    sec = _load_sections()
    if not sec:
        return False
    existing = body.get("system")
    if isinstance(existing, list) and existing:
        first = existing[0]
        if (
            isinstance(first, dict)
            and first.get("text") == sec["cliPrefix"]
            and len(existing) in (2, 3)
        ):
            return False
    provider = "zai-api" if model else None
    body["system"] = build_start_plan_system(existing, model, provider, resolve_env_info(account))
    return True


def _is_date_turn_text(text: object) -> bool:
    """注入的「当前日期」user 轮的标记（幂等判定与会话指纹共用同一真相源）。"""
    return isinstance(text, str) and "<system-reminder>" in text and "Today's date is" in text


def apply_current_date_turn(body: dict) -> bool:
    """把官方 meta_user 的「当前日期」附件补进首条 user 轮（3.17.1 起不在 system 块里）。

    形态：`<system-reminder>\nToday's date is YYYY-MM-DD.\n</system-reminder>` 的独立 user 轮。
    幂等：首条消息已是该形态则跳过。
    """
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return False
    first = messages[0]
    if isinstance(first, dict) and first.get("role") == "user":
        content = first.get("content")
        if isinstance(content, list) and content and isinstance(content[0], dict):
            if _is_date_turn_text(content[0].get("text")):
                return False
    date_text = f"<system-reminder>\nToday's date is {time.strftime('%Y-%m-%d')}.\n</system-reminder>"
    body["messages"] = [
        {"role": "user", "content": [{"type": "text", "text": date_text}]},
        *messages,
    ]
    return True


def apply_cache_control(body: dict) -> bool:
    """最后一条非 system 消息的最后一个 block 加 ephemeral 缓存标记。幂等。"""
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return False

    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if not isinstance(msg, dict):
            continue
        if msg.get("role") == "system":
            continue

        content = msg.get("content")
        if isinstance(content, str):
            msg["content"] = [{
                "type": "text",
                "text": content,
                "cache_control": {"type": "ephemeral"},
            }]
            return True
        if isinstance(content, list) and content:
            last_block = content[-1]
            if isinstance(last_block, dict) and not last_block.get("cache_control"):
                last_block["cache_control"] = {"type": "ephemeral"}
                return True
        return False
    return False


def apply_user_id(body: dict, user_id: str) -> bool:
    """注入 metadata.user_id（保留已有 metadata 其它字段）。幂等。"""
    existing = body.get("metadata")
    if _is_plain_dict(existing) and existing.get("user_id") == user_id:
        return False
    merged = dict(existing) if _is_plain_dict(existing) else {}
    merged["user_id"] = user_id
    body["metadata"] = merged
    return True


def attribution_user_id(device_mid: str | None, session_id: str | None) -> str:
    """官方 attribution 的 metadata.user_id 形态：JSON 字符串 blob。

    bundle `Bdt` / vlelyanin transform.js 实证：
      `{"device_id": <mid>, "account_uuid": "", "session_id": <sid>}`
    （旧版直塞 JWT sub 是错误形态）
    """
    blob = {
        "device_id": device_mid or "",
        "account_uuid": "",
        "session_id": session_id or "",
    }
    return json.dumps(blob, separators=(",", ":"))


def extract_first_user_text(body: dict) -> str:
    """提取会话首条非日期注入的用户文本。无有效文本返回空字符串。"""
    messages = body.get("messages")
    if not isinstance(messages, list):
        return ""
    for msg in messages:
        if not isinstance(msg, dict) or msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            if not _is_date_turn_text(content) and content.strip():
                return content.strip()
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text" \
                        and isinstance(block.get("text"), str):
                    candidate = block["text"]
                    if not _is_date_turn_text(candidate) and candidate.strip():
                        return candidate.strip()
    return ""


def conversation_session_id(body: dict, account_id: str = "") -> str | None:
    """同一会话的稳定 session_id（同首轮 → 同 id），无首轮文本返回 None。

    官方客户端的 `x-session-id` 与 metadata blob 的 `session_id` 在会话内
    稳定，prompt cache 的命中依赖它。hub 无会话态，用「会话首条 user 文本」
    做指纹：多轮请求里首轮文本不变，因此同一会话自然拿到同一 id。

    加入 account_id 隔离命名空间，避免不同账号/不同租户撞同一个 id
    （id 会随请求上报给上游，跨号相同是额外的关联信号）。

    ⚠️ 必须跳过注入的日期轮（2026-11-03 复现的缺陷）：`transform_body` 会把
    日期轮插到 messages[0] 且**原地**改写 body，而重试/换号路径会对同一个
    body 对象再次调用 `build_request`——不跳过的话，第二次起指纹就落在日期
    文本上，同一账号同一天所有会话撞成同一个 id（跨会话关联信号 + cache 键
    漂移）。跳过规则对「前插任意个匹配轮」幂等，指纹与首次调用一致。
    """
    text = extract_first_user_text(body)
    if not text:
        return None
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"zcode-hub:{account_id}:{digest}"))


def jwt_user_id(jwt_token: str | None) -> str | None:
    """从 JWT payload 解 user_id（sub / user_id 字段），失败返回 None。

    JWT 是账号凭证（短期有效），user_id 跟随 token 变化，因此不跨进程缓存。
    """
    if not jwt_token or jwt_token.count(".") != 2:
        return None
    try:
        payload_b64 = jwt_token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except (ValueError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    user_id = payload.get("user_id") or payload.get("sub")
    return str(user_id) if user_id else None


def transform_body(
    body: dict,
    user_id: str | None = None,
    model: str | None = None,
    account=None,
    device_mid: str | None = None,
    session_id: str | None = None,
) -> dict:
    """按 anthropic 通道变换 body（原地修改并返回）。变换失败静默保持原样。

    account 透传（Environment 段与身份头同源）；日期轮在此注入（官方客户端
    始终携带）。attribution 优先用 device_mid/session_id 组官方 blob，user_id
    （JWT sub）仅兜底。
    """
    try:
        apply_start_plan_system(body, model, account)
        apply_cache_control(body)
        apply_current_date_turn(body)
        attribution = attribution_user_id(device_mid, session_id) if (device_mid or session_id) else None
        if attribution:
            apply_user_id(body, attribution)
        elif user_id:
            apply_user_id(body, user_id)
        ctx = build_context_prefix_message()
        msgs = body.get("messages")
        if ctx and isinstance(msgs, list) and not _has_context_prefix(msgs):
            msgs.insert(0, ctx)
    except Exception:  # noqa: BLE001 - 变换永不放大请求失败
        pass
    return body


def _has_context_prefix(msgs: list) -> bool:
    """首条 user 消息是否已是 context_prefix（system-reminder 包裹）。幂等判定。"""
    if not msgs or not isinstance(msgs[0], dict):
        return False
    first = msgs[0]
    if first.get("role") != "user":
        return False
    content = first.get("content")
    if isinstance(content, list) and content and isinstance(content[0], dict):
        return str(content[0].get("text", "")).startswith("<system-reminder>")
    if isinstance(content, str):
        return content.lstrip().startswith("<system-reminder>")
    return False
