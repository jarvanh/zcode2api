"""GLM-5.3 家族思考预算契约（对齐官方 bundle anthropic 构造器口径）。

背景（2026-11-01 对比 TriDefender/zcode-api reasoning.ts / Agent2API
reasoning.rs，多用户主机共享项目事实）：GLM-5.3 / GLM-5.3-Flash 是恒思考
模型——「关思考」三种写法（thinking:disabled / reasoning_effort none/minimal）
全部被上游拒（400 code 1210），且思考与正文共用 max_tokens 这一个额度：
客户端给小额度（Codex CLI 标题生成类内部请求）时额度全部被思考吃掉，
content 空串而 HTTP 仍 200 / finish_reason=length——「200 但正文为空」。

官方客户端口径（bundle）：max_tokens 把思考预算加在上面（max_tokens +=
budget，再按模型 maxOutputTokens 截顶）——预算花在「回答额度之外」，真实
流量因此永远留得住正文空间。output_config.effort 是上游唯一消费的档位通道
（裸 reasoning_effort 被静默忽略），且必须配对 thinking.budget_tokens
（只发档位不配预算，上游仍按近零默认跑）。

档位与预算值：ZCode 目录 glm-5.3 defaultLevel=max，三档各配固定预算
（low 8K / high 16K / max 32K）+ 实测复核；OpenAI reasoning_effort 按
官方映射表向上取整（medium→high）。模型输出上限取 ZCode 目录值
（glm-5.3/-flash/-2 = 128K，5.1/5/turbo = 64K，4.7/4.6 = 131072）。

幂等性：本模块只在 openai_compat.openai_to_anthropic 的「新建 body」上
调用一次（max_tokens += 预算不可重入）；Anthropic 原生通道 /v1/messages
不经过此处——官方客户端自己负责加预算，网关透传即可。
"""

from __future__ import annotations

import re

from . import settings

# 三档合法 effort 与 ZCode 目录配对预算（只发档位不配预算 = 上游近零默认）
GLM53_EFFORT_LEVELS = ("low", "high", "max")
GLM53_DEFAULT_EFFORT = "max"
GLM53_THINKING_BUDGETS = {"low": 8_000, "high": 16_000, "max": 32_000}
# bundle：thinking 开而未给预算时 SDK 强制默认 1024（低于此档预算实测塌缩回近零）
GLM53_MIN_THINKING_BUDGET = 1_024

# ZCode 目录 maxOutputTokens（截顶用；未收录模型不截顶）
MODEL_MAX_OUTPUT_TOKENS = {
    "glm-5.3": 128_000,
    "glm-5.3-flash": 128_000,
    "glm-5.2": 128_000,
    "glm-5.1": 64_000,
    "glm-5": 64_000,
    "glm-5-turbo": 64_000,
    "glm-4.7": 131_072,
    "glm-4.6": 131_072,
}

# glm-5.3 家族（含 flash），排除 glm-5.30 之类未来编号；大小写不敏感
_GLM53_PATTERN = re.compile(r"glm-5\.3(?![0-9])", re.IGNORECASE)


def is_glm53_model(model: str | None) -> bool:
    """是否 GLM-5.3 家族（glm-5.3 / glm-5.3-flash，大小写不敏感）。"""
    return bool(model) and bool(_GLM53_PATTERN.search(model))


def normalize_effort(effort: object) -> str:
    """OpenAI reasoning_effort → 三档 GLM-5.3 effort（向上取整，未知回默认）。

    上游对档位缺失按近零默认跑，故取 ZCode 目录默认 max 而非 OpenAI 的
    medium；medium 映射到 high（向上取整，非就近）。
    """
    if effort in ("xhigh", "max", "ultra"):
        return "max"
    if effort in ("medium", "high"):
        return "high"
    if effort in ("none", "minimal", "light", "low"):
        return "low"
    return GLM53_DEFAULT_EFFORT


def _clamp_budget_to_model(budget: int, model: str) -> int:
    """预算按模型目录上限截顶（bundle：min(budget, maxOutputTokens - 1)）。"""
    model_max = MODEL_MAX_OUTPUT_TOKENS.get((model or "").lower())
    if not isinstance(model_max, int):
        return budget
    return min(budget, model_max - 1)


def _inject_glm53(body: dict, payload: dict, explicit: object) -> None:
    """GLM-5.3 家族：注入 thinking{enabled,budget} + output_config{effort}。

    - 显式 thinking:{type:disabled} 原样保留（flash 上确实生效；对 5.3 是
      最接近关思考的可用信号，不在网关层改写成档位）；
    - reasoning_effort: none/minimal 也走档位通道映射到 low（disabled 对
      5.3 无效，实测仍回思考）；
    - 显式预算 > 0 优先，档位预算兜底；output_config 恒附（缺它上游无视
      预算按自身默认跑）。
    """
    if isinstance(explicit, dict) and explicit.get("type") == "disabled":
        body["thinking"] = {"type": "disabled"}
        return
    effort = normalize_effort(payload.get("reasoning_effort"))
    budget = GLM53_THINKING_BUDGETS[effort]
    if isinstance(explicit, dict) and explicit.get("type") in ("enabled", "adaptive"):
        raw = explicit.get("budget_tokens", explicit.get("budgetTokens"))
        if isinstance(raw, (int, float)) and raw == raw and raw not in (float("inf"), float("-inf")):
            floored = int(raw)
            if floored > 0:
                budget = floored
    body["thinking"] = {"type": "enabled", "budget_tokens": _clamp_budget_to_model(budget, str(body.get("model") or ""))}
    body["output_config"] = {"effort": effort}


def _inherit_explicit_thinking(body: dict, explicit: object) -> None:
    """非 5.3 家族：仅显式 thinking 透传（预算取整、正数校验）；无则不注入。"""
    if not isinstance(explicit, dict):
        return
    kind = explicit.get("type")
    if kind == "disabled":
        body["thinking"] = {"type": "disabled"}
    elif kind in ("enabled", "adaptive"):
        entry: dict = {"type": kind}
        raw = explicit.get("budget_tokens", explicit.get("budgetTokens"))
        if isinstance(raw, (int, float)) and raw == raw and 0 < raw not in (float("inf"),):
            entry["budget_tokens"] = int(raw)
        body["thinking"] = entry


def _apply_thinking_compat(body: dict) -> None:
    """镜像 bundle anthropic 构造器 compat（thinking 注入之后执行）：

    - thinking enabled 无预算 → 补默认 1024（bundle 口径）；
    - thinking enabled → temperature/top_k/top_p 剔除（上游拒绝与扩展思考
      同发采样参数；真实客户端从不发该组合）；
    - thinking enabled → max_tokens += 预算，再按模型目录上限截顶；
    - 无 thinking + temperature 与 top_p 同设 → 剔 top_p（SDK 口径）。
    """
    thinking = body.get("thinking")
    enabled = isinstance(thinking, dict) and thinking.get("type") in ("enabled", "adaptive")
    if not enabled:
        if body.get("temperature") is not None and body.get("top_p") is not None:
            body.pop("top_p", None)
        return
    budget = thinking.get("budget_tokens")
    if not isinstance(budget, int) or budget <= 0:
        budget = GLM53_MIN_THINKING_BUDGET
        thinking["budget_tokens"] = budget
    body.pop("temperature", None)
    body.pop("top_k", None)
    body.pop("top_p", None)
    max_tokens = body.get("max_tokens")
    if isinstance(max_tokens, int):
        body["max_tokens"] = min(max_tokens + budget, MODEL_MAX_OUTPUT_TOKENS.get(str(body.get("model") or "").lower(), max_tokens + budget))


def apply_reasoning(body: dict, payload: dict) -> None:
    """OpenAI→Anthropic 入站思考档处理（唯一调用点 openai_to_anthropic 尾部）。

    变换失败静默保持原样（与 openai_compat 同规矩：转换层绝不放大请求失败）。
    ZCODE_THINKING_COMPAT=0 时整体旁路。
    """
    if not settings.THINKING_COMPAT:
        return
    try:
        explicit = payload.get("thinking")
        if is_glm53_model(str(body.get("model") or "")):
            _inject_glm53(body, payload, explicit)
        else:
            _inherit_explicit_thinking(body, explicit)
        _apply_thinking_compat(body)
    except Exception:  # noqa: BLE001 - 转换层绝不放大请求失败
        pass
