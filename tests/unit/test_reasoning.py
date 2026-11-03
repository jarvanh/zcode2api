"""思考预算契约（app/reasoning.py）单元测试。

契约出处（bundle 口径 + zcode-api reasoning.ts 实测互证）：
- GLM-5.3 家族恒思考，output_config.effort 是唯一被上游消费的档位通道，
  且必须配对 thinking.budget_tokens；
- max_tokens += 预算（预算花在回答额度之外），再按模型目录上限截顶；
- thinking enabled 时 temperature/top_k/top_p 被上游拒绝，必须剔除。
"""

from __future__ import annotations

import pytest

from app import reasoning, settings


def _payload(model="glm-5.3", max_tokens=4096, **extra):
    p = {"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": max_tokens}
    p.update(extra)
    return p


@pytest.fixture(autouse=True)
def _compat_on(monkeypatch):
    monkeypatch.setattr(settings, "THINKING_COMPAT", True)


def test_glm53_default_effort_max_and_budget_add():
    from app.openai_compat import openai_to_anthropic

    body, err = openai_to_anthropic(_payload())
    assert err is None
    assert body["thinking"] == {"type": "enabled", "budget_tokens": 32_000}
    assert body["output_config"] == {"effort": "max"}
    # 4096 + 32000 = 36096，未触 128K 顶
    assert body["max_tokens"] == 36_096


def test_glm53_effort_mapping_rounds_up():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(reasoning_effort="medium"))
    assert body["output_config"] == {"effort": "high"}
    assert body["thinking"]["budget_tokens"] == 16_000

    body, _ = openai_to_anthropic(_payload(reasoning_effort="none"))
    # disabled 对 5.3 无效 → 档位通道映射 low（最接近关思考的可用形态）
    assert body["output_config"] == {"effort": "low"}
    assert body["thinking"]["budget_tokens"] == 8_000


def test_glm53_explicit_budget_wins_effort_still_attached():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(
        max_tokens=100,
        reasoning_effort="high",
        thinking={"type": "enabled", "budget_tokens": 2048.7},
    ))
    # 显式预算取整优先；output_config 恒附（缺它上游按近零默认跑）
    assert body["thinking"] == {"type": "enabled", "budget_tokens": 2048}
    assert body["output_config"] == {"effort": "high"}
    # 100 + 2048 = 2148
    assert body["max_tokens"] == 2148


def test_glm53_small_max_tokens_gets_answer_room():
    """核心场景：客户端小额度（Codex 标题类请求）不再被思考吃光。"""
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(max_tokens=16))
    assert body["max_tokens"] == 16 + 32_000


def test_glm53_budget_clamped_to_model_ceiling():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(max_tokens=120_000))
    # 120000 + 32000 = 152000 > 128000 → 截顶
    assert body["max_tokens"] == 128_000


def test_glm53_flash_family_match():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(model="GLM-5.3-Flash"))
    assert body["thinking"]["budget_tokens"] == 32_000


def test_explicit_disabled_passthrough():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(model="glm-5.3-flash", thinking={"type": "disabled"}))
    assert body["thinking"] == {"type": "disabled"}
    assert "output_config" not in body
    assert body["max_tokens"] == 4096


def test_non_glm53_no_thinking_injection_unless_explicit():
    from app.openai_compat import openai_to_anthropic

    # 非 5.3 家族不注入 thinking 预算；但 reasoning_effort 是上游通用档位通道
    # （2.7.x 并入的 openai_compat 行为，官方客户端同形态），恒映射 output_config
    body, _ = openai_to_anthropic(_payload(model="glm-4.6", reasoning_effort="high"))
    assert "thinking" not in body
    assert body["output_config"] == {"effort": "high"}

    body, _ = openai_to_anthropic(_payload(model="glm-4.6", thinking={"type": "enabled", "budget_tokens": 4096}))
    assert body["thinking"] == {"type": "enabled", "budget_tokens": 4096}
    # 非目录收录模型不截顶（4096+4096=8192 < 无上限概念）
    assert body["max_tokens"] == 8192


def test_thinking_voids_sampling_params():
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(temperature=0.7, top_p=0.9))
    assert "temperature" not in body
    assert "top_p" not in body

    # 无 thinking + temperature 与 top_p 同设 → 仅剔 top_p（SDK 口径）
    body, _ = openai_to_anthropic(_payload(model="glm-4.6", temperature=0.7, top_p=0.9))
    assert body["temperature"] == 0.7
    assert "top_p" not in body


def test_compat_off_bypass(monkeypatch):
    monkeypatch.setattr(settings, "THINKING_COMPAT", False)
    from app.openai_compat import openai_to_anthropic

    body, _ = openai_to_anthropic(_payload(max_tokens=16))
    assert "thinking" not in body
    assert body["max_tokens"] == 16


def test_malformed_thinking_never_raises():
    from app.openai_compat import openai_to_anthropic

    body, err = openai_to_anthropic(_payload(thinking="garbage", reasoning_effort=123))
    assert err is None  # 转换层绝不放大请求失败
