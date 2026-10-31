"""start-plan system 三块装配（2026-10-31 WAF 文本校验事件回归）。

上游 405/3012 校验史：
  - 10-12 起按「形状」拦：非官方 3 块链（2 块子代理 / 4+ 块）全拒；
  - 10-31 起按「文本」拦：块1 cli_prefix 与块2 stable 必须与当前官方版本
    （3.17.1）逐字节一致——3.16.3 旧措辞与 Desktop Context 追加段均 405；
    块3 dynamic 不校验（离线二分实弹：base 405 / 3.17.1 块1+2 + 3.16.3 块3
    200，交叉混合逐块定位）。

本文件锁三件事：块1/块2 与 sidecar（3.17.1）逐字节一致、custom system 只
出现在块3 尾部、重复变换幂等。
"""

from __future__ import annotations

import json

from app import body_transform as bt
from app.body_transform import (
    _normalize_user_system,
    apply_start_plan_system,
    build_start_plan_system,
    has_custom_system_prompt,
)


def test_sidecar_is_3171_wording():
    """sidecar 必须是 3.17.1 措辞——旧文本会直接 405，这里锁死防回退。"""
    sec = bt._load_sections()
    assert sec["cliPrefix"] == "You are ZCode, an interactive agent"
    stable = "\n\n".join(sec["stableSections"])
    assert stable.startswith("\nYou are an agent working with the user toward their goals")
    assert "# ZCode Desktop Context" not in stable


def test_default_assembly_three_blocks():
    """无 custom：块1 无 cache_control，块2/3 ephemeral，块3 以 \\n\\n 开头。"""
    blocks = build_start_plan_system(None, "glm-5.3-flash", "zai-api", {"cwd": "/x", "platform": "darwin", "shell": "zsh", "osVersion": "24.5.0"})
    assert len(blocks) == 3
    assert blocks[0]["text"] == bt._load_sections()["cliPrefix"]
    assert "cache_control" not in blocks[0]
    assert blocks[1]["cache_control"] == {"type": "ephemeral"}
    assert blocks[2]["cache_control"] == {"type": "ephemeral"}
    assert blocks[2]["text"].startswith("\n\n")
    # 块2 = stable 段原样合并，不携带 Desktop Context
    assert blocks[1]["text"] == "\n\n".join(bt._load_sections()["stableSections"])


def test_custom_system_rides_block3_tail():
    """custom system 并入块3 尾部；块2 保持逐字节官方文本（10-31 实弹语义）。"""
    custom = "请准确回答用户的问题。"
    blocks = build_start_plan_system(custom, "glm-5.3-flash", "zai-api", {"cwd": "/x", "platform": "darwin", "shell": "zsh", "osVersion": "24.5.0"})
    assert len(blocks) == 3
    plain = build_start_plan_system(None, "glm-5.3-flash", "zai-api", {"cwd": "/x", "platform": "darwin", "shell": "zsh", "osVersion": "24.5.0"})
    assert blocks[1]["text"] == plain[1]["text"]  # 块2 不受 custom 影响
    assert blocks[2]["text"].endswith(custom)
    assert custom not in blocks[1]["text"]


def test_array_system_and_cache_control_dropped():
    """数组 system 归一为单串并入块3；客户端 cache_control 有意丢弃（见
    _custom_system_text 文档）。"""
    system = [
        {"type": "text", "text": "第一段", "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": "第二段"},
    ]
    assert has_custom_system_prompt(system)
    blocks = build_start_plan_system(system, "glm-5.3-flash", "zai-api", {"cwd": "/x", "platform": "darwin", "shell": "zsh", "osVersion": "24.5.0"})
    assert blocks[2]["text"].endswith("第一段\n\n第二段")


def test_apply_start_plan_system_idempotent():
    """幂等：重复应用（验证码重试同一 body）不叠加、不换形状。"""
    body = {"model": "glm-5.3-flash", "system": "custom rules", "messages": []}
    assert apply_start_plan_system(body, "glm-5.3-flash", None) is True
    first = json.dumps(body["system"], ensure_ascii=False)
    assert apply_start_plan_system(body, "glm-5.3-flash", None) is False
    assert json.dumps(body["system"], ensure_ascii=False) == first
    assert len(body["system"]) == 3


def test_normalize_user_system_malformed_noop():
    """畸形输入保持 no-op：非字符串/非 text 块一律忽略。"""
    assert _normalize_user_system(None) == []
    assert _normalize_user_system(12345) == []
    assert _normalize_user_system([{"type": "image", "url": "x"}]) == []
    assert _normalize_user_system(["", "  "]) == []
    assert _normalize_user_system("hello") == [{"type": "text", "text": "hello"}]
