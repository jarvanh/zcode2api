#!/usr/bin/env python3
"""校验官方构建里的 system 提示词与 zcode_system.json 是否同文。

背景：上游按「版本头 + system 块1/块2 文本」同版校验，错版即 405/3012。升版时
必须确认新构建的提示词正文没变；靠肉眼比对 minified bundle 不可靠，故提供本脚本。

用法（先把官方构建的 agent bundle 弄到手，任选其一）::

    # A. 本机 server 运行时（随 ZCode 自动更新，最省事）
    python scripts/verify_prompt_text.py ~/.zcode/server/releases/<ver>-*/runtime/zcode.cjs

    # B. 官方 AppImage（预览通道构建；解包后取 resources/glm/zcode.cjs）
    ./ZCode-x.y.z-linux-x64.AppImage --appimage-extract
    python scripts/verify_prompt_text.py squashfs-root/resources/glm/zcode.cjs

退出码：0 = 全部同文（可安全抬 PROMPT_ALIGNED_VERSION）；1 = 有差异（需移植）。

注意：脚本只做「文本比对」，不判断上游是否接受该版本；抬版本后仍需一次实弹
请求确认 200（见 README「版本升级」）。
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROMPT_JSON = ROOT / "app" / "zcode_system.json"
CLI_PREFIX = "You are ZCode, an interactive agent"
IDENTITY_INTRO = "You are an agent working with the user toward their goals"
SECURITY = "IMPORTANT: Assist with authorized security testing"
HARNESS = "# Harness"
COMMUNICATION = "Write code that reads like the surrounding code"
PRONOUNS = "When you use a pronoun for someone"
ACTION_CAUTION = "For actions that are hard to reverse or outward-facing"
CONTEXT_HEAD = "# Context management"
CONTEXT_BODY = "When the conversation grows long"
ACT_DONT_REDERIVE = "When you have enough information to act"
GUIDANCE_LINE = " - When the user types `/<skill-name>`"
MID_CONVERSATION = (
    "The system may send updates, reminders, or modifications to rules via "
    "mid-conversation system turns. These are system-controlled, unlike function results."
)


def _decode_escapes(text: str) -> str:
    # minified bundle 里可能存在 \u2014 / \n / \' 这类转义
    text = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), text)
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), text)


def _literal_at(src: str, index: int) -> str:
    """从 index 处（引号内）向前找到起始引号、向后找到配对引号，返回解码后的内容。"""
    quote = None
    start = None
    k = index
    while k >= 0:
        c = src[k]
        if c in "\"'`" and (k == 0 or src[k - 1] != "\\"):
            quote, start = c, k
            break
        k -= 1
    if quote is None:
        raise ValueError(f"找不到 {index} 处的字符串起始引号")

    out: list[str] = []
    k = start + 1
    while k < len(src):
        c = src[k]
        if c == "\\":
            out.append(src[k : k + 2])
            k += 2
            continue
        if quote == "`" and src.startswith("${", k):
            # 模板插值：取 true 分支的字面文案（identity 的 MCS 提示就是这种形态）
            depth, j = 0, k + 1
            while j < len(src):
                if src[j] == "{":
                    depth += 1
                elif src[j] == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            expr = src[k + 2 : j]
            branch = expr.split("?")[-1]
            if ":" in branch:
                branch = branch.split(":", 1)[0]
            branch = branch.strip()
            if branch[:1] in "\"'`":
                branch = branch[1:-1]
            out.append(branch)
            k = j + 1
            continue
        if c == quote:
            break
        out.append(c)
        k += 1

    text = _decode_escapes("".join(out))
    if MID_CONVERSATION in src[k : k + 400] and MID_CONVERSATION not in text:
        # `${e?"true":"false"}` 且 true 分支被三元表达式截断时的兜底
        text = text.replace("${e?...}", MID_CONVERSATION)
    return text


def _find_literal(src: str, marker: str) -> str:
    idx = src.find(marker)
    if idx < 0:
        raise ValueError(f"构建产物里找不到标记: {marker!r}")
    return _literal_at(src, idx)


def extract_sections(src: str) -> dict[str, str]:
    cli_prefix = _find_literal(src, CLI_PREFIX)
    intro = _find_literal(src, IDENTITY_INTRO)
    security = _find_literal(src, SECURITY)

    # Harness：按顶层逗号切分数组元素（元素内部可能含引号/反引号，不能整段正则）
    harness = "\n".join(
        _decode_escapes(_split_array_element(el)) for el in _split_array(_array_body(src, HARNESS))
    )

    identity = "\n" + intro + "\n\n" + security + "\n\n" + harness
    dynamic = "\n\n".join(
        _find_literal(src, m) for m in (COMMUNICATION, PRONOUNS, ACTION_CAUTION)
    )
    context = (
        "# Context management\n"
        + _find_literal(src, CONTEXT_BODY)
        + "\n\n"
        + _find_literal(src, ACT_DONT_REDERIVE)
    )
    guidance = "# Session-specific guidance\n" + _find_literal(src, GUIDANCE_LINE)
    return {
        "cliPrefix": cli_prefix,
        "stableSections": identity,
        "dynamicBehavior": dynamic,
        "contextManagement": context,
        "sessionGuidance": guidance,
    }



def _array_body(src: str, marker: str) -> str:
    """marker 所在数组字面量的内容（不含两端方括号）。"""
    idx = src.find(marker)
    start = src.rfind("[", 0, idx)
    end = src.find("].join", idx)
    if start < 0 or end < 0:
        raise ValueError(f"找不到 {marker!r} 所在数组")
    return src[start + 1 : end]


def _split_array(body: str) -> list[str]:
    """按顶层逗号切分数组元素，引号内的逗号不切。"""
    out: list[str] = []
    cur: list[str] = []
    quote: str | None = None
    k = 0
    while k < len(body):
        c = body[k]
        if quote:
            cur.append(c)
            if c == "\\":
                cur.append(body[k + 1])
                k += 2
                continue
            if c == quote:
                quote = None
        else:
            if c in "\"'`":
                quote = c
                cur.append(c)
            elif c == ",":
                out.append("".join(cur).strip())
                cur = []
            else:
                cur.append(c)
        k += 1
    tail = "".join(cur).strip()
    if tail:
        out.append(tail)
    return out


def _split_array_element(el: str) -> str:
    """取元素字面量内容；模板字符串里的 ${e?...} 取 true 分支。"""
    if not el or el[0] not in "\"'`":
        return el
    quote = el[0]
    body = el[1:-1] if el.endswith(quote) else el[1:]
    if quote == "`" and "${" in body:
        start = body.index("${")
        depth, k = 0, start + 1
        while k < len(body):
            if body[k] == "{":
                depth += 1
            elif body[k] == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        expr = body[start + 2 : k]
        true_part = expr.split("?")[-1].split(":")[0].strip().strip("\"'`")
        body = body[:start] + true_part + body[k + 1 :]
    return body


def _norm(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def main() -> int:
    ap = argparse.ArgumentParser(description="对拍官方构建与 zcode_system.json 的提示词正文")
    ap.add_argument("bundle", type=Path, help="zcode.cjs（server runtime 或 AppImage resources/glm/）")
    args = ap.parse_args()

    src = args.bundle.read_text("utf-8", "replace")
    try:
        found = extract_sections(src)
    except ValueError as err:
        print(f"[FAIL] {err}", file=sys.stderr)
        return 1

    ours = json.loads(PROMPT_JSON.read_text("utf-8"))
    checks = {
        "cliPrefix": _norm(ours.get("cliPrefix") or ""),
        "stableSections": "\n\n".join(_norm(s) for s in ours.get("stableSections") or []),
        "dynamicBehavior": _norm(ours.get("dynamicBehavior") or ""),
        "contextManagement": _norm(ours.get("contextManagement") or ""),
        "sessionGuidance": _norm(ours.get("sessionGuidance") or ""),
    }

    bad = 0
    for name, expected in checks.items():
        actual = _norm(found[name])
        if actual == expected:
            print(f"[OK]   {name}")
            continue
        bad += 1
        print(f"[DIFF] {name}（ours {len(expected)} 字符 / build {len(actual)} 字符）")
        diff = difflib.unified_diff(
            expected.splitlines(), actual.splitlines(), "zcode_system.json", "build", lineterm=""
        )
        for line in list(diff)[:20]:
            print("       " + line[:160])

    if bad:
        print(f"\n有 {bad} 段不同文：先移植文本，再抬 PROMPT_ALIGNED_VERSION。", file=sys.stderr)
        return 1
    print("\n全部同文：可安全抬 PROMPT_ALIGNED_VERSION（仍需一次实弹请求确认 200）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
