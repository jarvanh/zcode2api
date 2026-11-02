#!/usr/bin/env python3
"""官方升版 sidecar 对齐助手 —— 2026-11-03 随 3.17.2 对齐落地。

背景：上游 WAF 按当前官方版本逐字节校验 system 块1（cli_prefix）与块2
（stable）文本（见 app/body_transform.py 模块头）。sidecar 是手工跟进的
镜像，官方升版后过时即 405/3012 波次。本脚本把「对齐」从逆向手工活变成
一条命令：从本机官方客户端 bundle 静态提取块1/块2 并重组，与现行 sidecar
逐字节对拍。

用法（hub 本机）：
    .venv/bin/python scripts/capture_sections.py            # 对拍报告（check）
    .venv/bin/python scripts/capture_sections.py --write    # 漂移时写新侧车

提取原理（minify 改名免疫）：bundle 用 r(fn,"buildCliPrefixSection") 登记
可读函数名，顺着名字找函数体 → 找常量引用 → 找常量赋值。3.17.2 装配：
    块1 = buildCliPrefixSection()            → hBa
    块2 = buildIdentityPrompt(false,true)    → ["",gBa,"",cBo].join(LF) + LF + buildHarnessBlock(true)
gBa=agent 身份 / _Ba=Output Style 变体（自定义 output style 的官方客户端才用，
网关不伪装该形态）/ cBo=buildSecurityNotice。

已知边界（认不出就 exit 2 大声失败，绝不静默写错字节——写错=405 波次）：
  - 仅实现 3.17.2 实测的装配形状；官方改装配结构时脚本会 dump 函数体供
    人工判读，把新形状补进 _rebuild_identity。
  - 只覆盖块1/块2（唯一被校验的块）；块3 dynamic 与 context_prefix 不受
    校验（10-31 实弹），沿用旧版文本是既定策略（3171 文件同例）。
  - meta_user 迁移（3.17.2 起 env_info/model/current_date 走 meta_user 附件
    A3a）不影响块1/2，脚本不管。

--write 之后的对齐清单（脚本只做一半，剩下半截人工收口）：
  1. app/constants.py：CLIENT_APP_VERSION 与 BILLING_APP_VERSION 同步升
     （半更新 = 2026-10-31 组合病因，freshness 会告警）；
  2. app/body_transform.py：侧车文件名引用改到新文件；
  3. tests/unit/test_constants.py（3 处字面量）+ tests/unit/
     test_body_transform_system.py（_meta.client_version 断言）；
  4. .venv/bin/python -m pytest tests/unit -x 后重启服务；
  5. 真机冒烟一发（/v1/messages 小请求），风控面板盯 405。
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "app"
LF = "\n"

# bundle 里的命名函数注册（minify 后唯一稳定的锚点）
_ANCHORS = ("buildCliPrefixSection", "buildIdentityPrompt", "buildHarnessBlock", "buildSecurityNotice")


def locate_bundle() -> Path:
    """本机官方客户端 bundle：current.json.releaseDir 优先，releases 兜底取最新。"""
    cur = Path.home() / ".zcode" / "server" / "current.json"
    try:
        data = json.loads(cur.read_text("utf-8"))
        release_dir = data.get("releaseDir")
        if release_dir:
            bundle = Path(release_dir) / "runtime" / "zcode.cjs"
            if bundle.is_file():
                return bundle
    except (OSError, ValueError):
        pass
    releases = sorted((Path.home() / ".zcode" / "server" / "releases").glob("*/runtime/zcode.cjs"))
    if releases:
        return releases[-1]
    raise SystemExit("找不到本机官方客户端 bundle（~/.zcode/server/{current.json,releases}）")


def local_app_version() -> str:
    try:
        data = json.loads((Path.home() / ".zcode" / "server" / "current.json").read_text("utf-8"))
        return str(data.get("appVersion") or "")
    except (OSError, ValueError):
        return ""


def _function_body(src: str, fn_name: str) -> str:
    """按 r(X,"name") 锚点找 function X(){...} 的完整体（花括号配平）。"""
    reg = re.search(rf'r\(\s*(\w+)\s*,\s*"{re.escape(fn_name)}"\s*\)', src)
    if not reg:
        raise LookupError(f"锚点 r(fn,\"{fn_name}\") 不存在——bundle 结构变了，需人工判读")
    fn = reg.group(1)
    m = re.search(rf'function {re.escape(fn)}\(', src)
    if not m:
        raise LookupError(f"function {fn} 定义不存在（{fn_name}）")
    i = src.index("{", m.end() - 1)
    depth = 0
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[i:j + 1]
    raise LookupError(f"function {fn} 花括号不配平")


def _js_string(src: str, i: int) -> tuple[str, int]:
    """从 src[i]（须为引号）解析一个 JS 字符串字面量，返回 (值, 结束下标)。"""
    quote = src[i]
    if quote not in "\"'`":
        raise ValueError(f"位置 {i} 不是字符串字面量")
    out: list[str] = []
    j = i + 1
    while j < len(src):
        c = src[j]
        if c == "\\":
            nxt = src[j + 1]
            mapping = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "'": "'", "`": "`",
                       "\\": "\\", "/": "/", "0": "\0", "b": "\b", "f": "\f"}
            if nxt == "u":
                out.append(chr(int(src[j + 2:j + 6], 16)))
                j += 6
                continue
            if nxt == "x":
                out.append(chr(int(src[j + 2:j + 4], 16)))
                j += 4
                continue
            out.append(mapping.get(nxt, nxt))
            j += 2
            continue
        if c == quote:
            return "".join(out), j
        out.append(c)
        j += 1
    raise ValueError("字符串字面量未闭合")


def _const_value(src: str, ident: str) -> str:
    """minified 常量赋值 VAR="..." / VAR='...' / VAR=`...`（常量只赋值一次）。"""
    m = re.search(rf'(?<![\w$]){re.escape(ident)}=(["\'`])', src)
    if not m:
        raise LookupError(f"常量 {ident} 赋值未找到")
    val, _ = _js_string(src, m.end() - 1)
    return val


def _cli_prefix(src: str) -> str:
    body = _function_body(src, "buildCliPrefixSection")
    m = re.search(r'let \w+=(\w+);', body)
    if m:
        return _const_value(src, m.group(1))
    m = re.search(r'=((["\']))', body)
    if m:
        val, _ = _js_string(body, m.end() - 2)
        return val
    raise LookupError(f"buildCliPrefixSection 形状未识别：{body[:200]!r}")


def _identity_variants(src: str) -> tuple[str, str]:
    """buildIdentityPrompt 的 (false 分支=默认 agent 身份, true 分支=Output Style 变体)。"""
    body = _function_body(src, "buildIdentityPrompt")
    ternaries = re.findall(r'(\w+)\?(\w+):(\w+)', body)
    if len(ternaries) != 1:
        raise LookupError(
            f"buildIdentityPrompt 条件分支数={len(ternaries)}（预期 1）——装配形状变了，人工判读：\n{body[:400]}"
        )
    _cond, t_branch, f_branch = ternaries[0]
    return _const_value(src, f_branch), _const_value(src, t_branch)


def _security(src: str) -> str:
    body = _function_body(src, "buildSecurityNotice")
    m = re.fullmatch(r'\{return (\w+)\}', body)
    if not m:
        raise LookupError(f"buildSecurityNotice 形状未识别：{body[:200]!r}")
    return _const_value(src, m.group(1))


def _harness(src: str) -> str:
    """buildHarnessBlock：数组字面量（字符串/模板串，模板串取三元 true 分支）LF 连接。

    3.17.2 形状：kDn(e=!0) → ["# Harness"," - …"," - …",` - ${e?"A":"B"} …`,
    " - …"," - …"].join(LF)；主链默认 e=true。模板串内非「单三元」结构一律
    大声失败。
    """
    body = _function_body(src, "buildHarnessBlock")
    m = re.search(r'\[', body)
    if not m:
        raise LookupError("buildHarnessBlock 无数组字面量")
    i = m.start()
    depth = 0
    for j in range(i, len(body)):
        if body[j] == "[":
            depth += 1
        elif body[j] == "]":
            depth -= 1
            if depth == 0:
                arr = body[i + 1:j]
                break
    else:
        raise LookupError("buildHarnessBlock 数组不配平")

    items: list[str] = []
    k = 0
    while k < len(arr):
        c = arr[k]
        if c in "\"'":
            val, k = _js_string(arr, k)
            items.append(val)
            k += 1  # 越过收尾引号；逗号下一轮空白分支跳过
        elif c == "`":
            # 模板串：只支持「静态前缀 + ${e?"A":"B"} + 静态后缀」单三元
            tm = re.match(r'`([^{`]*)\$\{\w+\?((["\'])[\s\S]*?\3):((["\'])[\s\S]*?\5)\}([^`]*)`', arr[k:])
            if not tm:
                raise LookupError(f"buildHarnessBlock 模板串形状未识别：{arr[k:k+160]!r}")
            tval, _ = _js_string(tm.group(2), 0)  # 主链默认 e=true，取三元 true 分支
            items.append(tm.group(1) + tval + tm.group(6))
            k += tm.end()
        elif c in " \n\t,":
            k += 1
        else:
            raise LookupError(f"buildHarnessBlock 数组含未支持 token：{arr[k:k+80]!r}")
    return LF.join(items)


def rebuild_blocks(src: str) -> tuple[str, str]:
    """重组官方块1/块2（3.17.2 实测装配）。结构变化时在此补形状。"""
    cli = _cli_prefix(src)
    agent, _output_style = _identity_variants(src)
    sec = _security(src)
    harness = _harness(src)
    identity = LF.join(["", agent, "", sec])
    stable = LF.join([identity, "", harness])
    return cli, stable


def active_sidecar() -> tuple[Path, dict]:
    """现行侧车（app/zcode_sections_*.json 版本号最大者）。"""
    files = sorted(APP.glob("zcode_sections_*.json"))
    if not files:
        raise SystemExit("app/ 下没有 zcode_sections_*.json")
    return files[-1], json.loads(files[-1].read_text("utf-8"))


def diff_report(old: dict, cli: str, stable: str) -> tuple[list[str], bool]:
    rows: list[str] = []
    old_stable = "\n\n".join(old.get("stableSections") or [])
    cli_drift = old.get("cliPrefix") != cli
    stable_drift = old_stable != stable
    rows.append(f"块1 cli_prefix: {'漂移' if cli_drift else '一致'}")
    rows.append(f"块2 stable:      {'漂移' if stable_drift else '一致'}")
    if cli_drift:
        rows.append(f"  旧: {old.get('cliPrefix')!r}\n  新: {cli!r}")
    if stable_drift:
        import difflib
        rows.extend("  " + line for line in difflib.unified_diff(
            old_stable.splitlines(), stable.splitlines(), "sidecar", "bundle", lineterm=""))
    return rows, (cli_drift or stable_drift)


def main() -> int:
    ap = argparse.ArgumentParser(description="官方升版 sidecar 对拍/写盘")
    ap.add_argument("--write", action="store_true", help="漂移且提取成功时写 zcode_sections_<ver>.json")
    ap.add_argument("--force", action="store_true", help="同名侧车已存在时允许覆盖")
    args = ap.parse_args()

    bundle = locate_bundle()
    src = bundle.read_text("utf-8", errors="replace")
    ver = local_app_version()
    side_path, side = active_sidecar()
    print(f"bundle: {bundle}")
    print(f"官方版本: {ver or '未知'}    现行侧车: {side_path.name}"
          f"（_meta.client_version={side.get('_meta', {}).get('client_version')}）")

    try:
        cli, stable = rebuild_blocks(src)
    except LookupError as err:
        print(f"\n[!] 提取失败（结构变化，需人工判读，勿盲改侧车）：\n{err}", file=sys.stderr)
        return 2

    rows, drifted = diff_report(side, cli, stable)
    print("\n".join(rows))

    if not drifted:
        print("\n块1/块2 与官方重组逐字节一致：无需跟进（版本常量是否跟进另查 freshness）。")
        return 0

    if not args.write:
        print("\n存在漂移。确认上文 diff 后加 --write 落盘（随后按模块头清单收口常量/测试/重启）。")
        return 1

    stem = ("zcode_sections_" + ver.replace(".", "")) if ver else ""
    if not stem:
        print("[!] current.json 无 appVersion，拒绝盲写", file=sys.stderr)
        return 2
    out = APP / f"{stem}.json"
    if out.exists() and not args.force:
        print(f"[!] {out.name} 已存在（重复对齐？），确认覆盖请加 --force", file=sys.stderr)
        return 2

    new = dict(side)
    new["cliPrefix"] = cli
    new["stableSections"] = [stable]
    new["_meta"] = {
        "client_version": ver,
        "captured_at": datetime.date.today().isoformat(),
        "source": (
            f"官方 {ver} bundle（{bundle}）镜像：scripts/capture_sections.py 经"
            " buildCliPrefixSection/buildIdentityPrompt/buildHarnessBlock/buildSecurityNotice"
            " 重组块1/2；dynamic/contextPrefix 沿用旧版（块3 与 context_prefix 不受校验，"
            "见 app/body_transform.py 模块头）"
        ),
    }
    out.write_text(json.dumps(new, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n已写 {out}")
    print("收口清单：constants(CLIENT+BILLING 同步) → body_transform 文件引用 → "
          "tests(constants/wording) → pytest tests/unit → 重启 → 冒烟盯 405。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
