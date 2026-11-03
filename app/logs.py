"""日志工具 — 带时间戳的终端彩色输出 (Windows GBK 兼容)

2026-11-03 主人要求补时间戳：此前是裸 print，claim 日志**没有任何时刻**
（实测查「zai-156 的套餐几点领的」只能靠 DB 里的上游 starts_at 反推）。
现在每行前缀本地时间，`grep 'claim'` 的结果直接可读。

同一轮顺带修掉一个噪音：服务 StandardOutput 重定向到日志文件时
isatty() 为 False，ANSI 转义码仍照写（实测日志里 7 万+ 个 ESC，
`cat` 全是乱码、`cut` 宽度也算不准）。改为**非 TTY 自动关色**：
人看终端仍有颜色，落盘日志是干净 UTF-8 纯文本。

注意：消费方靠中文关键词 grep（如 tools/zcode2api-healthcheck.sh 的
grep -c '立刻换号'），加前缀不影响，勿改成把关键词挪出正文的格式。
"""

import sys
import time

# 颜色只在真终端生效；重定向到文件/管道时关掉（见模块 docstring）。
try:
    _COLOR = sys.stdout.isatty()
except Exception:  # noqa: BLE001 - 某些 Windows GBK 管道会抛，退化为无色
    _COLOR = False


def _c(code: str) -> str:
    """按是否终端决定返回 ANSI 码还是空串。"""
    return code if _COLOR else ""


_R = _c("\033[0m")
_G = _c("\033[32m")
_C = _c("\033[36m")
_Y = _c("\033[33m")
_RED = _c("\033[31m")
_DIM = _c("\033[90m")
_B = _c("\033[1m")
_MAG = _c("\033[35m")
_W = _c("\033[37m")


def _ts() -> str:
    """时间戳前缀：日志必须能回答「这事几点发生的」。"""
    return time.strftime("%Y-%m-%d %H:%M:%S")


def ok(module: str, msg: str):
    print(f"{_DIM}{_ts()}{_R}  {_G}[+]{_R} {_DIM}{module}{_R} {msg}")


def step(module: str, msg: str):
    print(f"{_DIM}{_ts()}{_R}  {_C}[>]{_R} {_DIM}{module}{_R} {msg}")


def info(module: str, msg: str):
    print(f"{_DIM}{_ts()}  [-] {module} {msg}{_R}")


def warn(module: str, msg: str):
    print(f"{_DIM}{_ts()}{_R}  {_Y}[~]{_R} {_DIM}{module}{_R} {msg}")


def err(module: str, msg: str):
    print(f"{_DIM}{_ts()}{_R}  {_RED}[!]{_R} {_DIM}{module}{_R} {msg}")


def header(module: str, msg: str):
    print(f"{_DIM}{_ts()}{_R}  {_B}{_MAG}{module}{_R} {msg}")


def req(req_id: str, model: str, stream: bool, last_msg: str):
    """请求日志 — 一行显示关键信息"""
    s = "stream" if stream else "sync"
    msg_preview = last_msg[:40] + ("..." if len(last_msg) > 40 else "")
    print(f"{_DIM}{_ts()}{_R}  {_C}>>>{_R} {_DIM}{req_id}{_R}  {_W}{model}{_R}  {_DIM}{s}{_R}  {_DIM}\"{msg_preview}\"{_R}")


def req_ok(req_id: str, tokens: int = 0):
    """请求完成"""
    t = f"  {_DIM}{tokens}tok{_R}" if tokens else ""
    print(f"{_DIM}{_ts()}{_R}  {_G}<<<{_R} {_DIM}{req_id}{_R}{t}")


def req_err(req_id: str, msg: str):
    """请求错误 — 只显示 req_id + 简短原因，不泄露上游"""
    print(f"{_DIM}{_ts()}{_R}  {_RED}<!>{_R} {_DIM}{req_id}{_R}  {msg}")


def banner(lines: list[str]):
    """横幅：装饰性多行块，不加时间戳（加了反而割裂边框）。"""
    import re

    strip_ansi = re.compile(r"\033\[[0-9;]*m")
    w = max(len(strip_ansi.sub("", line)) for line in lines) + 4
    border = f"{_DIM}{'=' * w}{_R}"
    print(f"\n{border}")
    for line in lines:
        print(f"  {line}")
    print(f"{border}\n")
