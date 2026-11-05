"""IP 级封锁（出口地址被阿里云 ESA/WAF 拦）的进程级静默。

对齐 magpie zcode 插件 0.1.43（plugins#649）的判定与处置：

- **405 + HTML 拦截页**（`errors.aliyun.com` / `<!doctypehtml` / `<html`）
  = 本机**出口 IP** 被上游网络防火墙拦。封的是地址，不是账号。
- **405 + JSON `{"code":3012}`** = Z.ai 应用层风控，算**账号**的
  （仍走 `_apply_risk_strike`，见 routes/gateway.py）。

为什么 IP 级要单独处理：封锁期内从同一地址继续请求（不管换哪个账号、
哪个会话）只会让防火墙计数继续涨，封锁被拉长。所以命中拦截页后，
进程内所有账号对 zcode plan 通道一起静默 `settings.EDGE_HOLD_SECONDS`；
静默期内本地直接回同款 405，让下游（OmniRoute / magpie）立刻切到别的
provider，而不是把它当 node 故障去重试。

两条不变量：
1. **不记 strike、不改账号状态** —— 罚账号对地址封锁毫无作用，只会把
   健康号打进冷却（2026-11-03 的 12 次拦截页就是这么烧掉的）。
2. **只作用于 plan 通道** —— `api.z.ai`（Coding Plan）是另一个 host，
   不受同一防火墙影响；有 Key 回退的账号在静默期内改走回退通道。
"""

from __future__ import annotations

import re
import time

from . import constants, settings

# 拦截页特征：阿里云错误页的域名，或干脆是 HTML（上游正常响应都是 JSON/SSE）。
# 与 magpie `EDGE_PAGE = /errors\.aliyun\.com|<html/i` 同义。
EDGE_PAGE = re.compile(
    "|".join(
        [re.escape(m) for m in constants.EDGE_PAGE_MARKERS]
        + [re.escape(m) for m in constants.EDGE_PAGE_HTML_HINTS]
    ),
    re.IGNORECASE,
)

# provider -> 静默截止时刻（epoch 秒）。进程级：同进程内所有账号共享。
_until: dict[str, float] = {}


def is_page(text: str | None) -> bool:
    """响应体是否是 WAF 拦截页（仅在 HTTP 405 时调用）。"""
    return bool(text) and EDGE_PAGE.search(text) is not None


def until(provider: str) -> float | None:
    """当前静默截止时刻；未静默返回 None。"""
    ts = _until.get(provider)
    if ts is None or ts <= time.time():
        return None
    return ts


def held(provider: str, now: float | None = None) -> bool:
    """该 provider 的 plan 通道是否仍在静默期。"""
    ts = _until.get(provider)
    return ts is not None and ts > (now if now is not None else time.time())


def remaining(provider: str, now: float | None = None) -> int:
    """静默剩余秒数（向上取整，最小 1）；未静默返回 0。"""
    ts = _until.get(provider)
    if ts is None:
        return 0
    cur = now if now is not None else time.time()
    return max(0, int(ts - cur + 0.999))


def arm(provider: str, seconds: float | None = None) -> float:
    """命中拦截页：把该 provider 的 plan 通道静默一段时间，返回截止时刻。

    多次命中取更晚的截止时刻（幂等向后延），不会因为并发命中缩短。
    """
    hold = settings.EDGE_HOLD_SECONDS if seconds is None else seconds
    ts = time.time() + max(0.0, hold)
    if ts > _until.get(provider, 0.0):
        _until[provider] = ts
    return _until[provider]


def reset(provider: str | None = None) -> None:
    """清空静默状态（测试 / 人工恢复用）。"""
    if provider is None:
        _until.clear()
    else:
        _until.pop(provider, None)
