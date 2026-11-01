"""失败请求诊断留存 —— 上游 4xx/5xx/业务错误时的双侧现场。

排障痛点（2026-10-31 405 波次实证）：「形状级」风控的判定依据在请求与响应
的字节细节里，而 reqlog 只留元数据、文本日志 50MB 滚动即丢，当时只能临时
搭捕获转发器做字节级对比。本模块把失败现场结构化落盘：

  - 响应侧：上游状态码 + 响应体（截断 DIAGLOG_MAX_BODY，默认 2KB）；
  - 请求侧：不落对话正文，只落「形状指纹」——body sha256、system 各块长度、
    消息数与角色序列、model/max_tokens/stream。二分实验里「块长度」正是
    定位触发块的钥匙（块1/块2 任一旧文本即 405）。

按天分文件（riskmetrics 同款北京日期 + 清理语义），保留 DIAGLOG_DAYS 天；
admin API GET /admin/api/diaglog 提供按日查询。best-effort：落盘失败只留痕，
绝不影响请求热路径。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import logs, settings

_BJ = timezone(timedelta(hours=8))
_PREFIX = "diaglog-"

_lock = threading.Lock()
_last_prune_day = ""

_HIST_DAYS = settings.DIAGLOG_DAYS
_MAX_BODY = settings.DIAGLOG_MAX_BODY


def _data_dir() -> Path:
    return Path(settings.DATA_DIR)


def _day_str(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), _BJ).strftime("%Y-%m-%d")


def _prune(now_day: str) -> None:
    global _last_prune_day
    if _last_prune_day == now_day:
        return
    _last_prune_day = now_day
    cutoff = (datetime.now(_BJ) - timedelta(days=_HIST_DAYS)).strftime("%Y-%m-%d")
    for f in _data_dir().glob(f"{_PREFIX}*.jsonl"):
        day = f.stem.removeprefix(_PREFIX)
        if day < cutoff:
            try:
                f.unlink()
            except OSError:
                pass


def _request_summary(body: dict) -> dict:
    """请求侧形状指纹（不含对话正文）。解析失败返回最小摘要，绝不抛出。"""
    out: dict = {}
    try:
        out["body_sha256"] = hashlib.sha256(
            json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
    except (TypeError, ValueError):
        pass
    system = body.get("system")
    if isinstance(system, list):
        out["system_block_lens"] = [
            len(b.get("text") or "") if isinstance(b, dict) else -1 for b in system
        ]
    messages = body.get("messages")
    if isinstance(messages, list):
        out["message_count"] = len(messages)
        out["roles"] = [m.get("role") for m in messages if isinstance(m, dict)][:8]
    for k in ("model", "max_tokens", "stream"):
        if body.get(k) is not None:
            out[k] = body.get(k)
    return out


def record_failure(req_id: str, *, account_name: str, mode: str, model: str,
                   url: str, status: int, text: str, request_body: dict | None = None) -> None:
    """落一条失败现场（best-effort：任何异常只留痕）。"""
    if not settings.DIAGLOG_ENABLED:
        return
    try:
        now = time.time()
        row = {
            "ts": round(now, 3),
            "req_id": str(req_id or "")[:16],
            "account": str(account_name or "")[:40],
            "mode": str(mode or ""),
            "model": str(model or "")[:40],
            "path": url.split("?")[0][-80:] if url else "",
            "status": int(status),
            "upstream_body": (text or "")[:_MAX_BODY],
        }
        if request_body is not None:
            row["request"] = _request_summary(request_body)
        day = _day_str(now)
        with _lock:
            _prune(day)
            f = _data_dir() / f"{_PREFIX}{day}.jsonl"
            with open(f, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as err:  # noqa: BLE001 - 诊断留存绝不影响请求
        logs.warn("diaglog", f"落盘失败: {err}")


def query(days: int = 3, limit: int = 200, status: int | None = None) -> list[dict]:
    """按天倒序读取诊断记录（admin API 数据源），最新在前。"""
    days = max(1, min(int(days), _HIST_DAYS))
    limit = max(1, min(int(limit), 1000))
    rows: list[dict] = []
    day = datetime.now(_BJ)
    for _ in range(days):
        f = _data_dir() / f"{_PREFIX}{day.strftime('%Y-%m-%d')}.jsonl"
        try:
            for line in f.read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if status is not None and r.get("status") != int(status):
                    continue
                rows.append(r)
        except OSError:
            pass
        if len(rows) >= limit * 2:  # 多读一点再截断，保证「最新在前」语义
            break
        day -= timedelta(days=1)
    rows.sort(key=lambda r: r.get("ts", 0), reverse=True)
    return rows[:limit]
