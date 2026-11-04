"""请求监控 —— 网关请求的实时环形日志 + 磁盘历史（管理端「请求监控」页数据源）。

设计要点：
- 实时层：内存 deque 上限 500 条（含在途），重启即清零，供「实时」页秒级观测
- 历史层：请求收口（成功/失败）时 best-effort 追加一行 JSONL 到
  data/reqlog-<北京日期>.jsonl，按天分文件、保留 HISTORY_DAYS 天自动清理，
  供「7 天 / 30 天」视图聚合 —— 补上内存日志「重启即清零、看不了趋势」的缺口
- 只落终态条目：在途条目（ok=None）不落盘，避免进程被杀留下永久幽灵行
- 粒度：一次客户端请求一条；多账号重试不分裂，账号字段记录最终归宿；
  up 标记是否真实发起过上游调用（调度失败·未达上游的行，账号名仅供参考）
- ok 三态：None=在途/未知，True=成功，False=失败（含客户端断开 status=499）
- 排障维度（2026-11-04 增补）：
    · 来源：client_ip / ua / path / probe（X-Probe 探测标记）
    · 路由：mode（凭证：jwt/key）、channel（业务通道：plan/apikey）、
      egress（出口：direct/proxy/account）
    · 尝试梯子：att 记录每次账号尝试（通道/状态/耗时/失败原因，最多 8 条）
    · 耗时拆解：wait_q 排队、wait_rl 429 等待、wait_5xx 重试等待、
      wait_cap 验证码等待；retry_* 为各原因重试计数
    · 错误分类：err_kind（client/risk/quota/rate/upstream/network/...）
      + err_code（上游业务码，如 3012/1005/1113）
    · 上游追踪：upstream_id（响应头 cf-ray / x-request-id 等）
- 隐私：client_ip/ua 只用于本机排障，随历史保留 HISTORY_DAYS 天，不对外回传
  除管理端外的任何接口。
- 纯观测：不触碰账号状态与调度逻辑；落盘失败只记日志，绝不影响请求
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import logs, settings

KEEP = 500
# 磁盘历史保留天数（与前端「30 天」视图上限对齐，超出按天文件整体删除）
HISTORY_DAYS = 30
# 聚合分组回传条数上限（账号/模型/客户端可能很多，超过按调用量截断）
TOP_GROUPS = 50
TOP_ERRORS = 8
# 单请求尝试梯子上限：MAX_ACCOUNT_ATTEMPTS=5，留一点余量给回退/补码重试
MAX_ATTEMPTS = 8

_BJ = timezone(timedelta(hours=8))
_PREFIX = "reqlog-"

# 耗时拆解/重试计数的合法键（拼错时静默忽略，观测代码不因笔误抛异常）
_WAIT_KEYS = ("q", "rl", "5xx", "cap")
_RETRY_KEYS = ("429", "5xx", "cap", "fb")

def _clip(value, limit: int) -> str:
    """观测字段统一截断（None → 空串，绝不把非字符串塞进日志）。"""
    return str(value or "")[:limit]


_lock = threading.Lock()
_entries: deque = deque(maxlen=KEEP)
_inflight: dict[str, dict] = {}

_hist_lock = threading.Lock()
_last_prune_day = ""

# ── 并发仪表（实时四口径 + 分钟峰值曲线）───────────────────────────────
# 口径为什么要四个（互不相等且必须同时看）：
#   client  网关在途：客户端连接还开着的请求（含排队/等 429/铸码）
#   slot    上游占槽：真正压在账号上的并发（gateway._inflight 汇总）——
#           _park_slot 会在等待前释放槽，所以「client 8 / slot 0」正是波次期典型形态
#   queue   排队深度：选不到号在限流排队的请求数，加号/调并发上限的唯一依据
#   per     每账号 n/limit：答「卡在并发满还是额度满」（以前只存在于选号日志）
# 精度分层（观测不得反压请求）：client/queue 在 begin/finish/入队时刻用零 IO
# 增量抬峰值（真·事件驱动）；slot/per/limit 由 1s tick 读快照（要碰 store，
# 只在后台心跳里做），tick 同时校准 client/queue 防计数漂移。
_CONC_PREFIX = "conc-"
_CONC_TICK = 1.0

_slot_source = None               # gateway 注入：读真实槽位快照（避开循环 import）
_queue_now = 0
_conc = {"client": 0, "slot": 0, "queue": 0, "limit": 0, "per": {}}
_conc_peak = {"client": 0, "slot": 0, "queue": 0, "at": 0.0, "kind": ""}
_conc_min: dict[int, dict] = {}   # 北京分钟序号 → 该分钟峰值（跨分钟时刷盘）
_conc_day = ""
_conc_flushed = -1              # 当日已落盘的最大分钟序号（水位线，防重复写）
_pending_flush: list[tuple[str, dict]] = []   # 已结束天的待落盘曲线


def register_slot_source(fn) -> None:
    """网关注入槽位快照函数：返回 {"total": int, "per": {账号: n}, "limit": int}。

    观测层不反向 import 网关（循环依赖）；没注册时 slot/per 口径恒为 0，
    其余口径照旧工作（测试环境即如此）。
    """
    global _slot_source
    _slot_source = fn


def _peak_locked(kind: str, value: int, now: float) -> None:
    """抬峰值（调用方持 _lock）。"""
    if value > _conc_peak[kind]:
        _conc_peak[kind] = value
        _conc_peak["at"] = now
        _conc_peak["kind"] = kind


def note_queue(delta: int) -> None:
    """排队深度增减（进/出 _wait_for_account 各一次）。零 IO。"""
    global _queue_now
    now = time.time()
    _queue_now = max(0, _queue_now + int(delta))
    with _lock:
        _conc["queue"] = _queue_now
        _peak_locked("queue", _queue_now, now)
        _bump_minute_locked()


def _bj_minute(ts: float | None = None) -> tuple[str, int]:
    """(北京日期, 当日分钟序号)：曲线按北京日分文件，与 reqlog 历史对齐。"""
    dt = datetime.now(_BJ) if ts is None else datetime.fromtimestamp(ts, _BJ)
    return dt.strftime("%Y-%m-%d"), dt.hour * 60 + dt.minute


def _bump_minute_locked() -> None:
    """把当前值归入所属分钟的 max（调用方持 _lock）。

    只做内存操作：跨天时把昨天整段移交 _pending_flush，真正的磁盘写由
    _drain_flush（后台心跳/收口落盘处，锁外）执行 —— 热路径（begin/finish
    持 _lock）绝不为观测做 IO，否则跨天那一发请求会把全部并发请求排在后面。
    """
    global _conc_day, _conc_min, _conc_flushed
    day, minute = _bj_minute()
    if _conc_day != day:
        if _conc_day and _conc_min:
            _pending_flush.append((_conc_day, _conc_min))
        _conc_min = {}
        _conc_day = day
        _conc_flushed = -1
    bucket = _conc_min.get(minute)
    if bucket is None:
        _conc_min[minute] = {"c": _conc["client"], "s": _conc["slot"],
                             "q": _conc["queue"], "n": 1}
        return
    bucket["c"] = max(bucket["c"], _conc["client"])
    bucket["s"] = max(bucket["s"], _conc["slot"])
    bucket["q"] = max(bucket["q"], _conc["queue"])
    bucket["n"] += 1


def _write_minutes(day: str, buckets: dict) -> None:
    """把一个天的分钟峰值写进 data/conc-<北京日期>.jsonl（best-effort，锁外调）。"""
    if not buckets:
        return
    try:
        data_dir = _hist_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps({"t": m, **b}, ensure_ascii=False, separators=(",", ":"))
                 for m, b in sorted(buckets.items())]
        with _hist_lock:
            _conc_prune(data_dir)
            with open(data_dir / f"{_CONC_PREFIX}{day}.jsonl", "a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
    except Exception as err:  # noqa: BLE001 - 曲线落盘失败不影响请求
        logs.warn("reqlog", f"并发曲线落盘失败（忽略）: {err}")


def _drain_flush() -> None:
    """落盘所有积压曲线（由心跳/收口/关机调用；不得在持 _lock 时调）。

    内部自吞异常：调用方之一是 _persist（请求落盘），排水失败绝不能把
    请求行落盘连带打断；多线程同时 pop 的竞争也用异常吞掉兼容。
    """
    while _pending_flush:
        try:
            day, buckets = _pending_flush.pop(0)
        except IndexError:
            return
        _write_minutes(day, buckets)


def _conc_prune(data_dir: Path) -> None:
    """并发曲线按天清理（与 reqlog 历史同保留期）。"""
    cutoff = (datetime.now(_BJ) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    for path in data_dir.glob(f"{_CONC_PREFIX}*.jsonl"):
        d = path.name[len(_CONC_PREFIX):-len(".jsonl")]
        if d < cutoff:
            try:
                path.unlink()
            except OSError as err:
                logs.warn("reqlog", f"并发曲线清理失败（忽略）: {path.name} {err}")


def sample() -> None:
    """1s 心跳：读槽位快照 + 校准 client/queue + 推分钟曲线（只许在后台调）。"""
    slot_total, per, limit = 0, {}, 0
    src = _slot_source
    if src is not None:
        try:
            view = src() or {}
            slot_total = int(view.get("total") or 0)
            per = dict(view.get("per") or {})
            limit = int(view.get("limit") or 0)
        except Exception as err:  # noqa: BLE001 - 仪表读失败不得拖垮心跳
            logs.warn("reqlog", f"槽位快照失败（忽略）: {err}")
    now = time.time()
    with _lock:
        _conc["slot"], _conc["per"], _conc["limit"] = slot_total, per, limit
        _conc["client"] = len(_inflight)   # 以真值校准，防极端路径漏减
        _conc["queue"] = _queue_now
        _peak_locked("slot", slot_total, now)
        _peak_locked("client", _conc["client"], now)
        _peak_locked("queue", _queue_now, now)
        _bump_minute_locked()
        due = _due_minutes_locked()
    _drain_flush()
    for day, minute, bucket in due:
        _write_minutes(day, {minute: bucket})


def _due_minutes_locked() -> list[tuple[str, int, dict]]:
    """取「已结束但尚未落盘」的分钟（当前分钟不落，防半截值当最终值）。

    两个约束都要满足：
      - 不能只在跨天时写盘：否则进程被杀会丢整天曲线，面板也看不到今天已发生的部分；
      - 不能把已写分钟从 _conc_min 抹掉：面板的分钟曲线要靠它（内存只留当日，
        上限 1440 个，代价忽略），因此用 _conc_flushed 水位线去重。
    """
    global _conc_flushed
    day, minute = _bj_minute()
    if _conc_day != day:
        _conc_flushed = -1
    due = [(day, m, dict(b)) for m, b in sorted(_conc_min.items())
           if m < minute and m > _conc_flushed]
    if due:
        _conc_flushed = due[-1][1]
    return due


def conc_snapshot() -> dict:
    """实时并发快照（管理端轮询）：当前值 + 启动以来峰值 + 当日分钟曲线尾巴。"""
    with _lock:
        now = dict(_conc)
        peak = dict(_conc_peak)
        minutes = [{"t": m, **b} for m, b in sorted(_conc_min.items())][-120:]
    return {"now": now, "peak": peak, "tick": _CONC_TICK, "minutes": minutes}


def conc_history(days: int = 7) -> dict:
    """磁盘并发曲线：逐日峰值 + 最近一天分钟曲线（历史视图用）。"""
    days = max(1, min(int(days or 1), HISTORY_DAYS))
    day_list = _recent_days(days)
    data_dir = _hist_dir()
    out: dict[str, dict] = {d: {"client": 0, "slot": 0, "queue": 0, "samples": 0}
                            for d in day_list}
    latest_minutes: list[dict] = []
    for day in day_list:
        path = data_dir / f"{_CONC_PREFIX}{day}.jsonl"
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            b = out[day]
            b["client"] = max(b["client"], int(row.get("c") or 0))
            b["slot"] = max(b["slot"], int(row.get("s") or 0))
            b["queue"] = max(b["queue"], int(row.get("q") or 0))
            b["samples"] += int(row.get("n") or 0)
            if day == day_list[-1]:
                latest_minutes.append(row)
    daily = [{"name": d, **out[d]} for d in day_list]
    return {
        "days": days,
        "daily": daily,
        "peak": {k: max([d[k] for d in daily] or [0]) for k in ("client", "slot", "queue")},
        "minutes": latest_minutes[-1440:],
    }


def start_sampler():
    """启动 1s 采样心跳（lifespan 调）；无 running loop 时返回 None（单测直接调 sample）。"""
    import asyncio

    async def _loop():
        while True:
            try:
                sample()
            except Exception as err:  # noqa: BLE001 - 心跳异常不许掉线
                logs.warn("reqlog", f"并发采样异常（继续）: {err}")
            await asyncio.sleep(_CONC_TICK)

    try:
        return asyncio.create_task(_loop())
    except RuntimeError:
        return None


def flush_conc() -> None:
    """关机刷盘：把当日未满的分钟也落下去（lifespan shutdown 调）。"""
    with _lock:
        day = _conc_day
        buckets = dict(_conc_min)
        _conc_min.clear()
    _drain_flush()
    _write_minutes(day, buckets)


def begin(req_id: str, endpoint: str, model: str, stream: bool, preview: str = "",
          *, path: str = "", client_ip: str = "", ua: str = "", probe: bool = False,
          msgs: int | None = None, max_tokens: int | None = None,
          model_req: str = "") -> None:
    """请求进入网关（鉴权通过、body 解析成功后）。

    新增字段全部带默认值：旧调用点不传也能工作；网关三个入口（messages /
    chat / responses）会补齐来源与请求形状。
    """
    entry = {
        "req_id": req_id,
        "ts": time.time(),
        "endpoint": endpoint,
        "path": _clip(path or endpoint, 80),
        "model": model,
        "model_req": _clip(model_req, 80),
        "stream": bool(stream),
        "preview": (preview or "")[:80],
        "client_ip": _clip(client_ip, 64),
        "ua": _clip(ua, 96),
        "probe": bool(probe),
        "msgs": msgs if isinstance(msgs, int) else None,
        "max_tokens": max_tokens if isinstance(max_tokens, int) else None,
        "account": "",
        "mode": "",
        "channel": "",
        "egress": "",
        "up": False,
        "att": [],
        "wait_q": 0.0,
        "wait_rl": 0.0,
        "wait_5xx": 0.0,
        "wait_cap": 0.0,
        "retry_429": 0,
        "retry_5xx": 0,
        "retry_cap": 0,
        "retry_fb": 0,
        "ok": None,
        "status": None,
        "error": "",
        "err_code": None,
        "err_kind": "",
        "t_first": None,
        "t_total": None,
        "input_tokens": None,
        "output_tokens": None,
        "cache_read": None,
        "sess": None,
        "sess_repeat": None,
        "upstream_id": "",
        "stop_reason": None,
        "t_up": None,
        "dedup": "",
    }
    with _lock:
        _entries.append(entry)
        _inflight[req_id] = entry
        _conc["client"] = len(_inflight)
        _peak_locked("client", _conc["client"], entry["ts"])
        _bump_minute_locked()


def tag_session(req_id: str, sess_key: str, repeat: bool) -> None:
    """会话亲和观测（observe 档，app/session_ctx.py）：会话键 + 是否复见。"""
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["sess"] = sess_key
            entry["sess_repeat"] = bool(repeat)


def mark_account(req_id: str, account_name: str, mode: str) -> None:
    """记录实际服务该请求的账号（多账号重试时最后一次生效）。"""
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["account"] = account_name
            entry["mode"] = mode


def mark_channel(req_id: str, channel: str) -> None:
    """记录最终命中的业务通道：plan（Coding Plan）或 apikey（回退）。"""
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["channel"] = _clip(channel, 20)


def mark_egress(req_id: str, egress: str) -> None:
    """记录最后一次尝试的出口：direct / proxy / account（账号级代理）。"""
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["egress"] = _clip(egress, 20)


def mark_upstream(req_id: str) -> None:
    """标记本请求已真实发往上游（首次置位后不复位）。

    与 mark_account 的区别：account 是重试梯子里「最后一次尝试」的账号名，
    该号可能从未发出请求（如排队超时放弃）；up 只在真正发起上游调用后置
    True —— 监控端据此区分「调度失败·未达上游」（账号名仅供参考）与
    「上游失败·账号归属真实」，消除 503 排队放弃行张冠李戴的误读。
    """
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["up"] = True


def mark_upstream_meta(req_id: str, upstream_id: str = "") -> None:
    """记录上游响应头里的追踪标识（cf-ray / x-request-id），便于跨系统对账。"""
    if not upstream_id:
        return
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["upstream_id"] = _clip(upstream_id, 120)


def mark_attempt(req_id: str, account_name: str, mode: str = "", channel: str = "",
                 status: int | None = None, ms: float | None = None,
                 error: str = "", code: int | None = None) -> None:
    """追加一次账号尝试（失败换号 / 回退 / 最终成败都算）。

    只留观测必需的元数据：账号名、凭证类型、通道、上游状态码、耗时、失败原因。
    错误原因压成单行 80 字，避免把整段上游 body 复制进环形日志。
    """
    with _lock:
        entry = _inflight.get(req_id)
        if entry is None:
            return
        att = entry["att"]
        if len(att) >= MAX_ATTEMPTS:
            return
        item = {
            "acc": _clip(account_name, 40),
            "mode": _clip(mode, 10),
            "ch": _clip(channel, 12),
            "st": status,
            "ms": round(ms, 3) if isinstance(ms, (int, float)) else None,
        }
        if code is not None:
            item["code"] = code
        err = " ".join((error or "").split())[:80]
        if err:
            item["err"] = err
        att.append(item)


def note_wait(req_id: str, kind: str, seconds: float | None) -> None:
    """累计一类等待耗时（q=排队 / rl=429 / 5xx / cap=验证码）。"""
    if kind not in _WAIT_KEYS or not isinstance(seconds, (int, float)) or seconds <= 0:
        return
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry[f"wait_{kind}"] += float(seconds)


def note_retry(req_id: str, kind: str) -> None:
    """计数一类重试（429 / 5xx / cap=补码 / fb=通道回退）。"""
    if kind not in _RETRY_KEYS:
        return
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry[f"retry_{kind}"] += 1


def _net_up(entry: dict) -> float | None:
    """上游处理净耗时 = 总耗时 − 网关侧等待（排队/429/5xx/验证码）。

    524 红线归因用：净 up 大 = 上游慢；wait 大而 up 小 = 我们自己等出来的。
    """
    total = entry.get("t_total")
    if not isinstance(total, (int, float)):
        return None
    wait = sum(float(entry.get(f"wait_{k}") or 0) for k in _WAIT_KEYS)
    return round(max(0.0, total - wait), 3)


def mark_dedup(req_id: str, state: str) -> None:
    """风暴去重标记：replayed=TTL 内同内容失败重放；merged=follower 等 leader 失败后重放。

    这两类请求不进调度（零号压力），以前在监控里完全隐形 —— 面板要靠它们回答
    「今天去重少烧了多少号」。leader 成功/超时后 follower 正常自走的场景不打标
    （那是真实调度，不是去重收益）。
    """
    with _lock:
        entry = _inflight.get(req_id)
        if entry is not None:
            entry["dedup"] = _clip(state, 12)


def finish_ok(req_id: str, t_first: float | None = None,
              input_tokens: int | None = None, output_tokens: int | None = None,
              status: int | None = None, cache_read: int | None = None,
              stop_reason: str | None = None) -> None:
    with _lock:
        entry = _inflight.pop(req_id, None)
        if entry is None:
            return
        _conc["client"] = len(_inflight)
        entry["ok"] = True
        entry["status"] = status or 200
        entry["t_first"] = t_first
        entry["t_total"] = time.time() - entry["ts"]
        entry["input_tokens"] = input_tokens
        entry["output_tokens"] = output_tokens
        entry["cache_read"] = cache_read
        if stop_reason:
            entry["stop_reason"] = _clip(stop_reason, 20)
        entry["t_up"] = _net_up(entry)
        row = dict(entry)
    _persist(row)


def finish_error(req_id: str, error: str, status: int | None = None,
                 t_first: float | None = None, *, code: int | None = None,
                 kind: str = "") -> None:
    with _lock:
        entry = _inflight.pop(req_id, None)
        if entry is None:
            return
        entry["ok"] = False
        entry["status"] = status
        entry["error"] = (error or "")[:200]
        entry["t_first"] = t_first
        entry["t_total"] = time.time() - entry["ts"]
        if code is not None:
            entry["err_code"] = code
        if kind:
            entry["err_kind"] = _clip(kind, 20)
        entry["t_up"] = _net_up(entry)
        _conc["client"] = len(_inflight)
        row = dict(entry)
    _persist(row)


def snapshot() -> list[dict]:
    """全部条目，最新在前（含在途）。"""
    with _lock:
        return [dict(e) for e in reversed(_entries)]


def clear() -> None:
    """清空内存实时层（磁盘历史不受影响）。

    故意不清 _inflight：在途条目已从 _entries 摘除但对象仍在 _inflight 里，
    收口时照旧落盘历史 —— 否则「清空」会把正在跑的请求的结局一起抹掉。
    """
    with _lock:
        _entries.clear()


# ── 磁盘历史 ─────────────────────────────────────────────────────────────────
def _hist_dir() -> Path:
    return Path(settings.DATA_DIR)


def _recent_days(days: int) -> list[str]:
    """近 days 天的北京日期列表（升序，末位为今天）。"""
    now = datetime.now(_BJ)
    return [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _maybe_prune(data_dir: Path) -> None:
    """按天清理过期历史文件（每个自然日最多一次）。"""
    global _last_prune_day
    today = datetime.now(_BJ).strftime("%Y-%m-%d")
    if _last_prune_day == today:
        return
    _last_prune_day = today
    cutoff = (datetime.now(_BJ) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    for path in data_dir.glob(f"{_PREFIX}*.jsonl"):
        day = path.name[len(_PREFIX):-len(".jsonl")]
        if day < cutoff:  # 日期字符串字典序 == 时间序
            try:
                path.unlink()
            except OSError as err:
                logs.warn("reqlog", f"历史文件清理失败（忽略）: {path.name} {err}")


def _retry_total(entry: dict) -> int:
    return sum(int(entry.get(f"retry_{k}") or 0) for k in _RETRY_KEYS)


def _persist(entry: dict) -> None:
    """终态条目落盘（best-effort）。"""
    try:
        data_dir = _hist_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        row = {
            "ts": round(entry.get("ts") or 0, 3),
            "ok": entry.get("ok"),
            "status": entry.get("status"),
            "kind": entry.get("err_kind") or "",
            "code": entry.get("err_code"),
            "account": entry.get("account") or "",
            "mode": entry.get("mode") or "",
            "ch": entry.get("channel") or "",
            "eg": entry.get("egress") or "",
            "up": bool(entry.get("up") or False),
            "model": entry.get("model") or "",
            "endpoint": entry.get("endpoint") or "",
            "path": entry.get("path") or "",
            "ip": entry.get("client_ip") or "",
            "ua": entry.get("ua") or "",
            "probe": bool(entry.get("probe")),
            "stream": bool(entry.get("stream")),
            "t_first": entry.get("t_first"),
            "t_total": entry.get("t_total"),
            "in": entry.get("input_tokens"),
            "out": entry.get("output_tokens"),
            "cache_read": entry.get("cache_read"),
            # 会话亲和观测（app/session_ctx.py）：sess 是内容哈希非正文
            "sess": entry.get("sess"),
            "sess_repeat": entry.get("sess_repeat"),
            # 尝试梯子只落条数与各原因重试计数，明细留在实时页
            "att": len(entry.get("att") or []),
            "rt": _retry_total(entry),
            "q": round(float(entry.get("wait_q") or 0), 2),
            "rl": round(float(entry.get("wait_rl") or 0), 2),
            "5xx": round(float(entry.get("wait_5xx") or 0), 2),
            "cap": round(float(entry.get("wait_cap") or 0), 2),
            "trace": entry.get("upstream_id") or "",
            "stop": entry.get("stop_reason") or "",
            "up_t": entry.get("t_up"),
            "dd": entry.get("dedup") or "",
            # 历史只留错误摘要（明细 preview 含用户内容，不进磁盘）
            "err": (entry.get("error") or "")[:160],
        }
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        day = datetime.now(_BJ).strftime("%Y-%m-%d")
        with _hist_lock:
            _maybe_prune(data_dir)
            with open(data_dir / f"{_PREFIX}{day}.jsonl", "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as err:  # noqa: BLE001 - 记账失败不能影响请求
        try:
            logs.warn("reqlog", f"请求历史落盘失败（不影响请求）: {err}")
        except Exception:
            pass


def _new_bucket(key: str = "") -> dict:
    return {"name": key, "calls": 0, "ok": 0, "fail": 0, "in": 0, "out": 0,
            "probe": 0, "wait": 0.0, "retry": 0, "net_sum": 0.0, "net_n": 0,
            "codes": {}, "stops": {}, "dedup": 0, "disc": 0,
            "ttfb_sum": 0.0, "ttfb_n": 0, "total_sum": 0.0, "total_n": 0,
            "last": 0.0}


def _acc(bucket: dict, row: dict) -> None:
    bucket["calls"] += 1
    ok = row.get("ok")
    if ok is True:
        bucket["ok"] += 1
    elif ok is False:
        bucket["fail"] += 1
    bucket["in"] += int(row.get("in") or 0)
    bucket["out"] += int(row.get("out") or 0)
    if row.get("probe"):
        bucket["probe"] += 1
    bucket["wait"] += float(row.get("q") or 0)
    bucket["retry"] += int(row.get("rt") or 0)
    tf = row.get("t_first")
    if isinstance(tf, (int, float)):
        bucket["ttfb_sum"] += float(tf)
        bucket["ttfb_n"] += 1
    tt = row.get("t_total")
    if isinstance(tt, (int, float)):
        bucket["total_sum"] += float(tt)
        bucket["total_n"] += 1
    ut = row.get("up_t")
    if isinstance(ut, (int, float)):
        bucket["net_sum"] += float(ut)
        bucket["net_n"] += 1
    if row.get("dd"):
        bucket["dedup"] += 1
    st = row.get("status")
    if st:
        key = f"{st}/{row['code']}" if row.get("code") else str(st)
        bucket["codes"][key] = bucket["codes"].get(key, 0) + 1
        if int(st) == 499:
            bucket["disc"] += 1
    stop = row.get("stop")
    if stop:
        bucket["stops"][stop] = bucket["stops"].get(stop, 0) + 1
    ts = row.get("ts") or 0
    if ts > bucket["last"]:
        bucket["last"] = ts


def _finalize(bucket: dict) -> dict:
    done = bucket["ok"] + bucket["fail"]
    codes = bucket["codes"]
    stops = bucket["stops"]
    return {
        "name": bucket["name"],
        "calls": bucket["calls"],
        "ok": bucket["ok"],
        "fail": bucket["fail"],
        "rate": round(bucket["ok"] / done * 100, 1) if done else None,
        "in": bucket["in"],
        "out": bucket["out"],
        "probe": bucket["probe"],
        "wait": round(bucket["wait"], 1),
        "retry": bucket["retry"],
        "net": round(bucket["net_sum"] / bucket["net_n"], 3) if bucket["net_n"] else None,
        "dedup": bucket["dedup"],
        "disc": bucket["disc"],
        "disc_rate": round(bucket["disc"] / bucket["calls"] * 100, 1) if bucket["calls"] else None,
        # 直方图按频次降序取前 8（429/5xx 与业务码混排，设阈值看的是码不是文案）
        "codes": [{"code": c, "count": n} for c, n in
                  sorted(codes.items(), key=lambda kv: -kv[1])[:8]],
        "stops": [{"reason": c, "count": n} for c, n in
                  sorted(stops.items(), key=lambda kv: -kv[1])[:6]],
        "ttfb": round(bucket["ttfb_sum"] / bucket["ttfb_n"], 3) if bucket["ttfb_n"] else None,
        "ttfb_n": bucket["ttfb_n"],
        "total": round(bucket["total_sum"] / bucket["total_n"], 3) if bucket["total_n"] else None,
        "last": bucket["last"] or None,
    }


def _top(buckets: dict[str, dict]) -> list[dict]:
    rows = [_finalize(b) for b in buckets.values()]
    rows.sort(key=lambda r: (-r["calls"], r["name"]))
    return rows[:TOP_GROUPS]


def history(days: int = 7) -> dict:
    """磁盘历史聚合（近 days 天，含今天）。

    只回传聚合结果：30 天可达数万条明细，全量回传既拖慢页面也无额外信息
    （明细由实时页覆盖）。文件按天读，缺失的天补零 —— 保证曲线连续。
    """
    days = max(1, min(int(days or 1), HISTORY_DAYS))
    day_list = _recent_days(days)
    data_dir = _hist_dir()

    daily: dict[str, dict] = {d: _new_bucket(d) for d in day_list}
    accounts: dict[str, dict] = {}
    models: dict[str, dict] = {}
    channels: dict[str, dict] = {}
    clients: dict[str, dict] = {}
    errors: dict[str, int] = {}
    total = _new_bucket()

    for day in day_list:
        path = data_dir / f"{_PREFIX}{day}.jsonl"
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue  # 半行写入（进程被杀）直接跳过，不拖垮整页
            _acc(total, row)
            _acc(daily[day], row)
            acc_key = row.get("account") or "—"
            mdl_key = row.get("model") or "—"
            ch_key = row.get("ch") or "—"
            cli_key = row.get("ip") or "—"
            accounts.setdefault(acc_key, _new_bucket(acc_key))
            models.setdefault(mdl_key, _new_bucket(mdl_key))
            channels.setdefault(ch_key, _new_bucket(ch_key))
            clients.setdefault(cli_key, _new_bucket(cli_key))
            _acc(accounts[acc_key], row)
            _acc(models[mdl_key], row)
            _acc(channels[ch_key], row)
            _acc(clients[cli_key], row)
            if row.get("ok") is False:
                err = (row.get("err") or f"HTTP {row.get('status') or '未知'}")[:80] or "失败"
                errors[err] = errors.get(err, 0) + 1

    totals = _finalize(total)
    return {
        "days": days,
        "from": day_list[0],
        "to": day_list[-1],
        "totals": totals,
        "conc": conc_history(days),
        "daily": [_finalize(daily[d]) for d in day_list],
        "accounts": _top(accounts),
        "models": _top(models),
        "channels": _top(channels),
        "clients": _top(clients),
        "errors": [{"error": e, "count": c} for e, c in
                   sorted(errors.items(), key=lambda kv: -kv[1])[:TOP_ERRORS]],
    }
