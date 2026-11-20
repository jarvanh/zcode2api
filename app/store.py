"""账号与设置的持久化存储（SQLite）。

数据保存在项目本目录下的 data/accounts.db，采用 WAL 模式，
与 grok2api 的本地 (local) 账号后端保持一致。

运行期账号对象常驻内存（保证轮询游标与状态实时性），
每次变更同步落库；进程启动时从 SQLite 读取快照。
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import closing

from . import egress, logs, settings
from .models import PROVIDERS, Account, Status, jwt_upstream_uid, window_usable

_TBL = "accounts"
_META = "meta"


def _harden_umask() -> None:
    """收紧进程新建文件权限：凭证库及其 WAL/SHM 不受宿主 umask 影响。"""
    try:
        os.umask(0o077)
    except OSError as err:  # 极少数平台不支持，权限收紧仍由显式 chmod 兜底
        logs.warn("store", f"umask 加固失败: {err}")


_harden_umask()


def _chmod_private(path, mode: int) -> None:
    """文件存在则收紧到指定权限；不存在（如 WAL 尚未创建）静默跳过。"""
    try:
        os.chmod(path, mode)
    except FileNotFoundError:
        pass
    except OSError as err:
        logs.warn("store", f"权限收紧失败 {path}: {err}")


def _secure_db_files() -> None:
    """accounts.db / -wal / -shm / -journal 一律 0600（含运行期新建文件）。"""
    _chmod_private(settings.DB_PATH, 0o600)
    for suffix in ("-wal", "-shm", "-journal"):
        _chmod_private(settings.DB_PATH.with_name(settings.DB_PATH.name + suffix), 0o600)


def _parse_hours(raw: str) -> list[int]:
    """解析领取窗口（北京时间小时，逗号分隔；空/* = 全天 → []）。升序去重，脏值跳过。"""
    raw = str(raw or "").strip()
    if raw in ("", "*"):
        return []
    hours: list[int] = []
    for part in raw.replace("，", ",").split(","):
        part = part.strip()
        try:
            hour = int(part)
        except ValueError:
            continue  # 脏值跳过，不让一个坏配置挂掉领取轮
        if 0 <= hour <= 23 and hour not in hours:
            hours.append(hour)
    return sorted(hours)


# 已告警过的坏配置值（解析发生在每次循环唤醒/设置读取，去重避免刷屏）
_HOURS_WARNED: set[str] = set()


class Store:
    """线程安全的账号 / 设置存储，含轮询游标。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._accounts: dict[str, list[Account]] = {p: [] for p in PROVIDERS}
        self._settings: dict = {}
        self._rotation: dict[str, int] = {p: 0 for p in PROVIDERS}
        # 粘性活跃集（magpie maxAccounts 语义，plugins#649）：活跃名额只给
        # 「结构失效」（禁用/额度耗尽/整号冷却）的号让位，短暂不可用（风控
        # lane 冷却、429 扣号、并发满）不换班 —— 备用号保持静默，避免同 IP
        # 多账号高频轮流敲门（2026-11-07 att=4 残留形态根因之一）。
        self._active_set: dict[str, set[str]] = {p: set() for p in PROVIDERS}
        self._init_db()
        self._load()

    # ── SQLite 基础 ──────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        _secure_db_files()
        return conn

    def _init_db(self) -> None:
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        _chmod_private(settings.DATA_DIR, 0o700)
        with closing(self._connect()) as conn:
            conn.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS {_META} (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS {_TBL} (
                    id          TEXT PRIMARY KEY,
                    provider    TEXT NOT NULL,
                    name        TEXT,
                    mode        TEXT,
                    status      TEXT,
                    enabled     INTEGER NOT NULL DEFAULT 1,
                    created_at  REAL,
                    data        TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_acc_provider ON {_TBL} (provider);
                CREATE INDEX IF NOT EXISTS idx_acc_status   ON {_TBL} (status);
                """
            )
            self._bootstrap_admin_key(conn)
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('gateway_key', '')"
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('quota_refresh_interval', ?)",
                (str(settings.QUOTA_REFRESH_INTERVAL),),
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('claim_round_interval', ?)",
                (str(settings.CLAIM_ROUND_INTERVAL),),
            )
            conn.commit()
            _secure_db_files()

    def _bootstrap_admin_key(self, conn: sqlite3.Connection) -> None:
        """首次启动写入后台密钥：优先环境变量，否则随机生成（不留默认口令）。"""
        row = conn.execute(
            f"SELECT value FROM {_META} WHERE key = 'admin_key'"
        ).fetchone()
        current = str(row["value"]).strip() if row is not None else ""
        configured = (settings.DEFAULT_ADMIN_KEY or "").strip()
        legacy_default = current == "zcode" and not configured
        if current and not legacy_default:
            return
        key = configured or secrets.token_urlsafe(32)
        conn.execute(
            f"INSERT OR REPLACE INTO {_META} (key, value) VALUES ('admin_key', ?)",
            (key,),
        )
        if legacy_default:
            logs.warn("store", f"检测到旧版默认后台密钥 zcode，已随机替换为：{key}")
        elif not configured:
            # 首启仅此一次：用户需要该口令登录后台；请立即在后台改密。
            logs.warn("store", f"未配置 ZCODE_ADMIN_KEY，已随机生成后台密钥：{key}")

    def _load(self) -> None:
        with closing(self._connect()) as conn:
            meta_rows = conn.execute(f"SELECT key, value FROM {_META}").fetchall()
            self._settings = {r["key"]: r["value"] for r in meta_rows}
            self._settings.setdefault("admin_key", settings.DEFAULT_ADMIN_KEY)
            self._settings.setdefault("gateway_key", "")
            self._settings.setdefault("bigmodel_channel_enabled",
                                      "1" if settings.BIGMODEL_CHANNEL_ENABLED else "0")
            self._settings.setdefault("quota_refresh_interval", str(settings.QUOTA_REFRESH_INTERVAL))
            self._settings.setdefault("claim_round_interval", str(settings.CLAIM_ROUND_INTERVAL))
            self._settings.setdefault("claim_round_stagger", str(settings.CLAIM_ROUND_STAGGER))

            self._accounts = {p: [] for p in PROVIDERS}
            rows = conn.execute(
                f"SELECT data FROM {_TBL} ORDER BY created_at ASC"
            ).fetchall()
            for row in rows:
                try:
                    blob = json.loads(row["data"])
                    if not isinstance(blob, dict):
                        raise TypeError(f"账号行非 object: {type(blob).__name__}")
                    account = Account.from_dict(blob)
                except (json.JSONDecodeError, TypeError, AttributeError,
                        ValueError, KeyError) as exc:
                    # 静默跳过 = 启动期数据丢失无人知晓（2026-11-14 review）；
                    # 非 objecJSON/缺字段（from_dict 的 .items()/键访问）都不能
                    # 崩掉整个启动——单行损坏只跳过该行（2026-11-15 review）
                    logs.err("store", f"账号行损坏已跳过: {exc}")
                    continue
                if account.provider in self._accounts:
                    self._accounts[account.provider].append(account)

            # 存量「风控升级禁用」一次性迁移为通道级有界冷却（修 P1/P4/P5）：
            # 这批号是被旧语义错定为终态的通道级封锁（#716：billing 全程正常、
            # 只有 grant 腿被拦），旧状态既等不到人工、又连带停掉自动领包轮与
            # 额度刷新。迁移后封锁记在 plan 通道上，到点自动回轮询。
            changed: dict[str, Account] = {}
            # 冷却到期归一：DB 里的 COOLING 是历史态，到期不应继续作为持久状态
            # 留给外部消费者（额度看板直接读 DB，会一直显示「冷却中」）。
            for accounts in self._accounts.values():
                for account in accounts:
                    if account.normalize_expired_cooling():
                        changed[account.id] = account
            if settings.RISK_LANE_SCOPED:
                migrated = []
                for accounts in self._accounts.values():
                    for account in accounts:
                        if account.migrate_legacy_risk_ban(
                            settings.RISK_LANE_BAN_COOLDOWN,
                            settings.RISK_LANE_COOLDOWN,
                            settings.RISK_STRIKE_DECAY_SECONDS,
                        ):
                            migrated.append(account)
                            changed[account.id] = account
                if migrated:
                    print(f"[risk-lane] 存量风控禁用已转通道级冷却 ×{len(migrated)}: "
                          f"{', '.join(a.name for a in migrated)}")
            for account in changed.values():
                conn.execute(
                    f"""INSERT OR REPLACE INTO {_TBL}
                        (id, provider, name, mode, status, enabled, created_at, data)
                        VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        account.id, account.provider, account.name, account.mode,
                        account.status, 1 if account.enabled else 0,
                        account.created_at,
                        json.dumps(account.to_dict(), ensure_ascii=False),
                    ),
                )
            if changed:
                conn.commit()
                print(f"[cooling] 已到期的冷却状态洗回 active ×{len(changed)}: "
                      f"{', '.join(a.name for a in changed.values())}")
        self._merge_duplicate_identities()
        # 启动补分配 per-account 出口（2.7.36；listener 池未配置时为 no-op）
        self.ensure_egress_assignments()

    def _merge_duplicate_identities(self) -> None:
        """启动合并同 uid 重复行（2026-11-15 生产事故修复）：同一上游账号
        重登产生新 JWT 时曾各建一行 → 一号绑 2 节点、双 IP 并行（风控暴露面）。
        合并规则：保留**新签**（created_at 最新且 enabled）那行；旧行删除且
        其 TP 节点释放（不再占用互斥集合）。uid 取不到（apiKey/畸形 JWT）
        的行不参与合并。"""
        from . import logs

        for provider in PROVIDERS:
            items = self._accounts[provider]
            by_uid: dict[str, Account] = {}
            drop: list[Account] = []
            for a in items:
                uid = jwt_upstream_uid(a.secret)
                if not uid:
                    continue
                keep = by_uid.get(uid)
                if keep is None:
                    by_uid[uid] = a
                    continue
                # 新签且 enabled 优先；并列时取 created_at 新者
                swap = (a.enabled, a.created_at) > (keep.enabled, keep.created_at)
                victim, winner = (keep, a) if swap else (a, keep)
                if swap:
                    by_uid[uid] = a
                # 历史合并回旧 winner：把计数并集进保留行（轻量保真）
                winner.use_count += victim.use_count
                winner.fail_count += victim.fail_count
                drop.append(victim)
            if not drop:
                continue
            for v in drop:
                logs.warn("store",
                          f"合并同上游身份重复账号行：{v.name}（{v.id}，节点"
                          f" {v.egress_node or '无'}）→ 保留 {by_uid[jwt_upstream_uid(v.secret)].id}")
            self._accounts[provider] = [a for a in items if a not in drop]
            for v in drop:
                self._delete_account(v.id)

    def _persist_account(self, account: Account) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                f"""INSERT OR REPLACE INTO {_TBL}
                    (id, provider, name, mode, status, enabled, created_at, data)
                    VALUES (?,?,?,?,?,?,?,?)""",
                (
                    account.id, account.provider, account.name, account.mode,
                    account.status, 1 if account.enabled else 0, account.created_at,
                    json.dumps(account.to_dict(), ensure_ascii=False),
                ),
            )
            conn.commit()

    def _delete_account(self, account_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(f"DELETE FROM {_TBL} WHERE id = ?", (account_id,))
            conn.commit()

    def _set_meta(self, key: str, value: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {_META} (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()

    def save(self) -> None:
        """全量落库（兜底接口）。"""
        with self._lock:
            for accounts in self._accounts.values():
                for account in accounts:
                    self._persist_account(account)

    # ── 设置 ─────────────────────────────────────────────────────────────────
    def get_setting(self, key: str, default=None):
        with self._lock:
            return self._settings.get(key, default)

    def set_setting(self, key: str, value) -> None:
        with self._lock:
            self._settings[key] = str(value)
            self._set_meta(key, str(value))

    def admin_key(self) -> str:
        return str(self.get_setting("admin_key", settings.DEFAULT_ADMIN_KEY) or "")

    def gateway_key(self) -> str:
        # 库里未设置时回退 .env 的 ZCODE_GATEWAY_KEY（上游 README 声明了该变量
        # 但代码未实现，此处补齐）；库里已设置（含后台改过）则以库为准。
        return str(self.get_setting("gateway_key", "") or settings.GATEWAY_KEY or "")

    def bigmodel_channel_enabled(self) -> bool:
        """bigmodel Key 回退通道开关（默认关闭，meta 表可覆盖）。"""
        return str(self.get_setting("bigmodel_channel_enabled", "") or "").strip().lower() \
            in ("1", "true", "yes", "on")

    def quota_refresh_interval(self) -> int:
        try:
            return max(0, int(self.get_setting("quota_refresh_interval", settings.QUOTA_REFRESH_INTERVAL)))
        except (TypeError, ValueError):
            return settings.QUOTA_REFRESH_INTERVAL

    def account_concurrency(self) -> int:
        """单账号并发上限（0 = 不限），控制台可改、即时生效（meta 表）。

        2026-11-04 曾按线上实测写死 2（当时「并发满 → 敲上游 → 扣号」会
        放大 3008 风暴）；2026-11-08 恢复可改 —— v2.9.32 startGate 排队闸
        把该放大链吃掉后，上限交还运营调节。历史 meta 值重新生效。
        """
        try:
            return max(0, int(self.get_setting(
                "account_concurrency", settings.ACCOUNT_CONCURRENCY)))
        except (TypeError, ValueError):
            return settings.ACCOUNT_CONCURRENCY

    def max_active_accounts(self) -> int:
        """同时投入的账号数上限，后台可改、即时生效。

        解析顺序（2026-11-15 主人确认的规则）：
        1. meta 显式设置 > 0：用 meta 值（含 0 = 不限，返回 0）；
        2. 否则动态推导：
           - 池内有绑定代理的账号 → 上限 = 代理数（1 代理 1 账号，与出口
             隔离/轮换机制一一对应）；
           - 全部直连 → 上限 = settings.DIRECT_ACTIVE_ACCOUNTS（默认 2，
             无代理时只保守使用 2 个直连账号）。

        超出的号不删不换，只是留作备用——在用的号冷却、额度用完或停用时，
        空出的名额由下一个顶上。
        """
        raw = self.get_setting("max_active_accounts", None)
        if raw is not None and str(raw).strip() != "":
            try:
                return max(0, int(raw))
            except (TypeError, ValueError):
                pass  # 脏 meta 值走动态推导
        proxy_count = sum(
            1 for accounts in self._accounts.values()
            for a in accounts if (a.proxy_url or "").strip()
        )
        return proxy_count if proxy_count > 0 else settings.DIRECT_ACTIVE_ACCOUNTS

    def claim_round_interval(self) -> int:
        """套餐自动领取轮间隔（0 = 关闭）。运行时可改（meta 表），改后即生效。"""
        try:
            return max(0, int(self.get_setting("claim_round_interval", settings.CLAIM_ROUND_INTERVAL)))
        except (TypeError, ValueError):
            return settings.CLAIM_ROUND_INTERVAL

    def next_claim_round_at(self) -> float | None:
        """下一轮套餐自动领取的预估时刻（epoch）；轮关闭返回 None。

        保守下界估计：领取轮每 interval 一轮（_round 结束后再等 interval），
        进程外无法知道上一轮的精确完成时刻，用 interval 直接外推——最坏比
        实际晚一个周期，用于 Retry-After 足够（额度恢复通常取「窗口重置 /
        领包」较早者，领包最多等一个周期）。
        """
        interval = self.claim_round_interval()
        if interval <= 0:
            return None
        return time.time() + interval

    def claim_round_stagger(self) -> int:
        """领取轮账号错峰上限秒数（0 = 关闭）。运行时可改（meta 表），下一轮生效。"""
        try:
            return max(0, int(self.get_setting("claim_round_stagger", settings.CLAIM_ROUND_STAGGER)))
        except (TypeError, ValueError):
            return settings.CLAIM_ROUND_STAGGER

    def claim_round_hours(self) -> list[int]:
        """自动领取窗口：北京时间小时升序列表；空 = 全天（不限时）。运行时可改。

        上游套餐按天投放（用户侧实证：每天 19:00 放量），窗口外轮询只会空转
        preview。窗口锚定**北京时钟**而非 host 时区（host 换成 UTC 也不漂）；
        中国无夏令时，固定 +8 即精确。
        """
        raw = str(self.get_setting("claim_round_hours", settings.CLAIM_ROUND_HOURS) or "").strip()
        hours = _parse_hours(raw)
        if not hours and raw not in ("", "*"):
            # 配置写了但一个有效小时都没有（env 笔误 / 手改 DB——管理端 PUT 有校验
            # 不会产生这种值）：**不允许坏配置静默升级成「全天轮询」**，回落默认并留痕。
            fallback = _parse_hours(settings.CLAIM_ROUND_HOURS)
            if raw not in _HOURS_WARNED:
                _HOURS_WARNED.add(raw)
                logs.warn("store", f"claim_round_hours={raw!r} 无有效小时，按默认处理 → {fallback or '全天'}")
            return fallback
        return hours

    # ── 账号读取 ─────────────────────────────────────────────────────────────
    def list_accounts(self, provider: str | None = None) -> list[Account]:
        with self._lock:
            if provider:
                return list(self._accounts.get(provider, []))
            return [a for p in PROVIDERS for a in self._accounts[p]]

    def find(self, provider: str, id_or_name: str) -> Account | None:
        with self._lock:
            return self._find_locked(provider, id_or_name)

    def find_any(self, id_or_name: str) -> Account | None:
        with self._lock:
            for p in PROVIDERS:
                for a in self._accounts[p]:
                    if a.id == id_or_name:
                        return a
        return None

    def _find_locked(self, provider: str, id_or_name: str) -> Account | None:
        for a in self._accounts.get(provider, []):
            if a.id == id_or_name or a.name == id_or_name:
                return a
        return None

    # ── 账号增删改 ───────────────────────────────────────────────────────────
    def add_account(self, provider: str, name: str, secret: str) -> Account:
        if provider not in PROVIDERS:
            raise ValueError(f"不支持的 provider: {provider}")
        account = Account.create(provider, name, secret)
        with self._lock:
            for a in self._accounts[provider]:
                if a.secret and a.secret == account.secret:
                    return a  # 跳过重复 token
            # uid 级去重（2026-11-15 生产教训：同号重登 JWT 变化绕过 token 相等
            # 去重 → 同一上游身份 2 行双节点并行，一号双 IP 风控暴露面）：
            # 同 uid 已存在 → 视为换签回写该行（保 id/绑定/指纹/计数），不建新行
            uid = jwt_upstream_uid(account.secret)
            for a in self._accounts[provider]:
                if a.mode == "jwt" and jwt_upstream_uid(a.secret) == uid and uid:
                    a.jwt_token = account.jwt_token
                    a.mode = "jwt"
                    # 保留 a.api_key：OAuth 兑换的回退通道跨重登仍有效（同号），
                    # 与 admin PUT 换 token 口径一致（review 2026-11-15 R3）
                    a.status = Status.ACTIVE
                    a.last_error = None
                    self._persist_account(a)
                    return a
            self._assign_fingerprint(account)  # 入池即分配独立设备指纹
            account.install_id = str(uuid.uuid4())  # 安装身份：稳定安装令牌
            if egress.tetherpool_enabled_for(account):
                # TP 供给：网关 URL 确定性构造（同步、无 HTTP；TP 绑定由
                # egress_pool 后台引导/网关首请求粘性建立）
                account.egress = egress.tetherpool_gateway_url(account)
            else:
                self._assign_egress(account)  # 入池即分配专属出口（每账号一个 IP）
            self._accounts[provider].append(account)
            self._persist_account(account)
        return account

    def _assign_egress(self, account: Account) -> None:
        """入池分配专属出口（per-account egress，2.7.36）：从 EGRESS_LISTENERS
        池取第一个未被占用的 listener URL 固化到账号。池未配置/已耗尽 →
        保持 None（回落共享出口，warn 留痕）。绑定跨重启稳定（验证码 token
        与出口 IP 绑定）。"""
        if account.egress or not settings.EGRESS_LISTENERS:
            return
        used = {a.egress for items in self._accounts.values() for a in items if a.egress}
        free = next((u for u in settings.EGRESS_LISTENERS if u not in used), None)
        if free is None:
            logs.warn("store", f"出口 listener 池已耗尽，账号 {account.name} 回落共享出口")
            return
        account.egress = free

    def ensure_egress_assignments(self) -> None:
        """启动/供给切换补分配（2.7.39 supply 感知）：

        - tetherpool：灰度命中账号 → 网关 URL（确定性构造）；非灰度账号若持
          网关 URL（模式回退残留）→ 清空回落 listener/共享。
        - listeners：网关 URL 形态的 egress 清空重领 listener（回滚通道）。
        """
        if settings.EGRESS_SUPPLY == "tetherpool":
            with self._lock:
                changed: list[Account] = []
                for items in self._accounts.values():
                    for account in sorted(items, key=lambda x: x.created_at):
                        old = account.egress
                        mutated = False
                        if egress.tetherpool_enabled_for(account):
                            want = egress.tetherpool_gateway_url(account)
                            if account.egress != want:
                                account.egress = want
                                mutated = True
                        elif egress.is_tp_gateway_url(old):
                            account.egress = None  # 模式回退残留清理
                            account.egress_node = None  # 语义对齐：无 TP 出口即无 TP 节点
                            mutated = True
                        if mutated and not account.egress:
                            # 灰度外账号：网关残留清空后同遍回落 listener
                            # （2.7.69：原先只清不填，空窗期以 None 走共享语义）
                            self._assign_egress(account)
                        if mutated:
                            changed.append(account)
                for account in changed:
                    self._persist_account(account)
            if changed:
                logs.ok("store", f"per-account 出口补分配（tetherpool 供给）{len(changed)} 个账号")
            return
        # listeners 供给：网关 URL 残留清空 + listener 池分配（2.7.36 行为）。
        # 清空与回填同遍完成（2.7.69 根修：原先 elif 短路——清完残留的账号
        # 要等下一次 ensure 才能领 listener，空窗期以 None 落回共享出口语义，
        # 违反一号一节点红线）。
        with self._lock:
            changed: list[Account] = []
            for items in self._accounts.values():
                for account in sorted(items, key=lambda x: x.created_at):
                    old = account.egress
                    mutated = False
                    if egress.is_tp_gateway_url(old):
                        account.egress = None
                        account.egress_node = None  # 语义对齐（回滚不留 TP 节点）
                        mutated = True
                    if not account.egress:
                        self._assign_egress(account)
                        mutated = mutated or bool(account.egress)
                    if mutated:
                        changed.append(account)
            for account in changed:
                self._persist_account(account)
        if changed:
            logs.ok("store", f"per-account 出口补分配（listeners 供给）{len(changed)} 个账号")

    @staticmethod
    def _assign_fingerprint(account: Account) -> None:
        """给账号分配客户端指纹并固化为 dict（随 asdict 落库，取用时还原）。"""
        from .fingerprint import profile_for

        profile = profile_for(account)
        account.fingerprint = {
            "platform": profile.platform, "arch": profile.arch,
            "os_version": profile.os_version, "language": profile.language,
            "timezone": profile.timezone, "screen": profile.screen,
            "device_mid": profile.device_mid,
        }

    def remove_account(self, provider: str, id_or_name: str) -> bool:
        with self._lock:
            items = self._accounts.get(provider, [])
            target = next((a for a in items if a.id == id_or_name or a.name == id_or_name), None)
            if not target:
                return False
            self._accounts[provider] = [a for a in items if a.id != target.id]
            self._delete_account(target.id)
            return True

    def update_account(self, account: Account) -> bool:
        """持久化某个账号的当前状态。

        账号已从内存池删除时拒绝写回，避免后台任务 INSERT OR REPLACE 把已删行救活。
        """
        with self._lock:
            if self._find_locked(account.provider, account.id) is None:
                return False
            self._persist_account(account)
            return True

    def set_enabled(self, provider: str, id_or_name: str, enabled: bool) -> bool:
        with self._lock:
            account = self._find_locked(provider, id_or_name)
            if not account:
                return False
            account.enabled = enabled
            if not enabled:
                account.status = Status.DISABLED
                # 禁用即清探针失败计数：健康循环不跑禁用账号，残留 streak
                # 会在重新启用后以旧账起算（review 2026-11-15 R2）
                from . import egress_pool

                egress_pool.egress_pool.clear_probe_streak(account.id)
            elif account.status in (Status.DISABLED, Status.COOLING):
                # 人工启用 = 确认恢复：禁用洗回 active；冷却中启用视为跳过等待
                #（风控退避本就是自动恢复语义，人工提前放行同样成立）
                account.status = Status.ACTIVE
                account.cooling_until = None
            if enabled:
                # 人工确认恢复即全新起算。旧实现只洗状态不清计数，而禁用是
                # 「strike 达标」的直接后果 → 启用后下一次命中立即再达标（零冷却
                # 缓冲），形成「启用→一发即封→再自动换指纹」的身份抖动循环（P3/P4.5）。
                account.risk_strikes = 0
                account.last_risk_at = None
                account.clear_all_risk_lanes()
                if (account.last_error or "").startswith(("风控封禁", "命中风控")):
                    account.last_error = None
            self._persist_account(account)
            return True

    # ── 负载均衡与亲和选择 ───────────────────────────────────────────────────
    def select(
        self,
        provider: str,
        skip_ids: set[str] | None = None,
        model: str | None = None,
        inflight: dict[str, int] | None = None,
        pinned_id: str | None = None,
        max_inflight: int = settings.ACCOUNT_CONCURRENCY,
        ignore_rate_hold: bool = False,
    ) -> Account | None:
        """选择下一个可用账号（fork 保留模型级判定 + 上游负载/亲和）。

        策略：
        1. 亲和账号优先：若指定 pinned_id 且未满（inflight < max_inflight）且可用，直接选用。
        2. 负载均衡分配（新请求 或 亲和号满载/不可用时溢出）：
           - 过滤掉 is_selectable 为 False、在 skip_ids 中、或已达并发上限（inflight >= max_inflight）的账号；
           - 主排序：当前在飞数 inflight 升序（最小负载优先）；
           - 次排序：最大剩余额度降序（同负载下优先大余额号。凌晨全池 inflight=0
             全平局时，纯轮询会把「只剩 5M」与「剩 3.05 亿」同权重随机分——
             2026-11-17 线上实证 use_count 长期失衡 4034 vs 68）。口径与
             window_usable 对齐：只数「remaining>0 且未过期」的窗口——已过期
             大窗不排富（快照陈旧期不得把流量导向真实无量的号）；窗口缺
             remaining 时按 total-used 回填（上游有的窗口只给 total/used）。
             全部不可用时记 0：未知快照（新号）**劣于**已知正余额号（其流量
             保障来自 inflight 主排序与全池未知时的轮询退化），全池未知退化为
             纯轮询；
           - 模型感知（2.7.55）：指定 model 时只看**该模型**的窗口排富——
             「GLM-5.3 rem=0 但 Flash 有 2.9 亿」的号在 GLM-5.3 请求眼里记 0，
             不再凭无关窗口的富余额被优先选中（2026-11-17 线上实证：plan_guard
             冷却被夜间重启洗掉后，这种号被排富前排、每请求撞一次必然失败的
             1005）。快照不含该模型窗口 = 额度未知 = 记 0（不歧视新号，行为
             与全池未知退化轮询一致）；model=None 保持原 max 口径。
           - 末排序：距离当前轮询指针的偏移量（额度同档时均匀轮换）；
           - 更新轮询指针为选中账号的下一位。
        """
        skip_ids = skip_ids or set()
        inflight_map = inflight or {}
        now = time.time()
        with self._lock:
            accounts = list(self._accounts.get(provider, []))
        # 同时投入账号数上限（0 = 不限）。粘性活跃集语义（magpie maxAccounts）：
        # 名额满时新请求只落在活跃集内；成员在选中时进入，仅当结构失效（禁用/
        # 额度耗尽/整号冷却）或被移出池子时让位 —— 短暂不可用不换班。
        # 集为空（冷启动/全部退场）时按 inflight 回退，避免历史行为骤变。
        # 必须在拿锁前读设置（get_setting 自己会拿同一把 RLock）。
        active_cap = self.max_active_accounts()
        active_set = self._active_set.get(provider, set())
        if active_cap > 0 and not active_set:
            active_set = {a.id for a in accounts if inflight_map.get(a.id, 0) > 0}
            self._active_set[provider] = active_set
        if active_cap > 0 and active_set:
            by_id = {a.id: a for a in accounts}
            vacated = {
                aid for aid in active_set
                if aid not in by_id
                or not by_id[aid].enabled
                or by_id[aid].status in (Status.EXHAUSTED, Status.INVALID, Status.DISABLED)
                or (by_id[aid].is_cooling() and by_id[aid].cooling_until
                    and by_id[aid].cooling_until > now)
            }
            if vacated:
                active_set -= vacated
        else:
            active_set = set()
        with self._lock:
            # 1. 亲和号尝试
            if pinned_id and pinned_id not in skip_ids:
                for a in accounts:
                    if a.id == pinned_id:
                        if a.is_selectable(now, model=model) and inflight_map.get(a.id, 0) < max_inflight:
                            return a
                        break

            # 2. 候选池过滤
            pool = [
                a for a in accounts
                if (a.is_selectable(now, model=model)
                    or (ignore_rate_hold and model and a.is_rate_held(model, now)
                        and not a.is_model_cooling(model, now)
                        and not a.lane_blocked_for_model(model, now)
                        and a.enabled and a.status not in (Status.EXHAUSTED, Status.INVALID, Status.DISABLED)))
                and a.id not in skip_ids
                and inflight_map.get(a.id, 0) < max_inflight
            ]
            if active_cap > 0 and pool:
                if ignore_rate_hold:
                    # 换号判定：忽略扣号/短暂不可用，看结构可用性（判定本身
                    # 不改活跃集，由 _dispatch 的正常选号路径维护成员）。
                    pass
                elif active_set:
                    if len(active_set) >= active_cap:
                        # 名额已满：恒收缩到集内（负载+轮询在集内分配，保缓存
                        # 保亲和）。集内号短暂不可用（429 扣号/lane 冷却）被
                        # 过滤后若无人可选 → 503/排队，**不拉备用顶班** ——
                        # magpie 换班只发生在结构失效让位（vacated）后。
                        pool = [a for a in pool if a.id in active_set]
                    # 名额未满：全池参与负载+轮询（v2.9.21 语义），选中的号
                    # 下方入集 —— 冷启动/重启恢复期逐步把活跃集补到 cap，
                    # 之后满员收缩，备用号保持静默。
            if not pool:
                return None

            cur_rot = self._rotation.get(provider, 0)
            n = len(pool)

            def _max_remaining(a: Account) -> int:
                """该号额度快照中最大的有效余量（全部不可用 = 0）。

                只做「同负载平局打破」用，非精确配额——max 而非 sum 是因为
                多模型窗口不可互抵（Flash 池与 Coding 池各自独立账本）。
                口径与 window_usable 对齐（2.7.51）：
                  - 已过期窗口不计入：快照陈旧期不得让过期大窗排富，把流量
                    导向真实无量的号（各处额度判定同一哲学）；
                  - remaining 缺失按 total-used 回填（上游有的窗口只给
                    total/used，window_usable 的「未知但有量」同源）；
                  - 全部窗口不可用/快照未知 → 0（劣于已知正余额号）。
                模型感知（2.7.55）：model 指定时只数该模型窗口；快照不含该
                窗口 = 额度未知 = 记 0（等价于「无此模型余量」，冷却丢失时
                也不被无关窗口排富选中）。
                """
                windows = (a.quota or {}).values()
                if model:
                    # 窗口名精确匹配（billing 快照窗口名 = entitlement show_name，
                    # 与请求 model 串同一形态，如 "GLM-5.3" / "GLM-5.3-Flash"）
                    w = (a.quota or {}).get(model)
                    windows = [w] if isinstance(w, dict) else []
                vals: list[int] = []
                for w in windows:
                    if not isinstance(w, dict):
                        continue
                    if not window_usable(w, now):
                        continue
                    rem = w.get("remaining")
                    if rem is None:
                        rem = max(
                            0,
                            (w.get("total") or 0) - (w.get("used") or 0),
                        )
                    vals.append(rem or 0)
                return max(vals) if vals else 0

            # 负载优先（最小在飞），同负载下大余额优先，再按轮询偏移平局。
            # 余额平局带：额度只在相对容差内才算「同档」——精确整数比较下
            # 快照间必然有微小漂移（used 逐请求累加），轮换偏移将永远被第二级
            # 压过、形同虚设（2026-11-17 tebi 实证：9 号全在 2.83~2.96 亿档，
            # 差距 <5%，连选 8 次恒中同一号）。容差带内视为同档交还轮换偏移；
            # 带外（rj > rk*(1+tol)）大余额才优先。单一判定式避免双口径打架
            # （abs 差值 vs 比例式在边界互相矛盾——1.05亿 vs 1亿曾被判带内）。
            tol = settings.SELECT_TIE_TOLERANCE

            # 预计算各候选的有效余量（避免 O(n²) 窗口扫描）
            rems = [_max_remaining(a) for a in pool]

            best_idx = 0
            best_tuple = (
                inflight_map.get(pool[0].id, 0),
                -rems[0],
                (0 - cur_rot) % n,
            )
            for i in range(1, n):
                cur_inflight = inflight_map.get(pool[i].id, 0)
                if cur_inflight < best_tuple[0]:
                    # 主排序：负载更低者直接胜出（余额只在同负载时参与）
                    best_tuple = (cur_inflight, -rems[i], (i - cur_rot) % n)
                    best_idx = i
                elif cur_inflight > best_tuple[0]:
                    continue
                # 同负载：带外显著更富者胜；容差带内按轮换偏移
                elif rems[i] > rems[best_idx] * (1 + tol):
                    best_tuple = (cur_inflight, -rems[i], (i - cur_rot) % n)
                    best_idx = i
                elif not rems[best_idx] > rems[i] * (1 + tol):
                    # 带内（含相等）→ 轮换偏移决定
                    if (i - cur_rot) % n < best_tuple[2]:
                        best_tuple = (cur_inflight, -rems[i], (i - cur_rot) % n)
                        best_idx = i
                # else：best 显著更富（带外），保持 best

            account = pool[best_idx]
            self._rotation[provider] = (best_idx + 1) % n
            # ignore_rate_hold 的调用（429 换号判定）只探可用性，不得把
            # 被跳过的备用号写入活跃集 —— 否则每次全池 429 风暴都会把备用
            # 号逐个拖进来轮着吃 429（2026-11-07 08:25 实测：storm 把
            # zai-176/131 顶进了集，破坏「正式工+备用」粘性）。
            if active_cap > 0 and not ignore_rate_hold:
                self._active_set[provider].add(account.id)
            return account

    def next_lane_uncooldown_at(self, provider: str, model: str) -> float | None:
        """池内因风控封锁而最早可重新承接该模型的时刻；无封锁返回 None。

        取所有候选账号里**最早**解封的那个（排队/Retry-After 关心的是何时
        有人能接，不是最晚）。与 next_model_uncooldown_at 分立：前者是「该模型
        无包」，后者是「该通道被风控」，语义不同不能共字段。整号冷却（含全腿
        哑火降级态）沿用既有 503 语义，不在此重复计数。
        """
        now = time.time()
        soonest: float | None = None
        for acc in self.list_accounts(provider):
            if not acc.enabled or acc.status in (Status.INVALID, Status.DISABLED):
                continue
            until = acc.lane_unblock_at(model, now)
            if acc.is_cooling() and acc.cooling_until:
                # 全腿退化 COOLING 的号也在等同一批 lane 解封；按 is_cooling
                # 跳过会让 Retry-After 变成无等待的 503。与既有整号冷却取较晚。
                until = max(until or 0.0, acc.cooling_until)
            if until and until > now and (soonest is None or until < soonest):
                soonest = until
        return soonest

    def next_model_uncooldown_at(self, provider: str, model: str) -> float | None:
        """池内该模型最早解除冷却的 epoch；无任何模型级冷却时返回 None。

        用于把「全部账号都因缺资源包而不可承接」的可预期等待，换算成准确的
        Retry-After 时刻（2026-10-28 报错语义整改）。
        """
        key = Account.model_key(model)
        if not key:
            return None
        now = time.time()
        soonest: float | None = None
        for acc in self.list_accounts(provider):
            until = float(acc.model_cooling.get(key) or 0)
            if until > now and (soonest is None or until < soonest):
                soonest = until
        return soonest

    # ── 导入 / 导出 ─────────────────────────────────────────────────────────
    def export(self) -> dict:
        with self._lock:
            return {
                "version": 1,
                "exported_at": time.time(),
                "providers": {
                    p: [
                        {"name": a.name, "mode": a.mode, "secret": a.secret}
                        for a in self._accounts[p]
                    ]
                    for p in PROVIDERS
                },
            }

    def import_accounts(self, payload: dict) -> int:
        providers = payload.get("providers", {})
        count = 0
        for provider, items in providers.items():
            if provider not in PROVIDERS or not isinstance(items, list):
                continue
            for it in items:
                secret = it.get("secret") or it.get("token") or it.get("jwtToken") or it.get("apiKey")
                if not secret:
                    continue
                self.add_account(provider, it.get("name", provider), secret)
                count += 1
        return count


# 单例
store = Store()
