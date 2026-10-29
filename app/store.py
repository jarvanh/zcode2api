"""账号与设置的持久化存储（SQLite）。

数据保存在项目本目录下的 data/accounts.db，采用 WAL 模式，
与 grok2api 的本地 (local) 账号后端保持一致。

运行期账号对象常驻内存（保证轮询游标与状态实时性），
每次变更同步落库；进程启动时从 SQLite 读取快照。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import closing

from . import settings
from .models import PROVIDERS, Account, Status

_TBL = "accounts"
_META = "meta"


class Store:
    """线程安全的账号 / 设置存储，含轮询游标。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._accounts: dict[str, list[Account]] = {p: [] for p in PROVIDERS}
        self._settings: dict = {}
        self._rotation: dict[str, int] = {p: 0 for p in PROVIDERS}
        self._init_db()
        self._load()

    # ── SQLite 基础 ──────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self) -> None:
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
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
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('admin_key', ?)",
                (settings.DEFAULT_ADMIN_KEY,),
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('gateway_key', '')"
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('quota_refresh_interval', ?)",
                (str(settings.QUOTA_REFRESH_INTERVAL),),
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('account_concurrency', ?)",
                (str(settings.ACCOUNT_CONCURRENCY),),
            )
            conn.execute(
                f"INSERT OR IGNORE INTO {_META} (key, value) VALUES ('claim_round_interval', ?)",
                (str(settings.CLAIM_ROUND_INTERVAL),),
            )
            conn.commit()

    def _load(self) -> None:
        with closing(self._connect()) as conn:
            meta_rows = conn.execute(f"SELECT key, value FROM {_META}").fetchall()
            self._settings = {r["key"]: r["value"] for r in meta_rows}
            self._settings.setdefault("admin_key", settings.DEFAULT_ADMIN_KEY)
            self._settings.setdefault("gateway_key", "")
            self._settings.setdefault("bigmodel_channel_enabled",
                                      "1" if settings.BIGMODEL_CHANNEL_ENABLED else "0")
            self._settings.setdefault("quota_refresh_interval", str(settings.QUOTA_REFRESH_INTERVAL))
            self._settings.setdefault("account_concurrency", str(settings.ACCOUNT_CONCURRENCY))
            self._settings.setdefault("claim_round_interval", str(settings.CLAIM_ROUND_INTERVAL))
            self._settings.setdefault("claim_round_stagger", str(settings.CLAIM_ROUND_STAGGER))

            self._accounts = {p: [] for p in PROVIDERS}
            rows = conn.execute(
                f"SELECT data FROM {_TBL} ORDER BY created_at ASC"
            ).fetchall()
            for row in rows:
                try:
                    account = Account.from_dict(json.loads(row["data"]))
                except (json.JSONDecodeError, TypeError):
                    continue
                if account.provider in self._accounts:
                    self._accounts[account.provider].append(account)

            # 存量「风控升级禁用」一次性迁移为通道级有界冷却（修 P1/P4/P5）：
            # 这批号是被旧语义错定为终态的通道级封锁（#716：billing 全程正常、
            # 只有 grant 腿被拦），旧状态既等不到人工、又连带停掉自动领包轮与
            # 额度刷新。迁移后封锁记在 plan 通道上，到点自动回轮询。
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
                for account in migrated:
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
                if migrated:
                    conn.commit()
                    print(f"[risk-lane] 存量风控禁用已转通道级冷却 ×{len(migrated)}: "
                          f"{', '.join(a.name for a in migrated)}")

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
        """单账号并发上限（0 = 不限）。运行时可改（meta 表），改后即生效。"""
        try:
            return max(0, int(self.get_setting("account_concurrency", settings.ACCOUNT_CONCURRENCY)))
        except (TypeError, ValueError):
            return settings.ACCOUNT_CONCURRENCY

    def claim_round_interval(self) -> int:
        """套餐自动领取轮间隔（0 = 关闭）。运行时可改（meta 表），改后即生效。"""
        try:
            return max(0, int(self.get_setting("claim_round_interval", settings.CLAIM_ROUND_INTERVAL)))
        except (TypeError, ValueError):
            return settings.CLAIM_ROUND_INTERVAL

    def claim_round_stagger(self) -> int:
        """领取轮账号错峰上限秒数（0 = 关闭）。运行时可改（meta 表），下一轮生效。"""
        try:
            return max(0, int(self.get_setting("claim_round_stagger", settings.CLAIM_ROUND_STAGGER)))
        except (TypeError, ValueError):
            return settings.CLAIM_ROUND_STAGGER

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
            self._assign_fingerprint(account)  # 入池即分配独立设备指纹
            account.install_id = str(uuid.uuid4())  # 安装身份：稳定安装令牌
            self._accounts[provider].append(account)
            self._persist_account(account)
        return account

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

    # ── 轮询选择 ─────────────────────────────────────────────────────────────
    def select(self, provider: str, skip_ids: set[str] | None = None,
               model: str | None = None) -> Account | None:
        """按 round-robin 选择下一个可用账号。用完 / 失效 / 该模型无额度冷却中自动跳过。"""
        skip_ids = skip_ids or set()
        now = time.time()
        with self._lock:
            pool = [
                a for a in self._accounts.get(provider, [])
                if a.is_selectable(now, model=model) and a.id not in skip_ids
            ]
            if not pool:
                return None
            idx = self._rotation.get(provider, 0) % len(pool)
            account = pool[idx]
            self._rotation[provider] = (idx + 1) % len(pool)
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
