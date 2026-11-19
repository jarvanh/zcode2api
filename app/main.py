"""FastAPI 应用工厂 + 生命周期。"""

from __future__ import annotations

import asyncio
import ipaddress
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import constants, endpoint_routing, freshness, hostinfo, logs, plan_guard, reqlog, settings, vitals
from .captcha import captcha_manager
from .claim import claim_round
from .egress_pool import egress_pool  # 单例实例（module.start 不存在——start 是实例方法）
from .quota import model_probe, monitor, risk_lane_probe
from .routes import admin_api, gateway, pages
from .store import store
from .version_tracker import version_tracker


class RevalidatingStaticFiles(StaticFiles):
    """StaticFiles with `Cache-Control: no-cache` — etag revalidation on every
    request so deploys are visible immediately (CF/browser never serve stale)."""

    def file_response(self, *args, **kwargs):
        resp = super().file_response(*args, **kwargs)
        resp.headers["Cache-Control"] = "no-cache"
        return resp


# 修正 Windows 中文控制台可能出现的乱码
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass


def _warn_version_drift() -> None:
    """钉死的 billing 版本 vs 本机官方客户端真实版 → 落伍就告警。

    hub 跟 zcode-switch 不同（后者 Windows 读注册表自动跟随本机真实版），
    只能钉常量。上游随时可按客户端版本拉高投放门槛，而 preview 空窗在日志里
    只表现为「无投放套餐」——没这道告警就得人工重新怀疑一轮（2026-10-31 走过）。
    本机无官方客户端（检测不到）时静默跳过。
    """
    local = hostinfo.local_client_version()
    if not local:
        return
    if local != constants.BILLING_APP_VERSION:
        logs.warn(
            "version",
            f"钉定的客户端版本 {constants.BILLING_APP_VERSION} 落后于本机官方 "
            f"{local}：改 app/constants.py 的 BILLING_APP_VERSION/CLIENT_APP_VERSION",
        )
    else:
        logs.info("version", f"客户端版本对齐官方 {local}")


def _display_host() -> str:
    # 0.0.0.0 / 空地址在浏览器中不可直接访问，展示为 127.0.0.1
    host = (settings.HOST or "").strip()
    return "127.0.0.1" if host in ("", "0.0.0.0", "::") else host


def _is_loopback_host(host: str) -> bool:
    host = (host or "").strip()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_security_defaults() -> None:
    """安全默认：未配置网关 Key 时不允许监听非回环地址。"""
    from .store import store

    if store.gateway_key():
        return
    if not _is_loopback_host(settings.HOST):
        raise RuntimeError(
            "网关 API Key 未配置（ZCODE_GATEWAY_KEY / 后台设置），"
            f"但 ZCODE_HOST={settings.HOST!r} 不是回环地址：拒绝启动。"
            "请设置网关 Key，或将 ZCODE_HOST 改为 127.0.0.1。"
        )


def _backfill_fingerprints() -> list:
    """启动时给无指纹或非桌面 SKU（旧版宿主机克隆）的存量账号换发生成档案。

    换机后清 installed_at，由 lifespan 补跑按账号安装序（新 device_mid 需要
    自己的 app_launch / app_daily_active）。返回被替换的账号列表。
    """
    from .fingerprint import DeviceProfile, is_generated_sku, profile_for
    from .store import store

    replaced = []
    for account in store.list_accounts():
        fp = account.fingerprint
        needs = False
        if not isinstance(fp, dict) or not fp.get("device_mid"):
            needs = True
        else:
            try:
                profile = DeviceProfile(
                    platform=fp["platform"], arch=fp["arch"], os_version=fp["os_version"],
                    language=fp["language"], timezone=fp["timezone"], screen=fp["screen"],
                    device_mid=fp["device_mid"],
                )
            except (KeyError, TypeError, ValueError):
                needs = True
            else:
                needs = not is_generated_sku(profile)
        if needs:
            account.fingerprint = None
            account.installed_at = None
            profile_for(account)
            store._assign_fingerprint(account)
            store.update_account(account)
            replaced.append(account)
    return replaced


def _backfill_install_ids() -> int:
    """启动时给无 install_id 的存量账号补配安装身份（纯本地，无网络）并落库。"""
    import uuid as _uuid

    from .store import store

    backfilled = 0
    for account in store.list_accounts():
        if not (account.install_id or "").strip():
            account.install_id = str(_uuid.uuid4())
            store.update_account(account)
            backfilled += 1
    return backfilled


# 启动安装序的后台任务引用：事件循环对 task 只持弱引用（asyncio 官方文档），
# 不保存引用任务可能被 GC 中途丢弃且无日志 —— 与 captcha._refill_task 同一模式
_install_task: asyncio.Task | None = None


def _startup_precheck() -> None:
    """发版偏斜自检（2.7.68）：发版 = scp 真身，偏斜窗口内「新 main.py + 旧
    egress_pool.py」曾以运行期 AttributeError 崩溃（err.log 实锤，supervisor
    autorestart 拉起后自愈）——把这类不匹配从运行期 AttributeError 提前为
    启动期一条明确版本配对错误，fail-fast 带完整上下文。"""
    missing = [name for name, obj, *attrs in (
        ("egress_pool", egress_pool, "start", "stop"),
        ("captcha_manager", captcha_manager, "start", "close"),
        ("claim_round", claim_round, "start", "stop"),
        ("version_tracker", version_tracker, "start", "stop"),
        ("monitor", monitor, "start", "stop"),
    ) if not all(hasattr(obj, a) for a in attrs)]
    if missing:
        raise RuntimeError(
            f"发版偏斜：缺少 {', '.join(missing)} 的生命周期方法 —— app/ 真身不完整"
            f"或版本不配对，请重新完整发版（不要只 scp 单文件）")


def _start_component(name: str, fn) -> None:
    """单组件启动隔离（2.7.68）：任一后台组件 start 抛异常只降级该组件并留痕，
    不再放大成整个 lifespan 失败 → 进程崩溃循环 + HA 看门狗连环重启（在途
    请求全砍）。"""
    try:
        fn()
    except Exception as err:  # noqa: BLE001 - 单组件失败不拖垮启动
        logs.err("lifespan",
                 f"{name} 启动失败（降级运行，其余组件不受影响）: "
                 f"{type(err).__name__}: {err}")


def _run_install_sequence_on_start() -> None:
    """启动后台执行一次安装序（官方客户端每次启动都拉 configs + 发 app_launch，
    日活去重在上游按 device_mid+日期）。失败只留痕，绝不影响启动。"""
    global _install_task

    from . import install

    async def _run() -> None:
        try:
            await install.run_install_sequence()
        except Exception as err:  # noqa: BLE001 —— 后台任务异常无人接收，必须自兜
            logs.err("install", f"安装序意外异常: {err}")

    _install_task = asyncio.create_task(_run())


@asynccontextmanager
async def lifespan(app: FastAPI):
    _startup_precheck()
    if settings.REQLOG_PERSIST:
        restored = reqlog.restore()
        if restored:
            logs.ok("reqlog", f"请求监控日志恢复 ×{restored}（持久化开启）")
    replaced = _backfill_fingerprints()
    if replaced:
        logs.ok("fingerprint", f"存量账号补配独立设备指纹 ×{len(replaced)}")
        for acc in replaced:
            admin_api._schedule_install(acc)
    installed = _backfill_install_ids()
    if installed:
        logs.ok("install", f"存量账号补配安装身份 ×{installed}")
    plan_guard.warm()  # 冷却表回读+过期行清理前移到启动，调度路径不承担磁盘 I/O（上游 2.7.68）
    # env 钉死告警（2.7.68 review P3）：admin_key 首启写库后 env 轮换静默失效
    # ——「以为换了锁其实没换」。启动时对账一条 warn。
    try:
        if os.getenv("ZCODE_ADMIN_KEY") and store.admin_key() != settings.DEFAULT_ADMIN_KEY:
            logs.warn("lifespan",
                      "env ZCODE_ADMIN_KEY 与数据库 admin_key 不一致，以数据库为准"
                      "（env 仅首次启动写库生效）；如需轮换请在后台设置页修改")
    except Exception:  # noqa: BLE001 - 纯观测告警，不阻断启动
        pass
    _start_component("quota monitor", monitor.start)
    _start_component("freshness", freshness.start)   # system sidecar 文本新鲜度巡检（fork）
    _start_component("model probe", model_probe.start)          # 模型冷却探测轮（fork）
    _start_component("risk lane probe", risk_lane_probe.start)  # plan-lane 风控探测轮（fork）
    _start_component("captcha pool", captcha_manager.start)
    _start_component("claim round", claim_round.start)
    _start_component("endpoint routing", endpoint_routing.start)  # 端点映射跟随轮（fork）
    _start_component("version tracker", version_tracker.init_from_store)
    _start_component("version tracker", version_tracker.start)
    _start_component("egress pool", egress_pool.start)
    restored = reqlog.restore_recent_from_disk()  # 实时层回放：重启后仍可见重启前请求（fork）
    conc_task = reqlog.start_sampler()   # 并发仪表心跳（fork）
    _start_component("vitals", vitals.start)  # 进程体征心跳（fork）

    _run_install_sequence_on_start()
    _warn_version_drift()
    base = f"http://{_display_host()}:{settings.PORT}"
    banner_lines = [
        f"{logs._B}{logs._MAG}zcode-hub{logs._R} {logs._DIM}v{settings.APP_VERSION} · Python{logs._R}",
        f"{logs._DIM}后台管理{logs._R}  {logs._C}{base}/admin/login{logs._R}",
        f"{logs._DIM}对话端点{logs._R}  {logs._C}{base}/v1/messages{logs._R}",
    ]
    if restored:
        banner_lines.append(
            f"{logs._DIM}实时观测回放{logs._R}  {restored} 条重启前请求（磁盘终态）")
    logs.banner(banner_lines)
    try:
        yield
    finally:
        # 停机逐段隔离（上游 2.7.68）：首段抛异常会跳过其余收口（在途请求证据
        # 丢失、后台任务逃逸）——每段独立 try/except，停机路径绝不中断。
        if conc_task is not None:
            conc_task.cancel()
        try:
            await vitals.stop()
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logs.err("lifespan", f"vitals 停机收口异常（已跳过）: {type(err).__name__}: {err}")
        reqlog.flush_conc()   # 关机前把当日未满分钟落盘，曲线不断尾（fork）
        for name, fn in (
            ("reqlog", lambda: reqlog.shutdown_flush()),
            ("opslog", lambda: logs.opslog_close()),
            ("monitor", monitor.stop),
            ("freshness", freshness.stop),
            ("model_probe", model_probe.stop),
            ("risk_lane_probe", risk_lane_probe.stop),
            ("claim_round", claim_round.stop),
            ("endpoint_routing", endpoint_routing.stop),
            ("version_tracker", version_tracker.stop),
            ("egress_pool", egress_pool.stop),
            ("captcha_manager", captcha_manager.close),
        ):
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    await result
                if name == "reqlog" and result:
                    logs.info("reqlog", f"停机收口在途请求 ×{result}")
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - 停机收口绝不互相牵连
                logs.err("lifespan", f"{name} 停机收口异常（已跳过）: "
                                     f"{type(err).__name__}: {err}")



def create_app() -> FastAPI:
    _validate_security_defaults()
    app = FastAPI(title="zcode-hub", version=settings.APP_VERSION, lifespan=lifespan)

    # 进程级在途计数（看板「实时/并发」小卡真源）。
    # 只统计业务请求路径，观测端点(/meta 等)不计入，避免看板轮询自抬曲线。
    from app import inflight_metrics
    app.add_middleware(inflight_metrics.InflightMiddleware)

    # 前后端分离（2.7.34）：前端默认同源 /static 服务，设置 ZCODE_CORS_ORIGINS
    # 后即可部署到任意源跨域调用本 API（Bearer 鉴权，无 cookie，CSRF 面为零）。
    origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
    if origins:
        from fastapi.middleware.cors import CORSMiddleware

        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
            max_age=86400,
        )

    app.include_router(pages.router)
    app.include_router(admin_api.router)
    app.include_router(gateway.router)
    app.mount("/static", RevalidatingStaticFiles(directory=str(settings.FRONTEND_DIR)), name="static")
    # 前端相对资产路径（js/... css/...）在 /admin/* 页面形态下的服务点；路由
    # （/admin/api/*、/admin/login 等）注册在前，优先级高于本挂载。
    # JS/CSS 等静态资产必须 no-cache（发版后 CF/浏览器立即 revalidate 新内容）：
    # StaticFiles 默认无 Cache-Control 头，CF heuristic 缓存会把旧 JS 边缘缓存数小时。
    app.mount("/admin", RevalidatingStaticFiles(directory=str(settings.FRONTEND_DIR / "admin"), html=True),
              name="admin-static")
    return app


app = create_app()
