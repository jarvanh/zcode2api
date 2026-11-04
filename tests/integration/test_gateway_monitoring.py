"""GW-013 请求监控（reqlog + /admin/api/monitoring）集成测试。"""

from __future__ import annotations

import asyncio

import pytest

_GOOD_JWT = "hM.eyJzdWIiOiJtIn0.sig"
_STREAM_JWT = "hS.eyJzdWIiOiJzIn0.sig"      # 流式测试独立 JWT（mock 场景序列按凭证前缀绑定）
_OAI_STREAM_JWT = "hT.eyJzdWIiOiJ0In0.sig"
_BG_JWT = "hB.eyJzdWIiOiJiIn0.sig"          # 后台任务测试独立 JWT（mock 序列按凭证共享，防串场）

ADMIN_AUTH = {"Authorization": "Bearer zcode"}  # 默认后台密钥


@pytest.fixture(autouse=True)
def _clean_reqlog():
    from app import reqlog

    reqlog._reset_for_tests()
    yield
    reqlog._reset_for_tests()


@pytest.mark.integration
class TestMessagesBodyValidation:
    async def test_non_object_body_returns_400(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="body-arr")
        res = await client.post("/v1/messages", json=["not", "an", "object"])
        assert res.status_code == 400
        data = res.json()
        assert data["error"]["type"] in ("invalid_request", "invalid_request_error")


@pytest.mark.integration
class TestMonitoringRecording:
    async def test_messages_success_recorded(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="mon-1")
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "max_tokens": 64,
            "messages": [{"role": "user", "content": "你好世界"}],
        })
        assert res.status_code == 200

        res = await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)
        entries = res.json()["entries"]
        assert len(entries) == 1
        e = entries[0]
        assert e["endpoint"] == "messages"
        assert e["model"] == "GLM-5.3-Flash"
        assert e["account"] == "mon-1"
        assert e["ok"] is True and e["status"] == 200
        assert e["t_first"] is not None and e["t_total"] is not None
        assert "你好世界" in e["preview"]

    async def test_chat_completions_tokens_recorded(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="mon-2")
        res = await client.post("/v1/chat/completions", json={
            "model": "glm-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200

        res = await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)
        e = res.json()["entries"][0]
        assert e["endpoint"] == "chat"
        assert e["ok"] is True
        assert e["input_tokens"] == 10 and e["output_tokens"] == 5  # mock 固定 usage

    async def test_no_account_error_recorded(self, fresh_app):
        from httpx import ASGITransport, AsyncClient

        from app.main import create_app

        app = create_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post("/v1/messages", json={
                "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
            })
            assert res.status_code == 503
            res = await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)
        e = res.json()["entries"][0]
        assert e["ok"] is False and e["status"] == 503
        assert "无可用账号" in e["error"]

    async def test_upstream_4xx_recorded(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="mon-3")
        mock.state.sequences[_GOOD_JWT[:16]] = ["not_found"]
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 404
        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is False and e["status"] == 404

    async def test_unexpected_dispatch_error_closes_entry(self, gateway_client, fresh_app, monkeypatch):
        """调度层抛非取消异常也必须收口监控条目（2026-09 review P3：
        此前只有 CancelledError 路径调 finish_error，条目会永久滞留「进行中」）。"""
        client, _ = gateway_client
        from app import reqlog
        from app.routes import gateway as gateway_module
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="mon-err")
        async def _boom(*args, **kwargs):
            raise RuntimeError("调度意外爆炸")
        monkeypatch.setattr(gateway_module, "_dispatch", _boom)

        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 500
        entries = reqlog.snapshot()
        assert len(entries) == 1
        e = entries[0]
        assert e["ok"] is False and e["status"] == 500
        assert "调度意外爆炸" in e["error"]

    async def test_safe_refresh_task_holds_strong_ref(self, gateway_client, fresh_app, monkeypatch):
        """成功请求触发的 _safe_refresh 必须被强引用持有（事件循环只持弱引用，
        裸 create_task 会被 GC 静默丢弃 —— 同 main.py 启动安装序已修过的缺陷）。"""
        client, _ = gateway_client
        from app.routes import gateway as gateway_module
        from tests.conftest import seed_account

        seed_account(fresh_app, _BG_JWT, name="mon-bg")
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_refresh(account):
            started.set()
            await release.wait()

        monkeypatch.setattr(gateway_module, "_safe_refresh", _slow_refresh)
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200
        # 直通路径 create_task 后立即返回响应；任务需让出一拍才真正起跑
        await asyncio.sleep(0.01)
        assert started.is_set()
        # 任务仍在飞行中：必须被模块级强引用持有
        assert len(gateway_module._bg_tasks) >= 1
        release.set()
        await asyncio.sleep(0)

    async def test_messages_stream_success_recorded(self, gateway_client, fresh_app):
        """流式透传路径流结束后也要闭环（finish_ok），tokens 未知记 None。"""
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _STREAM_JWT, name="mon-5")
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "stream": True, "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200
        assert "message_stop" in res.text  # 流已完整消费

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is True and e["stream"] is True
        assert e["t_total"] is not None and e["t_first"] is not None
        assert e["input_tokens"] is None  # 透传不解析 SSE，未知 ≠ 0

    async def test_openai_stream_tokens_recorded(self, gateway_client, fresh_app):
        """OpenAI 流式转换器带 usage（message_delta.output_tokens）→ 记录 completion tokens。"""
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _OAI_STREAM_JWT, name="mon-6")
        res = await client.post("/v1/chat/completions", json={
            "model": "glm-5.3-flash", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200
        assert res.text.rstrip().endswith("data: [DONE]")

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is True and e["endpoint"] == "chat"
        assert e["output_tokens"] == 5  # mock message_delta 固定 output_tokens=5
        assert e["input_tokens"] is None  # mock 无 message_start usage → 未知

    async def test_clear_endpoint(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _GOOD_JWT, name="mon-4")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        res = await client.post("/admin/api/monitoring/clear", headers=ADMIN_AUTH)
        assert res.status_code == 200
        res = await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)
        assert res.json()["entries"] == []

    async def test_monitoring_page_served(self, fresh_app):
        from httpx import ASGITransport, AsyncClient

        from app.main import create_app

        app = create_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get("/admin/monitoring")
        assert res.status_code == 200
        assert "请求监控" in res.text


_E1_JWT = "hE.eyJzdWIiOiJlMSJ9.sig"     # 增补维度用例独立凭证（避免与他案共享 mock 绑定）
_E2_JWT = "hF.eyJzdWIiOiJlMiJ9.sig"
_E3_JWT = "hG.eyJzdWIiOiJlMyJ9.sig"


@pytest.mark.integration
class TestMonitoringEnrichedFields:
    """2026-11-04 增补维度端到端：来源、通道/出口、尝试梯子、等待与错误分类。"""

    @pytest.fixture(autouse=True)
    def _clean_mock(self, gateway_client):
        """Mock 是 session 级：sequences/counters 会跨用例泄漏（本文件前面的
        not_found 场景会把后面的成功用例打成 404），每用例先清场。"""
        _client, mock = gateway_client
        mock.state.calls.clear()
        mock.state.counters.clear()
        mock.state.sequences.clear()
        return mock

    async def test_source_metadata_recorded(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-src")
        res = await client.post(
            "/v1/messages",
            json={"model": "GLM-5.3-Flash", "max_tokens": 128,
                  "messages": [{"role": "user", "content": "hi"}] * 3},
            headers={"CF-Connecting-IP": "203.0.113.9", "User-Agent": "claude-cli/2.0",
                     "X-Probe": "1"},
        )
        assert res.status_code == 200

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["client_ip"] == "203.0.113.9"
        assert e["ua"] == "claude-cli/2.0"
        assert e["path"] == "/v1/messages"
        assert e["probe"] is True          # X-Probe 落进监控行
        assert e["msgs"] == 3              # 请求形状（排障「超长上下文」用）
        assert e["max_tokens"] == 128

    async def test_channel_and_egress_recorded(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E2_JWT, name="mon-lane")
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        # Mock 上游挂在 plan 路径上 → 通道判定必须是 plan（不是靠参数猜的 apikey）
        assert e["channel"] == "plan"
        assert e["egress"] in ("direct", "proxy", "account")
        assert e["up"] is True

    async def test_attempt_ladder_across_accounts(self, gateway_client, fresh_app, monkeypatch):
        """全池被 429（换号优先）：梯子必须记下每个账号吃了什么码。

        这一条钉住「一次请求只留最终账号、重试过程不可见」的旧缺口：监控行
        要能回答「先试了谁、为何换号」，否则 429 波次只能翻文本日志碰运气。
        """
        client, mock = gateway_client
        from app import settings
        from app.routes import gateway as gw
        from tests.conftest import seed_account

        # 不排队、不原地等：结果只取决于「换号优先」路径，用例秒级结束
        monkeypatch.setattr(settings, "RETRY_429_WAIT", 0)
        monkeypatch.setattr(settings, "QUEUE_WAIT", 0)
        monkeypatch.setattr(settings, "QUEUE_WAIT_SYNC", 0)
        monkeypatch.setattr(gw, "_parse_retry_after", lambda _: None)

        seed_account(fresh_app, _E1_JWT, name="mono-a")
        seed_account(fresh_app, _E2_JWT, name="mono-b")
        # 两个凭证都绑 429：不依赖选号顺序，结果确定性
        mock.state.sequences[_E1_JWT[:16]] = ["rate_limited"]
        mock.state.sequences[_E2_JWT[:16]] = ["rate_limited"]
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 503  # 全池被限流且不排队 → 快速失败

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is False and e["status"] == 503
        att = e["att"]
        assert len(att) == 2
        assert {a["st"] for a in att} == {429}
        assert {a["acc"] for a in att} == {"mono-a", "mono-b"}
        assert all(a["ch"] == "plan" for a in att)
        assert e["retry_429"] == 2
        assert e["wait_rl"] == 0  # 换号优先 = 没有静默等待
        assert e["up"] is True

    async def test_upstream_error_classification(self, gateway_client, fresh_app):
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E3_JWT, name="mon-err404")
        mock.state.sequences[_E3_JWT[:16]] = ["not_found"]
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 404

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is False and e["status"] == 404
        assert e["err_kind"] == "upstream"

    async def test_business_code_classification(self, gateway_client, fresh_app):
        """上游 200 装业务错误（3006）：监控行要带 code 与 biz 分类。"""
        client, mock = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-biz")
        mock.state.sequences[_E1_JWT[:16]] = ["business_error_200_unknown"]
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code >= 400

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is False
        assert e["err_kind"] == "biz"
        assert e["err_code"] == 3006
        assert e["upstream_id"] == ""  # mock 不发 cf-ray：空值不得变成 None/缺失字段

    async def test_stop_reason_recorded(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E2_JWT, name="mon-stop")
        res = await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        assert res.status_code == 200

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        # mock 的同步 message 带 stop_reason=end_of_message
        assert e["stop_reason"]

    async def test_history_endpoint_exposes_new_groups(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-hist")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        data = (await client.get("/admin/api/monitoring/history?days=1",
                                 headers=ADMIN_AUTH)).json()
        for key in ("channels", "clients", "totals"):
            assert key in data
        assert data["totals"]["calls"] >= 1
        assert any(c["name"] == "plan" for c in data["channels"])


@pytest.mark.integration
class TestConcurrencyPanel:
    """实时并发四口径：API 形状 + 事件驱动峰值 + 收口修复不污染在途读数。"""

    @pytest.fixture(autouse=True)
    def _reset_slot_source(self):
        """槽位源是模块级注入点：用例间必须复位，否则假数据源泄漏到其他用例。"""
        from app import reqlog

        reqlog.register_slot_source(None)
        yield
        reqlog.register_slot_source(None)

    @pytest.fixture(autouse=True)
    def _clean_mock(self, gateway_client):
        """Mock 是 session 级：前一用例留下的场景队列会把本用例的上游响应钉死，
        并发用例虽然不断言状态码，但序列泄漏会让耗时/槽位形态失真。"""
        _client, mock = gateway_client
        mock.state.calls.clear()
        mock.state.counters.clear()
        mock.state.sequences.clear()
        return mock

    async def test_conc_api_shape(self, gateway_client):
        client, _ = gateway_client
        data = (await client.get("/admin/api/monitoring/concurrency", headers=ADMIN_AUTH)).json()
        assert {"now", "peak", "tick", "minutes"} <= set(data)
        assert {"client", "slot", "queue", "limit", "per"} <= set(data["now"])
        assert {"client", "slot", "queue", "at", "kind"} <= set(data["peak"])

    async def test_conc_history_api_shape(self, gateway_client):
        client, _ = gateway_client
        data = (await client.get("/admin/api/monitoring/concurrency/history?days=1", headers=ADMIN_AUTH)).json()
        assert {"days", "daily", "peak", "minutes"} <= set(data)

    async def test_client_gauge_returns_to_zero_after_traffic(self, gateway_client, fresh_app):
        """请求收口后在途读数必须归零（begin +1 / finish −1 成对）。

        这一条同时是 dedup 重放不收口 bug 的哨兵：那类条目永不收口，
        client 口径会一路虚高。
        """
        from app import reqlog

        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-conc")
        reqlog.register_slot_source(lambda: {"total": 1, "per": {"mon-conc": 1}, "limit": 2})
        reqlog.sample()
        # 基线：同进程其他用例可能留下真正未收口的条目，只断言「回到基线」
        # 而非「等于 0」——绝对断言在生产语义下没意义，但漏收口一定会抬高读数。
        base = reqlog.conc_snapshot()["now"]["client"]
        for _ in range(3):
            await client.post("/v1/messages", json={
                "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
            })
        reqlog.sample()
        snap = reqlog.conc_snapshot()
        assert snap["now"]["client"] == base, "在途读数未回到基线：有请求漏收口"
        assert snap["peak"]["client"] > base   # 峰值保留（事件驱动精确采样）
        assert snap["now"]["slot"] == 1 and snap["now"]["per"] == {"mon-conc": 1}
        assert snap["minutes"], "分钟曲线未累积"

    async def test_broken_slot_source_does_not_break_gauge(self, gateway_client):
        """坏数据源（抛异常）只让 slot 口径归零，心跳与其余三个口径照旧。"""
        from app import reqlog

        def _boom():
            raise RuntimeError("store 炸了")
        reqlog.register_slot_source(_boom)
        try:
            reqlog.sample()  # 不得抛出
            snap = reqlog.conc_snapshot()
            assert snap["now"]["slot"] == 0
            assert {"client", "queue"} <= set(snap["now"])
        finally:
            reqlog.register_slot_source(None)


@pytest.mark.integration
class TestDedupReplayClosesEntry:
    """风暴去重重放必须收口监控条目（2026-11-04 修复的 bug）。

    旧行为：_dispatch_guarded 的两个重放分支直接 return Response，不经过
    _dispatch，也就从不调 finish_* —— 条目永久滞留「进行中」，既让 _inflight
    无界增长，又把实时并发的 client 口径刷成虚高。
    """

    async def test_failure_replay_closed_with_flag(self, gateway_client, fresh_app, monkeypatch):
        import time

        client, mock = gateway_client
        mock.state.sequences.clear()
        from app.routes import gateway as gw
        from tests.conftest import seed_account

        seed_account(fresh_app, _E3_JWT, name="mon-dd")
        mock.state.sequences[_E3_JWT[:16]] = ["server_error"]
        body = {"model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "dedup-x"}]}

        first = await client.post("/v1/messages", json=body)
        assert first.status_code >= 400
        # 重放只对「波次暂停仍在」生效：注入暂停窗口，模拟同内容风暴
        monkeypatch.setitem(gw._risk_wave_until, "zai", time.time() + 60)
        second = await client.post("/v1/messages", json=body)
        assert second.status_code >= 400

        e = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()["entries"][0]
        assert e["ok"] is False, "重放条目未收口，会永久滞留进行中"
        assert e["dedup"] == "replayed" and e["err_kind"] == "dedup"
        # 实时并发读数不得被滞留条目污染
        conc = (await client.get("/admin/api/monitoring/concurrency", headers=ADMIN_AUTH)).json()
        assert conc["now"]["client"] == 0


@pytest.mark.integration
class TestHealthAndVitalsEndpoints:
    """号池健康 / 进程体征 / 值守数据源三个端点的契约。"""

    async def test_monitoring_carries_health(self, gateway_client, fresh_app):
        """主刷新端点内含健康窗口：前端不必为 P95 再开一个定时器。"""
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-mh")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        data = (await client.get("/admin/api/monitoring", headers=ADMIN_AUTH)).json()
        assert "health" in data and "conc" in data
        assert any(a["account"] == "mon-mh" for a in data["health"]["accounts"])

    async def test_conc_packet_carries_vitals_and_accounts(self, gateway_client, fresh_app):
        """1s 心跳一次往返拿齐并发+体征+账号徽标（同拍快照，交叉判读不错位）。"""
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E2_JWT, name="mon-vt")
        data = (await client.get("/admin/api/monitoring/concurrency",
                                 headers=ADMIN_AUTH)).json()
        assert {"now", "peak", "minutes", "vitals", "accounts"} <= set(data)
        for key in ("rss_mb", "solvers", "zombies", "browser_procs", "pool", "loop_ms"):
            assert key in data["vitals"]["now"]
        assert any(a["name"] == "mon-vt" for a in data["accounts"])

    async def test_health_endpoint_shape(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E1_JWT, name="mon-h")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        data = (await client.get("/admin/api/monitoring/health", headers=ADMIN_AUTH)).json()
        assert {"hours", "since", "accounts", "events"} <= set(data)
        row = next(a for a in data["accounts"] if a["account"] == "mon-h")
        assert row["calls"] >= 1 and row["ok"] >= 1
        assert row["ttfb_p95"] is not None

    async def test_health_hours_clamped(self, gateway_client):
        client, _ = gateway_client
        data = (await client.get("/admin/api/monitoring/health?hours=9999",
                                 headers=ADMIN_AUTH)).json()
        assert data["hours"] == 24.0

    async def test_vitals_endpoint_shape(self, gateway_client):
        client, _ = gateway_client
        from app import vitals

        vitals.sample(loop_lag_ms=1.0)
        data = (await client.get("/admin/api/monitoring/vitals", headers=ADMIN_AUTH)).json()
        for key in ("now", "series", "tick", "linux", "pid"):
            assert key in data
        for key in ("rss_mb", "solvers", "zombies", "browser_procs", "pool", "loop_ms"):
            assert key in data["now"]

    async def test_watch_endpoint_is_single_source_for_dog(self, gateway_client, fresh_app):
        """值守脚本靠这一个端点拿齐判定材料（含账号 strikes 与状态字符串）。"""
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E2_JWT, name="mon-watch")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        })
        data = (await client.get("/admin/api/monitoring/watch", headers=ADMIN_AUTH)).json()
        assert {"conc", "recent", "vitals", "health", "accounts"} <= set(data)
        acc = next(a for a in data["accounts"] if a["name"] == "mon-watch")
        # Status 是字符串常量类：必须是纯字符串（曾因 .value 让端点 500）
        assert isinstance(acc["status"], str)
        assert {"risk_strikes", "rate_strikes", "net_strikes", "enabled"} <= set(acc)
        assert "total" in data["recent"]

    async def test_history_exposes_trend_fields(self, gateway_client, fresh_app):
        client, _ = gateway_client
        from tests.conftest import seed_account

        seed_account(fresh_app, _E3_JWT, name="mon-trend")
        await client.post("/v1/messages", json={
            "model": "GLM-5.3-Flash", "messages": [{"role": "user", "content": "hi"}],
        }, headers={"CF-Connecting-IP": "5.6.7.8"})
        t = (await client.get("/admin/api/monitoring/history?days=1",
                             headers=ADMIN_AUTH)).json()["totals"]
        for key in ("cache", "cache_rate", "clients_n", "sess_n"):
            assert key in t
