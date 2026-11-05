"""IP 级拦截页判别与静默（2026-11-05，对齐 magpie zcode 0.1.43 / plugins#649）。

两类 405 必须分开：
  - HTML 拦截页（errors.aliyun.com / <html）→ 出口 IP 被 WAF 拦 → 进程级静默
  - JSON {"code":3012} → 账号风控 → lane 冷却（本文件不覆盖，见 risk_cooldown）
"""

from __future__ import annotations

from app import edge, settings


class TestEdgePageDetection:
    def test_aliyun_error_page_is_edge(self):
        assert edge.is_page(
            '<!doctypehtml><html><title>405</title>'
            '<div>blocked. errors.aliyun.com</div></html>'
        )

    def test_bare_html_is_edge(self):
        assert edge.is_page("<html><body>405</body></html>")

    def test_3012_json_is_not_edge(self):
        assert not edge.is_page('{"code":3012,"msg":"request has been blocked due to unusual activity."}')

    def test_plain_text_is_not_edge(self):
        assert not edge.is_page("request has been blocked due to unusual activity.")
        assert not edge.is_page("")
        assert not edge.is_page(None)


class TestEdgeHold:
    def setup_method(self):
        edge.reset()

    def teardown_method(self):
        edge.reset()

    def test_arm_holds_provider(self):
        until = edge.arm("zai", seconds=180)
        assert edge.held("zai")
        assert edge.until("zai") == until
        assert 1 <= edge.remaining("zai") <= 180

    def test_hold_is_per_provider(self):
        edge.arm("zai", seconds=60)
        assert edge.held("zai")
        assert not edge.held("bigmodel")

    def test_arm_extends_but_never_shortens(self):
        first = edge.arm("zai", seconds=60)
        second = edge.arm("zai", seconds=300)
        assert second > first
        # 更短的 hold 不会把截止时刻提前
        third = edge.arm("zai", seconds=1)
        assert third == second

    def test_hold_expires(self):
        edge.arm("zai", seconds=0)
        assert not edge.held("zai")
        assert edge.until("zai") is None
        assert edge.remaining("zai") == 0

    def test_reset_clears_all(self):
        edge.arm("zai", seconds=60)
        edge.reset()
        assert not edge.held("zai")

    def test_default_hold_matches_magpie(self):
        assert settings.EDGE_HOLD_SECONDS == 180
