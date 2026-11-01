"""失败请求诊断留存（diaglog）—— 形状指纹与保留清理语义。

锁三件事：落盘含双侧现场（响应体截断 + 请求形状指纹，不含对话正文）、
按天文件与保留清理、query 的 status 过滤与最新在前排序。
"""

from __future__ import annotations

import json

import pytest

from app import diaglog


@pytest.fixture(autouse=True)
def _isolated_data(tmp_path, monkeypatch):
    monkeypatch.setattr(diaglog, "_data_dir", lambda: tmp_path)
    monkeypatch.setattr(diaglog, "_last_prune_day", "")
    return tmp_path


def _record(req_id="r1", status=405, text='{"code":3012}', model="glm-5.3-flash"):
    body = {
        "model": model,
        "max_tokens": 30,
        "stream": False,
        "system": [
            {"type": "text", "text": "prefix-35-chars"},
            {"type": "text", "text": "stable-" + "x" * 100, "cache_control": {"type": "ephemeral"}},
        ],
        "messages": [{"role": "user", "content": "hi"}],
    }
    diaglog.record_failure(req_id, account_name="zai-131", mode="jwt", model=model,
                           url="https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages",
                           status=status, text=text, request_body=body)
    return body


def _read_all(tmp_path):
    rows = []
    for f in sorted(tmp_path.glob("diaglog-*.jsonl")):
        for line in f.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def test_record_failure_shapes_both_sides(tmp_path):
    body = _record()
    rows = _read_all(tmp_path)
    assert len(rows) == 1
    r = rows[0]
    assert r["status"] == 405
    assert r["account"] == "zai-131"
    assert r["upstream_body"] == '{"code":3012}'
    req = r["request"]
    # 形状指纹：块长度 + 消息数 + sha256，绝不落对话正文
    assert req["system_block_lens"] == [15, 107]
    assert req["message_count"] == 1 and req["roles"] == ["user"]
    assert req["model"] == "glm-5.3-flash" and req["max_tokens"] == 30
    assert len(req["body_sha256"]) == 16
    assert "hi" not in json.dumps(r, ensure_ascii=False)


def test_response_body_truncated(tmp_path, monkeypatch):
    monkeypatch.setattr(diaglog, "_MAX_BODY", 10)
    _record(text="x" * 500)
    assert _read_all(tmp_path)[0]["upstream_body"] == "x" * 10


def test_query_filters_by_status_and_newest_first(tmp_path):
    _record(req_id="old", status=405, text="a")
    import time as _t
    _t.sleep(0.01)
    _record(req_id="new", status=502, text="b")
    rows = diaglog.query(days=3, limit=10)
    assert [r["req_id"] for r in rows] == ["new", "old"]
    rows405 = diaglog.query(days=3, limit=10, status=405)
    assert [r["req_id"] for r in rows405] == ["old"]


def test_prune_drops_old_days(tmp_path, monkeypatch):
    _record()
    import datetime
    from app.diaglog import _BJ, _PREFIX
    old_day = (datetime.datetime.now(_BJ) - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    old_file = tmp_path / f"{_PREFIX}{old_day}.jsonl"
    old_file.write_text('{"ts": 1}\n')
    monkeypatch.setattr(diaglog, "_last_prune_day", "")  # 强制触发清理
    _record(req_id="r2")
    assert not old_file.exists()
    assert len(_read_all(tmp_path)) == 2


def test_disabled_is_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(diaglog.settings, "DIAGLOG_ENABLED", False)
    _record()
    assert _read_all(tmp_path) == []


def test_malformed_body_never_raises(tmp_path):
    diaglog.record_failure("r9", account_name="x", mode="jwt", model="m",
                           url="", status=500, text="boom", request_body={"system": 12345})
    rows = _read_all(tmp_path)
    assert rows[0]["status"] == 500
    assert "system_block_lens" not in rows[0]["request"]
