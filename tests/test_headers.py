"""上游请求头/探测体测试：对齐 opencode 客户端的 free 档指纹。"""

import re

from app.core.headers import probe_payload, zen_headers

SESSION_RE = re.compile(r"^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$")
REQUEST_RE = re.compile(r"^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$")


def test_session_shape_matches_opencode_client():
    """session 必须是 canonical 布局，且低 12 位为降序 counter 1（ffe）。"""
    headers = zen_headers("conversation-seed")
    session = headers["x-opencode-session"]
    assert SESSION_RE.match(session), session
    assert session[4:16].endswith("ffe"), session


def test_session_is_stable_per_conversation():
    """同一会话种子 → 同一 session（上游 prompt cache 亲和）。"""
    a = zen_headers("same conversation")["x-opencode-session"]
    b = zen_headers("same conversation")["x-opencode-session"]
    c = zen_headers("another conversation")["x-opencode-session"]
    assert a == b
    assert a != c


def test_request_id_shape():
    """x-opencode-request 使用真实客户端的 msg_ + 时间戳 + Base62。"""
    request_id = zen_headers()["x-opencode-request"]
    assert REQUEST_RE.match(request_id), request_id


def test_headers_match_live_client_set():
    """free 档要求的关键头齐全，且不带客户端不会发的 x-api-key。"""
    headers = zen_headers()
    assert headers["Authorization"] == "Bearer public"
    assert headers["x-opencode-client"] == "cli"
    assert headers["x-opencode-project"] == "global"
    assert headers["User-Agent"].startswith("opencode/1.18.31 ")
    assert headers["x-opencode-session"] == headers["x-opencode-session-id"]
    assert "x-api-key" not in headers


def test_probe_payload_is_agent_shaped():
    """探测也必须 stream=true 且带 read 工具，否则上游一律 403。"""
    body = probe_payload("space-bunny-free")
    assert body["stream"] is True
    assert body["tools"][0]["function"]["name"] == "read"
    assert body["max_tokens"] == 1
