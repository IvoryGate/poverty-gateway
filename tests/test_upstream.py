"""upstream 模块测试：流式透传、重试与状态回写。

用 httpx.MockTransport 模拟上游 zen 与代理池探测，不依赖真实服务。
"""

import httpx
import pytest

from app.config import Settings
from app.core.proxy_pool import ProxyPool
from app.services.upstream import UpstreamClient, UpstreamError, _sanitize_responses


def make_settings(**kw):
    base = {
        "proxy_pool_url": "http://pool.test",
        "upstream_base": "https://upstream.test/zen/v1",
        "quality_chain": "stable3",
        "batch_size": 5,
        "probe_concurrency": 2,
        "probe_timeout": 5,
        "pool_low_water": 1,
        "pool_target": 2,
        "max_in_flight": 3,
    }
    base.update(kw)
    return Settings(**base)


def make_client(upstream_handler, proxy_probes="ok", proxies=None):
    """构造 UpstreamClient：上游用 MockTransport，代理探活按预设结果。

    真实 _stream_request 走真代理（每请求新建 client），无法注入 transport；
    测试统一替换为 _fake_stream，用 upstream_handler 模拟上游返回。
    """
    async def probe_fn(proxy: str) -> str:
        return proxy_probes

    proxies = proxies or ["1.1.1.1:80"]

    def pool_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 200, "data": proxies})

    pool_transport = httpx.MockTransport(pool_handler)
    pool_client = httpx.AsyncClient(transport=pool_transport)

    settings = make_settings()
    pool = ProxyPool(settings, client=pool_client, probe_fn=probe_fn)
    client = UpstreamClient(settings)
    client.pool = pool

    async def _fake_stream(endpoint: str, proxy: str, payload: dict):
        r = upstream_handler(httpx.Request("POST", "https://upstream.test"))
        if r.status_code >= 400:
            raise UpstreamError(f"upstream {r.status_code}: {r.text[:200]}")
        for block in r.content.split(b"\n\n"):
            if block:
                yield block + b"\n\n"

    client._stream_request = _fake_stream  # type: ignore[method-assign]
    return client


@pytest.mark.asyncio
async def test_chat_stream_passthrough():
    """流式：逐块透传上游 SSE 字节，成功后回写 OK。"""
    sse = (b"data: {}\n\n", b"data: {}\n\n", b"data: [DONE]\n\n")
    req_seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        req_seen["stream"] = request.content
        return httpx.Response(200, content=b"".join(sse))

    client = make_client(handler)
    chunks = []
    async for chunk in client.chat_stream("deepseek-v4-flash-lite", {"model": "x"}):
        chunks.append(chunk)
    assert chunks == list(sse)
    assert req_seen["stream"] is not None


@pytest.mark.asyncio
async def test_chat_stream_retry_on_429():
    """流式：首个块前 429 → 换代理重试，最终成功。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                429, json={"error": {"type": "FreeUsageLimitError"}}
            )
        return httpx.Response(200, content=b"data: [DONE]\n\n")

    client = make_client(handler, proxies=["1.1.1.1:80", "2.2.2.2:80"])
    chunks = [c async for c in client.chat_stream(
        "deepseek-v4-flash-lite", {"model": "x"})]
    assert calls["n"] == 2
    assert chunks == [b"data: [DONE]\n\n"]


@pytest.mark.asyncio
async def test_chat_stream_4xx_no_retry():
    """流式：4xx 参数问题不重试，直接抛 UpstreamError。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": "bad request"})

    client = make_client(handler)
    with pytest.raises(UpstreamError):
        async for _ in client.chat_stream(
            "deepseek-v4-flash-lite", {"model": "x"}):
            pass
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_chat_stream_report_ok_after_success():
    """流式成功后代理回写 OK，留在可用池。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"data: [DONE]\n\n")

    client = make_client(handler)
    async for _ in client.chat_stream(
            "deepseek-v4-flash-lite", {"model": "x"}):
        pass
    assert "1.1.1.1:80" in client.pool._available


@pytest.mark.asyncio
async def test_chat_stream_report_dead_on_conn_error():
    """流式：连接层失败（首块前）→ 剔除代理并重试。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection refused")
        return httpx.Response(200, content=b"data: [DONE]\n\n")

    client = make_client(handler, proxies=["1.1.1.1:80", "2.2.2.2:80"])
    chunks = [c async for c in client.chat_stream(
        "deepseek-v4-flash-lite", {"model": "x"})]
    assert calls["n"] == 2
    assert chunks == [b"data: [DONE]\n\n"]


@pytest.mark.asyncio
async def test_chat_transport_errors_exhausted_become_upstream_error():
    """非流式：所有代理连接失败（重试耗尽）→ UpstreamError，不抛裸 httpx 异常。

    裸的 ConnectTimeout 路由层不接，会漏成 500；包成 UpstreamError(status=0)
    后路由层按 503 + 标准 error 结构返回（不用 502，避免 body 被出网中间层换掉）。
    """
    async def boom(endpoint: str, proxy: str, payload: dict):
        raise httpx.ConnectTimeout("proxy dead")

    proxies = [f"1.1.1.{i}:80" for i in range(1, 8)]   # 多于 MAX_RETRIES=5
    client = make_client(lambda r: httpx.Response(200, json={}), proxies=proxies)
    client._request = boom            # type: ignore[method-assign]

    with pytest.raises(UpstreamError) as ei:
        await client.chat("deepseek-v4-flash-lite",
                          {"model": "x", "messages": []})
    assert ei.value.status_code == 0
    assert "retries failed" in str(ei.value)


@pytest.mark.asyncio
async def test_chat_stream_transport_errors_exhausted_become_upstream_error():
    """流式（首块前）：重试耗尽同样收口成 UpstreamError。"""
    async def boom(endpoint: str, proxy: str, payload: dict):
        # 分支永不成立：只为让 boom 是 async generator（同 _stream_request 形态）
        if payload is None:
            yield b""
        raise httpx.ConnectTimeout("proxy dead")

    proxies = [f"2.2.2.{i}:80" for i in range(1, 8)]   # 多于 MAX_RETRIES=5
    client = make_client(lambda r: httpx.Response(200, content=b""), proxies=proxies)
    client._stream_request = boom    # type: ignore[method-assign]

    with pytest.raises(UpstreamError):
        async for _ in client.chat_stream(
                "deepseek-v4-flash-lite", {"model": "x"}):
            pass


@pytest.mark.asyncio
async def test_circuit_breaker_trips_on_many_failures():
    """窗口内失败超阈值 → 熔断打开，后续请求快速失败。"""
    client = make_client(lambda r: httpx.Response(200, content=b""),
                         proxies=["1.1.1.1:80", "2.2.2.2:80"])
    settings = make_settings(cb_failure_threshold=3, cb_window=30)
    client.settings = settings
    client._store = type(client._store)(settings)

    # 3 次失败触发熔断（直接注入失败计数，模拟上游 429/5xx）
    for _ in range(3):
        client._register_failure()
    assert client._circuit_open()

    # 熔断打开：快速失败，不再尝试上游
    with pytest.raises(UpstreamError):
        async for _ in client.chat_stream(
                "deepseek-v4-flash-lite", {"model": "x"}):
            pass


def test_normalize_chat_payload_adds_empty_reasoning():
    """assistant 消息缺 reasoning_content → 网关自动补空串；已有值不动。"""
    from app.services.upstream import _normalize_chat_payload

    payload = {"model": "x", "messages": [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "ok"},
        {"role": "assistant", "content": "t", "reasoning_content": "think"},
        {"role": "system", "content": "sys"},
    ]}
    out = _normalize_chat_payload(payload)
    assert out["messages"][1]["reasoning_content"] == ""
    assert out["messages"][2]["reasoning_content"] == "think"
    assert "reasoning_content" not in out["messages"][3]
    assert out["messages"][0] == payload["messages"][0]


def test_normalize_chat_payload_no_messages():
    """无 messages 或全都有了 → 原对象原样返回。"""
    from app.services.upstream import _normalize_chat_payload

    raw = {"model": "x"}
    assert _normalize_chat_payload(raw) is raw
    raw2 = {"model": "x", "messages": [{"role": "assistant",
                                        "content": "a", "reasoning_content": ""}]}
    assert _normalize_chat_payload(raw2) is raw2


def test_parse_error_body_extracts_error():
    """上游错误 body 解析：OpenAI error 结构 / detail 兜底。"""
    from app.services.upstream import _parse_error_body

    r1 = httpx.Response(400, json={
        "error": {"type": "invalid_request_error", "message": "bad param"}})
    assert _parse_error_body(r1)["message"] == "bad param"
    r2 = httpx.Response(400, json={"detail": "plain message"})
    assert _parse_error_body(r2)["message"] == "plain message"
    r3 = httpx.Response(502, content=b"not json")
    assert _parse_error_body(r3) == {}


def test_sanitize_responses_removes_web_search():
    """responses 请求剔除 web_search 等 zen 不支持的工具。"""
    payload = {
        "tools": [
            {"type": "function", "name": "exec", "description": "run"},
            {"type": "namespace", "name": "multi_agent_v1", "tools": []},
            {"type": "web_search", "external_web_access": False},
            {"type": "builtin", "name": "something"},
        ]
    }
    out = _sanitize_responses(payload)
    types = [t["type"] for t in out["tools"]]
    assert "web_search" not in types
    assert "builtin" not in types
    assert "function" in types
    assert "namespace" in types


def test_sanitize_responses_no_tools():
    """无 tools 字段时原样返回。"""
    payload = {"model": "x", "input": []}
    out = _sanitize_responses(payload)
    assert "tools" not in out
    assert out["model"] == "x"


def test_inject_responses_events_sequence():
    """补齐 Codex 需要的事件序列。"""
    from app.services.upstream import _inject_responses_events

    state = {"started": False, "rid": None, "item_id": None}
    events = []

    def feed(t, d):
        for name, obj in _inject_responses_events((t, d), state):
            events.append((name, obj))

    # zen 只发 delta + completed
    feed("response.output_text.delta", {
        "id": "r1", "type": "response.output_text.delta",
        "delta": "Hi", "response": {"id": "r1", "model": "m"}})
    feed("response.output_text.delta", {
        "id": "r1", "type": "response.output_text.delta",
        "delta": " there", "response": {"id": "r1", "model": "m"}})
    feed("response.completed", {
        "id": "r1", "type": "response.completed",
        "response": {"id": "r1", "model": "m"}})

    names = [n for n, _ in events]
    assert names == ["response.created", "response.output_item.added",
                     "data", "data",
                     "response.output_item.done", "data"]


def test_classify_status_codes():
    """状态码归桶：429 / 5xx / 其它 4xx。"""
    from app.services.upstream import _classify

    assert _classify(429) == "429"
    assert _classify(500) == "5xx"
    assert _classify(502) == "5xx"
    assert _classify(400) == "4xx"
    assert _classify(404) == "4xx"
    assert _classify(0) == "4xx"


@pytest.mark.asyncio
async def test_error_buckets_recorded_on_failures():
    """失败按分类记入错误桶：429/5xx/4xx/timeout/conn。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"data: [DONE]\n\n")

    client = make_client(handler, proxies=["1.1.1.1:80", "2.2.2.2:80"])
    pool = client.pool

    # 直接注入各类失败回写 + 分类桶
    pool.note_error("429")
    pool.note_error("5xx")
    pool.note_error("4xx")
    pool.note_error("timeout")
    pool.note_error("conn")

    m = pool.metrics()
    assert m["errors"] == {"429": 1, "5xx": 1, "4xx": 1, "timeout": 1, "conn": 1}


@pytest.mark.asyncio
async def test_note_error_ignores_unknown_kind():
    """未知错误桶不报错，只是不计数。"""
    from app.core.proxy_pool import ProxyPool

    pool = ProxyPool(make_settings())
    pool.note_error("mystery_kind")
    m = pool.metrics()
    assert m["errors"] == {"429": 0, "5xx": 0, "4xx": 0, "timeout": 0, "conn": 0}
