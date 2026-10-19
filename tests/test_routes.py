"""routes 层测试：宽松透传（额外字段不丢失）与模型校验。

用 httpx.ASGITransport 直连 FastAPI app，注入 MockTransport 的上游，
不依赖真实网络。
"""

import httpx
import pytest

from app.config import Settings
from app.core.proxy_pool import ProxyPool
from app.main import create_app
from app.services.upstream import UpstreamClient


def make_app(upstream_handler, proxies=None, api_keys=None):
    """构造 app：代理池探活放行、上游用 MockTransport 模拟。"""

    async def probe_fn(proxy: str) -> str:
        return "ok"

    proxies = proxies or ["1.1.1.1:80"]
    api_keys = api_keys or {
        "sk-alice": {"name": "alice", "rpm": 3},
    }

    def pool_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 200, "data": proxies})

    def upstream_fake(request: httpx.Request) -> httpx.Response:
        return upstream_handler(request)

    settings = Settings(
        proxy_pool_url="http://pool.test",
        upstream_base="https://upstream.test/zen/v1",
        quality_chain="stable3",
        batch_size=5,
        probe_concurrency=2,
        pool_low_water=1,
        pool_target=2,
        max_in_flight=3,
        api_keys=api_keys,
        auth_enabled=True,
        api_key_default_rpm=60,
        rate_limit_window=60,
    )

    # 替换 UpstreamClient 的 _request，用 MockTransport 模拟上游
    transport = httpx.MockTransport(upstream_fake)
    client = httpx.AsyncClient(transport=transport)

    pool_transport = httpx.MockTransport(pool_handler)
    pool_client = httpx.AsyncClient(transport=pool_transport)
    pool = ProxyPool(settings, client=pool_client, probe_fn=probe_fn)

    upstream = UpstreamClient(settings)
    upstream.pool = pool

    # 注入：把 _request 换成走 mock 的版本（真实 _request 走真代理无法 mock）
    async def fake_request(endpoint: str, proxy: str, payload: dict) -> dict:
        r = await client.post(f"https://upstream.test/{endpoint}", json=payload)
        return r.json()

    upstream._request = fake_request  # type: ignore[method-assign]

    # 非流式 chat 现在内部走流式聚合，因此也要 mock _stream_request
    async def fake_stream(endpoint: str, proxy: str, payload: dict):
        r = await client.post(f"https://upstream.test/{endpoint}", json=payload)
        if r.status_code >= 400:
            from app.services.upstream import UpstreamError
            raise UpstreamError(f"upstream {r.status_code}: {r.text}",
                                status_code=r.status_code)
        if r.content.lstrip().startswith(b"data:"):
            yield r.content
        else:
            import json
            yield ("data: " + json.dumps(r.json()) + "\n\n").encode()

    upstream._stream_request = fake_stream  # type: ignore[method-assign]

    # Responses 非流式也改成内部流式聚合，mock 对应入口
    async def fake_responses_stream(proxy: str, payload: dict):
        r = await client.post("https://upstream.test/responses", json=payload)
        if r.status_code >= 400:
            from app.services.upstream import UpstreamError
            raise UpstreamError(f"upstream {r.status_code}: {r.text}",
                                status_code=r.status_code)
        if r.content.lstrip().startswith((b"data:", b"event:")):
            yield r.content
        else:
            import json
            obj = r.json()
            yield ("data: " + json.dumps({
                "type": "response.completed", "response": obj,
            }) + "\n\n").encode()

    upstream._responses_stream_request = fake_responses_stream  # type: ignore[method-assign]

    app = create_app(settings)
    app.state.upstream = upstream
    return app


@pytest.mark.asyncio
async def test_extra_fields_passthrough():
    """tools/reasoning_effort 等额外字段原样转发，不被丢弃。"""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen["payload"] = json.loads(request.content)
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions",
                          headers={"Authorization": "Bearer sk-alice"}, json={
            "model": "deepseek-v4-flash-lite",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "low",
            "tools": [{"type": "function", "function": {"name": "x"}}],
            "tool_choice": "auto",
            "stream_options": {"include_usage": True},
        })
    assert r.status_code == 200
    p = seen["payload"]
    assert p["reasoning_effort"] == "low"
    # 原有工具原样保留；为通过上游 free 档校验额外补了 read 占位工具
    assert p["tools"][0] == {"type": "function", "function": {"name": "x"}}
    assert any(t.get("function", {}).get("name") == "read" for t in p["tools"])
    assert p["tool_choice"] == "auto"
    assert p["stream_options"] == {"include_usage": True}
    # model 被替换为内部真实名（同名字符串）
    assert p["model"] == "deepseek-v4-flash-free"


@pytest.mark.asyncio
async def test_missing_model_rejected():
    """缺 model 字段 → 400。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions",
                          headers={"Authorization": "Bearer sk-alice"}, json={
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_unknown_model_404():
    """未映射模型 → 404。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions",
                          headers={"Authorization": "Bearer sk-alice"}, json={
            "model": "no-such-model",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_auth_required():
    """无 API key → 401。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions", json={
            "model": "deepseek-v4-flash-lite",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_auth_valid_key_passes():
    """有效 API key → 正常转发。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_upstream_4xx_passthrough_not_502():
    """response_format=json_object 被上游拒 → 网关透传 400 + 标准 error 结构，
    不包装成网关类 5xx。"""
    from app.services.upstream import UpstreamError

    app = make_app(lambda r: httpx.Response(200, json={}))

    async def fail_chat(model, payload):
        raise UpstreamError(
            "upstream 400: bad", status_code=400,
            error_body={"type": "invalid_request_error",
                        "code": "unsupported_parameter",
                        "message": "json_object not supported"})

    app.state.upstream.chat = fail_chat  # type: ignore[method-assign]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}],
                  "response_format": {"type": "json_object"}})
    assert r.status_code == 400
    body = r.json()
    assert body["error"]["type"] == "invalid_request_error"
    assert body["error"]["code"] == "unsupported_parameter"
    assert body["error"]["message"] == "json_object not supported"


@pytest.mark.asyncio
async def test_upstream_429_retry_after_standard():
    """上游限流 → 网关 429 + Retry-After + 标准 rate_limit_error 结构。"""
    from app.services.upstream import UpstreamError

    app = make_app(lambda r: httpx.Response(200, json={}))

    async def fail_chat(model, payload):
        raise UpstreamError(
            "upstream 429: rate limited", status_code=429, retry_after=17,
            error_body={"type": "FreeUsageLimitError", "message": "Rate limited"})

    app.state.upstream.chat = fail_chat  # type: ignore[method-assign]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 429
    assert r.headers.get("retry-after") == "17"
    body = r.json()
    assert body["error"]["type"] == "rate_limit_error"


@pytest.mark.asyncio
async def test_gateway_unavailable_returns_503_json():
    """网关侧不可用（代理重试耗尽 / 上游 5xx）→ **503** + JSON error，不是 502。

    502 的 body 在出网时容易被中间层替换成自己的 HTML 错误页，客户端拿不到
    结构化 error；503 与 4xx/429 一样保留 JSON body。
    """
    from app.services.upstream import UpstreamError

    app = make_app(lambda r: httpx.Response(200, json={}))
    transport = httpx.ASGITransport(app=app)

    async def all_dead(model, payload):
        raise UpstreamError("all proxy retries failed: All connection attempts failed")

    async def upstream_500(model, payload):
        raise UpstreamError("upstream 500: boom", status_code=500,
                            error_body={"message": "boom"})

    payload = {"model": "deepseek-v4-flash-lite",
               "messages": [{"role": "user", "content": "hi"}]}
    headers = {"Authorization": "Bearer sk-alice"}

    app.state.upstream.chat = all_dead        # type: ignore[method-assign]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions", headers=headers, json=payload)
    assert r.status_code == 503
    body = r.json()
    assert "retries failed" in body["error"]["message"]
    assert body["error"]["type"] == "upstream_error"   # 不是 invalid_request_error

    app.state.upstream.chat = upstream_500    # type: ignore[method-assign]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions", headers=headers, json=payload)
    assert r.status_code == 503
    body = r.json()
    assert body["error"]["message"] == "boom"
    assert body["error"]["type"] == "upstream_error"


@pytest.mark.asyncio
async def test_assistant_reasoning_content_normalized():
    """多轮对话：assistant 消息缺 reasoning_content → 网关自动补空串再转发。"""
    import json as _json
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["payload"] = _json.loads(request.content)
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [
                      {"role": "user", "content": "hi"},
                      {"role": "assistant", "content": "first reply"},
                      {"role": "user", "content": "again"},
                  ]})
    assert r.status_code == 200
    msgs = seen["payload"]["messages"]
    assert msgs[1]["reasoning_content"] == ""
    # 正常回复字段不被破坏
    assert msgs[1]["content"] == "first reply"


@pytest.mark.asyncio
async def test_rate_limit_429():
    """每 key rpm 用尽 → 429。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for _ in range(3):
            r = await ac.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer sk-alice"},
                json={"model": "deepseek-v4-flash-lite",
                      "messages": [{"role": "user", "content": "hi"}]})
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 429


@pytest.mark.asyncio
async def test_no_version_endpoint_alias():
    """不带 /v1 前缀的 /chat/completions 与 /models 也可用（兼容各种 baseURL）。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r1 = await ac.post(
            "/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
        r2 = await ac.get("/models",
                          headers={"Authorization": "Bearer sk-alice"})
    assert r1.status_code == 200
    assert r2.status_code == 200


@pytest.mark.asyncio
async def test_responses_endpoint_passthrough():
    """Responses API（Codex）透传到上游 /responses，model 替换为真实名。"""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen["url"] = str(request.url)
        seen["payload"] = json.loads(request.content)
        return httpx.Response(200, json={
            "id": "resp_1", "object": "response", "created_at": 0,
            "model": "deepseek-v4-flash-lite",
            "output": [{"type": "message",
                        "content": [{"type": "output_text", "text": "ok"}]}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/responses",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "input": [{"role": "user",
                             "content": [{"type": "input_text", "text": "hi"}]}]})
    assert r.status_code == 200
    assert "responses" in seen["url"]
    assert seen["payload"]["model"] == "deepseek-v4-flash-free"
    assert seen["payload"]["input"][0]["role"] == "user"


@pytest.mark.asyncio
async def test_stats_tracks_usage():
    """请求后 /v1/stats 记录每 key 请求数与 token。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-lite",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                      "total_tokens": 15},
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
        r = await ac.get("/v1/stats",
                         headers={"Authorization": "Bearer sk-alice"})
    assert r.status_code == 200
    data = r.json()
    assert data["totals"]["requests"] == 1
    assert data["totals"]["tokens_in"] == 10
    assert data["totals"]["tokens_out"] == 5
    assert data["per_key"]["sk-alice"]["requests"] == 1
    assert "pool" in data
    assert "available" in data["pool"]
    assert data["pool"]["refill_count"] >= 1


@pytest.mark.asyncio
async def test_upstream_freetier_masked_as_503():
    """FreeTier 只留在网关内部：对外 503 upstream_unavailable，不透传文案。"""
    from app.services.upstream import UpstreamError

    app = make_app(lambda r: httpx.Response(200, json={}))

    async def fail_chat(model, payload):
        raise UpstreamError(
            "upstream 403: FreeTierError",
            status_code=403,
            error_body={
                "type": "FreeTierError",
                "message": "OpenCode's free models aren't available from your "
                           "current network because we've detected abusive traffic",
            })

    app.state.upstream.chat = fail_chat  # type: ignore[method-assign]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 503
    body = r.json()
    assert body["error"]["type"] == "upstream_unavailable"
    assert "FreeTier" not in body["error"]["message"]
    assert "OpenCode" not in body["error"]["message"]


@pytest.mark.asyncio
async def test_upstream_freetier_sse_masked():
    """流式 FreeTier 同样收口成 upstream_unavailable。"""
    from app.services.upstream import UpstreamError

    app = make_app(lambda r: httpx.Response(200))

    async def fail_stream(model, payload):
        raise UpstreamError(
            "upstream 403: FreeTierError",
            status_code=403,
            error_body={"type": "FreeTierError",
                        "message": "detected abusive traffic from this IP"})
        yield  # pragma: no cover

    app.state.upstream.chat_stream = fail_stream  # type: ignore[method-assign]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-lite", "stream": True,
                  "messages": [{"role": "user", "content": "hi"}]})
    text = r.text
    assert "upstream_unavailable" in text
    assert "FreeTier" not in text
    assert "abusive traffic" not in text
