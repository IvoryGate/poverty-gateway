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
    async def fake_request(proxy: str, payload: dict) -> dict:
        r = await client.post("https://upstream.test/chat", json=payload)
        return r.json()

    upstream._request = fake_request  # type: ignore[method-assign]

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
            "model": "deepseek-v4-flash-free",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/v1/chat/completions",
                          headers={"Authorization": "Bearer sk-alice"}, json={
            "model": "deepseek-v4-flash-free",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "low",
            "tools": [{"type": "function", "function": {"name": "x"}}],
            "tool_choice": "auto",
            "stream_options": {"include_usage": True},
        })
    assert r.status_code == 200
    p = seen["payload"]
    assert p["reasoning_effort"] == "low"
    assert p["tools"] == [{"type": "function", "function": {"name": "x"}}]
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
            "model": "deepseek-v4-flash-free",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_auth_valid_key_passes():
    """有效 API key → 正常转发。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-free",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "ok"}}],
        })

    app = make_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-free",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_rate_limit_429():
    """每 key rpm 用尽 → 429。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-free",
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
                json={"model": "deepseek-v4-flash-free",
                      "messages": [{"role": "user", "content": "hi"}]})
        r = await ac.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer sk-alice"},
            json={"model": "deepseek-v4-flash-free",
                  "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 429


@pytest.mark.asyncio
async def test_stats_tracks_usage():
    """请求后 /v1/stats 记录每 key 请求数与 token。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "x", "object": "chat.completion", "created": 0,
            "model": "deepseek-v4-flash-free",
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
            json={"model": "deepseek-v4-flash-free",
                  "messages": [{"role": "user", "content": "hi"}]})
        r = await ac.get("/v1/stats",
                         headers={"Authorization": "Bearer sk-alice"})
    assert r.status_code == 200
    data = r.json()
    assert data["totals"]["requests"] == 1
    assert data["totals"]["tokens_in"] == 10
    assert data["totals"]["tokens_out"] == 5
    assert data["per_key"]["sk-alice"]["requests"] == 1
