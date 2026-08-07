"""proxy_pool 模块测试：档位降级、连通性筛选、缓存逻辑。

用 httpx.MockTransport 模拟 proxy-pool /get 和上游探活，不依赖真实服务。
"""

import httpx
import pytest

from app.config import Settings
from app.core.proxy_pool import NoProxyAvailable, ProxyPool


def make_settings(**kw):
    base = {
        "proxy_pool_url": "http://pool.test",
        "upstream_base": "https://upstream.test/zen/v1",
        "quality_chain": "stable3,stable2,stable1",
        "batch_size": 5,
        "probe_concurrency": 5,
        "probe_timeout": 5,
        "pool_ttl": 60,
    }
    base.update(kw)
    return Settings(**base)


def make_pool(quality_to_proxies, **settings_kw):
    """构造 ProxyPool：proxy-pool 按 quality 返回 IP，探活全部放行。"""

    def pool_handler(request: httpx.Request) -> httpx.Response:
        q = request.url.params.get("quality")
        proxies = quality_to_proxies.get(q, [])
        return httpx.Response(200, json={"code": 200, "data": proxies})

    transport = httpx.MockTransport(pool_handler)
    client = httpx.AsyncClient(transport=transport)
    settings = make_settings(**settings_kw)
    pool = ProxyPool(settings, client=client, probe_fn=_probe_pass)
    return pool


async def _probe_pass(proxy: str) -> bool:
    """测试用探活：任何 IP 都认为可用（不发起真实网络请求）。"""
    return True


@pytest.mark.asyncio
async def test_prefer_stable3():
    """stable3 有货时优先用它，不降级。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80", "2.2.2.2:80"],
         "stable2": ["3.3.3.3:80"],
         "stable1": ["4.4.4.4:80"]},
    )
    proxy = await pool.get_proxy()
    assert proxy in ("1.1.1.1:80", "2.2.2.2:80")


@pytest.mark.asyncio
async def test_fallback_to_stable2():
    """stable3 空、stable2 有货时降级到 stable2。"""
    pool = make_pool(
        {"stable3": [],
         "stable2": ["3.3.3.3:80"],
         "stable1": ["4.4.4.4:80"]},
    )
    proxy = await pool.get_proxy()
    assert proxy == "3.3.3.3:80"


@pytest.mark.asyncio
async def test_fallback_to_stable1():
    """只有 stable1 有货时降级到 stable1。"""
    pool = make_pool(
        {"stable3": [], "stable2": [], "stable1": ["4.4.4.4:80"]},
    )
    proxy = await pool.get_proxy()
    assert proxy == "4.4.4.4:80"


@pytest.mark.asyncio
async def test_no_proxy_raises():
    """所有档位都空时抛 NoProxyAvailable。"""
    pool = make_pool({"stable3": [], "stable2": [], "stable1": []})
    with pytest.raises(NoProxyAvailable):
        await pool.get_proxy()


@pytest.mark.asyncio
async def test_cache_hits_within_ttl():
    """TTL 内第二次取不重新请求 proxy-pool（只统计 /get 调用）。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        return httpx.Response(200, json={"code": 200, "data": ["1.1.1.1:80"]})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(make_settings(), client=client, probe_fn=_probe_pass)
    pool._http = client

    await pool.get_proxy()
    await pool.get_proxy()
    # 首次取触发一次刷新，第二次命中缓存不再请求 proxy-pool
    assert get_calls["n"] == 1


@pytest.mark.asyncio
async def test_cache_expires():
    """TTL 过期后重新请求 proxy-pool。"""
    settings = make_settings(pool_ttl=0)
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        return httpx.Response(200, json={"code": 200, "data": ["1.1.1.1:80"]})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(settings, client=client, probe_fn=_probe_pass)
    pool._http = client

    await pool.get_proxy()
    await pool.get_proxy()
    assert get_calls["n"] == 2
