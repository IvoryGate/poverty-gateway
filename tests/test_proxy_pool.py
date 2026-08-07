"""proxy_pool 模块测试：档位降级、chat探测筛选、状态机（冷却/in-flight/补池）。

用 httpx.MockTransport 模拟 proxy-pool /get 和上游探活，不依赖真实服务。
"""

import httpx
import pytest

from app.config import Settings
from app.core.proxy_pool import (
    COOLDOWN,
    DEAD,
    OK,
    NoProxyAvailable,
    ProxyPool,
)


def make_settings(**kw):
    base = {
        "proxy_pool_url": "http://pool.test",
        "upstream_base": "https://upstream.test/zen/v1",
        "quality_chain": "stable3,stable2,stable1",
        "batch_size": 5,
        "probe_concurrency": 2,
        "probe_timeout": 5,
        "probe_model": "deepseek-v4-flash-free",
        "pool_low_water": 4,
        "pool_target": 8,
        "max_in_flight": 3,
    }
    base.update(kw)
    return Settings(**base)


def make_pool(quality_to_proxies, probe_results="ok", **settings_kw):
    """构造 ProxyPool：proxy-pool 按 quality 返回 IP，探活按预设结果判定。"""

    def pool_handler(request: httpx.Request) -> httpx.Response:
        q = request.url.params.get("quality")
        proxies = quality_to_proxies.get(q, [])
        return httpx.Response(200, json={"code": 200, "data": proxies})

    async def probe_fn(proxy: str) -> str:
        return probe_results

    transport = httpx.MockTransport(pool_handler)
    client = httpx.AsyncClient(transport=transport)
    settings = make_settings(**settings_kw)
    pool = ProxyPool(settings, client=client, probe_fn=probe_fn)
    return pool


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
async def test_cooldown_proxy_not_selected():
    """探测返回 COOLDOWN 的代理不进入可用池。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
        probe_results=COOLDOWN,
    )
    with pytest.raises(NoProxyAvailable):
        await pool.get_proxy()


@pytest.mark.asyncio
async def test_dead_proxy_not_selected():
    """探测返回 DEAD 的代理不进入可用池。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
        probe_results=DEAD,
    )
    with pytest.raises(NoProxyAvailable):
        await pool.get_proxy()


@pytest.mark.asyncio
async def test_refill_stops_at_target():
    """可用代理达到 pool_target 后，不再重复向 proxy-pool 取号。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        proxies = [f"1.1.1.{i}:80" for i in range(1, 6)]
        return httpx.Response(200, json={"code": 200, "data": proxies})

    async def probe_fn(proxy: str) -> str:
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(make_settings(pool_target=5), client=client, probe_fn=probe_fn)

    await pool.get_proxy()
    await pool.get_proxy()
    # 目标 5 个：首次补齐后第二次命中池子，不再取号
    assert get_calls["n"] == 1


@pytest.mark.asyncio
async def test_refill_when_low_water():
    """可用池低于 low_water 时补池。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        return httpx.Response(200, json={"code": 200, "data": ["1.1.1.1:80"]})

    async def probe_fn(proxy: str) -> str:
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    # 池目标 2、低水位 2：每次取都会低于水位，应多次补池（链上只有 stable3）
    pool = ProxyPool(
        make_settings(pool_target=2, pool_low_water=2, quality_chain="stable3"),
        client=client, probe_fn=probe_fn,
    )

    await pool.get_proxy()
    await pool.get_proxy()
    assert get_calls["n"] == 2


@pytest.mark.asyncio
async def test_in_flight_counts_and_release():
    """取号后 in-flight 计数，release 后归还。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
    )
    proxy = await pool.get_proxy()
    assert int(pool._store.get("inflight:" + proxy) or 0) == 1
    pool.release(proxy)
    assert int(pool._store.get("inflight:" + proxy) or 0) == 0


@pytest.mark.asyncio
async def test_report_ok_resets_cooldown():
    """冷却中的代理收到真实成功回写后清冷却，恢复可选。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
    )
    proxy = await pool.get_proxy()
    await pool.report(proxy, COOLDOWN)
    assert pool._store.exists("cooldown:" + proxy)
    # 冷却中仍保留在 available，但 get_proxy 会过滤掉
    await pool.report(proxy, OK)
    assert not pool._store.exists("cooldown:" + proxy)
    assert proxy in pool._available


@pytest.mark.asyncio
async def test_report_dead_removes():
    """真实请求连接失败：从可用池剔除。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
    )
    proxy = await pool.get_proxy()
    await pool.report(proxy, DEAD)
    assert proxy not in pool._available
    assert not pool._store.exists("cooldown:" + proxy)


@pytest.mark.asyncio
async def test_cooldown_expiry_allows_reuse():
    """冷却 TTL 到期后该代理自动恢复可选（无需重新探测）。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []},
    )
    proxy = await pool.get_proxy()
    await pool.report(proxy, COOLDOWN)
    assert pool._store.exists("cooldown:" + proxy)
    # 冷却期间不可选（低水位补池，但候选被冷却过滤）
    with pytest.raises(NoProxyAvailable):
        await pool.get_proxy()
    # 手动删除冷却键，模拟 TTL 到期
    pool._store.delete("cooldown:" + proxy)
    got = await pool.get_proxy()
    assert got == proxy
