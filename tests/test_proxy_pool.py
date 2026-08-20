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
    """可用池低于 low_water 时触发补池（后台任务，单飞）。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        # 每次 /get 返回一个全新 IP：低水位补池才有新货可入池
        return httpx.Response(200, json={"code": 200, "data": [f"{get_calls['n']}.1.1.1:80"]})

    async def probe_fn(proxy: str) -> str:
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(
        make_settings(pool_target=2, pool_low_water=2, quality_chain="stable3"),
        client=client, probe_fn=probe_fn,
    )

    # 冷启动：同步补池一次，首次 /get 返回 1.1.1.1
    await pool.get_proxy()
    assert get_calls["n"] == 1
    assert pool._usable_count() == 1

    # 低水位：触发后台补池，等待任务完成（第二次 /get 返回 2.1.1.1）
    await pool.get_proxy()
    task = pool._refill_task
    if task is not None:
        await task
    assert get_calls["n"] == 2
    assert pool._usable_count() >= 2


@pytest.mark.asyncio
async def test_schedule_refill_single_flight():
    """低水位多次触发只跑一个后台补池任务。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        return httpx.Response(200, json={"code": 200, "data": ["1.1.1.1:80"]})

    async def probe_fn(proxy: str) -> str:
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(
        make_settings(pool_target=2, pool_low_water=2, quality_chain="stable3"),
        client=client, probe_fn=probe_fn,
    )
    # 先完成冷启动补池
    await pool.get_proxy()
    if pool._refill_task is not None:
        await pool._refill_task
    calls_before = get_calls["n"]

    # 连续两次低水位触发：应只启动一个后台任务
    pool._schedule_refill()
    first = pool._refill_task
    pool._schedule_refill()
    assert pool._refill_task is first  # 单飞：仍是同一个任务
    if pool._refill_task is not None:
        await pool._refill_task
    assert get_calls["n"] > calls_before


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
async def test_fetch_candidates_redis_full():
    """取候选优先读 proxy-pool Redis 全量（不依赖 https 标记）。"""
    pool = make_pool({"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []})
    # mock Redis 读返回全量
    pool._read_all_from_redis = lambda: ["1.1.1.1:80", "2.2.2.2:80", "3.3.3.3:80"]  # type: ignore[method-assign]
    ips = await pool._fetch_candidates("stable3")
    assert len(ips) == 3


@pytest.mark.asyncio
async def test_fetch_candidates_fallback_api():
    """Redis 读不到时回退 proxy-pool /get。"""
    pool = make_pool({"stable3": ["1.1.1.1:80"], "stable2": [], "stable1": []})
    pool._read_all_from_redis = list  # type: ignore[method-assign]
    ips = await pool._fetch_candidates("stable3")
    assert "1.1.1.1:80" in ips


@pytest.mark.asyncio
async def test_cooldown_uses_retry_after():
    """429 带上游 retry-after 时，冷却时长用上游解封时间。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80", "2.2.2.2:80"], "stable2": [], "stable1": []},
    )
    proxy = await pool.get_proxy()
    await pool.report(proxy, COOLDOWN, cooldown_seconds=3600)
    # 冷却键应带 3600s TTL（memory 态通过 _mem_ts 判断）
    ts = pool._store._mem_ts.get("cooldown:" + proxy, 0)
    assert ts > 0
    # 无 retry_after 时走指数退避（约 60s）
    proxy2 = await pool.get_proxy()
    await pool.report(proxy2, COOLDOWN)
    ts2 = pool._store._mem_ts.get("cooldown:" + proxy2, 0)
    assert 0 < ts2 < ts


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


@pytest.mark.asyncio
async def test_zen_score_skips_known_bad():
    """对 zen 历史表现差的候选，补池时排到后面（好 IP 优先探测）。"""

    def handler(request: httpx.Request) -> httpx.Response:
        proxies = ["1.1.1.1:80", "2.2.2.2:80"]
        return httpx.Response(200, json={"code": 200, "data": proxies})

    async def probe_fn(proxy: str) -> str:
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(
        make_settings(pool_target=2, quality_chain="stable3"),
        client=client, probe_fn=probe_fn,
    )
    # 给 1.1.1.1:80 灌入低于阈值的 score：补池排序时它排最后
    for _ in range(5):
        pool._bump_zen_score("1.1.1.1:80", -1)
    assert pool._zen_score("1.1.1.1:80") < 0  # 已被标记差

    await pool.get_proxy()
    # 两个候选最终都会入池（排序只影响探测顺序，不硬跳过）
    assert "2.2.2.2:80" in pool._available
    assert "1.1.1.1:80" in pool._available


@pytest.mark.asyncio
async def test_reverify_removes_dead_members():
    """重验池内成员：DEAD 剔除、OK 保留、429 进冷却。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80", "2.2.2.2:80"], "stable2": [], "stable1": []},
        probe_results=OK,
    )
    # 模拟快照回灌：三个成员里一个死了、一个被限流、一个活着
    pool._available = {
        "1.1.1.1:80": {"quality": "stable3"},
        "2.2.2.2:80": {"quality": "stable3"},
        "3.3.3.3:80": {"quality": "stable3"},
    }
    pool._warm = True

    async def probe_fn(proxy: str) -> str:
        return {"1.1.1.1:80": OK, "2.2.2.2:80": DEAD,
                "3.3.3.3:80": COOLDOWN}[proxy]

    pool._probe_fn = probe_fn  # type: ignore[method-assign]
    pool._probe_retry["3.3.3.3:80"] = 0

    await pool._reverify_pool_members(2, 10)
    assert "1.1.1.1:80" in pool._available      # 活着 → 保留
    assert "2.2.2.2:80" not in pool._available  # 死了 → 剔除
    assert "3.3.3.3:80" in pool._available      # 429 → 保留占位但进冷却
    assert pool._store.exists("cooldown:3.3.3.3:80")


@pytest.mark.asyncio
async def test_refill_reverifies_existing_then_probes_new():
    """补池先重验池内成员，再探测新候选。"""
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith("http://pool.test/get"):
            get_calls["n"] += 1
        return httpx.Response(200, json={"code": 200,
                                         "data": ["9.9.9.9:80", "8.8.8.8:80"]})

    async def probe_fn(proxy: str) -> str:
        # 池内僵尸死亡，新候选可用
        if proxy in ("1.1.1.1:80",):
            return DEAD
        return OK

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    pool = ProxyPool(
        make_settings(pool_target=3, pool_low_water=1, quality_chain="stable3"),
        client=client, probe_fn=probe_fn,
    )
    pool._available = {"1.1.1.1:80": {"quality": "stable3"}}
    pool._warm = True

    await pool._refill()
    assert "1.1.1.1:80" not in pool._available       # 僵尸被重验剔除
    assert "9.9.9.9:80" in pool._available           # 新候选入池
    assert "8.8.8.8:80" in pool._available


@pytest.mark.asyncio
async def test_pick_prefers_low_latency():
    """同可用的 IP 里优先选延迟低、in-flight 少的。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80", "2.2.2.2:80"], "stable2": [], "stable1": []},
    )
    pool._available = {
        "1.1.1.1:80": {"quality": "stable3"},
        "2.2.2.2:80": {"quality": "stable3"},
    }
    pool._warm = True
    pool._update_latency("1.1.1.1:80", 8000)   # 慢
    pool._update_latency("2.2.2.2:80", 2000)   # 快

    # 冷启动补池一次后，两次取号都应是延迟低的 2.2.2.2（除非 in-flight 抢占）
    got = await pool.get_proxy()
    assert got == "2.2.2.2:80"


@pytest.mark.asyncio
async def test_pick_prefers_positive_score_over_bad():
    """正分 IP 优先于负分 IP（僵尸不占养号位置）。"""
    pool = make_pool(
        {"stable3": ["1.1.1.1:80", "2.2.2.2:80"], "stable2": [], "stable1": []},
    )
    pool._available = {
        "1.1.1.1:80": {"quality": "stable3"},
        "2.2.2.2:80": {"quality": "stable3"},
    }
    pool._warm = True
    pool._bump_zen_score("1.1.1.1:80", -3)
    pool._bump_zen_score("2.2.2.2:80", +3)

    got = await pool.get_proxy()
    assert got == "2.2.2.2:80"
