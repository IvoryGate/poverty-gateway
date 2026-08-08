"""model_sync 模块测试：free 模型筛选、探测、映射更新。"""

import httpx
import pytest

from app.config import Settings
from app.core.model_map import ModelMapper
from app.core.model_sync import ModelSync
from app.core.proxy_pool import ProxyPool


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
        "model_denylist": [],
    }
    base.update(kw)
    return Settings(**base)


def make_sync(model_ids, probe_results, denylist=None):
    """构造 ModelSync：/v1/models 返回 model_ids，探测按预设结果。"""
    settings = make_settings(model_denylist=denylist or [])

    async def pool_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 200, "data": ["1.1.1.1:80"]})

    async def probe_fn(proxy: str) -> str:
        return "ok"

    pool_transport = httpx.MockTransport(pool_handler)
    pool_client = httpx.AsyncClient(transport=pool_transport)
    pool = ProxyPool(settings, client=pool_client, probe_fn=probe_fn)

    mapper = ModelMapper(settings)
    sync = ModelSync(settings, mapper=mapper, pool=pool)

    # 替换两个网络方法为 mock
    async def fake_fetch() -> list[str]:
        return list(model_ids)

    async def fake_probe(model: str) -> bool:
        return probe_results.get(model, False)

    sync._fetch_free_models = fake_fetch  # type: ignore[method-assign]
    sync._probe_model = fake_probe        # type: ignore[method-assign]
    return sync


@pytest.mark.asyncio
async def test_sync_discovers_free_models():
    """同步发现可用 free 模型并更新映射。"""
    sync = make_sync(
        model_ids=["deepseek-v4-flash-free", "mimo-v2.5-free", "gpt-5"],
        probe_results={"deepseek-v4-flash-free": True, "mimo-v2.5-free": True},
    )
    result = await sync.sync_once()
    assert "gpt-5" not in result["available"]  # 非 free 被筛掉
    assert sync.mapper.has("deepseek-v4-flash-lite")
    assert sync.mapper.has("mimo-v2.5-lite")
    assert not sync.mapper.has("gpt-5")


@pytest.mark.asyncio
async def test_sync_keeps_existing_on_temp_failure():
    """旧映射中仍在候选、但本次探测失败的模型不摘除（防限流误伤）。"""
    sync = make_sync(
        model_ids=["deepseek-v4-flash-free", "new-model-free"],
        probe_results={"new-model-free": True},
    )
    # deepseek 是默认映射之一，本次探测失败但仍在候选 → 保留
    await sync.sync_once()
    assert sync.mapper.has("deepseek-v4-flash-lite")
    assert sync.mapper.has("new-model-lite")


@pytest.mark.asyncio
async def test_sync_removes_models_no_longer_in_candidates():
    """上游候选列表里已消失的模型（真下架）被摘除。"""
    sync = make_sync(
        model_ids=["old-free", "good-free"],
        probe_results={"old-free": True, "good-free": True},
    )
    await sync.sync_once()
    assert sync.mapper.has("old-lite")
    # 二次同步：old-free 从候选消失 → 摘除
    async def fetch_second() -> list[str]:
        return ["good-free"]
    sync._fetch_free_models = fetch_second  # type: ignore[method-assign]
    async def probe2(model: str) -> bool:
        return True
    sync._probe_model = probe2  # type: ignore[method-assign]
    result = await sync.sync_once()
    assert result["removed"] == ["old-lite"]
    assert sync.mapper.has("good-lite")
    assert not sync.mapper.has("old-lite")


@pytest.mark.asyncio
async def test_sync_respects_denylist():
    """配置禁用名单的模型不纳入。"""
    sync = make_sync(
        model_ids=["good-free", "bad-free"],
        probe_results={"good-free": True, "bad-free": True},
        denylist=["bad-free"],
    )
    await sync.sync_once()
    assert sync.mapper.has("good-lite")
    assert not sync.mapper.has("bad-lite")


@pytest.mark.asyncio
async def test_sync_reports_added_removed():
    """返回 added/removed 差异。"""
    sync = make_sync(
        model_ids=["a-free", "b-free"],
        probe_results={"a-free": True, "b-free": True},
    )
    result = await sync.sync_once()
    assert result["added"] == ["a-lite", "b-lite"]

    # 二次同步 b 下架：应报告 removed=["b-free"]
    async def fetch_second() -> list[str]:
        return ["a-free"]
    sync._fetch_free_models = fetch_second  # type: ignore[method-assign]
    result2 = await sync.sync_once()
    assert result2["removed"] == ["b-lite"]
    assert sync.mapper.has("a-lite")
    assert not sync.mapper.has("b-lite")
