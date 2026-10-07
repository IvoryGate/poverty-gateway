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

    # 替换两个网络方法为 mock。fetch 照抄真实 _fetch_free_models 的筛选
    # （只留 id 含 "-free" 的），否则非 free 模型会被当成候选。
    async def fake_fetch() -> list[str]:
        return [m for m in model_ids if "-free" in m]

    async def fake_probe(model: str) -> bool:
        return probe_results.get(model, False)

    sync._fetch_free_models = fake_fetch  # type: ignore[method-assign]
    sync._probe_model = fake_probe        # type: ignore[method-assign]
    return sync


@pytest.mark.asyncio
async def test_sync_discovers_free_models():
    """同步发现可用 free 模型并更新映射。"""
    sync = make_sync(
        model_ids=["deepseek-v4-flash-free", "mimo-v2.6-flash-free", "gpt-5"],
        probe_results={"deepseek-v4-flash-free": True,
                       "mimo-v2.6-flash-free": True},
    )
    result = await sync.sync_once()
    assert "gpt-5" not in result["available"]  # 非 free 被筛掉
    assert sync.mapper.has("deepseek-v4-flash-lite")
    assert sync.mapper.has("mimo-v2.6-flash-lite")
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
async def test_sync_exposes_candidates_even_if_probe_fails():
    """上游在架即暴露：探测失败的候选照样进列表，并记进 probe_failed。

    代理偶发失败只影响观测，不影响对外模型清单（否则新模型因一次
    探测失败永远进不了列表，好模型会被误摘）。
    """
    sync = make_sync(
        model_ids=["deepseek-v4-flash-free", "brand-new-free"],
        probe_results={},  # 全部探测失败
    )
    result = await sync.sync_once()
    assert sync.mapper.has("deepseek-v4-flash-lite")
    assert sync.mapper.has("brand-new-lite")
    assert result["added"] == ["brand-new-lite"]
    assert result["available"] == []
    assert result["probe_failed"] == ["brand-new-lite", "deepseek-v4-flash-lite"]


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
async def test_sync_excludes_default_denylist():
    """上游在架但被默认 denylist 屏蔽的模型（如已 410 的 mimo-v2.5）不暴露。"""
    sync = make_sync(
        model_ids=["good-free", "mimo-v2.5-free"],
        probe_results={"good-free": True, "mimo-v2.5-free": True},
        denylist=Settings().model_denylist,
    )
    await sync.sync_once()
    assert sync.mapper.has("good-lite")
    assert not sync.mapper.has("mimo-v2.5-lite")


def test_default_probe_model_is_not_deprecated():
    """默认探测模型不能是上游已 410 的模型，否则每个 IP 都被判 DEAD。"""
    s = Settings()
    assert s.probe_model not in s.model_denylist
    assert s.probe_model != "mimo-v2.5-free"


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
