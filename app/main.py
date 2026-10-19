"""FastAPI 应用入口：组装依赖，暴露 OpenAI 兼容路由。"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.config import Settings
from app.core.auth import AuthManager
from app.core.model_map import ModelMapper
from app.core.model_sync import ModelSync
from app.core.proxy_pool import ProxyPool
from app.core.redis_store import RedisStore
from app.core.usage import UsageTracker
from app.services.upstream import UpstreamClient


def create_app(settings: Settings = None):
    settings = settings or Settings()
    store = RedisStore(settings)
    mapper = ModelMapper(settings, store=store)
    pool = ProxyPool(settings, store=store)
    client = UpstreamClient(settings, store=store, pool=pool, models=mapper)
    auth = AuthManager(settings, store=store)
    usage = UsageTracker(settings)
    model_sync = ModelSync(settings, mapper=mapper, pool=pool)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.load_snapshot()
        pool.warm_async()

        async def periodic_snapshot():
            """每 24h 落盘一次可用池快照（重启兜底）。"""
            try:
                while True:
                    await asyncio.sleep(86400)
                    pool.save_snapshot()
            except asyncio.CancelledError:
                pass

        snapshot_task = asyncio.create_task(periodic_snapshot())
        task = asyncio.create_task(model_sync.run_forever())
        yield
        task.cancel()
        snapshot_task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        try:
            await snapshot_task
        except asyncio.CancelledError:
            pass
        pool.save_snapshot()

    app = FastAPI(title="poverty-gateway", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.upstream = client
    app.state.auth = auth
    app.state.usage = usage
    app.state.model_sync = model_sync
    app.include_router(router)
    return app


app = create_app()
