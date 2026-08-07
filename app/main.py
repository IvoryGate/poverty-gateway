"""FastAPI 应用入口：组装依赖，暴露 OpenAI 兼容路由。"""

from fastapi import FastAPI

from app.api.routes import router
from app.config import Settings
from app.services.upstream import UpstreamClient


def create_app(settings: Settings = None):
    settings = settings or Settings()
    client = UpstreamClient(settings)

    app = FastAPI(title="poverty-gateway", version="0.1.0")
    app.state.settings = settings
    app.state.upstream = client
    app.include_router(router)
    return app


app = create_app()
