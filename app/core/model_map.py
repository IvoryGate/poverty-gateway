"""模型名映射：对外暴露的模型名 → 内部真实模型名。

对外名与内部名当前同名直出（保留映射形式，未来可改伪名，改动集中在此）。
映射运行时由 ModelSync 自动更新，并落 Redis 共享，保证多 worker 一致。

铁律：客户端传进来的 model 名先经此映射；未知模型名一律拒绝。
"""

import json

from app.config import Settings
from app.core.redis_store import RedisStore

# 初始对外模型名 → 内部真实模型名（free 模型同名直出）
DEFAULT_MODEL_MAP = {
    "deepseek-v4-flash-free": "deepseek-v4-flash-free",
    "mimo-v2.5-free": "mimo-v2.5-free",
    "longcat-2.0-free": "longcat-2.0-free",
}

# Redis 键：当前模型映射（JSON）
_K_MODELS = "models:map"


class ModelMapper:
    def __init__(self, settings: Settings = None, store: RedisStore = None):
        self.settings = settings or Settings()
        self._store = store or RedisStore(settings)
        self._local = dict(DEFAULT_MODEL_MAP)

    def to_upstream(self, public_name: str) -> str:
        """对外模型名 → 内部真实模型名。未知名抛 KeyError。"""
        m = self.models()
        if public_name not in m:
            raise KeyError(f"Unknown model: {public_name}")
        return m[public_name]

    def list_public(self) -> list[dict]:
        """对外模型列表（动态）。"""
        return [{"id": name, "object": "model"} for name in self.models()]

    def has(self, public_name: str) -> bool:
        return public_name in self.models()

    def set_models(self, models: dict[str, str]):
        """整体替换对外模型映射（ModelSync 调用），并落 Redis 共享。"""
        self._local = dict(models)
        if self._store.using_redis:
            self._store.setex(_K_MODELS, 3600, json.dumps(models))

    def models(self) -> dict[str, str]:
        """当前完整映射快照（Redis 有则读 Redis，保证多 worker 一致）。"""
        if self._store.using_redis:
            raw = self._store.get(_K_MODELS)
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        return parsed
                except (ValueError, TypeError):
                    pass
        return dict(self._local)
