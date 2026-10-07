"""模型名映射：对外暴露的模型名 → 内部真实模型名。

对外名用 -lite 后缀（与 opencode 自带 free 模型区分），内部映射到 zen 的
-free 真实名。例如 deepseek-v4-flash-lite → deepseek-v4-flash-free。
映射运行时由 ModelSync 自动更新，并落 Redis 共享，保证多 worker 一致。

铁律：客户端传进来的 model 名先经此映射；未知模型名一律拒绝。
"""

import json

from app.config import Settings
from app.core.redis_store import RedisStore

# 初始对外模型名(lite) → 内部真实模型名(free)。
# 与 zen 当前在架 free 模型对齐（denylist 里的除外，如已 410 的 mimo-v2.5）。
# ModelSync 会动态增删，此处为兜底初值。
DEFAULT_MODEL_MAP = {
    "jev-1.13-lite": "jev-1.13-free",
    "exo-lite": "exo-free",
    "deepseek-v4-flash-lite": "deepseek-v4-flash-free",
    "muse-spark-1.3-contributor-lite": "muse-spark-1.3-contributor-free",
    "muse-spark-1.2-contributor-lite": "muse-spark-1.2-contributor-free",
    "mimo-v2.6-flash-lite": "mimo-v2.6-flash-free",
    "space-bunny-lite": "space-bunny-free",
    "longcat-2.5-preview-lite": "longcat-2.5-preview-free",
    "ling-3.0-flash-fin-lite": "ling-3.0-flash-fin-free",
    "nemotron-3-ultra-lite": "nemotron-3-ultra-free",
    "nemotron-3.5-lightning-lite": "nemotron-3.5-lightning-free",
    "fledge-alpha-lite": "fledge-alpha-free",
    "ling-3.1-flash-lite": "ling-3.1-flash-free",
}

# 对外模型 → 上下文/输出限制（tokens）。取 models.dev 中 opencode 注册的
# -free 变体真值（网关上游即 -free 端点），如实上报给 /v1/models，
# 供读取 context_length 的客户端（opencode/Cursor/Codex 等）正确配置。
# models.dev 未收录的模型（如 jev-1.13）不在此表，走 limits() 默认值。
DEFAULT_MODEL_LIMITS = {
    "exo-lite": {"context": 1048576, "output": 131072},
    "deepseek-v4-flash-lite": {"context": 200000, "output": 128000},
    "muse-spark-1.3-contributor-lite": {"context": 1048576, "output": 131072},
    "muse-spark-1.2-contributor-lite": {"context": 1048576, "output": 131072},
    "mimo-v2.6-flash-lite": {"context": 200000, "output": 32000},
    "space-bunny-lite": {"context": 1048576, "output": 524288},
    "longcat-2.5-preview-lite": {"context": 1000000, "output": 131072},
    "ling-3.0-flash-fin-lite": {"context": 262144, "output": 32768},
    "nemotron-3-ultra-lite": {"context": 1000000, "output": 128000},
    "nemotron-3.5-lightning-lite": {"context": 262144, "output": 262144},
    "fledge-alpha-lite": {"context": 1048576, "output": 131072},
    "ling-3.1-flash-lite": {"context": 262144, "output": 32768},
}

# Redis 键：当前模型映射（JSON）
_K_MODELS = "models:map"


def to_public(upstream_name: str) -> str:
    """内部 zen 模型名 → 对外名：-free 后缀换 -lite。"""
    if upstream_name.endswith("-free"):
        return upstream_name[:-len("-free")] + "-lite"
    return upstream_name


class ModelMapper:
    def __init__(self, settings: "Settings | None" = None,
                 store: "RedisStore | None" = None):
        self.settings = settings or Settings()
        self._store = store or RedisStore(self.settings)
        self._local = dict(DEFAULT_MODEL_MAP)
        self._limits = dict(DEFAULT_MODEL_LIMITS)

    def to_upstream(self, public_name: str) -> str:
        """对外模型名 → 内部真实模型名。未知名抛 KeyError。"""
        m = self.models()
        if public_name not in m:
            raise KeyError(f"Unknown model: {public_name}")
        return m[public_name]

    def list_public(self) -> list[dict]:
        """对外模型列表（动态），带 context_length 供客户端正确配置上下文。

        未收录进 DEFAULT_MODEL_LIMITS 的新模型走 limits() 默认值
        （1M/128k），不报 0——0 会让部分客户端把上下文当"不支持"。
        """
        out = []
        for name in self.models():
            lim = self.limits(name)
            out.append({"id": name, "object": "model",
                        "context_length": lim.get("context", 0),
                        "output_tokens": lim.get("output", 0)})
        return out

    def has(self, public_name: str) -> bool:
        return public_name in self.models()

    def set_models(self, models: dict[str, str]):
        """整体替换对外模型映射（ModelSync 调用），并落 Redis 共享。

        limits 保留已知模型的上下文/输出值；新模型用默认 1M 上下文。
        """
        self._local = dict(models)
        if self._store.using_redis:
            self._store.setex(_K_MODELS, 3600, json.dumps(models))

    def limits(self, public_name: str) -> dict:
        """某对外模型的 (context, output) 限制（未知模型给默认 1M/128k）。"""
        return self._limits.get(public_name, {"context": 1048576, "output": 128000})

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
