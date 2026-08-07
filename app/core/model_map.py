"""模型名映射：对外暴露自定义伪名，内部映射到真实 free 模型。

对外名（客户端可见） → 内部名（上游实际请求）。
避免暴露 deepseek-v4-flash-free 等真实模型名 / 品牌名。

铁律：客户端传进来的 model 名先经此映射；未知模型名一律拒绝。
"""

from app.config import Settings

# 对外伪名 → 内部真实模型名
MODEL_MAP = {
    "gpt-4o-mini": "deepseek-v4-flash-free",
    "claude-haiku": "mimo-v2.5-free",
    "gemini-flash": "ling-3.0-flash-free",
}

# 对外暴露的模型列表（/v1/models 响应）
PUBLIC_MODELS = [{"id": name, "object": "model"} for name in MODEL_MAP]


class ModelMapper:
    def __init__(self, settings: Settings = None):
        self.settings = settings or Settings()

    def to_upstream(self, public_name: str) -> str:
        """对外伪名 → 内部真实模型名。未知名抛 KeyError。"""
        if public_name not in MODEL_MAP:
            raise KeyError(f"Unknown model: {public_name}")
        return MODEL_MAP[public_name]

    def list_public(self) -> list[dict]:
        """对外模型列表。"""
        return PUBLIC_MODELS
