"""model_map 测试：对外命名约定、上下文限制上报、动态列表。"""

from app.core.model_map import (
    DEFAULT_MODEL_LIMITS,
    DEFAULT_MODEL_MAP,
    ModelMapper,
    to_public,
)


def test_default_map_naming_convention():
    """对外名 = 内部名 -free 换 -lite，且不重复。"""
    assert len(DEFAULT_MODEL_MAP) == len(set(DEFAULT_MODEL_MAP))
    for public, upstream in DEFAULT_MODEL_MAP.items():
        assert upstream.endswith("-free")
        assert public == to_public(upstream)
        assert public.endswith("-lite")


def test_default_limits_cover_map_and_no_orphans():
    """限制表必须是映射表的子集（否则永远查不到）。"""
    orphan = set(DEFAULT_MODEL_LIMITS) - set(DEFAULT_MODEL_MAP)
    assert not orphan, f"limits 里有映射表没有的模型: {sorted(orphan)}"


def test_list_public_reports_configured_limits():
    """在册模型如实上报 models.dev 真值。"""
    mapper = ModelMapper()
    rows = {m["id"]: m for m in mapper.list_public()}
    row = rows["deepseek-v4-flash-lite"]
    assert row["context_length"] == 200000
    assert row["output_tokens"] == 128000
    assert row["object"] == "model"


def test_list_public_never_reports_zero_for_new_model():
    """新模型（未收录进 limits）走默认 1M/128k，而不是 0。

    0 会让 Cursor/Codex 等读 context_length 的客户端认为不支持上下文。
    """
    mapper = ModelMapper()
    mapper.set_models({**DEFAULT_MODEL_MAP, "fresh-model-lite": "fresh-model-free"})
    row = {m["id"]: m for m in mapper.list_public()}["fresh-model-lite"]
    assert row["context_length"] == 1048576
    assert row["output_tokens"] == 128000


def test_to_upstream_rejects_unknown_model():
    mapper = ModelMapper()
    try:
        mapper.to_upstream("nope-lite")
    except KeyError as e:
        assert "nope-lite" in str(e)
    else:
        raise AssertionError("未知模型应抛 KeyError")
