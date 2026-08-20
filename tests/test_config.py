"""config 核心逻辑测试：双上游端点开关。"""

from app.config import Settings


def test_default_upstream_endpoint_is_https():
    s = Settings()
    assert s.upstream_endpoint == "https://opencode.ai/zen/v1"


def test_http_switch_uses_http_endpoint():
    s = Settings(upstream_use_http=True)
    assert s.upstream_endpoint == "http://opencode.ai/zen/v1"


def test_http_switch_off_uses_https_endpoint():
    s = Settings(upstream_use_http=False)
    assert s.upstream_endpoint == "https://opencode.ai/zen/v1"


def test_custom_http_base():
    s = Settings(upstream_use_http=True,
                 upstream_base_http="http://custom.test/zen/v1")
    assert s.upstream_endpoint == "http://custom.test/zen/v1"


def test_env_http_switch():
    import os
    os.environ["UPSTREAM_USE_HTTP"] = "1"
    try:
        s = Settings()
        assert s.upstream_endpoint == "http://opencode.ai/zen/v1"
    finally:
        del os.environ["UPSTREAM_USE_HTTP"]