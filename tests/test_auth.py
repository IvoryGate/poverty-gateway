"""auth 模块测试：Bearer 鉴权、每 key 独立限额、窗口计数。"""

import time

import pytest

from app.config import Settings
from app.core.auth import AuthError, AuthManager, RateLimitExceeded


def make_auth(keys=None, default_rpm=60, window=60, enabled=True):
    keys = keys or {
        "sk-alice": {"name": "alice", "rpm": 3},
        "sk-bob": {"name": "bob", "rpm": 0},   # 0 = 不限额
    }
    settings = Settings(
        api_keys=keys,
        api_key_default_rpm=default_rpm,
        rate_limit_window=window,
        auth_enabled=enabled,
    )
    return AuthManager(settings)


def test_parse_keys_config():
    """API_KEYS JSON 解析为 {key: {name, rpm}}。"""
    settings = Settings(api_keys='{"sk-1": {"name": "alice", "rpm": 5}}')
    assert settings.api_keys == {"sk-1": {"name": "alice", "rpm": 5}}


def test_bearer_auth_ok():
    """标准 Bearer 头通过。"""
    auth = make_auth()
    assert auth.authenticate("Bearer sk-alice") == "sk-alice"


def test_bearer_plain_ok():
    """无 Bearer 前缀直接传 key 也通过。"""
    auth = make_auth()
    assert auth.authenticate("sk-alice") == "sk-alice"


def test_missing_header_rejected():
    """无 Authorization 头 → AuthError。"""
    auth = make_auth()
    with pytest.raises(AuthError):
        auth.authenticate(None)


def test_unknown_key_rejected():
    """未知 key → AuthError。"""
    auth = make_auth()
    with pytest.raises(AuthError):
        auth.authenticate("Bearer sk-evil")


def test_rate_limit_exceeded():
    """rpm=3：第 4 次请求 429。"""
    auth = make_auth()
    for _ in range(3):
        auth.authenticate("Bearer sk-alice")
    with pytest.raises(RateLimitExceeded):
        auth.authenticate("Bearer sk-alice")


def test_rate_limit_zero_unlimited():
    """rpm=0 表示不限额。"""
    auth = make_auth()
    for _ in range(10):
        assert auth.authenticate("Bearer sk-bob") == "sk-bob"


def test_rate_limit_per_key_independent():
    """每 key 限额独立：alice 超限不影响 bob。"""
    auth = make_auth()
    for _ in range(3):
        auth.authenticate("Bearer sk-alice")
    with pytest.raises(RateLimitExceeded):
        auth.authenticate("Bearer sk-alice")
    # bob 仍可正常使用
    assert auth.authenticate("Bearer sk-bob") == "sk-bob"


def test_window_resets():
    """窗口过期后计数重置。"""
    auth = make_auth(window=1)
    for _ in range(3):
        auth.authenticate("Bearer sk-alice")
    with pytest.raises(RateLimitExceeded):
        auth.authenticate("Bearer sk-alice")
    time.sleep(1.1)
    assert auth.authenticate("Bearer sk-alice") == "sk-alice"


def test_auth_disabled():
    """auth_enabled=False 时路由层直接放行（manager 不抛错）。"""
    auth = make_auth(enabled=False)
    # enabled=False 时路由层不会调用 authenticate；这里验证属性即可
    assert auth.enabled is False
