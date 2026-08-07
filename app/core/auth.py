"""API-Key 鉴权：Bearer 头校验 + 每 key 独立限流。

- 支持 OpenCode / Cursor / Codex 等标准的 `Authorization: Bearer <key>`。
- 每 key 可配独立 rpm（requests per minute）；缺省用全局默认值。
- 固定窗口计数：窗口内超限返回 429。
- /health 不鉴权（放给监控探活）。
"""

import threading
import time

from app.config import Settings


class AuthError(Exception):
    """鉴权失败：无有效 key。"""


class RateLimitExceeded(Exception):
    """超限流：窗口内请求过多。"""


class AuthManager:
    def __init__(self, settings: Settings = None):
        self.settings = settings or Settings()
        # key -> {"name": str, "rpm": int}
        self._keys = dict(self.settings.api_keys or {})
        # key -> (window_start, count)  固定窗口计数
        self._windows: dict[str, tuple[float, int]] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.settings.auth_enabled

    @property
    def key_count(self) -> int:
        return len(self._keys)

    def authenticate(self, auth_header: str | None) -> str:
        """校验 Bearer key，返回 key 字符串。无效/超限抛异常。"""
        key = self._extract_key(auth_header)
        if not key or key not in self._keys:
            raise AuthError("invalid or missing API key")
        self._check_rate_limit(key)
        return key

    def _extract_key(self, auth_header: str | None) -> str | None:
        if not auth_header:
            return None
        # 兼容 "Bearer sk-xxx" 与直接 "sk-xxx"
        parts = auth_header.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1].strip()
        if len(parts) == 1:
            return parts[0].strip()
        return None

    def _check_rate_limit(self, key: str):
        """固定窗口限流：每 key 每窗口 rpm 次。"""
        rpm = self._keys[key].get("rpm") or self.settings.api_key_default_rpm
        if rpm <= 0:
            return  # rpm=0 表示不限额
        window = self.settings.rate_limit_window
        now = time.time()
        with self._lock:
            start, count = self._windows.get(key, (0, 0))
            if now - start >= window:
                # 新窗口
                self._windows[key] = (now, 1)
                return
            if count >= rpm:
                raise RateLimitExceeded(
                    f"rate limit exceeded ({rpm}/min)")
            self._windows[key] = (start, count + 1)
