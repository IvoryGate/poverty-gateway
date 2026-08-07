"""全局配置：所有可从环境变量覆盖的运行时参数。"""

import json
import os


def _env_int(key, default):
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def _env_float(key, default):
    try:
        return float(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def _env_bool(key, default):
    v = os.getenv(key)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")


class Settings:
    """集中配置：默认从环境变量读取，也支持构造参数覆盖（测试用）。

    用法：settings = Settings() 或 Settings(pool_ttl=30)。
    """

    def __init__(self, **overrides):
        self._overrides = overrides or {}

        # 上游 proxy-pool 地址（同机 Docker 映射端口）
        self.proxy_pool_url = self._get("PROXY_POOL_URL", "http://127.0.0.1:5010")

        # 上游 OpenCode AI 端点（对外隐藏真实名字，只在这里出现）
        self.upstream_base = self._get(
            "UPSTREAM_BASE", "https://opencode.ai/zen/v1")

        # IP 档位降级链：优先 stable3，其次 stable2，再 stable1。
        # 某档取不到就试下一档；全取不到则本次请求返回 503（宁缺毋滥）。
        chain = self._get("QUALITY_CHAIN", "stable3,stable2,stable1")
        self.quality_chain = [q.strip() for q in chain.split(",")]

        # 单次从 proxy-pool 批量取的候选数（连通性筛选后取其一）
        self.batch_size = self._get_int("BATCH_SIZE", 8)

        # 对候选 IP 做 zen 连通性筛选的并发数（探测严格限制，最多 2）
        self.probe_concurrency = self._get_int("PROBE_CONCURRENCY", 2)

        # 连通性筛选单 IP 超时（秒）
        self.probe_timeout = self._get_float("PROBE_TIMEOUT", 15)

        # 探测/冷启动使用的最小 chat 模型（能出对话即视为可用）
        self.probe_model = self._get("PROBE_MODEL", "deepseek-v4-flash-free")

        # 可用池低水位：低于此数触发补池
        self.pool_low_water = self._get_int("POOL_LOW_WATER", 4)

        # 可用池目标规模：补池到此数即停
        self.pool_target = self._get_int("POOL_TARGET", 8)

        # 单个 IP 最大在途并发：超过则取号时跳过，避免同 IP 被打爆
        self.max_in_flight = self._get_int("MAX_IN_FLIGHT", 3)

        # API-Key 鉴权：是否启用（默认启用；测试可关）
        self.auth_enabled = self._get_bool("AUTH_ENABLED", True)

        # API-Key 列表（JSON 字符串或文件）：key -> {name, rpm}
        # 例: {"sk-xxx1": {"name": "alice", "rpm": 60}, "sk-xxx2": {"name": "bob", "rpm": 30}}
        # rpm = 每分钟最大请求数；缺省用默认 rpm。
        # 两种来源：API_KEYS 环境变量（JSON 字符串）或 API_KEYS_FILE（指向 JSON 文件）。
        self.api_keys = self._parse_api_keys(
            self._get("API_KEYS", "") or self._read_keys_file(self._get("API_KEYS_FILE", ""))
        )
        # 每 key 默认限流（每分钟请求数）
        self.api_key_default_rpm = self._get_int("API_KEY_DEFAULT_RPM", 60)

        # 限流窗口（秒）
        self.rate_limit_window = self._get_int("RATE_LIMIT_WINDOW", 60)

        # 对外服务端口
        self.port = self._get_int("PORT", 8000)

    @staticmethod
    def _read_keys_file(path: str) -> str:
        if not path:
            return ""
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except OSError:
            return ""

    @staticmethod
    def _parse_api_keys(raw) -> dict:
        """解析 API_KEYS 为 {key: {"name": str, "rpm": int}}。

        兼容两种输入：JSON 字符串（环境变量）或 dict（测试覆盖）。
        """
        if not raw:
            return {}
        if isinstance(raw, dict):
            data = raw
        else:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        if not isinstance(data, dict):
            return {}
        keys = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                v = {}
            keys[k] = {
                "name": str(v.get("name", "")),
                "rpm": int(v.get("rpm", 0)),
            }
        return keys

    def _get(self, key, default):
        # 支持两种 override key：环境变量名（大写）或属性名（小写）
        attr = key.lower()
        if attr in self._overrides:
            return self._overrides[attr]
        return os.getenv(key, default)

    def _get_int(self, key, default):
        attr = key.lower()
        if attr in self._overrides:
            return int(self._overrides[attr])
        return _env_int(key, default)

    def _get_float(self, key, default):
        attr = key.lower()
        if attr in self._overrides:
            return float(self._overrides[attr])
        return _env_float(key, default)

    def _get_bool(self, key, default):
        attr = key.lower()
        if attr in self._overrides:
            v = self._overrides[attr]
            if isinstance(v, bool):
                return v
            return str(v).lower() in ("1", "true", "yes", "on")
        return _env_bool(key, default)
