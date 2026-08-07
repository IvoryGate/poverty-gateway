"""全局配置：所有可从环境变量覆盖的运行时参数。"""

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
        self.batch_size = self._get_int("BATCH_SIZE", 10)

        # 对候选 IP 做 zen 连通性筛选的并发数
        self.probe_concurrency = self._get_int("PROBE_CONCURRENCY", 8)

        # 连通性筛选单 IP 超时（秒）
        self.probe_timeout = self._get_float("PROBE_TIMEOUT", 15)

        # 可用代理在进程内缓存的 TTL（秒），避免每次请求都重新取 IP
        self.pool_ttl = self._get_int("POOL_TTL", 120)

        # 对外服务端口
        self.port = self._get_int("PORT", 8000)

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
