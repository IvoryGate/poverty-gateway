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
        # https 端点的 http 对应端点（同主机不同协议，http-only 代理可连）
        self.upstream_base_http = self._get(
            "UPSTREAM_BASE_HTTP", "http://opencode.ai/zen/v1")
        # 整体 http 开关：1 时探测/转发/模型同步全部走 http 端点。
        # 阶段 18：proxy-pool 放宽入库后 https 占比掉到 ~1%，池 99% 是
        # http-only 代理，网关 https 端点只能吃 1% → 可用被饿死。先整体切
        # http 解锁全池，等池侧 https 水位恢复（stage 18 已保证 200）再切回。
        self.upstream_use_http = self._get_bool("UPSTREAM_USE_HTTP", False)

        # IP 档位降级链：优先 stable3，其次 stable2，再 stable1。
        # 某档取不到就试下一档；全取不到则本次请求返回 503（宁缺毋滥）。
        chain = self._get("QUALITY_CHAIN", "stable3,stable2,stable1")
        self.quality_chain = [q.strip() for q in chain.split(",")]

        # 单次从 proxy-pool 批量取的候选数（proxy-pool 实测最多稳定给 ~14 个）
        self.batch_size = self._get_int("BATCH_SIZE", 14)

        # 对候选 IP 做 zen 连通性筛选的并发数（探测克制，防自我限流。
        # 3 太低——几百个候选分批串行探测很慢，补池筛不完；12 兼顾并发与克制，
        # 上游池扩到 2760 后需要更快消化候选）
        self.probe_concurrency = self._get_int("PROBE_CONCURRENCY", 12)

        # 单次补池最多探测的候选数（候选可能上百个，全量探测太慢；
        # 上游 pool 扩量后提到 120，单轮能筛更多）
        self.probe_batch = self._get_int("PROBE_BATCH", 120)

        # 连通性筛选单 IP 超时（秒）：连不上的快速判定 dead，加速补池
        self.probe_timeout = self._get_float("PROBE_TIMEOUT", 5)

        # 选号延迟硬上限（ms）：超过此值的 IP 不再被选中（宁缺毋滥），
        # 从"只排序不剔除"升级为"直接过滤"，避免慢/挂代理拖垮请求延迟。
        # 阶段 16 网关缺的是"质量下限"，延迟中位数已到 7s，设 15s 兜底剔除极慢者。
        self.max_latency_ms = self._get_int("MAX_LATENCY_MS", 15000)

        # 可用池成员"闲置多久后需要重验"（秒）：超过此值未被真实请求/探测
        # 命中的 IP 会被周期性真实请求 spot-check，通了留、死了剔，保证池子
        # 里没有"放久了变僵尸"的占位。默认 15 分钟。
        self.stale_after_s = self._get_int("STALE_AFTER_S", 900)

        # 单次 spot-check 最多重验的闲置成员数（避免一次性烧太多探测额度）
        self.stale_check_batch = self._get_int("STALE_CHECK_BATCH", 30)

        # 探测/冷启动使用的最小 chat 模型（能出对话即视为可用）
        self.probe_model = self._get("PROBE_MODEL", "deepseek-v4-flash-free")

        # 可用池目标规模：补池到此数即停（候选源持续增长，多 key 需更大池）。
        # 阶段 16：源库存充足（stable3 ~160+），池子越大 = 更多已验证的好 IP
        # 缓冲，分摊 429 与延迟；补池会先重验去僵尸，大池不等于垃圾占位。
        # 阶段 17：proxy-pool 扩到 2760（全球大源冲量），网关 target 90 太小
        # 只用了 3% 候选。提到 150 让更多已验证 IP 做缓冲，吃透上游供给。
        self.pool_target = self._get_int("POOL_TARGET", 150)

        # 可用池低水位：低于 target 的 2/3 触发补池（默认），可用环境变量覆盖
        self.pool_low_water = self._get_int(
            "POOL_LOW_WATER", int(self.pool_target * 2 / 3))

        # 单个 IP 最大在途并发：超过则取号时跳过，避免同 IP 被打爆
        self.max_in_flight = self._get_int("MAX_IN_FLIGHT", 5)

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

        # Redis 共享状态地址（多 worker 用）。空 = 单进程内存态
        self.redis_url = self._get("REDIS_URL", "")

        # 熔断：窗口内失败次数达到阈值则短暂熔断，避免硬顶被限流的上游
        self.cb_window = self._get_int("CB_WINDOW", 30)        # 统计窗口（秒）
        self.cb_failure_threshold = self._get_int("CB_FAILURE_THRESHOLD", 10)
        self.cb_open_seconds = self._get_int("CB_OPEN_SECONDS", 15)  # 熔断时长

        # 模型源同步：自动发现/摘除 free 模型（一天一次足够，上游不会频繁调整）
        self.model_sync_interval = self._get_int("MODEL_SYNC_INTERVAL", 86400)  # 秒

        # 禁用模型名单（逗号分隔），自动同步时排除
        deny = self._get("MODEL_DENYLIST", "")
        if isinstance(deny, (list, tuple)):
            self.model_denylist = [str(d) for d in deny]
        elif deny:
            self.model_denylist = [d.strip() for d in deny.split(",") if d.strip()]
        else:
            self.model_denylist = []

        # 对外服务端口
        self.port = self._get_int("PORT", 8000)

    @property
    def upstream_endpoint(self):
        """当前生效的上游端点：UPSTREAM_USE_HTTP=1 时用 http 版（解锁 http-only 代理）。"""
        return self.upstream_base_http if self.upstream_use_http else self.upstream_base

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
