"""Redis 共享存储：多 worker 共享限流计数 / in-flight / 冷却区。

设计：
- 可选启用（REDIS_URL 为空则用内存态单进程实现，便于测试与降级）。
- 原子 INCR/EXPIRE 保证多 worker 计数一致。
- 连接失败自动降级到内存态，不阻断主流程。
"""

import time

import redis

from app.config import Settings


class RedisStore:
    """限流/状态计数存储接口。Redis 不可用时降级为进程内存态。"""

    def __init__(self, settings: Settings = None, client: redis.Redis = None):
        self.settings = settings or Settings()
        self._client = client
        if self._client is None and self.settings.redis_url:
            try:
                self._client = redis.Redis.from_url(
                    self.settings.redis_url, decode_responses=True)
                self._client.ping()
            except redis.RedisError:
                self._client = None
        self._memory: dict[str, int] = {}   # key -> 计数（内存降级）
        self._mem_ts: dict[str, float] = {}  # key -> 过期时间戳

    @property
    def using_redis(self) -> bool:
        return self._client is not None

    def incr_window(self, bucket: str, ttl: int) -> int:
        """窗口内自增，返回当前计数。bucket 带窗口号。"""
        if self._client is not None:
            try:
                n = self._client.incr(bucket)
                if n == 1:
                    self._client.expire(bucket, ttl)
                return n
            except redis.RedisError:
                self._client = None
        now = time.time()
        if bucket in self._mem_ts and now > self._mem_ts[bucket]:
            self._memory[bucket] = 0
        self._memory[bucket] = self._memory.get(bucket, 0) + 1
        self._mem_ts[bucket] = now + ttl
        return self._memory[bucket]

    def incr(self, key: str) -> int:
        """无条件自增（in-flight 用）。"""
        if self._client is not None:
            try:
                return self._client.incr(key)
            except redis.RedisError:
                self._client = None
        self._memory[key] = self._memory.get(key, 0) + 1
        return self._memory[key]

    def decr(self, key: str) -> int:
        """自减，最小值 0（in-flight 归还用）。"""
        if self._client is not None:
            try:
                return max(self._client.decr(key), 0)
            except redis.RedisError:
                self._client = None
        self._memory[key] = max(self._memory.get(key, 0) - 1, 0)
        return self._memory[key]

    def get(self, key: str) -> str | None:
        """读取原始值（字符串或 None）。需要数值时调用方自行转换。"""
        if self._client is not None:
            try:
                return self._client.get(key)
            except redis.RedisError:
                self._client = None
        if key in self._memory:
            return str(self._memory[key])
        return None

    def setex(self, key: str, ttl: int, value: str = "1"):
        """带 TTL 写入（冷却区用）。"""
        if self._client is not None:
            try:
                self._client.setex(key, ttl, value)
                return
            except redis.RedisError:
                self._client = None
        self._memory[key] = 1
        self._mem_ts[key] = time.time() + ttl

    def setex_once(self, key: str, ttl: int, value: str = "1") -> bool:
        """SET NX + TTL：仅当 key 不存在时写入，返回是否成功（抢锁用）。"""
        if self._client is not None:
            try:
                return bool(self._client.set(key, value, ex=ttl, nx=True))
            except redis.RedisError:
                self._client = None
        if key in self._memory and time.time() <= self._mem_ts.get(key, 0):
            return False
        self._memory[key] = 1
        self._mem_ts[key] = time.time() + ttl
        return True

    def exists(self, key: str) -> bool:
        if self._client is not None:
            try:
                return bool(self._client.exists(key))
            except redis.RedisError:
                self._client = None
        return key in self._memory and time.time() <= self._mem_ts.get(key, 0)

    def delete(self, *keys: str):
        if self._client is not None:
            try:
                self._client.delete(*keys)
                return
            except redis.RedisError:
                self._client = None
        for k in keys:
            self._memory.pop(k, None)
            self._mem_ts.pop(k, None)

    def keys(self, pattern: str) -> list[str]:
        if self._client is not None:
            try:
                return self._client.keys(pattern)
            except redis.RedisError:
                self._client = None
        return [k for k in self._memory if pattern.replace("*", "") in k]

    # ---------- 熔断 ----------

    def circuit_register_failure(self, bucket: str, ttl: int) -> int:
        """记录一次上游失败，返回窗口内累计失败数（Redis 共享）。"""
        return self.incr_window(bucket, ttl)

    def circuit_open(self, bucket: str) -> bool:
        """熔断开关：连续失败触发后置位，TTL 到期自动半开。"""
        return self.exists(bucket)

    def circuit_trip(self, bucket: str, ttl: int):
        """熔断打开：置位熔断键，期间 circuit_open() 返回 True。"""
        self.setex(bucket, ttl, "open")
