"""代理池取用模块。

职责：从 proxy-pool（同机 Docker 服务）按 stable 档位降级链取可用 IP，
并做"能否连通上游 zen"的筛选，进程内缓存可用代理供转发使用。

关键约束（项目铁律）：
- 完全禁用本地 IP 直连——只走 proxy-pool 提供的 IP。
- 档位降级链：stable3 → stable2 → stable1，全无则停止（503）。
"""

import asyncio
import random
import time

import httpx

from app.config import Settings


class NoProxyAvailable(Exception):
    """候选档位链全空，无可用代理。"""


class ProxyPool:
    def __init__(self, settings: Settings = None, client: httpx.AsyncClient = None,
                 probe_fn=None):
        self.settings = settings or Settings()
        self._cache: list[dict] = []      # 可用代理列表 [{proxy, quality}]
        self._cache_ts = 0.0              # 缓存时间戳
        self._lock = asyncio.Lock()       # 取用/刷新互斥
        # 测试时可注入 MockTransport 的 client；probe_fn 可注入探活函数
        self._http = client or httpx.AsyncClient(timeout=10)
        self._probe_fn = probe_fn or self._probe

    async def get_proxy(self, exclude: set[str] | None = None) -> str:
        """取一个可用代理。优先用缓存，缓存过期则刷新。

        exclude: 本次要避开的代理集合（被限流的先不选）。
        返回 "ip:port"。无可用代理时抛 NoProxyAvailable。
        """
        exclude = exclude or set()
        async with self._lock:
            if self._cache_expired():
                await self._refresh()
            if not self._cache:
                raise NoProxyAvailable("代理池无可用代理")
            candidates = [c for c in self._cache if c["proxy"] not in exclude]
            if not candidates:
                # 全部被排除：重置排除集，全部重来（或刷新缓存拿新 IP）
                candidates = self._cache
            pick = random.choice(candidates)
            return pick["proxy"]

    def _cache_expired(self) -> bool:
        if not self._cache:
            return True
        return time.time() - self._cache_ts > self.settings.pool_ttl

    async def _refresh(self):
        """按档位降级链取 IP + 连通性筛选，重建缓存。"""
        chain = self.settings.quality_chain
        usable = []
        for quality in chain:
            quality = quality.strip()
            raw = await self._fetch_candidates(quality)
            if not raw:
                continue
            good = await self._probe_all(raw)
            if good:
                usable = [{"proxy": p, "quality": quality} for p in good]
                break
        self._cache = usable
        self._cache_ts = time.time()

    async def _fetch_candidates(self, quality: str) -> list[str]:
        """从 proxy-pool /get 批量取候选 IP。返回 ["ip:port", ...] 或 []。"""
        url = f"{self.settings.proxy_pool_url}/get"
        params = {
            "need": "global",
            "type": "https",
            "count": self.settings.batch_size,
            "quality": quality,
        }
        try:
            r = await self._http.get(url, params=params)
            if r.status_code != 200:
                return []
            data = r.json().get("data")
            if not isinstance(data, list):
                return []
            return [str(p) for p in data if p]
        except (httpx.HTTPError, ValueError):
            return []

    async def _probe_all(self, proxies: list[str]) -> list[str]:
        """并发对候选 IP 做 zen 连通性筛选，返回能连通上游的代理。"""
        sem = asyncio.Semaphore(self.settings.probe_concurrency)

        async def one(proxy: str) -> str | None:
            async with sem:
                if await self._probe_fn(proxy):
                    return proxy
                return None

        results = await asyncio.gather(*(one(p) for p in proxies))
        return [p for p in results if p]

    async def _probe(self, proxy: str) -> bool:
        """单个 IP 探活：能连通上游 zen 即认为可用。

        用上游的 models 端点做轻量探活（返回 200/401 都算连通——401 表示
        代理本身能到上游，只是该路径对某些模型无认证）。

        注：httpx 0.28 的 proxies 参数只在 client 构造时生效，
        每个候选 IP 需独立 client（代理不同），这里临时建一个。
        """
        url = f"{self.settings.upstream_base}/models"
        proxy_url = f"http://{proxy}"
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url, timeout=self.settings.probe_timeout
            ) as client:
                r = await client.get(url)
                return r.status_code in (200, 401, 403)
        except httpx.HTTPError:
            return False
