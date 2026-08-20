"""模型源自动治理：定时同步 zen 可用 free 模型。

- 定期（MODEL_SYNC_INTERVAL）从上游 /v1/models 拉取模型列表。
- 筛选 free 模型（id 含 "-free"），排除配置禁用的。
- 对候选逐个做最小 chat 探测（走代理池），只保留能出对话的。
- 更新 ModelMapper 的对外映射（同名直出）。
- 已下架/不可用的模型自动摘除。

注意：探测经代理池走 chat 最小请求，频率低（每个候选一次），
避免重复占用免费额度。
"""

import asyncio
import logging

import httpx

from app.config import Settings
from app.core.headers import zen_headers
from app.core.model_map import ModelMapper, to_public
from app.core.proxy_pool import NoProxyAvailable, ProxyPool

logger = logging.getLogger(__name__)


class ModelSync:
    def __init__(self, settings: Settings = None, mapper: ModelMapper = None,
                 pool=None):
        self.settings = settings or Settings()
        self.mapper = mapper or ModelMapper(settings)
        self._pool = pool  # 复用主链路 ProxyPool（含共享 store）

    async def sync_once(self) -> dict:
        """执行一次同步，返回 {added: [...], removed: [...]}（对外名）。"""
        candidates = await self._fetch_free_models()          # 内部 -free 名
        denied = set(self.settings.model_denylist)
        candidates = [m for m in candidates if m not in denied]

        available = []
        sem = asyncio.Semaphore(min(self.settings.probe_concurrency, 2))

        async def check(m: str) -> str | None:
            async with sem:
                if await self._probe_model(m):
                    return m
                return None

        results = await asyncio.gather(*(check(m) for m in candidates))
        available = [m for m in results if m]                 # 内部 -free 名

        old = self.mapper.models()                            # {lite: free}
        old_keys = set(old.keys())                            # lite 集合
        # 对外名集合：候选与可用都转成 lite
        cand_pub = {to_public(m) for m in candidates}
        avail_pub = {to_public(m) for m in available}

        if not cand_pub:
            # 候选列表本身为空（拉取失败），保留现状
            return {"added": [], "removed": [], "available": []}

        # 增量更新：保留"本次探测通过" + "旧映射里仍在候选但本次临时失败"的
        # （避免一次限流把好模型全摘掉；真正下架的会从候选列表消失）
        keep = avail_pub | (old_keys & cand_pub)
        merged = {p: old.get(p, p.replace("-lite", "-free"))
                  for p in keep}
        self.mapper.set_models(merged)

        added = sorted(avail_pub - old_keys)
        removed = sorted(old_keys - cand_pub)
        return {"added": added, "removed": removed,
                "available": sorted(avail_pub)}

    def _get_pool(self):
        return self._pool or ProxyPool(self.settings)

    async def _fetch_free_models(self) -> list[str]:
        """经代理拉取上游模型列表，筛出 free 模型 id。"""
        import httpx

        pool = self._get_pool()
        try:
            pool_proxy = await pool.get_proxy()
        except NoProxyAvailable:
            return []
        url = f"{self.settings.upstream_endpoint}/models"
        try:
            async with httpx.AsyncClient(
                proxy=f"http://{pool_proxy}", timeout=15
            ) as client:
                r = await client.get(url, headers=zen_headers())
            if r.status_code != 200:
                return []
            data = r.json()
            items = data.get("data", data if isinstance(data, list) else [])
            ids = [m.get("id", "") for m in items if isinstance(m, dict)]
            return [i for i in ids if "-free" in i]
        except httpx.HTTPError:
            return []
        finally:
            pool.release(pool_proxy)

    async def _probe_model(self, model: str) -> bool:
        """经代理池对单个模型做最小 chat 探测（能出对话即可用）。"""
        pool = self._get_pool()
        try:
            proxy = await pool.get_proxy()
        except NoProxyAvailable:
            return False
        url = f"{self.settings.upstream_endpoint}/chat/completions"
        body = {
            "model": model,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
        }
        try:
            async with httpx.AsyncClient(
                proxy=f"http://{proxy}", timeout=self.settings.probe_timeout
            ) as client:
                r = await client.post(url, headers=zen_headers(), json=body)
            return r.status_code == 200
        except httpx.HTTPError:
            return False
        finally:
            pool.release(proxy)

    async def run_forever(self):
        """后台循环：定期同步（多 worker 用 Redis 锁只让一个执行）。"""
        interval = self.settings.model_sync_interval
        while True:
            try:
                if self._acquire_lock(interval):
                    try:
                        await self.sync_once()
                    finally:
                        self._release_lock()
            except (httpx.HTTPError, NoProxyAvailable, OSError) as e:
                logger.warning("model sync failed: %s", e)
            await asyncio.sleep(interval)

    _LOCK_KEY = "models:sync_lock"

    def _acquire_lock(self, ttl: int) -> bool:
        """SET NX 抢锁：成功才执行同步，避免多 worker 重复探测。"""
        return self._pool is not None and bool(
            self._pool._store.setex_once(self._LOCK_KEY, ttl, "1"))

    def _release_lock(self):
        self._pool._store.delete(self._LOCK_KEY)
