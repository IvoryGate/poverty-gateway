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
        """直连上游拉取模型列表，筛出 free 模型 id。

        注意：改直连而非经代理。之前经代理拉取会卡在 pool.get_proxy()
        （代理池冷启动 refill 慢/无代理），导致同步长期失败、Redis 里的
        模型映射永远不更新——客户端只看到旧列表，新 free 模型（hy3/
        nemotron/muse 等）发现不了。读模型列表不涉及流量转发，直连安全。
        真正的模型可用性探测（_probe_model）仍走代理池。
        """
        import httpx

        url = f"{self.settings.upstream_base}/models"
        try:
            async with httpx.AsyncClient(timeout=20, verify=False) as client:
                r = await client.get(url, headers=zen_headers())
            if r.status_code != 200:
                return []
            data = r.json()
            items = data.get("data", data if isinstance(data, list) else [])
            ids = [m.get("id", "") for m in items if isinstance(m, dict)]
            return [i for i in ids if "-free" in i]
        except httpx.HTTPError:
            return []

    async def _probe_model(self, model: str) -> bool:
        """经代理池对单个模型做最小 chat 探测（能出对话即可用）。"""
        pool = self._get_pool()
        try:
            proxy = await asyncio.wait_for(pool.get_proxy(), timeout=20)
        except (NoProxyAvailable, asyncio.TimeoutError):
            return False
        url = f"{self.settings.upstream_base}/chat/completions"
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
        """后台循环：定期同步（多 worker 用 Redis 锁只让一个执行）。

        同步整体加超时：_probe_model 的 get_proxy 在代理池冷启动时可能很慢，
        不让它无限卡死占住这个协程；超时失败下轮（1h 后）重试。
        """
        interval = self.settings.model_sync_interval
        while True:
            try:
                # 锁 TTL 用短值（5 分钟）：sync 卡死/超时后锁自动过期，
                # 不会把其它 worker 的同步挡掉一整轮（原来用 interval 当 TTL，
                # 卡一次要等 1h 才能再抢到锁）。
                if self._acquire_lock(300):
                    try:
                        await asyncio.wait_for(self.sync_once(), timeout=180)
                    except asyncio.TimeoutError:
                        logger.warning("model sync timed out, will retry next interval")
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
