"""代理池取用模块（状态机版）。

职责：从 proxy-pool（同机 Docker 服务）按 stable 档位降级链取可用 IP，
每个 IP 只有通过"最小 chat 探测"才算可用，并维护 in-flight 计数与
冷却记忆，避免并发撞同一 IP、避免反复踩被限流的 IP。

多 worker 一致性：in-flight 与冷却区落 RedisStore（Redis 或内存降级），
`_available` 可用池为进程内（各 worker 独立补池，量小成本低）。

关键约束（项目铁律）：
- 完全禁用本地 IP 直连——只走 proxy-pool 提供的 IP。
- 档位降级链：stable3 → stable2 → stable1，全无则停止（503）。
- "可用" = 刚验证能出对话（chat 最小请求 200），不是"能连上"。
- 主动探测频率尽量低：冷启动/池子告急才补，稳态靠真实请求回写状态。
"""

import asyncio

import httpx

from app.config import Settings
from app.core.headers import zen_headers
from app.core.redis_store import RedisStore

# 探测/回写状态常量
OK = "ok"                 # 真实请求成功，可用
COOLDOWN = "cooldown"     # 限流，短期回避（指数退避）
DEAD = "dead"             # 连接失败，剔除

# Redis 键前缀
_K_INFLIGHT = "inflight:"   # in-flight 计数
_K_COOLDOWN = "cooldown:"   # 冷却区（值 = "strike:quality"，带 TTL）


class NoProxyAvailable(Exception):
    """候选档位链全空，无可用代理。"""


class ProxyPool:
    def __init__(self, settings: Settings = None, client: httpx.AsyncClient = None,
                 probe_fn=None, store: RedisStore = None):
        self.settings = settings or Settings()
        self._lock = asyncio.Lock()
        # 测试时可注入 MockTransport 的 client；probe_fn 可注入探活函数
        self._http = client or httpx.AsyncClient(timeout=10)
        self._probe_fn = probe_fn or self._probe
        self._store = store or RedisStore(settings)

        # 状态机（_available 进程内；in-flight/冷却走 store）
        self._available: dict[str, dict] = {}   # proxy -> {"quality": str}
        self._warm = False                      # 是否已做过首次补池

        # 观测指标
        self._metrics = {
            "refill_count": 0,       # 补池次数
            "cooldown_reports": 0,   # 真实请求 429 回写次数
            "dead_reports": 0,       # 真实请求连接失败回写次数
            "no_proxy_errors": 0,    # 取号失败次数
        }

    # ---------- 对外接口 ----------

    async def get_proxy(self, exclude: set[str] | None = None) -> str:
        """取一个可用代理：优先冷启动补池/低水位补池，然后选最空闲的。

        exclude: 本次要避开的代理集合（当前请求已失败过的不再选）。
        返回 "ip:port"。无可用代理时抛 NoProxyAvailable。
        """
        exclude = exclude or set()
        async with self._lock:
            if not self._warm or self._low_water():
                await self._refill()
            if not self._available:
                self._metrics["no_proxy_errors"] += 1
                raise NoProxyAvailable("代理池无可用代理")
            candidates = [
                p for p in self._available
                if p not in exclude
                and self._store.get(_K_INFLIGHT + p) < self.settings.max_in_flight
                and not self._store.exists(_K_COOLDOWN + p)
            ]
            if not candidates:
                # 全部在途满/冷却/被排除：取最空闲且未冷却的兜底
                candidates = [
                    p for p in self._available
                    if p not in exclude and not self._store.exists(_K_COOLDOWN + p)
                ]
            if not candidates:
                # 冷却区排除了全部：退回全部可用池挑最空闲（本次请求兜底）
                candidates = list(self._available)
            if not candidates:
                self._metrics["no_proxy_errors"] += 1
                raise NoProxyAvailable("代理池无可用代理（全部被排除）")
            pick = min(candidates, key=lambda p: self._store.get(_K_INFLIGHT + p))
            self._store.incr(_K_INFLIGHT + pick)
            return pick

    def release(self, proxy: str):
        """请求结束（无论成败）归还 in-flight 计数。"""
        self._store.decr(_K_INFLIGHT + proxy)

    async def report(self, proxy: str, status: str):
        """真实请求结果回写池状态（隐式探测，不额外消耗上游）。"""
        async with self._lock:
            self._store.decr(_K_INFLIGHT + proxy)
            if status == OK:
                self._store.delete(_K_COOLDOWN + proxy)
                if proxy not in self._available:
                    self._available[proxy] = {"quality": ""}
            elif status == COOLDOWN:
                self._metrics["cooldown_reports"] += 1
                self._mark_cooldown(proxy)
            elif status == DEAD:
                self._metrics["dead_reports"] += 1
                self._available.pop(proxy, None)
                self._store.delete(_K_COOLDOWN + proxy)

    def metrics(self) -> dict:
        """代理池水位与健康指标（快照）。"""
        in_flight = sum(self._store.get(_K_INFLIGHT + p) for p in self._available)
        cooldown_n = len(self._store.keys(_K_COOLDOWN + "*"))
        return {
            "available": len(self._available),
            "in_flight": in_flight,
            "cooldown": cooldown_n,
            "target": self.settings.pool_target,
            "low_water": self.settings.pool_low_water,
            "refill_count": self._metrics["refill_count"],
            "cooldown_reports": self._metrics["cooldown_reports"],
            "dead_reports": self._metrics["dead_reports"],
            "no_proxy_errors": self._metrics["no_proxy_errors"],
        }

    # ---------- 池子维护 ----------

    def _low_water(self) -> bool:
        """可用代理低于低水位时触发补池。"""
        return len(self._available) < self.settings.pool_low_water

    def _mark_cooldown(self, proxy: str, quality: str = ""):
        """进入冷却：指数退避 60s*2^strike，上限 10min（store TTL）。"""
        val = self._store.get(_K_COOLDOWN + proxy)
        # 无记忆取数时按 strike=0 起步
        cur_strike = 0
        if val:
            try:
                cur_strike = int(str(val).split(":")[0])
            except (TypeError, ValueError):
                cur_strike = 0
        delay = min(60 * (2 ** cur_strike), 600)
        self._store.setex(
            _K_COOLDOWN + proxy, delay, f"{cur_strike + 1}:{quality}")
        self._available.pop(proxy, None)

    def _in_cooldown(self, proxy: str) -> bool:
        return self._store.exists(_K_COOLDOWN + proxy)

    async def _refill(self):
        """补池：按档位降级链取候选，并发做最小 chat 探测。

        只探测"当前既不在可用池、也不在冷却区"的新 IP，避免重复劳动。
        探测并发严格限制为 1-2，防止打爆免费额度/自我限流。
        """
        self._warm = True
        self._metrics["refill_count"] += 1
        chain = self.settings.quality_chain
        probe_conc = min(self.settings.probe_concurrency, 2)
        for quality in chain:
            quality = quality.strip()
            raw = await self._fetch_candidates(quality)
            if not raw:
                continue
            new_ip = [
                p for p in raw
                if p not in self._available and not self._in_cooldown(p)
            ]
            if not new_ip:
                continue
            good = await self._probe_all(new_ip, probe_conc, quality)
            for p in good:
                self._available[p] = {"quality": quality}
            if len(self._available) >= self.settings.pool_target:
                break

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
            if isinstance(data, str):        # proxy-pool 返回单个字符串的情况
                return [data] if data else []
            if not isinstance(data, list):
                return []
            return [str(p) for p in data if p]
        except (httpx.HTTPError, ValueError):
            return []

    async def _probe_all(self, proxies: list[str], concurrency: int,
                         quality: str) -> list[str]:
        """并发对候选 IP 做最小 chat 探测，返回通过（OK）的代理。"""
        sem = asyncio.Semaphore(concurrency)

        async def one(proxy: str) -> str | None:
            async with sem:
                status = await self._probe_fn(proxy)
                if status == OK:
                    return proxy
                if status == COOLDOWN:
                    self._mark_cooldown(proxy, quality)
                return None

        results = await asyncio.gather(*(one(p) for p in proxies))
        return [p for p in results if p]

    async def _probe(self, proxy: str) -> str:
        """单个 IP 的最小 chat 探测：能出对话才算可用。

        探测请求用最小 token 的 chat（max_tokens=1），只求拿到 200。
        429 → 限流（COOLDOWN）；连不上/其他 → DEAD。
        注：httpx 0.28 代理参数在 client 构造时传，每 IP 独立 client。
        """
        url = f"{self.settings.upstream_base}/chat/completions"
        proxy_url = f"http://{proxy}"
        body = {
            "model": self.settings.probe_model,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
        }
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url, timeout=self.settings.probe_timeout
            ) as client:
                r = await client.post(url, headers=zen_headers(), json=body)
        except httpx.HTTPError:
            return DEAD
        if r.status_code == 200:
            return OK
        if r.status_code == 429:
            return COOLDOWN
        return DEAD
