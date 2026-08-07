"""代理池取用模块（状态机版）。

职责：从 proxy-pool（同机 Docker 服务）按 stable 档位降级链取可用 IP，
每个 IP 只有通过"最小 chat 探测"才算可用，并维护 in-flight 计数与
冷却记忆，避免并发撞同一 IP、避免反复踩被限流的 IP。

关键约束（项目铁律）：
- 完全禁用本地 IP 直连——只走 proxy-pool 提供的 IP。
- 档位降级链：stable3 → stable2 → stable1，全无则停止（503）。
- "可用" = 刚验证能出对话（chat 最小请求 200），不是"能连上"。
- 主动探测频率尽量低：冷启动/池子告急才补，稳态靠真实请求回写状态。
"""

import asyncio
import time

import httpx

from app.config import Settings
from app.core.headers import zen_headers

# 探测/回写状态常量
OK = "ok"                 # 真实请求成功，可用
COOLDOWN = "cooldown"     # 限流，短期回避（指数退避）
DEAD = "dead"             # 连接失败，剔除


class NoProxyAvailable(Exception):
    """候选档位链全空，无可用代理。"""


class ProxyPool:
    def __init__(self, settings: Settings = None, client: httpx.AsyncClient = None,
                 probe_fn=None):
        self.settings = settings or Settings()
        self._lock = asyncio.Lock()
        # 测试时可注入 MockTransport 的 client；probe_fn 可注入探活函数
        self._http = client or httpx.AsyncClient(timeout=10)
        self._probe_fn = probe_fn or self._probe

        # 状态机
        self._available: dict[str, dict] = {}   # proxy -> {"quality": str}
        self._in_flight: dict[str, int] = {}    # proxy -> 在途请求数
        self._cooldown: dict[str, dict] = {}    # proxy -> {"until": ts, "strike": n}
        self._warm = False                      # 是否已做过首次补池

    # ---------- 对外接口 ----------

    async def get_proxy(self, exclude: set[str] | None = None) -> str:
        """取一个可用代理：优先冷启动补池/低水位补池，然后选最空闲的。

        exclude: 本次要避开的代理集合（当前请求已失败过的不再选）。
        返回 "ip:port"。无可用代理时抛 NoProxyAvailable。
        """
        exclude = exclude or set()
        async with self._lock:
            self._sweep_cooldown()
            if not self._warm or self._low_water():
                await self._refill()
            if not self._available:
                raise NoProxyAvailable("代理池无可用代理")
            candidates = [
                p for p in self._available
                if p not in exclude and self._in_flight.get(p, 0) < self.settings.max_in_flight
            ]
            if not candidates:
                # 全部在途满或被排除：取最空闲的兜底（避免本次请求饿死）
                candidates = [p for p in self._available if p not in exclude]
            if not candidates:
                raise NoProxyAvailable("代理池无可用代理（全部被排除）")
            pick = min(candidates, key=lambda p: self._in_flight.get(p, 0))
            self._in_flight[pick] = self._in_flight.get(pick, 0) + 1
            return pick

    def release(self, proxy: str):
        """请求结束（无论成败）归还 in-flight 计数。

        同步方法：in_flight 是普通 dict，单线程事件循环下增减原子。
        """
        n = self._in_flight.get(proxy, 0) - 1
        if n > 0:
            self._in_flight[proxy] = n
        else:
            self._in_flight.pop(proxy, None)

    async def report(self, proxy: str, status: str):
        """真实请求结果回写池状态（隐式探测，不额外消耗上游）。"""
        async with self._lock:
            self._in_flight.pop(proxy, None)
            if status == OK:
                info = self._cooldown.pop(proxy, None)
                if info is not None:
                    # 从冷却区恢复：保留原 quality，重新进入可用池
                    self._available[proxy] = {"quality": info.get("quality", "")}
                elif proxy not in self._available:
                    self._available[proxy] = {"quality": ""}
            elif status == COOLDOWN:
                self._mark_cooldown(proxy)
            elif status == DEAD:
                self._available.pop(proxy, None)
                self._cooldown.pop(proxy, None)

    # ---------- 池子维护 ----------

    def _low_water(self) -> bool:
        """可用代理低于低水位时触发补池。"""
        return len(self._available) < self.settings.pool_low_water

    def _sweep_cooldown(self):
        """冷却到期的代理重新放回可用池（不再额外探测，靠真实请求验证）。"""
        now = time.time()
        expired = [p for p, c in self._cooldown.items() if c["until"] <= now]
        for p in expired:
            info = self._cooldown.pop(p)
            if p not in self._available:
                self._available[p] = {"quality": info.get("quality", "")}

    def _mark_cooldown(self, proxy: str, quality: str = ""):
        """进入冷却：指数退避 60s*2^strike，上限 10min。"""
        cur = self._cooldown.get(proxy, {"until": 0, "strike": 0, "quality": quality})
        strike = cur.get("strike", 0)
        delay = min(60 * (2 ** strike), 600)
        self._cooldown[proxy] = {
            "until": time.time() + delay,
            "strike": strike + 1,
            "quality": quality,
        }
        self._available.pop(proxy, None)

    async def _refill(self):
        """补池：按档位降级链取候选，并发做最小 chat 探测。

        只探测"当前既不在可用池、也不在冷却区"的新 IP，避免重复劳动。
        探测并发严格限制为 1-2，防止打爆免费额度/自我限流。
        """
        self._warm = True
        chain = self.settings.quality_chain
        probe_conc = min(self.settings.probe_concurrency, 2)
        for quality in chain:
            quality = quality.strip()
            raw = await self._fetch_candidates(quality)
            if not raw:
                continue
            new_ip = [
                p for p in raw
                if p not in self._available and p not in self._cooldown
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
