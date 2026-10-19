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
import json
import logging
import time

import httpx
import redis as redis_py

from app.config import Settings
from app.core.freetier import is_abuse_message
from app.core.headers import probe_payload, zen_headers
from app.core.redis_store import RedisStore

logger = logging.getLogger(__name__)

# 探测/回写状态常量
OK = "ok"                 # 真实请求成功，可用
COOLDOWN = "cooldown"     # 限流，短期回避（指数退避）
DEAD = "dead"             # 连接失败，剔除
ABUSE = "abuse"           # 上游 FreeTier abuse：重罚 + 按模型长期禁用

# Redis 键前缀
_K_INFLIGHT = "inflight:"   # in-flight 计数
_K_COOLDOWN = "cooldown:"   # 冷却区（值 = "strike:quality"，带 TTL）
_K_LATENCY = "latency:"     # 延迟估计（ms，EWMA，跨 worker 共享，TTL 6h）
_K_ABUSE = "abuse:"         # abuse 黑名单：abuse:<model>:<proxy>
_K_SNAPSHOT = "pool_snapshot"   # 可用池快照（JSON，TTL 24h）
_SNAPSHOT_TTL = 86400
_LATENCY_TTL = 6 * 3600
_ABUSE_TTL = 12 * 3600      # abuse 一次禁 12h（可被 env 覆盖）


def _as_int(raw) -> int:
    """store.get 返回字符串，安全转 int。"""
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


class NoProxyAvailable(Exception):
    """候选档位链全空，无可用代理。"""


class ProxyPool:
    def __init__(self, settings: Settings = None, client: httpx.AsyncClient = None,
                 probe_fn=None, store: RedisStore = None):
        self.settings = settings or Settings()
        self._lock = asyncio.Lock()
        self._refill_lock = asyncio.Lock()      # 补池并入专用锁（避免与主锁死锁）
        self._refill_task: asyncio.Task | None = None   # 后台补池任务（单飞）
        # 测试时可注入 MockTransport 的 client；probe_fn 可注入探活函数
        self._http = client or httpx.AsyncClient(timeout=10)
        self._probe_fn = probe_fn or self._probe
        self._store = store or RedisStore(settings)

        # 状态机（_available 进程内；in-flight/冷却走 store）
        self._available: dict[str, dict] = {}   # proxy -> {"quality": str}
        self._warm = False                      # 是否已做过首次补池
        self._last_refill = 0.0                 # 上次补池开始时间（节流用）
        # 延迟表：proxy -> EWMA 耗时(ms)，真实请求/探测回写，取号按此排序
        self._latency: dict[str, float] = {}    # proxy -> 最近平均耗时 ms

        # 观测指标
        self._metrics = {
            "refill_count": 0,       # 补池次数
            "cooldown_reports": 0,   # 真实请求 429 回写次数
            "dead_reports": 0,       # 真实请求连接失败回写次数
            "abuse_reports": 0,      # 上游 FreeTier abuse 回写次数
            "no_proxy_errors": 0,    # 取号失败次数
            "errors": {              # 错误构成：429 / 5xx / 4xx / timeout / conn
                "429": 0,
                "5xx": 0,
                "4xx": 0,
                "timeout": 0,
                "conn": 0,
            },
        }
        # 探测出的 retry-after（proxy -> 秒），供 _mark_cooldown 精确冷却
        self._probe_retry: dict[str, int] = {}

    # ---------- 对外接口 ----------

    async def get_proxy(self, exclude: set[str] | None = None,
                        model: str | None = None) -> str:
        """取一个可用代理：优先冷启动补池/低水位补池，然后选最空闲的。

        exclude: 本次要避开的代理集合（当前请求已失败过的不再选）。
        model: 本次请求的对外模型；用于避开该模型被 abuse 的 IP。
        返回 "ip:port"。无可用代理时抛 NoProxyAvailable。
        """
        exclude = exclude or set()
        async with self._lock:
            if not self._warm:
                await self._refill()
            elif self._low_water():
                self._schedule_refill()
            if not self._available:
                self._metrics["no_proxy_errors"] += 1
                raise NoProxyAvailable("代理池无可用代理")
            base_candidates = [
                p for p in self._available
                if p not in exclude
                and _as_int(self._store.get(_K_INFLIGHT + p)) < self.settings.max_in_flight
                and not self._store.exists(_K_COOLDOWN + p)
                and not self._in_abuse(p, model)
            ]
            if not base_candidates:
                # 全部在途满/冷却/被排除：取最空闲且未冷却的兜底（宁缺毋滥）
                base_candidates = [
                    p for p in self._available
                    if p not in exclude
                    and not self._store.exists(_K_COOLDOWN + p)
                    and not self._in_abuse(p, model)
                ]
            if not base_candidates:
                self._metrics["no_proxy_errors"] += 1
                raise NoProxyAvailable("代理池无可用代理（全部被排除）")
            # 按 zen 命中历史分档：正分（对 zen 成功过）优先，负分兜底。
            # 冷却恢复的 IP 多数仍连不上 zen，不能让它们和好 IP 同权重竞争。
            positive = [p for p in base_candidates if self._zen_score(p) > 0]
            if positive:
                candidates = positive
            else:
                candidates = base_candidates
            # 延迟上限过滤（阶段 17 移植）：已知延迟超过 max_latency_ms 的
            # 直接剔除，防止慢/挂代理被选中拖垮请求（回退版缺此逻辑，池里
            # 未测速/超慢 IP 全被挑中导致 50s 超时）。
            # 未采样延迟按 inf 处理（而非 0）：无样本=不知道快慢，不应插队。
            fast = [
                p for p in candidates
                if self._latency_of(p) is None
                or self._latency_of(p) <= self.settings.max_latency_ms
            ]
            if fast:
                candidates = fast
            pick = min(
                candidates,
                key=lambda p: (
                    _as_int(self._store.get(_K_INFLIGHT + p)),   # 优先最空闲
                    self._latency_of(p) or float("inf"),          # 同空闲取延迟低
                ),
            )
            # TTL 兜底：任何泄漏（异常/未归还）的计数 10 分钟后自动清除
            self._store.incr(_K_INFLIGHT + pick, ttl=600)
            return pick

    def release(self, proxy: str):
        """请求结束（无论成败）归还 in-flight 计数。"""
        self._store.decr(_K_INFLIGHT + proxy)

    async def report(self, proxy: str, status: str, cooldown_seconds: int = 0,
                     latency_ms: float = 0.0, model: str | None = None):
        """真实请求结果回写池状态（隐式探测，不额外消耗上游）。

        cooldown_seconds: 429 时上游返回的解封秒数（retry-after），
        有则用它精确设定冷却时长，到期第一时间复活。
        latency_ms: 本次请求总耗时（ms），成功时并入 EWMA 延迟表，
        取号按此排序优先选快 IP。
        model: 本次请求的模型；abuse 黑名单按模型隔离，避免 GLM 的
        403 把 space-bunny 也能用的 IP 一起打死。
        """
        async with self._lock:
            self._store.decr(_K_INFLIGHT + proxy)
            if status == OK:
                self._store.delete(_K_COOLDOWN + proxy)
                self._clear_abuse(proxy, model)
                if latency_ms > 0:
                    self._update_latency(proxy, latency_ms)
                if proxy not in self._available:
                    self._available[proxy] = {"quality": ""}
                self._bump_zen_score(proxy, +1)
            elif status == COOLDOWN:
                self._metrics["cooldown_reports"] += 1
                self._mark_cooldown(proxy, cooldown_seconds=cooldown_seconds)
            elif status == DEAD:
                self._metrics["dead_reports"] += 1
                self._available.pop(proxy, None)
                self._store.delete(_K_COOLDOWN + proxy)
                self._bump_zen_score(proxy, -1)
            elif status == ABUSE:
                self._metrics["abuse_reports"] += 1
                self._mark_abuse(proxy, model)

    def _update_latency(self, proxy: str, latency_ms: float):
        """EWMA 更新某 IP 的延迟估计，平滑抖动（α=0.3 偏重近期）。

        存 Redis（key=latency:<proxy>, TTL 6h）跨 worker 共享——多 worker
        各持内存表会导致延迟过滤失效（另一个 worker 的采样看不到）。
        """
        old = self._latency_of(proxy)
        if old is None:
            new = float(latency_ms)
        else:
            new = 0.3 * float(latency_ms) + 0.7 * old
        try:
            self._store.setex(_K_LATENCY + proxy, _LATENCY_TTL, str(round(new, 1)))
        except Exception:
            self._latency[proxy] = new

    def _latency_of(self, proxy: str) -> float | None:
        """读某 IP 的延迟估计（ms）。先 Redis（跨 worker 共享）后内存。"""
        try:
            raw = self._store.get(_K_LATENCY + proxy)
            if raw is not None:
                return float(raw)
        except Exception:
            pass
        return self._latency.get(proxy)

    def note_error(self, kind: str):
        """记一次错误分类（429/5xx/4xx/timeout/conn），供面板拆解错误率。"""
        bucket = self._metrics["errors"]
        if kind in bucket:
            bucket[kind] += 1

    def metrics(self) -> dict:
        """代理池水位与健康指标（快照）。"""
        in_flight = sum(_as_int(self._store.get(_K_INFLIGHT + p)) for p in self._available)
        cooldown_n = len(self._store.keys(_K_COOLDOWN + "*"))
        abuse_n = len(self._store.keys(_K_ABUSE + "*"))
        # 真实可用 = 不在冷却中的占位（冷却的不算可用）
        usable = self._usable_count()
        lat = self._store.keys(_K_LATENCY + "*")
        lat_vals = []
        for k in lat:
            v = self._store.get(k)
            if v is not None:
                try:
                    lat_vals.append(float(v))
                except (TypeError, ValueError):
                    pass
        lat_vals.sort()
        latency_median = lat_vals[len(lat_vals) // 2] if lat_vals else 0.0
        return {
            "available": usable,
            "pool_size": len(self._available),
            "in_flight": in_flight,
            "cooldown": cooldown_n,
            "abuse": abuse_n,
            "target": self.settings.pool_target,
            "low_water": self.settings.pool_low_water,
            "refill_count": self._metrics["refill_count"],
            "cooldown_reports": self._metrics["cooldown_reports"],
            "dead_reports": self._metrics["dead_reports"],
            "abuse_reports": self._metrics["abuse_reports"],
            "no_proxy_errors": self._metrics["no_proxy_errors"],
            "latency_median_ms": round(latency_median, 1),
            "latency_sampled": len(lat),
            "errors": dict(self._metrics["errors"]),
        }

    # ---------- 池子维护 ----------

    def _low_water(self) -> bool:
        """可用代理低于低水位时触发补池。

        只统计"不在冷却中"的 IP——冷却中的占位不算可用，否则冷却的 IP
        一直占着 _available 的位置导致计数虚高、不补池，真实可用持续缩水
        （面板 available 看着多，实际能用的一大半在冷却）。
        """
        return self._usable_count() < self.settings.pool_low_water

    def _usable_count(self) -> int:
        """真实可用数：_available 里不在冷却/全局 abuse 中的 IP 数。"""
        return sum(
            1 for p in self._available
            if not self._store.exists(_K_COOLDOWN + p)
            and not self._in_abuse(p, None))

    def save_snapshot(self):
        """把当前可用池快照写入 Redis（优雅停机/定时落盘用）。

        cooldown/in-flight 是带 TTL 的临时状态，随 Redis 天然保存，无需入快照。
        多 worker 各自持有独立池，保存时与 Redis 已有快照取"更大者"，
        避免池空/池小的 worker 覆盖掉池大的 worker 写的好快照。
        """
        if not self._available:
            print("snapshot: skip (empty pool)", flush=True)
            return
        import json
        if len(self._available) < self._existing_snapshot_size():
            print("snapshot: skip (existing bigger)", flush=True)
            return
        self._store.snapshot_set(
            _K_SNAPSHOT, _SNAPSHOT_TTL, json.dumps(self._available))
        print(f"snapshot: saved {len(self._available)} proxies", flush=True)

    def _existing_snapshot_size(self) -> int:
        raw = self._store.snapshot_get(_K_SNAPSHOT)
        if not raw:
            return 0
        import json
        try:
            parsed = json.loads(raw)
            return len(parsed) if isinstance(parsed, dict) else 0
        except (ValueError, TypeError):
            return 0

    def load_snapshot(self):
        """启动时从 Redis 加载上次可用池，跳过冷启动重探测。

        加载的 IP 若已被限流（cooldown 键还在）会在 get_proxy 过滤掉；
        Redis 不可用/无快照则从空池开始正常补池。
        """
        raw = self._store.snapshot_get(_K_SNAPSHOT)
        if not raw:
            print("snapshot: none to load", flush=True)
            return
        import json
        try:
            parsed = json.loads(raw)
        except (ValueError, TypeError):
            return
        if not isinstance(parsed, dict):
            return
        loaded = {str(k): v for k, v in parsed.items()}
        if loaded:
            self._available = loaded
            self._warm = True

    def _mark_cooldown(self, proxy: str, quality: str = "",
                       cooldown_seconds: int = 0):
        """进入冷却：优先用上游 retry-after 解封时间，否则指数退避。

        冷却中的 IP 保留在 _available（避免池子缩水），但 get_proxy 会过滤
        冷却键；冷却键过期后 IP 自动恢复可选，无需额外回池逻辑。
        """
        if cooldown_seconds and cooldown_seconds > 0:
            delay = min(cooldown_seconds, 86400)  # 上限 24h 防异常值
            self._store.setex(_K_COOLDOWN + proxy, delay, f"0:{quality}")
        else:
            val = self._store.get(_K_COOLDOWN + proxy)
            cur_strike = 0
            if val:
                try:
                    cur_strike = int(str(val).split(":")[0])
                except (TypeError, ValueError):
                    cur_strike = 0
            delay = min(60 * (2 ** cur_strike), 600)
            self._store.setex(
                _K_COOLDOWN + proxy, delay, f"{cur_strike + 1}:{quality}")
        if proxy not in self._available:
            self._available[proxy] = {"quality": quality}

    def _in_cooldown(self, proxy: str) -> bool:
        return self._store.exists(_K_COOLDOWN + proxy)

    @staticmethod
    def _abuse_key(proxy: str, model: str | None) -> str:
        return f"{_K_ABUSE}{model or '*'}:{proxy}"

    def _in_abuse(self, proxy: str, model: str | None = None) -> bool:
        """该 IP 是否被全局或指定模型 abuse 禁用。"""
        if self._store.exists(self._abuse_key(proxy, None)):
            return True
        return bool(model and self._store.exists(self._abuse_key(proxy, model)))

    def _mark_abuse(self, proxy: str, model: str | None = None,
                    quality: str = ""):
        """FreeTier abuse：直接移出可用池 + 按模型禁用 12h + 重扣信任分。

        abuse 是上游对出口 IP 的明确拒绝，比连接失败严重得多：
        - 立即从 _available 移除，当前请求换 IP；
        - 写 abuse:<model>:<proxy>，TTL 内该模型取号直接跳过；
        - zenhit -10，即使 TTL 到期也不会马上被当成好 IP。
        """
        self._available.pop(proxy, None)
        self._store.delete(_K_COOLDOWN + proxy)
        self._store.setex(
            self._abuse_key(proxy, model),
            self.settings.abuse_ttl,
            str(int(time.time())),
        )
        self._bump_zen_score(proxy, -10)

    def _clear_abuse(self, proxy: str, model: str | None = None):
        """真实请求成功：清掉该模型的 abuse 记录（说明 IP 已恢复）。"""
        self._store.delete(self._abuse_key(proxy, model))

    _K_ZEN_SCORE = "zenhit:"   # 候选 IP 对 zen 的历史表现计数（score）
    _ZEN_BAD = -3              # score 低于此视为"对 zen 长期不可用"，补池跳过

    def _bump_zen_score(self, proxy: str, delta: int):
        """记忆候选 IP 对 zen 的历史表现：成功 +1 / 失败 -1。

        补池据此排序：历史表现好的优先探测；连续失败多次的直接跳过，
        避免反复探测对 zen 无用的死代理（它们通用存活但对 zen 连不上/超时）。
        score 带 TTL（默认 6h），过期重置，给"死代理复活"留机会。
        """
        key = self._K_ZEN_SCORE + proxy
        cur = 0
        try:
            cur = int(self._store.get(key) or 0)
        except (TypeError, ValueError):
            cur = 0
        self._store.setex(key, 6 * 3600, str(cur + delta))

    def _zen_score(self, proxy: str) -> int:
        try:
            return int(self._store.get(self._K_ZEN_SCORE + proxy) or 0)
        except (TypeError, ValueError):
            return 0

    def _schedule_refill(self):
        """低水位时触发后台补池（非阻塞，防 get_proxy 长时间持锁）。

        单飞保护：同一时刻只有一个补池任务在跑。
        最小间隔：两次补池之间至少隔 refill_interval 秒，避免每个请求都
        触发一轮全网探测（免费代理/上游都扛不住）。
        """
        now = time.monotonic()
        if now - self._last_refill < self.settings.refill_interval:
            return
        if self._refill_task is not None and not self._refill_task.done():
            return  # 已有补池在跑
        task = asyncio.create_task(self._refill())
        task.add_done_callback(self._refill_done)
        self._refill_task = task

    def warm_async(self):
        """启动时在后台补池：不阻塞第一个请求，先把能用的 IP 尽量收进来。"""
        self._warm = True
        self._schedule_refill()

    def _refill_done(self, task: asyncio.Task):
        """补池结束回调：消费异常，避免 'Task exception was never retrieved' 刷屏。"""
        if not task.cancelled() and task.exception() is not None:
            logger.warning("后台补池异常: %r", task.exception())

    async def _refill(self):
        """补池：按档位降级链取候选，分批探测直到真实可用达标。

        只探测"当前既不在可用池、也不在冷却区"的新 IP，避免重复劳动。
        候选来自 Redis 全量（可能上百个），按 probe_batch 分片循环探测，
        直到"非冷却的可用 IP"达到 pool_target，或候选耗尽。

        注意达标判断用"真实可用数"（排除冷却占位）：冷却中的 IP 进过池
        就占着 _available 的位置，用它判断会提前停，导致可用数长期不足。

        探测阶段不持主锁（get_proxy 用），只在并入 _available 时短暂取锁。
        """
        self._warm = True
        self._last_refill = time.monotonic()
        self._metrics["refill_count"] += 1
        chain = self.settings.quality_chain
        probe_conc = min(self.settings.probe_concurrency, 16)
        batch = max(self.settings.probe_batch, 20)
        for quality in chain:
            quality = quality.strip()
            raw = await self._fetch_candidates(quality)
            if not raw:
                continue
            # 过滤出需探测的新 IP：不在可用池、不在冷却、不被 abuse 禁用。
            # 负分太低的 IP 直接跳过（_ZEN_BAD），避免反复探测已知死货。
            pending = [
                p for p in raw
                if p not in self._available
                and not self._in_cooldown(p)
                and not self._in_abuse(p, self.settings.probe_model)
                and self._zen_score(p) > self._ZEN_BAD
            ]
            pending.sort(key=self._zen_score, reverse=True)
            pos = 0
            while pos < len(pending):
                chunk = pending[pos:pos + batch]
                pos += batch
                good = await self._probe_all(chunk, probe_conc, quality)
                if good:
                    async with self._refill_lock:
                        for p in good:
                            self._available[p] = {"quality": quality}
                if self._usable_count() >= self.settings.pool_target:
                    break
            if self._usable_count() >= self.settings.pool_target:
                break

    async def _fetch_candidates(self, quality: str) -> list[str]:
        """取候选 IP：优先 proxy-pool 的 use_proxy:https（目标可达），
        再回退到全量 hash / /get。

        为什么优先 https：网关只走 HTTPS 上游，proxy-pool 的 https 集合已经
        验证过"能连到 opencode.ai"。全量 hash 里大量 HTTP-only 代理永远
        不可能转发成功，探测它们纯属浪费（也是"池子长期补不满"的原因之一）。
        """
        ips = await asyncio.to_thread(self._read_https_from_redis)
        if not ips:
            ips = await asyncio.to_thread(self._read_all_from_redis)
        if not ips:
            ips = await self._fetch_from_api(quality)
        return ips

    def _read_https_from_redis(self) -> list[str]:
        """从 proxy-pool Redis db0 读 https 候选，按 score/延迟排序。"""
        if not self.settings.redis_url:
            return []
        try:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(self.settings.redis_url)
            clean = urlunparse(parsed._replace(path="/0"))
            r = redis_py.Redis.from_url(
                clean, decode_responses=True, socket_connect_timeout=3,
                socket_timeout=3,
            )
            members = set(r.smembers("use_proxy:https") or [])
            if not members:
                return []
            meta = r.hgetall("use_proxy") or {}
            ordered = []
            for proxy in members:
                raw = meta.get(proxy)
                score, latency = 0, float("inf")
                if raw:
                    try:
                        item = json.loads(raw)
                    except (TypeError, ValueError):
                        item = {}
                    if item.get("tampered"):
                        continue
                    if item.get("last_status") is False:
                        continue
                    try:
                        score = int(item.get("score") or 0)
                    except (TypeError, ValueError):
                        score = 0
                    if item.get("latency_ms") is not None:
                        try:
                            latency = float(item["latency_ms"])
                        except (TypeError, ValueError):
                            latency = float("inf")
                ordered.append((-score, latency, proxy))
            ordered.sort()
            return [proxy for _, _, proxy in ordered]
        except (redis_py.RedisError, ImportError, ValueError):
            return []

    def _read_all_from_redis(self) -> list[str]:
        """从 proxy-pool Redis（db0 use_proxy）读全量 IP。

        用 urllib 解析 db 号替换为 0（proxy-pool 数据在 db0），避免字符串
        replace 误伤 host 里的数字。
        """
        try:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(self.settings.redis_url)
            # 强制 db=0（proxy-pool 数据所在库）
            clean = urlunparse(parsed._replace(path="/0"))
            r = redis_py.Redis.from_url(
                clean, decode_responses=True, socket_connect_timeout=3,
                socket_timeout=3,
            )
            keys = r.hkeys("use_proxy") or []
            return [str(k) for k in keys if k]
        except (redis_py.RedisError, ImportError, ValueError):
            return []

    async def _fetch_from_api(self, quality: str) -> list[str]:
        """回退：proxy-pool /get 批量取候选。"""
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
                    self._bump_zen_score(proxy, +1)
                    # 探测成功必须清掉残留的 cooldown key：另一 worker 曾对
                    # 同一 IP 探测 429 打过冷却，不清的话本 worker 的 _available
                    # 里虽有它，get_proxy 仍会被 cooldown 过滤掉（跨 worker 撕裂）。
                    self._store.delete(_K_COOLDOWN + proxy)
                    return proxy
                if status == COOLDOWN:
                    # 探测 429：用上游 retry-after（若有）精确冷却，避免反复探测
                    self._mark_cooldown(
                        proxy, quality,
                        cooldown_seconds=self._probe_retry.get(proxy, 0))
                elif status == ABUSE:
                    # 上游明确 abuse：按探测模型长期禁用，不再反复探测
                    self._mark_abuse(proxy, self.settings.probe_model, quality)
                else:
                    # DEAD/超时/连不上：记忆负面表现，下次补池降低优先级
                    self._bump_zen_score(proxy, -1)
                return None

        results = await asyncio.gather(*(one(p) for p in proxies))
        return [p for p in results if p]

    async def _probe(self, proxy: str) -> str:
        """单个 IP 的最小 chat 探测：能出对话才算可用。

        探测必须伪装成 opencode 的 agent 请求（stream=true + read 工具），
        否则 2026-10-14 起上游对所有 free 模型返回 403 FreeTierError，
        所有候选 IP 都会被误判成 DEAD（阶段 19 的池子饿死根因）。
        只用 max_tokens=1，拿到响应头即可，不读完整输出。

        429 → 限流（COOLDOWN），并把 retry-after 记录到 _probe_retry；
        连不上/其他 → DEAD；403 若是"请求形状不对"而非 IP 滥用，记日志，
        方便下次上游改校验时第一时间发现。
        探测同时记录耗时到 EWMA 延迟表，冷启动时就有延迟排序依据，
        无需额外请求。
        注：httpx 0.28 代理参数在 client 构造时传，每 IP 独立 client。
        """
        url = f"{self.settings.upstream_base}/chat/completions"
        proxy_url = f"http://{proxy}"
        body = probe_payload(self.settings.probe_model)
        t0 = time.monotonic()
        retry_after = 0
        status = 0
        abuse = False
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url, timeout=self.settings.probe_timeout,
                follow_redirects=False,
            ) as client, client.stream(
                "POST", url, headers=zen_headers(), json=body,
            ) as r:
                status = r.status_code
                if status == 429:
                    try:
                        retry_after = max(int(
                            r.headers.get("retry-after", "0") or 0), 0)
                    except (TypeError, ValueError):
                        retry_after = 0
                elif status == 403:
                    raw = await r.aread()
                    detail = raw[:300].decode("utf-8", errors="replace")
                    if is_abuse_message(detail):
                        abuse = True
                    else:
                        logger.warning(
                            "探测被上游形状校验拒绝（请求头/body 可能又变了）:"
                            " %s", detail)
        except httpx.HTTPError:
            return DEAD
        elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
        if status == 200:
            self._update_latency(proxy, elapsed_ms)
            return OK
        if status == 429:
            self._probe_retry[proxy] = retry_after
            return COOLDOWN
        if abuse:
            return ABUSE
        return DEAD
