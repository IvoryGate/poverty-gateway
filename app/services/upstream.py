"""上游转发服务：把 OpenAI 兼容请求通过代理转发到上游 zen。

只经代理转发（proxy_pool.get_proxy()），绝不直连。
"""

import time

import httpx

from app.config import Settings
from app.core.headers import zen_headers
from app.core.model_map import ModelMapper
from app.core.proxy_pool import (
    COOLDOWN,
    DEAD,
    OK,
    NoProxyAvailable,
    ProxyPool,
)
from app.core.redis_store import RedisStore

MAX_RETRIES = 5

_CB_FAIL = "cb:fail:"    # 失败计数窗口桶
_CB_OPEN = "cb:open"     # 熔断开关


class UpstreamClient:
    """转发器：把 /v1/chat/completions 请求经代理发到上游。"""

    def __init__(self, settings: Settings = None, store: RedisStore = None,
                 pool=None, models=None):
        self.settings = settings or Settings()
        self.pool = pool or ProxyPool(settings)
        self.models = models or ModelMapper(settings)
        self._store = store or RedisStore(settings)

    def _circuit_open(self) -> bool:
        """熔断是否打开：打开则快速失败，避免硬顶被限流的上游。"""
        return self._store.circuit_open(_CB_OPEN)

    def _register_failure(self):
        """记录一次上游失败（429/5xx），超过阈值触发熔断。"""
        now = int(time.time())
        bucket = f"{_CB_FAIL}{now // self.settings.cb_window}"
        n = self._store.circuit_register_failure(
            bucket, self.settings.cb_window + 5)
        if n >= self.settings.cb_failure_threshold:
            self._store.circuit_trip(_CB_OPEN, self.settings.cb_open_seconds)

    async def chat(self, public_model: str, payload: dict):
        """转发一次 chat completion（非流式）。

        public_model: 对外模型名。
        payload: 客户端传来的完整 body（model 会被替换为真实名）。

        失败时换代理重试，并把结果回写代理池（隐式探测）：
        - 429/上游5xx：与当前代理 IP 相关 → 换代理重试，标记冷却
        - 连接失败     → 剔除该代理，换一个重试
        - 其他 4xx     → 参数问题，重试无意义，直接抛
        """
        if self._circuit_open():
            raise UpstreamError("upstream circuit open (too many failures)")

        upstream_model = self.models.to_upstream(public_model)
        payload = dict(payload)
        payload["model"] = upstream_model

        last_err = None
        excluded: set[str] = set()
        for attempt in range(MAX_RETRIES):
            proxy = await self.pool.get_proxy(exclude=excluded)
            try:
                result = await self._request(proxy, payload)
                await self.pool.report(proxy, OK)
                return result
            except UpstreamError as e:
                last_err = e
                if "429" not in str(e) and not str(e).startswith("upstream 5"):
                    # 非限流且非上游 5xx（如 4xx 参数问题）重试无意义
                    await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                    raise
                self._register_failure()
                await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                # 连接/传输层失败：该代理不可用，剔除后换一个
                last_err = e
                await self.pool.report(proxy, DEAD)
                excluded.add(proxy)
                continue
        raise last_err or UpstreamError("no proxy retries left")

    async def chat_stream(self, public_model: str, payload: dict):
        """转发一次 chat completion（流式 SSE），逐块产出原始字节。

        返回异步生成器：每项是一块上游返回的原始字节（SSE 格式）。
        限流/5xx/连接失败在**首个块到达前**可换代理重试；
        一旦开始出流，错误只能透传/中断（响应头已定，无法重试）。
        """
        if self._circuit_open():
            raise UpstreamError("upstream circuit open (too many failures)")

        upstream_model = self.models.to_upstream(public_model)
        payload = dict(payload)
        payload["model"] = upstream_model
        payload["stream"] = True

        last_err = None
        excluded: set[str] = set()
        for attempt in range(MAX_RETRIES):
            proxy = await self.pool.get_proxy(exclude=excluded)
            started = False
            try:
                async for chunk in self._stream_request(proxy, payload):
                    started = True
                    yield chunk
                await self.pool.report(proxy, OK)
                return
            except UpstreamError as e:
                last_err = e
                if started:
                    # 已在出流中途失败：无法重试，标记状态后抛
                    await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                    raise
                if "429" not in str(e) and not str(e).startswith("upstream 5"):
                    await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                    raise
                self._register_failure()
                await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                last_err = e
                await self.pool.report(proxy, DEAD)
                if started:
                    raise
                excluded.add(proxy)
                continue
        raise last_err or UpstreamError("no proxy retries left")

    async def _stream_request(self, proxy: str, payload: dict):
        """对单个代理发起流式 POST，产出上游原始字节块。"""
        url = f"{self.settings.upstream_base}/chat/completions"
        proxy_url = f"http://{proxy}"
        timeout = httpx.Timeout(connect=15, read=300, write=30, pool=15)
        async with httpx.AsyncClient(
            timeout=timeout, proxy=proxy_url
        ) as client, client.stream(
            "POST", url, headers=zen_headers(), json=payload
        ) as r:
            if r.status_code >= 400:
                body = await r.aread()
                detail = body[:500].decode("utf-8", errors="replace")
                raise UpstreamError(
                    f"upstream {r.status_code}: {detail}",
                    retry_after=_parse_retry_after(r),
                )
            async for chunk in r.aiter_bytes():
                yield chunk

    async def _request(self, proxy: str, payload: dict) -> dict:
        url = f"{self.settings.upstream_base}/chat/completions"
        proxy_url = f"http://{proxy}"
        async with httpx.AsyncClient(
            timeout=120, proxy=proxy_url
        ) as client:
            r = await client.post(
                url, headers=zen_headers(), json=payload
            )
        if r.status_code >= 400:
            detail = r.text[:500]
            raise UpstreamError(
                f"upstream {r.status_code}: {detail}",
                retry_after=_parse_retry_after(r),
            )
        return r.json()

    async def health(self) -> dict:
        """网关自身健康状态：代理池有无可用 + 上游探活（走代理）。"""
        try:
            proxy = await self.pool.get_proxy()
        except NoProxyAvailable:
            return {"status": "degraded", "pool": "empty"}
        try:
            upstream_ok = await self._probe_upstream(proxy)
        finally:
            self.pool.release(proxy)
        return {
            "status": "ok" if upstream_ok else "degraded",
            "pool": "ok",
            "upstream": "ok" if upstream_ok else "unreachable",
        }

    async def _probe_upstream(self, proxy: str) -> bool:
        proxy_url = f"http://{proxy}"
        try:
            async with httpx.AsyncClient(
                timeout=15, proxy=proxy_url
            ) as client:
                r = await client.get(f"{self.settings.upstream_base}/models")
                return r.status_code in (200, 401, 403)
        except httpx.HTTPError:
            return False


class UpstreamError(Exception):
    """上游返回错误。"""

    def __init__(self, message: str, retry_after: int = 0):
        super().__init__(message)
        self.retry_after = retry_after


def _parse_retry_after(r) -> int:
    """解析 429 响应的 retry-after 头（秒）。无则返回 0。"""
    val = r.headers.get("retry-after")
    if not val:
        return 0
    try:
        return max(int(val.strip()), 0)
    except (TypeError, ValueError):
        return 0
