"""上游转发服务：把 OpenAI 兼容请求通过代理转发到上游 zen。

只经代理转发（proxy_pool.get_proxy()），绝不直连。
"""

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

MAX_RETRIES = 5


class UpstreamClient:
    """转发器：把 /v1/chat/completions 请求经代理发到上游。"""

    def __init__(self, settings: Settings = None):
        self.settings = settings or Settings()
        self.pool = ProxyPool(settings)
        self.models = ModelMapper(settings)

    async def chat(self, public_model: str, payload: dict):
        """转发一次 chat completion（非流式）。

        public_model: 对外模型名。
        payload: 客户端传来的完整 body（model 会被替换为真实名）。

        失败时换代理重试，并把结果回写代理池（隐式探测）：
        - 429/上游5xx：与当前代理 IP 相关 → 换代理重试，标记冷却
        - 连接失败     → 剔除该代理，换一个重试
        - 其他 4xx     → 参数问题，重试无意义，直接抛
        """
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
                    await self.pool.report(proxy, COOLDOWN)
                    raise
                await self.pool.report(proxy, COOLDOWN)
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                # 连接/传输层失败：该代理不可用，剔除后换一个
                last_err = e
                await self.pool.report(proxy, DEAD)
                excluded.add(proxy)
                continue
        raise last_err or UpstreamError("no proxy retries left")

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
            raise UpstreamError(f"upstream {r.status_code}: {detail}")
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
