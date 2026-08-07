"""上游转发服务：把 OpenAI 兼容请求通过代理转发到上游 zen。

只经代理转发（proxy_pool.get_proxy()），绝不直连。
"""

import uuid

import httpx

from app.config import Settings
from app.core.model_map import ModelMapper
from app.core.proxy_pool import NoProxyAvailable, ProxyPool


def _zen_headers() -> dict[str, str]:
    """构造上游要求的请求头。

    说明：上游按请求头做 IP 级限流识别；session/request id 每次唯一，
    避免被当作重复请求。这些头为公共端点约定（无认证、无 key）。
    """
    return {
        "Authorization": "Bearer public",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.15.0",
        "x-api-key": "public",
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": f"msg_{uuid.uuid4().hex[:20]}",
        "x-opencode-session": f"ses_{uuid.uuid4().hex[:20]}",
    }


class UpstreamClient:
    """转发器：把 /v1/chat/completions 请求经代理发到上游。"""

    def __init__(self, settings: Settings = None):
        self.settings = settings or Settings()
        self.pool = ProxyPool(settings)
        self.models = ModelMapper(settings)

    async def chat(self, public_model: str, payload: dict):
        """转发一次 chat completion（非流式）。

        public_model: 对外伪名。
        payload: 客户端传来的完整 body（model 会被替换为真实名）。

        限流/失败时自动换代理重试（免费代理常被限流，需轮换）。
        """
        upstream_model = self.models.to_upstream(public_model)
        payload = dict(payload)
        payload["model"] = upstream_model

        last_err = None
        excluded: set[str] = set()
        for _ in range(3):
            try:
                proxy = await self.pool.get_proxy(exclude=excluded)
                return await self._request(proxy, payload)
            except UpstreamError as e:
                last_err = e
                if "429" not in str(e) and not str(e).startswith("upstream 5"):
                    # 非限流且非上游 5xx（如 4xx 参数问题）重试无意义
                    raise
                # 限流或上游 5xx：与当前代理 IP 相关，换代理重试
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
                url, headers=_zen_headers(), json=payload
            )
        if r.status_code >= 400:
            detail = r.text[:500]
            raise UpstreamError(f"upstream {r.status_code}: {detail}")
        return r.json()

    async def health(self) -> dict:
        """网关自身健康状态：代理池有无可用 + 上游探活（走代理）。"""
        try:
            proxy = await self.pool.get_proxy()
            upstream_ok = await self._probe_upstream(proxy)
        except NoProxyAvailable:
            return {"status": "degraded", "pool": "empty"}
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
