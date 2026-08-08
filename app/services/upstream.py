"""上游转发服务：把 OpenAI 兼容请求通过代理转发到上游 zen。

只经代理转发（proxy_pool.get_proxy()），绝不直连。
"""

import json
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
        """
        return await self._forward(
            "chat/completions", public_model, payload, force_stream=False)

    async def chat_stream(self, public_model: str, payload: dict):
        """转发一次 chat completion（流式 SSE），逐块产出原始字节。

        返回异步生成器：每项是一块上游返回的原始字节（SSE 格式）。
        限流/5xx/连接失败在**首个块到达前**可换代理重试；
        一旦开始出流，错误只能透传/中断（响应头已定，无法重试）。
        """
        async for chunk in self._forward_stream(
                "chat/completions", public_model, payload):
            yield chunk

    async def responses(self, public_model: str, payload: dict):
        """转发一次 Responses API 请求（非流式，Codex 用）。"""
        return await self._forward(
            "responses", public_model, _sanitize_responses(payload),
            force_stream=False)

    async def responses_stream(self, public_model: str, payload: dict):
        """转发一次 Responses API 请求（流式，Codex 用）。"""
        async for chunk in self._forward_stream(
                "responses", public_model, _sanitize_responses(payload)):
            yield chunk

    async def _forward(self, endpoint: str, public_model: str, payload: dict,
                       force_stream: bool = False):
        """通用非流式透传：经代理池转发到上游指定端点。

        endpoint: 上游路径（如 "chat/completions" 或 "responses"）。
        payload: 客户端 body（model 会被替换为真实名）。
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
            t0 = time.monotonic()
            try:
                result = await self._request(endpoint, proxy, payload)
                elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                await self.pool.report(proxy, OK, latency_ms=elapsed_ms)
                if isinstance(result, dict) and "model" in result:
                    result["model"] = public_model
                return result
            except UpstreamError as e:
                last_err = e
                self.pool.note_error(_classify(e.status_code))
                if "429" not in str(e) and not str(e).startswith("upstream 5"):
                    # 非限流且非上游 5xx（如 4xx 参数问题）重试无意义
                    await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                    raise
                self._register_failure()
                await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after)
                excluded.add(proxy)
                continue
            except httpx.TimeoutException as e:
                # 传输超时：代理慢/挂，剔除换一个（记入 timeout 桶）
                last_err = e
                self.pool.note_error("timeout")
                await self.pool.report(proxy, DEAD)
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                # 连接/传输层失败：该代理不可用，剔除后换一个
                last_err = e
                self.pool.note_error("conn")
                await self.pool.report(proxy, DEAD)
                excluded.add(proxy)
                continue
        raise last_err or UpstreamError("no proxy retries left")

    async def _forward_stream(self, endpoint: str, public_model: str,
                              payload: dict):
        """通用流式透传：经代理池转发，逐块产出原始字节。"""
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
            t0 = time.monotonic()
            try:
                if endpoint == "responses":
                    gen = self._responses_stream_request(proxy, payload)
                else:
                    gen = self._stream_request(endpoint, proxy, payload)
                gen = _rewrite_model_bytes(gen, upstream_model, public_model)
                async for chunk in gen:
                    if not started:
                        started = True
                        # 首字节延迟 = 上游响应快慢的核心信号，记入延迟表
                        ttfb = round((time.monotonic() - t0) * 1000, 1)
                        await self.pool.report(proxy, OK, latency_ms=ttfb)
                    yield chunk
                if not started:
                    await self.pool.report(proxy, OK)
                return
            except UpstreamError as e:
                last_err = e
                self.pool.note_error(_classify(e.status_code))
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
            except httpx.TimeoutException as e:
                last_err = e
                self.pool.note_error("timeout")
                await self.pool.report(proxy, DEAD)
                if started:
                    raise
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                last_err = e
                self.pool.note_error("conn")
                await self.pool.report(proxy, DEAD)
                if started:
                    raise
                excluded.add(proxy)
                continue
        raise last_err or UpstreamError("no proxy retries left")

    async def _request(self, endpoint: str, proxy: str, payload: dict) -> dict:
        url = f"{self.settings.upstream_base}/{endpoint}"
        proxy_url = f"http://{proxy}"
        timeout = httpx.Timeout(connect=15, read=300, write=30, pool=15)
        async with httpx.AsyncClient(
            timeout=timeout, proxy=proxy_url
        ) as client:
            r = await client.post(
                url, headers=zen_headers(), json=payload
            )
        if r.status_code >= 400:
            detail = r.text[:500]
            raise UpstreamError(
                f"upstream {r.status_code}: {detail}",
                retry_after=_parse_retry_after(r),
                status_code=r.status_code,
            )
        return r.json()

    async def _stream_request(self, endpoint: str, proxy: str, payload: dict):
        """对单个代理发起流式 POST，产出上游原始字节块。"""
        url = f"{self.settings.upstream_base}/{endpoint}"
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
                    status_code=r.status_code,
                )
            async for chunk in r.aiter_bytes():
                yield chunk

    async def _responses_stream_request(self, proxy: str, payload: dict):
        """Responses 流式透传 + 补齐 OpenAI 标准事件。

        zen 只返回 delta + completed，Codex 的 SDK 需要 response.created /
        output_item.added / output_item.done 才能组装输出。这里解析 SSE 流，
        在合适位置注入缺失事件。
        """
        url = f"{self.settings.upstream_base}/responses"
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
                    status_code=r.status_code,
                )
            state = {"started": False, "rid": None, "item_id": None}
            async for event in _iter_sse_events(r):
                # 合成补齐事件并格式化为 SSE 字节输出（每个请求独立状态）
                for name, obj in _inject_responses_events(event, state):
                    yield _sse_event_bytes(name, obj)

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

    def __init__(self, message: str, retry_after: int = 0, status_code: int = 0):
        super().__init__(message)
        self.retry_after = retry_after
        self.status_code = status_code


def _parse_retry_after(r) -> int:
    """解析 429 响应的 retry-after 头（秒）。无则返回 0。"""
    val = r.headers.get("retry-after")
    if not val:
        return 0
    try:
        return max(int(val.strip()), 0)
    except (TypeError, ValueError):
        return 0


def _classify(status_code: int) -> str:
    """把 HTTP 状态码归为错误桶：429 / 5xx / 4xx。"""
    if status_code == 429:
        return "429"
    if 500 <= status_code < 600:
        return "5xx"
    return "4xx"


def _sanitize_responses(payload: dict) -> dict:
    """过滤 Responses 请求里 zen 不支持的工具。

    Codex 会发 OpenAI 标准的 web_search 工具（无 function.name），
    zen 上游解析会 400（tools[i].function 缺 name）。剔除 web_search
    （及其它非 function/namespace 类型），保留可用的工具。
    """
    payload = dict(payload)
    tools = payload.get("tools")
    if isinstance(tools, list):
        kept = [t for t in tools
                if isinstance(t, dict) and t.get("type") in ("function", "namespace")]
        payload["tools"] = kept
    return payload


async def _iter_sse_events(r):
    """把流式响应按 SSE 帧切分，逐个 yield (event_type, data_dict)。

    兼容 zen 的 `event: xxx` + `data: {json}` 格式，以及裸 `data:` 格式。
    """
    buf = b""
    async for chunk in r.aiter_bytes():
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            text = frame.decode("utf-8", errors="replace")
            event_type = None
            data_line = None
            for line in text.splitlines():
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data_line = line[5:].strip()
            if data_line and data_line != "[DONE]":
                try:
                    obj = json.loads(data_line)
                except json.JSONDecodeError:
                    obj = {"raw": data_line}
                yield event_type, obj


def _inject_responses_events(ev, state: dict):
    """根据 zen 事件流补齐 OpenAI Responses 标准事件。

    返回要输出的 (event_name, data) 序列（可多个）。维护当前 response id
    与 output item 状态，在 delta 前注入 output_item.added，在 completed
    前注入 output_item.done。
    """
    event_type, data = ev
    out = []
    t = data.get("type") or event_type

    # 从任何事件里取 response id
    resp = data.get("response") or {}
    rid = resp.get("id") or data.get("response_id") or data.get("id")

    if t == "response.output_text.delta":
        if rid and not state["started"]:
            state["started"] = True
            state["rid"] = rid
            out.append(("response.created", {
                "type": "response.created",
                "response": {"id": rid, "object": "response",
                             "status": "in_progress"},
            }))
            item_id = f"msg_{rid}"
            state["item_id"] = item_id
            out.append(("response.output_item.added", {
                "type": "response.output_item.added",
                "response_id": rid, "item_id": item_id,
                "item": {"id": item_id, "type": "message",
                         "role": "assistant", "content": []},
            }))
        out.append(("data", data))

    elif t == "response.completed":
        if state["started"]:
            item_id = state["item_id"] or f"msg_{rid}"
            out.append(("response.output_item.done", {
                "type": "response.output_item.done",
                "response_id": rid, "item_id": item_id,
                "item": {"id": item_id, "type": "message", "role": "assistant",
                         "content": [{"type": "output_text", "text": "",
                                      "annotations": []}]},
            }))
        out.append(("data", data))
        state["started"] = False

    else:
        # ping 等其它事件原样透传
        out.append(("data", data))
    return out


def _sse_event_bytes(name: str, obj: dict) -> bytes:
    """把事件格式化为 SSE 字节（兼容带 event: 与裸 data: 两种）。"""
    body = json.dumps(obj, ensure_ascii=False)
    if name == "data":
        return f"data: {body}\n\n".encode()
    return f"event: {name}\ndata: {body}\n\n".encode()


async def _rewrite_model_bytes(gen, upstream_model: str, public_model: str):
    """逐 SSE 帧把 model 字段从上游名改写成对外名，流式返回。

    SSE 帧以 \\n\\n 分隔，缓冲到完整帧再替换，保证 model 名不被截断。
    不缓存整个响应，仅逐帧处理，对吞吐影响可忽略。
    """
    up = upstream_model.encode()
    pub = public_model.encode()
    buf = b""
    async for frame in gen:
        buf += frame
        while b"\n\n" in buf:
            chunk, buf = buf.split(b"\n\n", 1)
            yield chunk.replace(up, pub) + b"\n\n"
    if buf:
        yield buf.replace(up, pub)
