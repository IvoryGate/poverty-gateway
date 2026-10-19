"""上游转发服务：把 OpenAI 兼容请求通过代理转发到上游 zen。

只经代理转发（proxy_pool.get_proxy()），绝不直连。
"""

import asyncio
import contextlib
import json
import time

import httpx

from app.config import Settings
from app.core.freetier import is_abuse_message
from app.core.headers import zen_headers
from app.core.model_map import ModelMapper
from app.core.proxy_pool import (
    ABUSE,
    COOLDOWN,
    DEAD,
    OK,
    NoProxyAvailable,
    ProxyPool,
)
from app.core.redis_store import RedisStore

MAX_RETRIES = 3

_CB_FAIL = "cb:fail:"    # 失败计数窗口桶
_CB_OPEN = "cb:open"     # 熔断开关


def _first_event_timeout(settings, payload: dict) -> float:
    """按请求体大小选择首字节超时。

    小请求 15s 足够识别"连上但不吐字"的坏代理；opencode 带完整历史/工具时
    请求体可达几百 KB，上游 prefill 本身就要 30~120s，必须放宽，否则会把
    正常请求误杀成"首字节超时"并反复重试。
    """
    base = settings.first_event_timeout
    if base <= 0:
        return 0
    try:
        size = len(json.dumps(payload, ensure_ascii=False))
    except (TypeError, ValueError):
        size = 0
    if size >= 300_000:
        return max(base, settings.first_event_timeout_large)
    if size >= 80_000:
        return max(base, settings.first_event_timeout_medium)
    return base


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

        上游 free 档 2026-10-14 起只接受 ``stream=true`` + read 工具的
        agent 形状，因此这里强制走上游流式，再把 SSE 聚合成标准
        ``chat.completion`` JSON 返回给客户端。
        """
        payload = _ensure_read_tool(payload, "chat/completions")
        collector = _ChatStreamCollector(public_model)
        async for chunk in self._forward_stream(
                "chat/completions", public_model, payload):
            collector.feed(chunk)
        return collector.result()

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
        """转发一次 Responses API 请求（非流式，Codex 用）。

        上游 free 档只接受 stream=true，这里内部走流式并聚合出标准
        ``response`` 对象，避免非流式请求撞 FreeTierError。
        """
        payload = _sanitize_responses(payload)
        collector = _ResponsesStreamCollector(public_model)
        async for chunk in self._forward_stream(
                "responses", public_model, payload):
            collector.feed(chunk)
        return collector.result()

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
        if endpoint == "chat/completions":
            payload = _normalize_chat_payload(payload)
        payload = _ensure_read_tool(payload, endpoint)

        last_err = None
        excluded: set[str] = set()
        for attempt in range(MAX_RETRIES):
            proxy = await self.pool.get_proxy(exclude=excluded, model=upstream_model)
            t0 = time.monotonic()
            try:
                result = await self._request(endpoint, proxy, payload)
                elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                await self.pool.report(proxy, OK, latency_ms=elapsed_ms, model=upstream_model)
                if isinstance(result, dict) and "model" in result:
                    result["model"] = public_model
                return result
            except UpstreamError as e:
                last_err = e
                self.pool.note_error(_classify(e.status_code))
                if e.status_code == 429 or 500 <= e.status_code < 600:
                    # 限流 / 上游 5xx：换代理重试
                    self._register_failure()
                    await self.pool.report(
                        proxy, COOLDOWN, cooldown_seconds=e.retry_after, model=upstream_model)
                    excluded.add(proxy)
                    continue
                if _is_abuse(e):
                    # 上游按 IP 判定滥用：这个 IP 对 free 档不可用，换下一个
                    await self.pool.report(proxy, ABUSE, model=upstream_model)
                    excluded.add(proxy)
                    continue
                # 其余 4xx（参数/模型不支持/形状校验）是请求本身的问题，
                # 重试和惩罚代理都无意义：仅归还 in-flight，原样抛给客户端。
                self.pool.release(proxy)
                raise
            except httpx.TimeoutException as e:
                # 传输超时：代理慢/挂，剔除换一个（记入 timeout 桶）
                last_err = e
                self.pool.note_error("timeout")
                await self.pool.report(proxy, DEAD, model=upstream_model)
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                # 连接/传输层失败：该代理不可用，剔除后换一个
                last_err = e
                self.pool.note_error("conn")
                await self.pool.report(proxy, DEAD, model=upstream_model)
                excluded.add(proxy)
                continue
        raise _retries_exhausted(last_err)

    async def _forward_stream(self, endpoint: str, public_model: str,
                              payload: dict):
        """通用流式透传：经代理池转发，逐块产出原始字节。"""
        if self._circuit_open():
            raise UpstreamError("upstream circuit open (too many failures)")

        upstream_model = self.models.to_upstream(public_model)
        payload = dict(payload)
        payload["model"] = upstream_model
        payload["stream"] = True
        if endpoint == "chat/completions":
            payload = _normalize_chat_payload(payload)
        payload = _ensure_read_tool(payload, endpoint)

        last_err = None
        excluded: set[str] = set()
        first_timeout = _first_event_timeout(self.settings, payload)
        request_start = time.monotonic()
        for attempt in range(MAX_RETRIES):
            remaining = (self.settings.request_deadline
                         - (time.monotonic() - request_start))
            if remaining <= 0:
                last_err = last_err or UpstreamError("request deadline exceeded")
                break
            attempt_timeout = (
                min(first_timeout, remaining) if first_timeout > 0 else 0)
            proxy = await self.pool.get_proxy(exclude=excluded, model=upstream_model)
            started = False
            reported = False
            gen = None
            t0 = time.monotonic()
            try:
                if endpoint == "responses":
                    gen = self._responses_stream_request(proxy, payload)
                else:
                    gen = self._stream_request(endpoint, proxy, payload)
                gen = _rewrite_model_bytes(gen, upstream_model, public_model)
                it = gen.__aiter__()
                while True:
                    try:
                        if not started and attempt_timeout > 0:
                            chunk = await asyncio.wait_for(
                                it.__anext__(),
                                timeout=attempt_timeout)
                        else:
                            chunk = await it.__anext__()
                    except StopAsyncIteration:
                        break
                    if not started:
                        started = True
                        # 首字节延迟 = 上游响应快慢的核心信号，记入延迟表
                        ttfb = round((time.monotonic() - t0) * 1000, 1)
                        await self.pool.report(proxy, OK, latency_ms=ttfb, model=upstream_model)
                        reported = True
                    yield chunk
                if not started:
                    await self.pool.report(proxy, OK, model=upstream_model)
                    reported = True
                return
            except asyncio.TimeoutError:
                # 连上了但迟迟不吐首字节：换代理，别让请求挂 300s
                last_err = UpstreamError(
                    f"upstream first event timeout ({attempt_timeout:g}s)")
                self.pool.note_error("timeout")
                if gen is not None:
                    with contextlib.suppress(Exception):
                        await gen.aclose()
                if started:
                    await self.pool.report(proxy, COOLDOWN, model=upstream_model)
                    reported = True
                    raise last_err
                await self.pool.report(proxy, COOLDOWN, cooldown_seconds=30, model=upstream_model)
                reported = True
                excluded.add(proxy)
                continue
            except UpstreamError as e:
                last_err = e
                self.pool.note_error(_classify(e.status_code))
                if started:
                    # 已在出流中途失败：无法重试，标记状态后抛
                    await self.pool.report(proxy, COOLDOWN, cooldown_seconds=e.retry_after, model=upstream_model)
                    reported = True
                    raise
                if e.status_code == 429 or 500 <= e.status_code < 600:
                    self._register_failure()
                    await self.pool.report(
                        proxy, COOLDOWN, cooldown_seconds=e.retry_after, model=upstream_model)
                    reported = True
                    excluded.add(proxy)
                    continue
                if _is_abuse(e):
                    await self.pool.report(proxy, ABUSE, model=upstream_model)
                    reported = True
                    excluded.add(proxy)
                    continue
                # 其余 4xx 是请求本身的问题，不惩罚代理
                self.pool.release(proxy)
                reported = True
                raise
            except httpx.TimeoutException as e:
                last_err = e
                self.pool.note_error("timeout")
                await self.pool.report(proxy, DEAD, model=upstream_model)
                reported = True
                if started:
                    raise
                excluded.add(proxy)
                continue
            except httpx.HTTPError as e:
                last_err = e
                self.pool.note_error("conn")
                await self.pool.report(proxy, DEAD, model=upstream_model)
                reported = True
                if started:
                    raise
                excluded.add(proxy)
                continue
            finally:
                # 客户端断开 / 生成器被关闭时也要归还 in-flight，避免面板
                # 长期显示"在途"、把代理并发额度占死。
                if not reported:
                    self.pool.release(proxy)
        raise _retries_exhausted(last_err)

    async def _request(self, endpoint: str, proxy: str, payload: dict) -> dict:
        url = f"{self.settings.upstream_base}/{endpoint}"
        proxy_url = f"http://{proxy}"
        timeout = httpx.Timeout(connect=15, read=300, write=30, pool=15)
        async with httpx.AsyncClient(
            timeout=timeout, proxy=proxy_url
        ) as client:
            r = await client.post(
                url, headers=zen_headers(_conversation_seed(payload)), json=payload
            )
        if r.status_code >= 400:
            detail = r.text[:500]
            raise UpstreamError(
                f"upstream {r.status_code}: {detail}",
                retry_after=_parse_retry_after(r),
                status_code=r.status_code,
                error_body=_parse_error_body(r),
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
            "POST", url, headers=zen_headers(_conversation_seed(payload)),
            json=payload
        ) as r:
            if r.status_code >= 400:
                body = await r.aread()
                detail = body[:500].decode("utf-8", errors="replace")
                raise UpstreamError(
                    f"upstream {r.status_code}: {detail}",
                    retry_after=_parse_retry_after(r),
                    status_code=r.status_code,
                    error_body=_parse_error_body(r),
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
            "POST", url, headers=zen_headers(_conversation_seed(payload)),
            json=payload
        ) as r:
            if r.status_code >= 400:
                body = await r.aread()
                detail = body[:500].decode("utf-8", errors="replace")
                raise UpstreamError(
                    f"upstream {r.status_code}: {detail}",
                    retry_after=_parse_retry_after(r),
                    status_code=r.status_code,
                    error_body=_parse_error_body(r),
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
                timeout=httpx.Timeout(connect=5, read=8, write=5, pool=5),
                proxy=proxy_url,
            ) as client:
                r = await client.get(f"{self.settings.upstream_base}/models")
                return r.status_code in (200, 401, 403)
        except httpx.HTTPError:
            return False


class UpstreamError(Exception):
    """上游返回错误。"""

    def __init__(self, message: str, retry_after: int = 0, status_code: int = 0,
                 error_body: dict | None = None):
        super().__init__(message)
        self.retry_after = retry_after
        self.status_code = status_code
        self.error_body = error_body or {}


def _retries_exhausted(last_err) -> Exception:
    """重试耗尽后的收口：传输层原始异常包成 UpstreamError（→502）。

    裸的 httpx.ConnectTimeout/ConnectError 路由层不接，会漏成 500，
    客户端看到的是网关内部错误而不是标准 502 error 结构。
    """
    if isinstance(last_err, UpstreamError):
        return last_err
    if isinstance(last_err, httpx.HTTPError):
        return UpstreamError(f"all proxy retries failed: {last_err}")
    return last_err or UpstreamError("no proxy retries left")


def _parse_retry_after(r) -> int:
    """解析 429 响应的 retry-after 头（秒）。无则返回 0。"""
    val = r.headers.get("retry-after")
    if not val:
        return 0
    try:
        return max(int(val.strip()), 0)
    except (TypeError, ValueError):
        return 0


def _parse_error_body(r) -> dict:
    """从上游错误响应里提取标准 OpenAI error 对象，供网关重包装。

    上游多数返回 {"error": {...}}；个别用 {"detail": ...} 或裸消息。能解析出
    error 对象则返回它，否则返回空 dict（调用方降级用 message 兜底）。
    """
    try:
        obj = json.loads(r.text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(obj, dict):
        return {}
    err = obj.get("error")
    if isinstance(err, dict):
        return err
    for key in ("message", "detail"):
        if key in obj and isinstance(obj[key], str):
            return {"type": "invalid_request_error", "message": obj[key]}
    return {}


def _conversation_seed(payload: dict) -> str | None:
    """从请求体取会话种子：首条 user 消息（多轮历史增长时保持同一 session）。

    上游按 session 做 prompt cache 亲和；每轮随机 session 会让缓存全部失效。
    Responses 的 input 既可能是字符串，也可能是 item 列表。
    """
    input_value = payload.get("input")
    if isinstance(input_value, str) and input_value.strip():
        return input_value
    for field in ("messages", "input"):
        items = payload.get(field)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            role = item.get("role")
            if role not in (None, "user"):
                continue
            content = item.get("content")
            if isinstance(content, str) and content:
                return content
            if content is not None:
                try:
                    return json.dumps(content, ensure_ascii=False, sort_keys=True)
                except (TypeError, ValueError):
                    continue
    return None


# 上游 free 档要求请求里出现名为 read 的工具（legacy /zen/v1 只校验名字）。
# 客户端没带时补一个"已废弃、不要调用"的占位工具，避免改变模型行为。
_DEPRECATED_READ_DESCRIPTION = (
    "DEPRECATED: This tool is deprecated and disabled. "
    "Do NOT call or reference this tool under any circumstances; ignore it completely."
)


def _read_tool(endpoint: str) -> dict:
    if endpoint == "responses":
        return {
            "type": "function",
            "name": "read",
            "description": _DEPRECATED_READ_DESCRIPTION,
            "parameters": {"type": "object", "properties": {}},
        }
    return {
        "type": "function",
        "function": {
            "name": "read",
            "description": _DEPRECATED_READ_DESCRIPTION,
            "parameters": {"type": "object", "properties": {}},
        },
    }


def _tool_name(tool, endpoint: str) -> str:
    if not isinstance(tool, dict):
        return ""
    if endpoint == "responses":
        name = tool.get("name")
    else:
        fn = tool.get("function")
        name = fn.get("name") if isinstance(fn, dict) else None
    return name if isinstance(name, str) else ""


def _ensure_read_tool(payload: dict, endpoint: str) -> dict:
    """保证请求带 read 工具；已带则原样返回。"""
    tools = payload.get("tools")
    if isinstance(tools, list):
        if any(_tool_name(t, endpoint) == "read" for t in tools):
            return payload
        return dict(payload, tools=[*tools, _read_tool(endpoint)])
    return dict(payload, tools=[_read_tool(endpoint)])


class _ChatStreamCollector:
    """把上游 chat SSE 聚合成一个非流式 ``chat.completion`` 对象。"""

    def __init__(self, public_model: str):
        self.public_model = public_model
        self.buf = ""
        self.id = ""
        self.created = 0
        self.content: list[str] = []
        self.reasoning: list[str] = []
        self.tool_calls: dict[int, dict] = {}
        self.finish_reason = None
        self.usage = None

    def feed(self, chunk: bytes):
        """喂入原始 SSE 字节，按空行切帧（兼容 \\r\\n）。"""
        self.buf += chunk.decode("utf-8", errors="replace")
        self.buf = self.buf.replace("\r\n", "\n")
        while "\n\n" in self.buf:
            frame, self.buf = self.buf.split("\n\n", 1)
            self._handle_frame(frame)

    def _handle_frame(self, frame: str):
        for line in frame.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get("id"):
                self.id = obj["id"]
            if obj.get("created"):
                self.created = obj["created"]
            if isinstance(obj.get("usage"), dict) and obj["usage"]:
                self.usage = obj["usage"]
            choices = obj.get("choices")
            if not isinstance(choices, list) or not choices:
                continue
            choice = choices[0]
            if not isinstance(choice, dict):
                continue
            if choice.get("finish_reason"):
                self.finish_reason = choice["finish_reason"]
            message = choice.get("message")
            if isinstance(message, dict):
                # 上游若直接返回整段 message（测试/非标准流），按完整消息收下
                if isinstance(message.get("content"), str):
                    self.content.append(message["content"])
                if isinstance(message.get("reasoning_content"), str):
                    self.reasoning.append(message["reasoning_content"])
                for call in message.get("tool_calls") or []:
                    if isinstance(call, dict):
                        index = len(self.tool_calls)
                        self.tool_calls[index] = {
                            "id": call.get("id", ""),
                            "type": call.get("type", "function"),
                            "function": dict(call.get("function") or {
                                "name": "", "arguments": ""}),
                        }
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                continue
            if isinstance(delta.get("content"), str):
                self.content.append(delta["content"])
            if isinstance(delta.get("reasoning_content"), str):
                self.reasoning.append(delta["reasoning_content"])
            for call in delta.get("tool_calls") or []:
                if not isinstance(call, dict):
                    continue
                index = call.get("index", 0)
                if not isinstance(index, int):
                    index = 0
                slot = self.tool_calls.setdefault(
                    index, {"id": "", "type": "function",
                            "function": {"name": "", "arguments": ""}})
                if call.get("id"):
                    slot["id"] = call["id"]
                if call.get("type"):
                    slot["type"] = call["type"]
                fn = call.get("function")
                if isinstance(fn, dict):
                    if fn.get("name"):
                        slot["function"]["name"] += fn["name"]
                    if fn.get("arguments"):
                        slot["function"]["arguments"] += fn["arguments"]

    def result(self) -> dict:
        message = {"role": "assistant"}
        text = "".join(self.content)
        if self.tool_calls:
            message["tool_calls"] = [
                {k: v for k, v in self.tool_calls[i].items() if k != "index"}
                for i in sorted(self.tool_calls)
            ]
            message["content"] = text or None
            finish = self.finish_reason or "tool_calls"
        else:
            message["content"] = text
            finish = self.finish_reason or "stop"
        reasoning = "".join(self.reasoning)
        if reasoning:
            message["reasoning_content"] = reasoning
        result = {
            "id": self.id or "chatcmpl",
            "object": "chat.completion",
            "created": self.created or int(time.time()),
            "model": self.public_model,
            "choices": [{"index": 0, "message": message,
                         "finish_reason": finish}],
        }
        if self.usage:
            result["usage"] = self.usage
        return result


class _ResponsesStreamCollector:
    """把上游 Responses SSE 聚合成一个非流式 ``response`` 对象。"""

    def __init__(self, public_model: str):
        self.public_model = public_model
        self.buf = ""
        self.response: dict | None = None
        self.text: list[str] = []

    def feed(self, chunk: bytes):
        self.buf += chunk.decode("utf-8", errors="replace")
        self.buf = self.buf.replace("\r\n", "\n")
        while "\n\n" in self.buf:
            frame, self.buf = self.buf.split("\n\n", 1)
            self._handle_frame(frame)

    def _handle_frame(self, frame: str):
        for line in frame.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            event_type = obj.get("type")
            if event_type == "response.completed" and isinstance(
                    obj.get("response"), dict):
                self.response = obj["response"]
            elif event_type == "response.output_text.delta":
                if isinstance(obj.get("delta"), str):
                    self.text.append(obj["delta"])
            elif obj.get("object") == "response" and obj.get("output"):
                self.response = obj

    def result(self) -> dict:
        text = "".join(self.text)
        response = dict(self.response) if self.response else {}
        response["model"] = self.public_model
        if not response.get("id"):
            response["id"] = f"resp_{int(time.time() * 1000)}"
        response.setdefault("object", "response")
        response.setdefault("status", "completed")
        if not response.get("output") and text:
            response["output"] = [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }]
        return response


def _normalize_chat_payload(payload: dict) -> dict:
    """归一化 chat 请求里的 assistant 消息，规避上游 reasoning_content 校验。

    zen/DeepSeek 推理模型要求：会话中出现过真实思考过的 assistant 消息后，
    后续同位置的 assistant 消息必须带 reasoning_content（须回传）。实测空串
    "" 合法。这里给「缺该字段」的 assistant 消息统一补空串，避免客户端
    （DSH/pi-ai 等）因历史消息没带字段被上游 400。已有该字段的不动。
    """
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return payload
    normalized = list(messages)
    changed = False
    for i, m in enumerate(normalized):
        if isinstance(m, dict) and m.get("role") == "assistant" \
                and "reasoning_content" not in m:
            normalized[i] = dict(m, reasoning_content="")
            changed = True
    if not changed:
        return payload
    return dict(payload, messages=normalized)


def _classify(status_code: int) -> str:
    """把 HTTP 状态码归为错误桶：429 / 5xx / 4xx。"""
    if status_code == 429:
        return "429"
    if 500 <= status_code < 600:
        return "5xx"
    return "4xx"


def _is_abuse(e: "UpstreamError") -> bool:
    """上游 403 的 message 是否为 IP 滥用判定（该代理不可用）。

    与"请求形状不对"（free tier can only be used from within OpenCode）
    区分开：后者是我们自己的头/body 问题，换代理也没用，不应把 IP 拉黑。
    """
    if e.status_code != 403:
        return False
    message = ""
    if isinstance(e.error_body, dict):
        message = str(e.error_body.get("message") or "")
    return is_abuse_message(message or str(e))


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
