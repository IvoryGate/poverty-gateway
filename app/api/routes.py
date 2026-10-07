"""OpenAI 兼容路由：/v1/models、/v1/chat/completions、/health、/v1/stats、/panel。

设计原则：**宽松透传**——请求体以原始 dict 接收，只校验 model/messages 必需，
其余字段（tools、tool_choice、reasoning_effort、stream_options、stop、n 等）
原样保留并转发给上游。这样才能兼容 Cursor / Codex / opencode / Trae 等
主流 OpenAI 兼容工具，它们能用的能力我们也能用。
"""

import json
import os
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from app.core.auth import AuthError, AuthManager, RateLimitExceeded
from app.core.proxy_pool import NoProxyAvailable
from app.core.usage import UsageTracker
from app.services.upstream import UpstreamClient, UpstreamError

router = APIRouter()


def _require_key(request: Request) -> str:
    """鉴权依赖：校验 Bearer key，返回 key 字符串。失败抛 401/429。"""
    auth: AuthManager = request.app.state.auth
    if not auth.enabled:
        return "anonymous"
    try:
        return auth.authenticate(request.headers.get("Authorization"))
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except RateLimitExceeded as e:
        raise HTTPException(status_code=429, detail=str(e))


def _models_response(client: UpstreamClient) -> dict:
    """模型列表：同时兼容 OpenAI 标准（data）与 Codex 标准（models+slug）。"""
    base = client.models.list_public()
    data = base
    # Codex 的 Models API 期望每个模型带 slug 等元数据
    models = [
        {"id": m["id"], "slug": m["id"], "object": "model",
         "owned_by": "opencode"}
        for m in data
    ]
    return {"object": "list", "data": data, "models": models}


@router.get("/v1/models")
async def list_models(request: Request, _key: str = Depends(_require_key)):
    client: UpstreamClient = request.app.state.upstream
    return _models_response(client)


@router.get("/models")
async def list_models_no_version(request: Request, _key: str = Depends(_require_key)):
    """兼容 baseURL 不带 /v1 的客户端。"""
    client: UpstreamClient = request.app.state.upstream
    return _models_response(client)


@router.post("/v1/chat/completions")
async def chat_completions(request: Request, _key: str = Depends(_require_key)):
    return await _chat_completions(request, _key)


@router.post("/chat/completions")
async def chat_completions_no_version(request: Request, _key: str = Depends(_require_key)):
    """兼容 baseURL 不带 /v1 的客户端（@ai-sdk/openai-compatible 等）。"""
    return await _chat_completions(request, _key)


@router.post("/v1/responses")
async def responses(request: Request, _key: str = Depends(_require_key)):
    """Responses API（Codex 用）：宽松透传到上游 /responses。"""
    return await _responses(request, _key)


@router.post("/responses")
async def responses_no_version(request: Request, _key: str = Depends(_require_key)):
    """兼容 baseURL 不带 /v1 的客户端。"""
    return await _responses(request, _key)


async def _responses(request: Request, key: str):
    client: UpstreamClient = request.app.state.upstream
    usage: UsageTracker = request.app.state.usage
    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    model = payload.get("model")
    if not isinstance(model, str):
        raise HTTPException(status_code=400, detail="field 'model' is required")

    t0 = time.time()
    if payload.get("stream"):
        async def gen():
            ok = False
            ti = to = 0
            try:
                async for chunk in client.responses_stream(model, payload):
                    ok = True
                    yield chunk
                    ti, to = _scan_stream_usage(chunk, ti, to)
            except KeyError as e:
                yield _sse_error(f"Unknown model: {e}", "invalid_request_error")
            except NoProxyAvailable as e:
                yield _sse_error(str(e))
            except UpstreamError as e:
                yield _sse_error(str(e), _sse_error_type(e))
            finally:
                usage.record(key, success=ok,
                             latency_ms=(time.time() - t0) * 1000,
                             tokens_in=ti, tokens_out=to)
        return StreamingResponse(
            gen(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    try:
        result = await client.responses(model, payload)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NoProxyAvailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    except UpstreamError as e:
        return _upstream_error(e)
    usage.record(key, success=True, latency_ms=(time.time() - t0) * 1000)
    return result


async def _chat_completions(request: Request, key: str):
    client: UpstreamClient = request.app.state.upstream
    usage: UsageTracker = request.app.state.usage
    try:
        payload = await request.json()
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    model = payload.get("model")
    if not isinstance(model, str):
        raise HTTPException(status_code=400, detail="field 'model' is required")
    if not isinstance(payload.get("messages"), list) or not payload["messages"]:
        raise HTTPException(status_code=400, detail="field 'messages' is required")

    if payload.get("stream"):
        return await _stream_completions(client, model, payload, key, usage)

    t0 = time.time()
    try:
        result = await client.chat(model, payload)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NoProxyAvailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    except UpstreamError as e:
        return _upstream_error(e)
    finally:
        pass
    usage.record(key, success=True, latency_ms=(time.time() - t0) * 1000,
                 tokens_in=_usage_num(result, "prompt_tokens"),
                 tokens_out=_usage_num(result, "completion_tokens"))
    return result


async def _stream_completions(client: UpstreamClient, model: str, payload: dict,
                              key: str, usage: UsageTracker):
    """SSE 流式透传：把上游字节块边读边发给客户端。"""

    async def gen():
        t0 = time.time()
        ok = False
        ti = to = 0
        try:
            async for chunk in client.chat_stream(model, payload):
                ok = True
                yield chunk
                ti, to = _scan_stream_usage(chunk, ti, to)
        except KeyError as e:
            yield _sse_error(f"Unknown model: {e}", "invalid_request_error")
        except NoProxyAvailable as e:
            yield _sse_error(str(e))
        except UpstreamError as e:
            yield _sse_error(str(e), _sse_error_type(e))
        finally:
            usage.record(key, success=ok, latency_ms=(time.time() - t0) * 1000,
                         tokens_in=ti, tokens_out=to)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse_error(message: str, err_type: str = "proxy_pool_error") -> bytes:
    """把错误编码为 SSE error 事件（兼容 OpenAI 客户端解析）。"""
    body = {"error": {"message": message, "type": err_type, "param": None}}
    return f"data: {json.dumps(body)}\n\n".encode()


def _sse_error_type(e: UpstreamError) -> str:
    """从上游错误里推断 SSE error 事件的 type（429→rate_limit_error 等）。"""
    if e.status_code == 429:
        return "rate_limit_error"
    body = e.error_body or {}
    t = body.get("type")
    return t if isinstance(t, str) and t else "upstream_error"


def _upstream_error(e: UpstreamError) -> JSONResponse:
    """把上游错误转成客户可识别的响应：

    - 上游 4xx（如 response_format=json_object 被上游拒绝）→ 同码透传，
      并保留上游 error 对象，客户端可识别"参数/格式不支持"并优雅降级；
    - 上游 429 → 429 + Retry-After + 标准 rate_limit_error 结构，便于退避；
    - 其余（5xx / 连接失败 / 熔断）→ **503** 网关兜底。
      不用 502：502 的 body 在出网时容易被中间层（CDN/反代）替换成自己的
      HTML 错误页，客户端就拿不到结构化 error；503 与 4xx/429 一样保留 JSON。
    """
    body = e.error_body or {}
    msg = body.get("message") or str(e)
    if e.status_code == 429:
        etype = "rate_limit_error"
    elif isinstance(body.get("type"), str) and body.get("type"):
        etype = body["type"]
    elif e.status_code and 400 <= e.status_code < 500:
        etype = "invalid_request_error"     # 上游拒的是请求本身
    else:
        etype = "upstream_error"            # 连接失败 / 熔断 / 上游 5xx，与 SSE 口径一致
    error = {"message": msg, "type": etype, "param": None}
    if body.get("code"):
        error["code"] = body["code"]

    if e.status_code == 429:
        status = 429
        headers = {}
        if e.retry_after:
            headers["Retry-After"] = str(e.retry_after)
        return JSONResponse(status_code=status, content={"error": error},
                            headers=headers or None)
    if e.status_code and 400 <= e.status_code < 500:
        return JSONResponse(status_code=e.status_code, content={"error": error})
    headers = {"Retry-After": str(e.retry_after)} if e.retry_after else None
    return JSONResponse(status_code=503, content={"error": error},
                        headers=headers)


def _usage_num(result: dict, key: str) -> int:
    """从非流式响应 usage 里取 token 数。"""
    usage = result.get("usage") or {}
    try:
        return int(usage.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def _scan_stream_usage(chunk: bytes, ti: int, to: int) -> tuple[int, int]:
    """扫描流式块里的 usage 字段（SSE 末尾 data: {usage}），返回累计 token。"""
    text = chunk.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        if line.startswith("data:"):
            body = line[5:].strip()
            if body and body != "[DONE]":
                try:
                    obj = json.loads(body)
                    u = obj.get("usage")
                    if isinstance(u, dict):
                        p = u.get("prompt_tokens")
                        c = u.get("completion_tokens")
                        if isinstance(p, int):
                            ti = p
                        if isinstance(c, int):
                            to = c
                except json.JSONDecodeError:
                    continue
    return ti, to


@router.get("/v1/stats")
async def stats(request: Request, _key: str = Depends(_require_key)):
    """用量 + 代理池水位统计（需有效 API key）。"""
    usage: UsageTracker = request.app.state.usage
    client: UpstreamClient = request.app.state.upstream
    data = usage.stats()
    data["pool"] = client.pool.metrics()
    return data


@router.get("/health")
async def health(request: Request):
    client: UpstreamClient = request.app.state.upstream
    return await client.health()


@router.get("/", response_class=HTMLResponse)
async def root_panel():
    """根路径：监控面板页面。"""
    return await _panel_html()


@router.get("/panel", response_class=HTMLResponse)
async def panel():
    """监控面板页面（/panel 别名）。"""
    return await _panel_html()


async def _panel_html():
    """读取并返回面板 HTML。"""
    import asyncio
    path = os.path.join(os.path.dirname(__file__), "panel.html")

    def _read():
        with open(path, encoding="utf-8") as f:
            return f.read()

    try:
        return HTMLResponse(await asyncio.to_thread(_read))
    except OSError:
        raise HTTPException(status_code=500, detail="panel not found")
