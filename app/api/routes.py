"""OpenAI 兼容路由：/v1/models、/v1/chat/completions、/health、/v1/stats。

设计原则：**宽松透传**——请求体以原始 dict 接收，只校验 model/messages 必需，
其余字段（tools、tool_choice、reasoning_effort、stream_options、stop、n 等）
原样保留并转发给上游。这样才能兼容 Cursor / Codex / opencode / Trae 等
主流 OpenAI 兼容工具，它们能用的能力我们也能用。
"""

import json
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

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


@router.get("/v1/models")
async def list_models(request: Request, _key: str = Depends(_require_key)):
    client: UpstreamClient = request.app.state.upstream
    return {"object": "list", "data": client.models.list_public()}


@router.get("/models")
async def list_models_no_version(request: Request, _key: str = Depends(_require_key)):
    """兼容 baseURL 不带 /v1 的客户端。"""
    client: UpstreamClient = request.app.state.upstream
    return {"object": "list", "data": client.models.list_public()}


@router.post("/v1/chat/completions")
async def chat_completions(request: Request, _key: str = Depends(_require_key)):
    return await _chat_completions(request, _key)


@router.post("/chat/completions")
async def chat_completions_no_version(request: Request, _key: str = Depends(_require_key)):
    """兼容 baseURL 不带 /v1 的客户端（@ai-sdk/openai-compatible 等）。"""
    return await _chat_completions(request, _key)


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
        raise HTTPException(status_code=502, detail=str(e))
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
            yield _sse_error(f"Unknown model: {e}")
        except NoProxyAvailable as e:
            yield _sse_error(str(e))
        except UpstreamError as e:
            yield _sse_error(str(e))
        finally:
            usage.record(key, success=ok, latency_ms=(time.time() - t0) * 1000,
                         tokens_in=ti, tokens_out=to)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse_error(message: str) -> bytes:
    """把错误编码为 SSE error 事件（兼容 OpenAI 客户端解析）。"""
    body = {"error": {"message": message, "type": "proxy_pool_error"}}
    return f"data: {json.dumps(body)}\n\n".encode()


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
