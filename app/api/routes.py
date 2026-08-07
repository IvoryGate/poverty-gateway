"""OpenAI 兼容路由：/v1/models、/v1/chat/completions、/health。

设计原则：**宽松透传**——请求体以原始 dict 接收，只校验 model/messages 必需，
其余字段（tools、tool_choice、reasoning_effort、stream_options、stop、n 等）
原样保留并转发给上游。这样才能兼容 Cursor / Codex / opencode / Trae 等
主流 OpenAI 兼容工具，它们能用的能力我们也能用。
"""

import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.model_map import ModelMapper
from app.core.proxy_pool import NoProxyAvailable
from app.services.upstream import UpstreamClient, UpstreamError

router = APIRouter()


@router.get("/v1/models")
async def list_models(request: Request):
    mapper = ModelMapper(request.app.state.settings)
    return {"object": "list", "data": mapper.list_public()}


@router.post("/v1/chat/completions")
async def chat_completions(request: Request):
    client: UpstreamClient = request.app.state.upstream
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
        return await _stream_completions(client, model, payload)

    try:
        result = await client.chat(model, payload)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NoProxyAvailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    except UpstreamError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return result


async def _stream_completions(client: UpstreamClient, model: str, payload: dict):
    """SSE 流式透传：把上游字节块边读边发给客户端。"""

    async def gen():
        try:
            async for chunk in client.chat_stream(model, payload):
                yield chunk
        except KeyError as e:
            yield _sse_error(f"Unknown model: {e}")
        except NoProxyAvailable as e:
            yield _sse_error(str(e))
        except UpstreamError as e:
            yield _sse_error(str(e))

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse_error(message: str) -> bytes:
    """把错误编码为 SSE error 事件（兼容 OpenAI 客户端解析）。"""
    body = {"error": {"message": message, "type": "proxy_pool_error"}}
    return f"data: {json.dumps(body)}\n\n".encode()


@router.get("/health")
async def health(request: Request):
    client: UpstreamClient = request.app.state.upstream
    return await client.health()
