"""OpenAI 兼容路由：/v1/models、/v1/chat/completions、/health。

对外形态与 OpenAI API 一致，便于 Cursor / Codex / Continue 等直接接入。
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.model_map import ModelMapper
from app.core.proxy_pool import NoProxyAvailable
from app.services.upstream import UpstreamClient, UpstreamError

router = APIRouter()


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    temperature: float | None = Field(default=None)
    max_tokens: int | None = Field(default=None)
    stream: bool | None = Field(default=False)


@router.get("/v1/models")
async def list_models(request: Request):
    mapper = ModelMapper(request.app.state.settings)
    return {"object": "list", "data": mapper.list_public()}


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request):
    client: UpstreamClient = request.app.state.upstream
    payload = req.model_dump(exclude_none=True)
    if req.stream:
        return await _stream_completions(client, req.model, payload)
    try:
        result = await client.chat(req.model, payload)
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
    import json
    body = {"error": {"message": message, "type": "proxy_pool_error"}}
    return f"data: {json.dumps(body)}\n\n".encode()


@router.get("/health")
async def health(request: Request):
    client: UpstreamClient = request.app.state.upstream
    return await client.health()
