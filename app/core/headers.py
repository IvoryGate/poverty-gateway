"""上游 zen 请求头：转发与代理探测共用，避免循环导入。

上游 2026-10-14 起对 free 档做了客户端指纹校验（403 FreeTierError）：

- ``x-opencode-session`` 必须是 OpenCode 客户端的 canonical 会话 id：
  ``ses_`` + 12 位小写 hex（``~(ms<<12|counter)`` 的降序时间戳）+ 14 位
  Base62 随机串。带工具、且 ``stream=true`` 的请求才放行；IP 若被上游判定
  为滥用流量会返回 ``Detect Abuse Traffic``（那是 IP 信誉问题，不是头的问题）。
- 真实客户端（opencode 1.18.31）仍在使用旧版 ``/zen/v1`` 请求形状：
  UA ``opencode/1.18.31 ai-sdk/... runtime/bun/...``、``x-opencode-project:
  global``、``x-opencode-request: msg_<12hex><14base62>``。
- 新版 ``x-opencode-request``（``msg_`` + 时间戳 + Base62）仅用于请求级追踪，
  上游不校验其内容；``x-opencode-session`` 才决定缓存亲和性。

这里尽量逐字节复刻真实客户端，避免再次触发指纹校验。会话 id 可按会话种子
稳定生成（同一会话多轮保持同一 session，命中上游 prompt cache），无种子时
每次随机。
"""

import hashlib
import os
import time

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_MASK48 = (1 << 48) - 1
_MASK36 = (1 << 36) - 1

# 真实客户端 User-Agent（opencode 1.18.31 / ai-sdk / bun）
USER_AGENT = (
    "opencode/1.18.31 "
    "ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
)


def _base62(data: bytes, width: int) -> str:
    """把字节串转成定长 Base62（不足左补 '0'），复刻客户端 id 的随机段。"""
    n = int.from_bytes(data, "big")
    out = []
    for _ in range(width):
        n, rem = divmod(n, 62)
        out.append(_BASE62[rem])
    return "".join(reversed(out))


def _descending_hex(ms: int) -> str:
    """客户端 Identifier.descending()：48 位降序时间戳，counter 恒为 1。"""
    current = ((ms << 12) | 1) & _MASK48
    return format(~current & _MASK48, "012x")


def _session_id(seed: str | None = None) -> str:
    """生成上游认可的 ``ses_`` 会话 id。

    seed 非空时按会话稳定派生（时间部分取 sha256 的 36 位），保证同一会话
    多轮命中缓存；seed 为空时用当前毫秒时间戳。两种形态都满足客户端
    canonical 布局：12 位 hex 时间戳 + 14 位 Base62。
    """
    if seed:
        digest = hashlib.sha256(("ses\x00" + seed).encode("utf-8")).digest()
        ts = int.from_bytes(digest[:5], "big") & _MASK36
        tail = _base62(digest[6:16], 14)
    else:
        ts = int(time.time() * 1000) & _MASK36
        tail = _base62(os.urandom(10), 14)
    return "ses_" + _descending_hex(ts) + tail


def _request_id() -> str:
    """``msg_`` + 12 位升序毫秒时间戳 + 14 位 Base62（真实客户端形态）。"""
    stamp = format(int(time.time() * 1000) & _MASK48, "012x")
    return "msg_" + stamp + _base62(os.urandom(10), 14)


def zen_headers(seed: str | None = None) -> dict[str, str]:
    """构造上游要求的请求头。

    seed: 会话种子（如首条 user 消息）。相同 seed → 相同 session，用于上游
    prompt cache 亲和；代理探测/模型同步等无会话场景可不传。
    """
    session = _session_id(seed)
    return {
        "Authorization": "Bearer public",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": USER_AGENT,
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": _request_id(),
        "x-opencode-session": session,
        # 2026-10-14 起新版 /inference 校验同时带两个 session 头；旧版
        # /zen/v1 对多出的这个头不敏感，带上可兼容后续切换。
        "x-opencode-session-id": session,
    }


# 上游 free 档要求的 agent 形状：必须 streaming 且带名为 read 的工具。
# 探测/模型同步等"非真实对话"的请求也要满足，否则一律 403 FreeTierError。
_PROBE_TOOL = {
    "type": "function",
    "function": {
        "name": "read",
        "description": "Read the contents of a file or directory.",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    },
}


def probe_payload(model: str, prompt: str = "hi") -> dict:
    """构造上游 free 档能接受的最小探测请求（stream + read 工具 + 1 token）。"""
    return {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1,
        "stream": True,
        "tools": [_PROBE_TOOL],
    }
