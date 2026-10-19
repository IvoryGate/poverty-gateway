"""上游 FreeTier 错误分类。

上游的 abuse 提示文案会变（2026-10 已从 "Detect Abuse Traffic" 改成
"detected abusive traffic from this IP address / aren't available from your
current network"）。这里集中判断，避免网关和代理探测各写一份、漏掉新文案。

注意：``free tier can only be used from within OpenCode`` 是**请求形状**
不对（缺 tools/stream/头），不是 IP abuse；不能因为 403 就按 abuse 拉黑 IP。
"""

_ABUSE_MARKERS = (
    "detect abuse",
    "abusive traffic",
    "aren't available from your current network",
    "are not available from your current network",
    "not available from your current network",
)

_SHAPE_MARKERS = (
    "free tier can only be used from within opencode",
    "download it at https://opencode.ai",
)


def is_abuse_message(message: str) -> bool:
    """根据上游 error message 判断是否是 IP abuse（而不是请求形状错误）。"""
    text = (message or "").lower()
    if not text:
        return False
    if any(marker in text for marker in _SHAPE_MARKERS):
        return False
    return any(marker in text for marker in _ABUSE_MARKERS)


def is_freetier_message(message: str) -> bool:
    """是否是任何 FreeTier 相关错误（形状错误或 IP abuse）。

    这类错误只应存在于网关内部：对外统一收口成 503 / upstream_unavailable，
    不把 "OpenCode free tier" 之类的上游文案透给客户端。
    """
    text = (message or "").lower()
    return any(marker in text for marker in (
        "freetiererror",
        "free tier",
        "free models aren't available",
        "free models are not available",
        "abusive traffic",
        "detect abuse",
        "from within opencode",
    ))
