"""上游 zen 请求头：转发与代理探测共用，避免循环导入。"""

import uuid


def zen_headers() -> dict[str, str]:
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
