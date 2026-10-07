"""上游 zen 请求头：转发与代理探测共用，避免循环导入。"""

import uuid


def zen_headers() -> dict[str, str]:
    """构造上游要求的请求头。

    说明：上游按请求头做校验；session/request id 每次唯一。
    - id 主体必须是 26 位小写 hex，否则 mimo 系模型返回 403 FreeTierError
      （实测 20 位/base62/超长均 403，26 位 hex 稳定 200）。
    - UA 版本过旧（如 1.15.0）会触发 426 UpgradeRequired，需跟客户端新版。
    这些头为公共端点约定（无认证、无 key）。
    """
    return {
        "Authorization": "Bearer public",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        "x-api-key": "public",
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": f"msg_{uuid.uuid4().hex[:26]}",
        "x-opencode-session": f"ses_{uuid.uuid4().hex[:26]}",
    }
