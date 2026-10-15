"""上游 zen 请求头：转发与代理探测共用，避免循环导入。"""

import os
import time

# opencode 客户端 id 随机段字母表（tU() 里随机字节 %62 查表）
_ID_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_MASK48 = (1 << 48) - 1


def _gen_id(prefix: str, invert: bool, ms: int) -> str:
    """复刻 opencode 客户端的 id 生成算法（tU）。

    结构：12 位 hex（毫秒时间戳<<12|counter 的低 48 位，ses 取反配对）
          + 14 位随机（14 字节各 %62 查字母表）。
    上游校验此结构：纯随机 uuid（如 msg_+26位hex）2026-10-13 起返回
    403 FreeTierError；按算法生成稳定 200。
    """
    n = (ms << 12) | 1  # counter：真客户端首生恒为 1；随机值会触发 403
    a = ~n if invert else n
    head = format(a & _MASK48, "012x")
    tail = "".join(_ID_ALPHABET[b % 62] for b in os.urandom(14))
    return prefix + head + tail


def zen_headers() -> dict[str, str]:
    """构造上游要求的请求头。

    说明：上游按请求头做校验：
    - x-opencode-request/session 必须符合客户端 id 算法（见 _gen_id），
      否则 mimo 等模型返回 403 FreeTierError。
    - UA 版本过旧（如 1.15.0）会触发 426 UpgradeRequired，需跟客户端新版。
    这些头为公共端点约定（无认证、无 key）。
    """
    now = int(time.time() * 1000)
    return {
        "Authorization": "Bearer public",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        "x-api-key": "public",
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": _gen_id("msg_", False, now),
        "x-opencode-session": _gen_id("ses_", True, now),
    }
