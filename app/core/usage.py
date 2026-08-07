"""用量统计：每 key 请求数 / token / 延迟 / 成功率。

进程内存聚合（阶段 05 再落 Redis 支持多 worker）。线程锁保证并发安全。
提供 record() 记录 + stats() 查询。
"""

import threading
import time


class UsageTracker:
    def __init__(self, settings=None):
        self._lock = threading.Lock()
        self._start_ts = time.time()
        self._totals = {
            "requests": 0,
            "success": 0,
            "error": 0,
            "latency_ms": 0,   # 累计
            "tokens_in": 0,
            "tokens_out": 0,
        }
        self._by_key: dict[str, dict] = {}

    def record(self, key: str, success: bool, latency_ms: float,
               tokens_in: int = 0, tokens_out: int = 0):
        """记录一次请求。

        key: API key（auth_enabled=False 时为 "anonymous"）。
        success: 是否成功。
        latency_ms: 请求总耗时（毫秒）。
        tokens_in/out: 上游 usage 的 prompt/completion tokens。
        """
        with self._lock:
            self._totals["requests"] += 1
            self._totals["success"] += 1 if success else 0
            self._totals["error"] += 0 if success else 1
            self._totals["latency_ms"] += latency_ms
            self._totals["tokens_in"] += tokens_in
            self._totals["tokens_out"] += tokens_out

            k = self._by_key.setdefault(key, {
                "requests": 0, "success": 0, "error": 0,
                "latency_ms": 0, "tokens_in": 0, "tokens_out": 0,
            })
            k["requests"] += 1
            k["success"] += 1 if success else 0
            k["error"] += 0 if success else 1
            k["latency_ms"] += latency_ms
            k["tokens_in"] += tokens_in
            k["tokens_out"] += tokens_out

    def stats(self) -> dict:
        """聚合统计快照。"""
        with self._lock:
            t = self._totals
            uptime = max(1, int(time.time() - self._start_ts))
            per_key = {
                k: {
                    "requests": v["requests"],
                    "success": v["success"],
                    "error": v["error"],
                    "error_rate": _rate(v["error"], v["requests"]),
                    "avg_latency_ms": _avg(v["latency_ms"], v["requests"]),
                    "tokens_in": v["tokens_in"],
                    "tokens_out": v["tokens_out"],
                    "tokens_total": v["tokens_in"] + v["tokens_out"],
                }
                for k, v in sorted(
                    self._by_key.items(),
                    key=lambda x: x[1]["requests"], reverse=True)
            }
            return {
                "uptime_seconds": uptime,
                "totals": {
                    "requests": t["requests"],
                    "success": t["success"],
                    "error": t["error"],
                    "error_rate": _rate(t["error"], t["requests"]),
                    "avg_latency_ms": _avg(t["latency_ms"], t["requests"]),
                    "tokens_in": t["tokens_in"],
                    "tokens_out": t["tokens_out"],
                    "tokens_total": t["tokens_in"] + t["tokens_out"],
                    "requests_per_min": round(
                        t["requests"] * 60 / uptime, 1),
                },
                "per_key": per_key,
            }


def _avg(total: float, n: int) -> float:
    return round(total / n, 1) if n else 0.0


def _rate(num: int, den: int) -> float:
    return round(num * 100 / den, 2) if den else 0.0
