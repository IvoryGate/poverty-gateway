#!/usr/bin/env python3
"""大海捞针（Needle in Haystack）上下文压测工具 —— 加强版。

科学测量模型真实有效上下文：
- 多针位置：head / 25% / mid / 75% / tail（长上下文位置效应明显）
- 重复试验：每尺寸每位置多轮，统计成功率而非单发判断
- 对照模式：--direct 直连上游（不经网关/代理池），隔离链路干扰
- 判据收紧：HTTP 200 **且输出包含针** 才算可用
- 强制约束提问：让模型"只输出 passphrase"，减少注意力随机性误判

用法：
  python tools/context_probe.py --model deepseek-v4-flash-lite \
      --sizes 128000,512000,1000000 --positions head,mid,tail --trials 3

  # 直连上游对照（不经网关）
  python tools/context_probe.py --model deepseek-v4-flash-free --direct \
      --upstream https://opencode.ai/zen/v1

说明：
- 草垛按 token 数生成（英文 4 字符 ≈ 1 token），体积受 nginx client_max_body_size
  限制（当前 10m，1M 内安全）。
- 每次请求随机新针（独立 trial），避免记忆针/文本缓存干扰。
- 默认走网关（生产路径，经代理池转发）；--direct 直连上游对比。
"""

import argparse
import asyncio
import json
import os
import random
import time

import httpx

FILLER_BLOCK = (
    "The quick brown fox jumps over the lazy dog near the river. "
    "Machine learning systems process vast amounts of information. "
    "Attention mechanisms allow transformers to weigh token relationships. "
    "The weather today is sunny with a slight breeze from the east. "
    "Modern databases index rows for fast retrieval during queries. "
)
# 运行时按 4 chars/token 计算，保证"目标 token 数"与真实体积一致
FILLER_BLOCK_TOKENS = max(1, len(FILLER_BLOCK) // 4)

# 针位置定义（草垛比例）
POSITIONS = {
    "head": 0.03,
    "25": 0.25,
    "mid": 0.50,
    "75": 0.75,
    "tail": 0.97,
}


def make_haystack(filler_tokens: int, needle: str, needle_pos: float):
    """按目标 token 数生成草垛，并把针埋在指定比例位置。"""
    n_blocks = max(1, filler_tokens // FILLER_BLOCK_TOKENS)
    parts = [FILLER_BLOCK] * n_blocks
    needle_idx = min(len(parts) - 1, int(len(parts) * needle_pos))
    parts[needle_idx] = FILLER_BLOCK.split(".")[0] + ". " + needle + " " + FILLER_BLOCK
    return "".join(parts)


def count_tokens(text: str) -> int:
    """近似 token 数（英文 4 字符 ≈ 1 token）。"""
    return max(1, len(text) // 4)


def new_needle() -> str:
    """随机生成一枚新针。"""
    return (f"KEY {random.choice(('orange','violet','cobalt','amber','teal'))}-"
            f"{random.randint(1000,9999)}-{random.choice(('xray','kite','moss','node','aero'))}")


class Probe:
    def __init__(self, base_url: str, headers: dict, timeout: float = 600.0):
        self.headers = headers
        self.timeout = timeout
        self._base = base_url

    async def send(self, model: str, messages: list[dict],
                   max_tokens: int = 64) -> tuple[int, str]:
        """发一次请求，返回 (http_code, 响应文本/错误摘要)。"""
        body = {"model": model, "messages": messages, "max_tokens": max_tokens}
        async with httpx.AsyncClient(
            timeout=self.timeout, trust_env=False
        ) as client:
            try:
                r = await client.post(
                    f"{self._base}/chat/completions", json=body,
                    headers=self.headers)
            except httpx.HTTPError as e:
                return 0, f"transport-error: {type(e).__name__}"
        if r.status_code != 200:
            return r.status_code, r.text[:300]
        try:
            data = r.json()
            return 200, data["choices"][0]["message"]["content"] or ""
        except (ValueError, KeyError, IndexError):
            return r.status_code, r.text[:300]

    async def trial(self, model: str, filler_tokens: int, needle: str,
                    needle_pos: float):
        """单轮：建草垛→强制约束提问→判定找回是否成功。"""
        hay = make_haystack(filler_tokens, needle, needle_pos)
        messages = [
            {"role": "user", "content": (
                "You are a retrieval test. Read the entire text below carefully. "
                "It contains exactly one passphrase of the form "
                "'KEY <word>-<number>-<word>'. "
                "At the very end of your reply, output ONLY that passphrase.\n\n"
                f"TEXT:\n{hay}")},
        ]
        t0 = time.monotonic()
        code, resp = await self.send(model, messages)
        elapsed = time.monotonic() - t0
        if code != 200:
            return {"code": code, "recall": "hard_error", "elapsed_s": round(elapsed, 1),
                    "resp": resp}
        exact = needle in resp
        # 部分找回：针的关键段（word-number）出现
        core = needle.split("KEY ")[1]
        partial = (not exact) and (
            core.split("-")[0] in resp and core.split("-")[1] in resp)
        recall = "exact" if exact else ("partial" if partial else "miss")
        return {"code": code, "recall": recall, "elapsed_s": round(elapsed, 1),
                "resp": resp[:120]}


def gateway_headers(key: str) -> dict:
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def upstream_headers() -> dict:
    from app.core.headers import zen_headers
    return zen_headers()


async def run(args):
    sizes = [int(s) for s in args.sizes.split(",")]
    positions = [POSITIONS[p] for p in args.positions.split(",")]

    if args.direct:
        base = args.upstream
        headers = upstream_headers()
        model = args.model
        path = "直连上游"
    else:
        base = args.base_url
        headers = gateway_headers(args.key)
        model = args.model
        path = "网关代理"

    probe = Probe(base, headers, args.timeout)
    print(f"模型: {model}  链路: {path}  上游: {base}")
    print(f"尺寸: {sizes}  位置: {args.positions}  每格重复: {args.trials}\n")

    header = f"{'tokens':>9} | {'位置':>5} | {'trial':>5} | {'recall':>9} | {'耗时s':>6} | 备注"
    print(header)
    print("-" * 72)

    summary = {}
    for size in sizes:
        cell = {}
        for pos_name, pos in zip(args.positions.split(","), positions):
            results = []
            for t in range(1, args.trials + 1):
                needle = new_needle()
                r = await probe.trial(model, size, needle, pos)
                results.append(r)
                note = r["resp"] if r["recall"] == "hard_error" else ""
                print(f"{size:>9} | {pos_name:>5} | {t:>5} | {r['recall']:>9} | "
                      f"{r['elapsed_s']:>6} | {note}", flush=True)
                await asyncio.sleep(args.gap)
            recalls = [r["recall"] for r in results]
            exact = recalls.count("exact")
            part = recalls.count("partial")
            err = recalls.count("hard_error")
            cell[pos_name] = {
                "exact": exact, "partial": part, "hard_error": err,
                "success_rate": round((exact + part) / len(results), 2),
                "avg_s": round(sum(r["elapsed_s"] for r in results) / len(results), 1),
            }
        summary[str(size)] = cell
        # 全格全 miss/硬失败：此尺寸不可用，停止增大
        all_fail = all(
            (v["exact"] + v["partial"]) == 0 for v in cell.values())
        if all_fail:
            print(f"  ^^ 在 {size} tokens 全部不可用，停止继续增大。")

    print("\n=== 汇总（success_rate = (exact+partial)/trials） ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    print("\n=== 按尺寸结论 ===")
    for size_s, cell in summary.items():
        rows = ", ".join(
            f"{k}:{v['success_rate']:.0%}({v['exact']}/{v['partial']}/{v['hard_error']})"
            for k, v in cell.items())
        # 可用定义：>50% 格子成功率 >= 0.67
        usable = sum(1 for v in cell.values() if v["success_rate"] >= 0.67)
        tag = "✅可用" if usable > len(cell) / 2 else "⚠️退化"
        print(f"  {int(size_s):>9} tokens  {tag}  [{rows}]")


def main():
    ap = argparse.ArgumentParser(description="大海捞针上下文压测（加强版）")
    ap.add_argument("--model", default="deepseek-v4-flash-lite")
    ap.add_argument("--base-url", default="https://api.pixelreeds.com")
    ap.add_argument("--key", default=os.environ.get("OPENCODE_KEY", ""))
    ap.add_argument("--sizes", default="128000,512000,1000000")
    ap.add_argument("--positions", default="head,mid,tail")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--gap", type=float, default=2.0, help="每轮之间间隔秒")
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--direct", action="store_true",
                    help="直连上游（不经网关/代理池），作对照")
    ap.add_argument("--upstream", default="https://opencode.ai/zen/v1",
                    help="直连时的上游地址")
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
