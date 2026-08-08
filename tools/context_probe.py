#!/usr/bin/env python3
"""大海捞针（Needle in Haystack）上下文压测工具。

科学测量代理网关后真实模型的有效上下文窗口：
- 把唯一"针"（随机关键词）埋在递增的"草垛"（填充文本）中
- 问模型能否找回针 → 反映"可用上下文"（不只是"能接受请求"）
- 每个尺寸多轮重复，输出成功率、平均耗时、HTTP 硬失败

用法：
  python tools/context_probe.py --model deepseek-v4-flash-lite \
      --sizes 8000,16000,32000,64000,128000 --trials 3

说明：
- 草垛按 token 数生成（英文 4 字符 ≈ 1 token），体积受 nginx client_max_body_size
  限制（当前 10m ≈ 250 万 token，1M 内安全）。
- 针位置取草垛的 60% 处（偏后，考察长程记忆）。
- 默认走网关（生产路径，经代理池转发）；--direct 可直连上游对比。
"""

import argparse
import asyncio
import json
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


def make_haystack(filler_tokens: int, needle: str, needle_pos: float = 0.6):
    """按目标 token 数生成草垛，并把针埋在指定比例位置。"""
    tokens_per_block = FILLER_BLOCK_TOKENS
    n_blocks = max(1, filler_tokens // tokens_per_block)
    parts = [FILLER_BLOCK] * n_blocks
    needle_idx = int(len(parts) * needle_pos)
    parts[needle_idx] = FILLER_BLOCK.split(".")[0] + ". " + needle + " " + FILLER_BLOCK
    return "".join(parts)


def count_tokens(text: str) -> int:
    """近似 token 数（英文 4 字符 ≈ 1 token）。"""
    return max(1, len(text) // 4)


class Probe:
    def __init__(self, base_url: str, key: str, timeout: float = 600.0):
        self.headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
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

    async def trial(self, model: str, filler_tokens: int,
                    needle: str, needle_pos: float = 0.6):
        """单轮：建草垛→提问→判定找回是否成功。"""
        hay = make_haystack(filler_tokens, needle, needle_pos)
        messages = [
            {"role": "user", "content": (
                "I will show you a long text. Memorize the unique passphrase "
                "that appears in it (it looks like 'KEY <word>-<num>-<word>'). "
                "At the very end, repeat that passphrase exactly, and nothing else.\n\n"
                f"TEXT:\n{hay}")},
        ]
        t0 = time.monotonic()
        code, resp = await self.send(model, messages)
        elapsed = time.monotonic() - t0
        if code != 200:
            return {"code": code, "recall": "hard_error", "elapsed_s": round(elapsed, 1),
                    "resp": resp}
        exact = needle in resp
        # 部分找回：针的关键部分（word-num）出现
        core = needle.split("KEY ")[1] if "KEY " in needle else needle
        partial = not exact and core.split("-")[0] in resp
        recall = "exact" if exact else ("partial" if partial else "miss")
        return {"code": code, "recall": recall, "elapsed_s": round(elapsed, 1),
                "resp": resp[:120]}


async def run(args):
    sizes = [int(s) for s in args.sizes.split(",")]
    needle = f"KEY {random.choice(('orange','violet','cobalt'))}-{random.randint(1000,9999)}-{random.choice(('xray','kite','moss'))}"
    probe = Probe(args.base_url, args.key)
    print(f"模型: {args.model}  上游: {args.base_url}")
    print(f"针: {needle}\n")
    print(f"{'tokens':>9} | {'trial':>5} | {'recall':>9} | {'耗时s':>6} | 备注")
    print("-" * 70)
    summary = {}
    for size in sizes:
        results = []
        for t in range(1, args.trials + 1):
            r = await probe.trial(args.model, size, needle)
            results.append(r)
            note = r["resp"] if r["recall"] == "hard_error" else ""
            print(f"{size:>9} | {t:>5} | {r['recall']:>9} | {r['elapsed_s']:>6} | {note}")
            await asyncio.sleep(args.gap)
        recalls = [r["recall"] for r in results]
        ok = recalls.count("exact")
        part = recalls.count("partial")
        err = recalls.count("hard_error")
        summary[str(size)] = {
            "exact": ok, "partial": part, "hard_error": err,
            "success_rate": round((ok + part) / len(results), 2),
        }
        # 已发生硬失败/全 miss，更大尺寸大概率更差，尽早停
        if err == args.trials or (ok + part) == 0:
            print(f"  ^^ 在 {size} tokens 已不可用，停止继续增大。")
            break
    print("\n=== 汇总 ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("\n=== 结论 ===")
    for size_s, s in summary.items():
        tag = "✅可用" if s["success_rate"] >= 0.67 else "⚠️退化"
        print(f"  {int(size_s):>9} tokens  {tag}  exact={s['exact']} partial={s['partial']} err={s['hard_error']} rate={s['success_rate']:.0%}")


def main():
    ap = argparse.ArgumentParser(description="大海捞针上下文压测")
    ap.add_argument("--model", default="deepseek-v4-flash-lite")
    ap.add_argument("--base-url", default="https://api.pixelreeds.com")
    ap.add_argument("--key", default="sk-REDACTED")
    ap.add_argument("--sizes", default="128000,256000,512000,768000,1000000")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--gap", type=float, default=2.0, help="每轮之间间隔秒")
    ap.add_argument("--timeout", type=float, default=600.0)
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
