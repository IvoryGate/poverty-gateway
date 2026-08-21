#!/usr/bin/env python3
"""同步网关模型列表到 opencode 配置。

opencode 对自定义 provider（@ai-sdk/openai-compatible）**不会**自动调用
/v1/models 发现模型——它只认 opencode.jsonc 里 provider.models 硬编码的
列表。所以模型一变（新增/下线），其它设备的 opencode 模型选择器不会跟着变。

本脚本解决这个割裂：拉取网关 /v1/models 的真实模型表，更新 opencode.jsonc
里 poverty provider 的 models 段（保留已有的 name/limit 显示配置，新模型
补默认 1M 上下文）。

用法：
  python tools/sync_opencode_models.py                # 写 ~/.config/opencode/opencode.jsonc
  python tools/sync_opencode_models.py --config /path/to/opencode.jsonc
  python tools/sync_opencode_models.py --base-url https://api.pixelreeds.com
  python tools/sync_opencode_models.py --dry-run      # 只看差异不写

依赖：仅标准库。API key 从 /etc/poverty-gateway/keys.json 读（root 可读）。
"""

import argparse
import json
import os
import sys
import urllib.request

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_CONFIG = os.path.expanduser("~/.config/opencode/opencode.jsonc")
KEYS_FILE = "/etc/poverty-gateway/keys.json"
PROVIDER_ID = "poverty"
DEFAULT_LIMIT = {"context": 1048576, "output": 131072}


def read_api_key():
    """从 keys.json 读第一个 API key。"""
    try:
        with open(KEYS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        print(f"[错误] 读 keys.json 失败: {e}", file=sys.stderr)
        sys.exit(1)
    keys = data.get("keys") or data.get("api_keys") or []
    if not keys and isinstance(data, dict):
        # keys.json 实际结构：{sk-xxx: {"name":..., "rpm":...}}，key 即 API key
        keys = [k for k in data if isinstance(k, str) and k.startswith("sk-")]
    if not keys:
        print("[错误] keys.json 里没找到 sk- key", file=sys.stderr)
        sys.exit(1)
    return keys[0]


def fetch_models(base_url: str, api_key: str) -> list[dict]:
    """GET /v1/models，返回模型列表。"""
    url = f"{base_url.rstrip('/')}/v1/models"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        print(f"[错误] 拉取 {url} 失败: {e}", file=sys.stderr)
        sys.exit(1)
    items = data.get("data", [])
    return [m for m in items if m.get("id")]


def read_config(path: str) -> dict:
    """读取 opencode.jsonc（容忍尾逗号/注释的宽松解析）。"""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        print(f"[错误] 读配置失败: {e}", file=sys.stderr)
        sys.exit(1)
    # jsonc 去注释 + 去尾逗号后按 json 解析。
    # 注意：只删"行首注释"（// 开头），不能碰 URL 里的 //（https://）。
    import re
    text = re.sub(r"(^|\n)\s*//[^\n]*", r"\1", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r",(\s*[}\]])", r"\1", text)
    try:
        return json.loads(text)
    except ValueError as e:
        print(f"[错误] 配置 JSON 解析失败（可能是合法 jsonc 但手写注释多）: {e}",
              file=sys.stderr)
        print("       可改用 --config 指向导出的纯净 JSON。", file=sys.stderr)
        sys.exit(1)


def write_config(path: str, cfg: dict):
    """写回 opencode.jsonc（保持 jsonc 风格：保留 $schema 注释风格）。"""
    backup = f"{path}.bak"
    try:
        with open(path, encoding="utf-8") as f:
            with open(backup, "w", encoding="utf-8") as b:
                b.write(f.read())
        print(f"[备份] 原配置 → {backup}")
    except OSError:
        pass
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n")


def main():
    ap = argparse.ArgumentParser(description="同步网关模型列表到 opencode 配置")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL,
                    help=f"网关地址（默认 {DEFAULT_BASE_URL}）")
    ap.add_argument("--config", default=DEFAULT_CONFIG,
                    help=f"opencode 配置文件（默认 {DEFAULT_CONFIG}）")
    ap.add_argument("--dry-run", action="store_true", help="只显示差异不写文件")
    args = ap.parse_args()

    api_key = read_api_key()
    models = fetch_models(args.base_url, api_key)
    cfg = read_config(args.config)

    provider = cfg.get("provider", {}).get(PROVIDER_ID)
    if provider is None:
        print(f"[错误] 配置里没有 provider.{PROVIDER_ID}", file=sys.stderr)
        sys.exit(1)

    old = provider.get("models", {})
    new = {}
    for m in models:
        mid = m["id"]
        if mid in old:
            new[mid] = dict(old[mid])  # 保留已有 name/limit
        else:
            new[mid] = {"name": mid.replace("-lite", "").title(),
                        "limit": dict(DEFAULT_LIMIT)}
    # 已下线模型：保留还是删除？删除——让客户端跟着网关走
    provider["models"] = new

    added = [m for m in new if m not in old]
    removed = [m for m in old if m not in new]
    print(f"网关模型 ({len(models)}): {sorted(new)}")
    if added:
        print(f"[新增] {added}")
    if removed:
        print(f"[移除] {removed}")
    if not added and not removed:
        print("[无变化]")
        return

    if args.dry_run:
        print("[dry-run] 未写文件")
        return
    write_config(args.config, cfg)
    print(f"[完成] 已更新 {args.config}")
    print("       重启 opencode 生效。")


if __name__ == "__main__":
    main()
