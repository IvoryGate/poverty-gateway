# poverty-gateway

自研 LLM 代理网关：经免费代理 IP 池转发请求到 zen 免费模型，对外提供
OpenAI 兼容的 API-Key 服务。

## 架构

```
客户端 (opencode/Cursor/Codex/curl)
    │  OpenAI 兼容 API (https://api.pixelreeds.com)
    ▼
nginx 反代 → uvicorn (2 workers, :8000)
    │
    ├─ 鉴权/限流 (API-Key, per-key 速率)
    ├─ 模型映射 (-lite 对外 / -free 上游)
    ├─ 代理池取号 (proxy-pool 候选 + zen 命中筛选)
    │      ├─ 后台补池（非阻塞、单飞）
    │      ├─ zenhit score 记忆 (Redis, TTL 6h)
    │      └─ 取号分档：正分优先 / 负分兜底
    └─ 上游转发 (经池 IP → https://opencode.ai/zen/v1)
         ├─ 流式透传 (SSE)
         └─ 错误分桶 (429/5xx/4xx/timeout/conn)
```

共享状态在 Redis（db1）：in-flight、冷却、zenhit、用量统计、模型映射、
池快照。uvicorn 2 workers 各自持有内存池，跨 worker 状态靠 Redis 同步。

## 快速开始

```bash
pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 2
```

生产由 systemd 托管（`poverty-gateway.service`），nginx 反代 HTTPS。
依赖本机 proxy-pool（Redis db0 读候选）。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `API_KEYS_FILE` | `/etc/poverty-gateway/keys.json` | key 列表 |
| `REDIS_URL` | `redis://127.0.0.1:6379/1` | 网关状态库 |
| `UPSTREAM_BASE` | `https://opencode.ai/zen/v1` | 上游 zen |
| `PROXY_POOL_URL` | `http://127.0.0.1:5010` | proxy-pool API |
| `POOL_TARGET` / `POOL_LOW_WATER` | 60 / 40 | 池水位 |
| `PROBE_CONCURRENCY` | 8 | 候选探测并发 |
| `PROBE_MODEL` | `deepseek-v4-flash-free` | 探测用上游模型 |

## API

- `GET /v1/models` — 模型列表（含 context_length=1M）
- `GET /v1/stats` — 池水位 / 用量 / 错误构成
- `POST /v1/chat/completions` — chat 补全（非流式 + SSE 流式）
- `POST /v1/responses` — Responses API 透传

兼容两种 baseURL：`https://api.pixelreeds.com` 与 `.../v1`。

## 测试

```bash
.venv/bin/python -m pytest      # 59 passed
.venv/bin/ruff check .          # All checks passed
```

## 文档

阶段式开发记录见 `docs/PROGRESS.md`（索引）+ `docs/NN-<阶段>.md`（详录）。
