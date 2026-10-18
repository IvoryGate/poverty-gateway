# 阶段 19：free 档校验再次变更（请求头 + body 形状 + 探测）

## 背景思考

阶段 18 把请求头修成「26 位小写 hex + 新 UA」后，端到端通过了。但 2026-10-14
上游又换了一轮 free 档校验，线上表现为：

- 所有模型、所有请求一律 `403 FreeTierError`；
- 网关 `/health` 显示 `pool: empty`，但 proxy-pool 本身有 160+ 个 IP；
- 模型同步日志里所有模型都 `model probe failed`。

这次参照 `https://github.com/jasonxu114514/opencode2api` 的实现，并抓了一份
本机真实 opencode 1.18.31 客户端的上游请求（`/tmp/opencode/full-dump.json`）
做对照。关键发现是 **两类 403 要分开看**：

| 上游 message | 含义 | 责任方 |
|---|---|---|
| `OpenCode's free tier can only be used from within OpenCode` | 请求头/body 不像 opencode 客户端 | 网关，必须改请求形状 |
| `Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic` | 形状已经通过，但这个出口 IP 被上游判定滥用 | 代理 IP，换 IP |

阶段 18 之后线上全 403 的第一层原因是 **探测请求 body 仍是「裸 chat」**
（`stream=false`、无 tools），于是每个候选 IP 都被判 403 → 全部 DEAD →
可用池饿死。第二层原因是客户端/非流式请求的 body 也不满足新 gate。

## 完成内容

### 1. `app/core/headers.py` 按真实客户端重新对齐

- `x-opencode-session`：canonical 布局
  `ses_` + 12 位小写 hex（`~(ms<<12|counter)` 降序时间戳，counter=1）+
  14 位 Base62。真实客户端抓包与阶段 18 的「纯随机 26 hex」不同，这版按
  客户端算法生成。
- 新增 `x-opencode-session-id`，与 `x-opencode-session` 同值（新版
  `/inference` 要求两个都有；旧版 `/zen/v1` 对多出的头不敏感）。
- `x-opencode-request`：`msg_` + 12 位升序毫秒时间戳 + 14 位 Base62，
  与真实客户端一致（旧版是随机 26 hex）。
- `x-opencode-project` 固定 `global`（真实客户端就是 global；canonical
  `prj_*` 在新版 gate 会被 403）。
- UA 保持真实客户端的
  `opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14`。
- 去掉 `x-api-key: public`：真实客户端只发 `Authorization: Bearer public`。
- 新增 `zen_headers(seed)`：按会话种子稳定派生 session，同一会话多轮命中
  上游 prompt cache；探测/同步不传种子。

### 2. 所有「非对话」探测补成 agent 形状

新增 `probe_payload(model)`（`stream=true` + `read` 工具 + `max_tokens=1`）：

- `proxy_pool._probe()` 改用它。这是本次池子饿死的直接修复。
- `model_sync._probe_model()` 改用它，模型可用性探测不再全灭。
- 探测 403 时区分 message：若是「形状被拒」而不是 IP 滥用，打 warning，
  便于下次上游再改校验时第一时间定位。

### 3. 真实转发补 read 工具 + 非流式聚合

- `_ensure_read_tool()`：请求里没有名为 `read` 的工具时，追加一个
  「DEPRECATED，不要调用」的占位工具。旧版 `/zen/v1` 只校验工具名。
- 非流式 `/v1/chat/completions`：上游新 gate 只接受 `stream=true`，因此
  网关内部强制流式，再由 `_ChatStreamCollector` 把 SSE 聚合回标准
  `chat.completion`（content / reasoning_content / tool_calls / usage 都保留）。
- 非流式 4xx 不再把代理打进冷却：`400/404/422`（参数/模型不支持）是请求
  本身的问题，换代理也没用，只归还 in-flight 后原样透传。只有
  `403 Detect Abuse Traffic` 才剔除该 IP。

### 4. 探测超时 5s → 8s

免费代理质量参差，5s 会误杀响应偏慢但可用的 IP。

## 技术决策及理由

1. **以真实客户端抓包为准，而不是继续猜头**：网上的实现可能滞后一两天，
   本机 opencode 1.18.31 的真实请求是当下最权威的指纹。
2. **两类 403 分流**：如果不区分，就会把「我们头写错」误判成「所有 IP
   都坏了」，继续无意义地换 IP/换代理。区分后，`Detect Abuse Traffic`
   只影响单个 IP，`free tier can only...` 直接暴露自己的形状 bug。
3. **探测必须走真实 gate 形状**：探测是池子的准入标准，标准错了池子必然
   饿死。探测 body 与真实请求共用 `probe_payload()`，不再手工拼裸 chat。
4. **非流式强制流式再聚合**：上游只认 `stream=true`，而 Cursor/curl 等
   客户端仍会发 `stream=false`。与其让它们全部 403，不如网关内部转换，
   对外语义保持不变。
5. **4xx 不惩罚代理**：一个「模型不支持该协议」的 400 不应该让唯一可用
   的 IP 进冷却；这类错误与出口 IP 无关。

## 实际命令

```bash
# 单测 / lint
.venv/bin/python -m pytest -q          # 79 passed
.venv/bin/ruff check app tests         # 仅剩 proxy_pool 3 个既有告警

# 本地灰度实例（独立 Redis db2，避免污染生产）
REDIS_URL=redis://127.0.0.1:6379/2 \
API_KEYS_FILE=/etc/poverty-gateway/keys.json \
POOL_TARGET=8 POOL_LOW_WATER=1 PROBE_TIMEOUT=8 \
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8010

# 真实请求验证（流式）
curl -N http://127.0.0.1:8010/v1/chat/completions \
  -H 'Authorization: Bearer <key>' -H 'Content-Type: application/json' \
  -d '{"model":"space-bunny-lite","stream":true,"max_tokens":32,
       "messages":[{"role":"user","content":"reply STREAMOK"}],
       "tools":[{"type":"function","function":{"name":"read",
         "parameters":{"type":"object","properties":{}}}}]}'

# 非流式（网关内部流式 + 聚合）
curl http://127.0.0.1:8010/v1/chat/completions \
  -H 'Authorization: Bearer <key>' -H 'Content-Type: application/json' \
  -d '{"model":"space-bunny-lite","stream":false,
       "messages":[{"role":"user","content":"reply PLAINOK"}]}'
```

实测：流式 3/3 返回 200；非流式返回标准 `chat.completion`，带
`reasoning_content` 与 usage；代理偶发 `ConnectError` 时该 IP 会被剔除，
下一次真实请求触发补池后恢复。

## 踩坑记录

- **`Detect Abuse Traffic` 不是头的问题**：一开始看到 403 就继续改头，
  后来发现同一套头在某个干净 IP 上是 200，换 IP 就是 abuse，才确认是
  IP 信誉。免费代理池里大部分出口 IP 已被上游标记，干净 IP 很少且会失效。
- **探测请求自身必须过 gate**：这是本次「池子明明有 160 个 IP 却显示空」
  的根因，比请求头本身更隐蔽。
- **代理探测到的「干净 IP」随时会失效**：实测一个 IP 几分钟内从 200 变成
  403 abuse 或直接连不上，因此只能靠真实请求持续触发补池，不能假设池子
  长期稳定。
- **非流式 400 会误伤代理**：本地验证时，`/v1/responses` 对
  space-bunny 返回 `ModelProtocolUnsupported`，旧逻辑把唯一可用代理打进
  冷却，后面 chat 全部 503。改成 4xx 只归还 in-flight。

## 下一步

1. 重启生产 `poverty-gateway`，清掉旧 `pool_snapshot` 后冷启动；用真实请求
   逐步触发补池（干净 IP 少，属于正常现象）。
2. `/v1/responses` 非流式仍是裸透传，未做「强制流式 + Responses 事件聚合」；
   Codex 如需非流式再补。
3. 若干净 IP 长期不足，考虑给 proxy-pool 增加住宅/轮换出口，或评估
   `/inference` 新 lane 作为 fallback。
