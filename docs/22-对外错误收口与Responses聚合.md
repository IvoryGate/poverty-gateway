# 阶段 22：对外错误收口 + Responses 非流式聚合

## 背景思考

用户的核心要求：**网关对外提供服务时，不应该把上游的 FreeTier 错误透给客户端**。
这正是网关存在的意义之一——客户端只应该看到「服务暂时不可用/稍后重试」，
而不是 `OpenCode's free models aren't available...` 这种上游内部文案。

阶段 21 已经让 abuse 按模型拉黑，但还有两个漏洞：

1. **错误文案会变**：上游 2026-10 把 `Detect Abuse Traffic` 改成
   `detected abusive traffic from this IP address / aren't available from
   your current network`。旧分类只认第一种，新文案会被当成普通 4xx 透传。
2. **非流式 Responses 仍是裸透传**：`payload["stream"]=false` 发上游，
   free 档直接返回 FreeTierError（请求形状不符）。Codex 正常用流式不受影响，
   但任何非流式 `/v1/responses` 都会撞这个错。

## 完成内容

### 1. FreeTier 错误统一分类

`app/core/freetier.py`：

- `is_abuse_message()`：识别新旧 abuse 文案
  （`detect abuse` / `abusive traffic` / `not available from your current network`）。
- 明确排除 `free tier can only be used from within OpenCode`：那是**请求形状**
  错误，不能按 IP abuse 拉黑。
- `is_freetier_message()`：任何 FreeTier 相关错误，供对外收口使用。

### 2. 对外错误收口（绝不透传 FreeTier）

`app/api/routes.py`：

- 非流式：FreeTier → **503** `upstream_unavailable`
  （429 时仍是 429 `rate_limit_error`）。
- 流式 SSE：FreeTier → `type: upstream_unavailable` 的 error 事件。
- 对外 message 固定为：
  - `upstream free tier is temporarily unavailable; please retry later`
  - 429: `upstream free tier is rate limited; please retry later`
- 不再把 `FreeTierError`、`OpenCode`、`abusive traffic` 等字样发给客户端。
- 非 FreeTier 的 4xx/429/5xx 行为保持不变。

### 3. Responses 非流式也走「内部流式 + 聚合」

`app/services/upstream.py`：

- `responses()` 不再 `stream=false` 裸透传，改为：
  - 强制 stream=true + read 工具（复用 `_forward_stream`）；
  - `_responses_stream_request` 补齐标准事件；
  - `_ResponsesStreamCollector` 聚合 `response.completed` / delta，
    返回标准 `response` 对象。
- 这样非流式 `/v1/responses` 不会再触发 free 档的形状校验。

## 验证

- 新增测试：
  - 新旧 abuse 文案分类；
  - 非流式 FreeTier → 503 `upstream_unavailable`，且不含上游文案；
  - 流式 FreeTier → SSE 事件收口；
  - Responses 非流式聚合。
- `89 passed`。
- 本地 8010 实测：FreeTier/首字节失败对外返回
  `{"error":{"message":"...","type":"upstream_error"}}` / `upstream_unavailable`，
  不再出现 `FreeTierError`。

## 技术决策及理由

1. **FreeTier 是网关口，不是客户端口**：客户端无法处理「OpenCode free tier」
   这种语义；统一映射成 503/429 才是 OpenAI 兼容网关应有的行为。
2. **错误文案集中分类**：代理探测和真实转发共用 `freetier.py`，上游再改
   文案只改一处。
3. **Responses 非流式也内部流式**：和 chat 保持一致，避免留下第二套
   「stream=false 撞 free 档」的路径。
4. **不把 abuse 当请求错误**：abuse 是 IP 问题，按模型拉黑 + 换 IP；
   只有重试耗尽才对客户端返回 503，而不是 403 FreeTier。

## 下一步

1. 重启网关加载本阶段代码。
2. 观察面板 `abuse_reports` / `abuse`，以及客户端是否还看到 FreeTier 文案。
3. 如果所有 IP 都 abuse，客户端会收到 503；这表示需要更多干净出口，
   而不是请求头问题。
