# 阶段 08/09 — Codex Responses API 支持 + 监控面板

## 背景思考

阶段 06/07 打通了 opencode 实测闭环，但对外宣称兼容主流工具只验证了 opencode。
两个诉求：

1. **Codex**（OpenAI 官方 CLI）走的是 **Responses API**（`/v1/responses`），
   不是 Chat Completions（`/v1/chat/completions`），且带 `web_search` 等工具。
   不实现 Responses 端点，Codex 无法接入。
2. **可观测性缺失**：池水位、每 key 用量、错误率都要靠手动 curl stats，没有可视化。

## 阶段 08：Codex Responses API 支持

### 完成内容

`app/services/upstream.py` + `app/api/routes.py` 新增：

- **`/v1/responses` 与 `/responses`** 端点：接受 OpenAI Responses 格式请求，
  内部转调上游 `responses` 端点，复用同一套代理池 + 熔断 + 重试。
- **`_sanitize_responses`**：过滤不支持的工具（实测 `web_search` 会 4xx），
  保留对话与基础工具。
- **流式事件补齐**：zen 只返回 `delta` + `completed`，Codex SDK 需要
  `response.created` / `response.output_item.added` / `response.output_item.done`
  才能组装输出。`_inject_responses_events` 在合适位置注入缺失事件。

### 技术决策

| 决策 | 理由 |
|---|---|
| 独立端点而非复用 chat | Responses 请求结构/事件流与 chat 不同，混用会破坏事件语义 |
| web_search 工具过滤掉 | Codex 用它做联网检索，但上游免费层不支持，传了会 4xx |
| 流式注入补齐事件 | 让 Codex SDK 能正确组装出内容，而非收到裸 delta 无法渲染 |

### 踩坑

1. **Codex 请求会带 `web_search` 工具**：直接透传上游返回 4xx。逐层排查
   发现是工具不支持，过滤后才通过。
2. **Codex SDK 收不到事件就挂**：只有 delta/completed 不够，必须补齐
   created/added/done 事件链，否则输出框永远空白。
3. **本机 Codex 卡在 chatgpt.com MCP**：网关侧已完成，但服务器连不上
   chatgpt.com（网络限制），Codex 客户端无法完成初始化。判定为环境问题，
   用户本机用 Codex（带登录态）实测通过即可，网关侧无需再改。

## 阶段 09：监控面板

### 完成内容

`app/api/panel.html` + 路由：

- **根路径 `/` 与 `/panel`** 双入口：深色企业风单页。
- 需 API key 鉴权（`Authorization: Bearer`）。
- 5s 自动刷新，展示：
  - **KPI 卡片**：请求量 / 成功率 / 平均延迟 / tokens
  - **代理池水位**：available / cooldown / target / low_water 实时条
  - **每 key 用量**：各 key 请求数与错误率
  - **模型列表**：`/v1/models` 动态展示

数据全部来自已有 `/v1/stats`、`/v1/models`，前端只做展示，无新增后端统计。

### 技术决策

| 决策 | 理由 |
|---|---|
| 静态 HTML + fetch 轮询 | 零后端负担，5s 一次轻量 GET |
| 面板本身要鉴权 | 暴露池/用量数据给公网，必须挡在 API key 后 |
| 放根路径 | 部署到 api.pixelreeds.com/ 即达，无需记路径 |

### 踩坑

1. **面板也走代理链**：开发时面板 HTML 是直接读文件，不经过代理池，
   避免面板打开也消耗代理 IP。
2. **nginx 根路径反代**：`/` 要同时服务面板和 API，nginx location `/`
   全量转 8000，由网关按路径分发。

## 实测

- Codex 网关侧全链路通过（补事件后 SDK 能组装输出）。
- 面板在 https://api.pixelreeds.com/ 与 /panel 均可打开，鉴权生效，
  5s 刷新池水位实时跳动。
