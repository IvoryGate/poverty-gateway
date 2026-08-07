# AGENTS.md — poverty-gateway

> 任何 agent 进入本项目应首先阅读本文件。它保证每个 agent 会话都能：
> 知道项目在做什么、怎么运行、有哪些约定、当前推进到哪里、有哪些禁忌。

## 项目背景

**poverty-gateway**：零成本 LLM 网关。通过 proxy-pool（同机 `127.0.0.1:5010`）
提供的免费代理 IP，转发请求到 OpenCode AI 免费模型（`deepseek-v4-flash-free`
等），对外暴露 **OpenAI 兼容接口**。自用为主。

核心约束：
- **完全禁用本地 IP 直连**——所有上游请求必须走代理池提供的 IP（即使本机 IP 可用）。
- **IP 档位降级链**：优先 `stable3` → `stable2` → `stable1`，全无则停止服务（宁缺毋滥）。
- **对外模型名映射**：对外暴露自定义伪名，不暴露 `deepseek-v4-flash-free` 等真实名。
- **对外必须 HTTPS**（走 nginx 二级域名 + 通配证书），HTTP 不安全。
- 鉴权暂缓：先跑通转发，后续再加 API-Key。

## 仓库现状

- 从零自研项目（非开源网关的 fork）。
- 当前进度以 `docs/PROGRESS.md` 为准。
- 可运行命令（随阶段补充）：
  - `pip install -r requirements.txt`（安装依赖）
  - `uvicorn app.main:app --host 0.0.0.0 --port 8000`（本地启动）
  - `python -m pytest`（测试）
  - `ruff check .`（lint）

## 依赖的上游服务

- **proxy-pool**（同机 `http://127.0.0.1:5010`）：提供代理 IP。
  - `/get?need=global&type=https&count=N&quality=stableX`
  - `/health` `/count` 可查池子状态。
  - 池子地址可经 `PROXY_POOL_URL` 环境变量覆盖。

## 工程纪律约定（勿删）

1. 每个**开发阶段**建立独立文档 `docs/XX-<阶段名>.md`，口语化、尽量详细地记录过程。
   - 字段模板：背景思考 / 完成内容 / 技术决策及理由 / 实际命令 / 踩坑记录 / 下一步
2. 阶段完成时更新 `docs/PROGRESS.md`（总索引）：追加一行阶段记录 + 指向详细文档。
3. 阶段完成时使用规范的 git commit 消息（附 `stage: <编号>`，如 `stage: 01`）。
4. **不要跨目录修改无关文件**；如需联动外部仓库，遵守其自身边界。
5. 提交时绝不 include 密钥 / 凭证。

## Git 提交规范（Conventional Commits）

| type | 用途 |
|---|---|
| `feat:` | 新功能 |
| `fix:` | 修复 |
| `refactor:` | 重构（不改行为） |
| `docs:` | 文档 / 进度记录 |
| `chore:` | 杂项 / 依赖 / CI |
| `perf:` | 性能 |

阶段完成时建议附加 `stage: <阶段名>`。

## 当前阶段

以 `docs/PROGRESS.md` 最新记录为准。
