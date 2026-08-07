---
name: engineering-discipline
description: 工程纪律与可追溯开发流程——在任何软件项目的每个阶段强制执行。Used when starting a new project, at the start of every development stage, when about to write a git commit, when updating progress docs, or when setting up/reviewing CI. Codifies conventional-commits + stage-based workflow (AGENTS.md + numbered stage docs + stage-tagged commits) + GitHub Actions as a consistent, agent-agnostic engineering baseline.
compatibility: any language or framework
---

# Engineering Discipline（工程纪律）

一个让任何编程 agent 都能"稳定推进"且"进程可追溯"的通用流程。它不依赖具体技术栈，由三块可组合的规范构成：

1. **协作模式**：New project → `AGENTS.md` + 按阶段记录的文档（`docs/XX-<阶段名>.md`）+ 总索引 `docs/PROGRESS.md` → 提交时按 `stage: <编号>` 标注。
2. **提交规范**：Conventional Commits（`feat:`/`fix:`/`docs:`/`chore:`/`refactor:`…）。
3. **CI 兜底**：GitHub Actions 自动运行 lint/typecheck/test/build，防止坏代码落 `main`。

其余 skill（前端、Python、CI 专项）按项目技术栈按需补充——本技能只保证"推进方式"一致。

## 何时使用（Trigger）

- 用户新建项目 / 打开一个空仓库时 → 初始化 `AGENTS.md`
- 每个**开发阶段**开始时 → 要求写独立阶段文档
- 每次写 git commit 时 → 校验 Conventional Commits
- 每次阶段完成时 → 更新 `PROGRESS.md` 总索引 + 提交（带 `stage:`）
- 首次搭建 或 复用 仓库 CI 时 → 套用 CI 模板

## 核心工作流

### 1. 新项目初始化

启动新项目的第一件事是建立三份文件，让后续所有 agent 会话都有"共同上下文"：

```text
project-root/
├── AGENTS.md            # 给所有 agent 的项目手册（必建）
├── docs/
│   ├── PROGRESS.md      # 总索引：最新状态 + 阶段总览表
│   └── 01-<阶段名>.md   # 当前阶段详细记录
└── .github/workflows/ci.yml   # CI（可选，建议尽早建）
```

### 2. 阶段化推进（核心增量单元）

把工作切成**阶段（stage）**，每阶段遵循固定节奏：

1. **开始阶段时**：明确阶段目标 + 拆成可执行 todo
2. **过程中**：每完成一个逻辑单元就跑一次 lint + typecheck/build 验证
3. **阶段结束时**：更新 `PROGRESS.md` → 写详细阶段文档 → 提交
4. **不要跨目录/跨阶段乱改**，一个 commit 聚焦一个逻辑变更

### 3. Commit 规范（Conventional Commits）

每个 commit 消息形如 `<type>[scope]: <描述>`：

| type | 用途 | 例子 |
|---|---|---|
| `feat:` | 新功能 | `feat: add proxy health checker` |
| `fix:` | 修 bug | `fix: handle connection timeout` |
| `refactor:` | 重构（不改行为） | `refactor: extract pool manager` |
| `docs:` | 文档/进度记录 | `docs: stage 02 进度记录` |
| `chore:` | 杂项/依赖/CI | `chore: pin actions to SHA` |
| `perf:` | 性能 | `perf: reduce lock contention` |

- 描述用**祈使句**，不省略 `.`
- **阶段性提交附 `stage: <编号>`**，例如：
  `feat: 代理池状态管理 (stage: 02)`
- 破坏性改动用 `BREAKING CHANGE:` 或 `feat!:` 后缀

**提交纪律**：
- 只暂存本次意图内的文件，**绝不提交密钥/凭证**
- 提交前 `git status` / `git diff` 检查
- 不要在无关阶段乱塞变更

### 4. 进度记录（文档约定）

- 每个阶段独立文档 `docs/XX-<阶段名>.md`，**口语化、尽量详细**，字段模板：
  `背景思考 / 完成内容 / 技术决策及理由 / 实际命令 / 踩坑记录 / 下一步`
- 阶段完成时往总索引 `docs/PROGRESS.md` **追加一行**并附文档链接
- 这样 git log + docs 双轨，任何 agent 或人都能快速"读进度"

### 5. CI 兜底（GitHub Actions）

用模板 `.github/workflows/ci.yml`，在推送上自动跑 lint + typecheck + build + test。
这是"稳定推进"的最后一道闸：本地 agent 可能漏验证，CI 保证坏代码进不了主干。

**CI 安全底线**（改任何 workflow 后必须审计）：
- 最小权限 `permissions: contents: read`
- Action 钉到版本/SHA（`actions/checkout@v4`，开源如需安全用 SHA + pinact）
- 不要 `echo` 未经校验的上下文字符串到 shell（注入风险）→ 用 `env:` 传变量
- 用 `gh run watch --exit-status`、`gh run view --log-failed` 调式

## 验证清单（本技能是否执行到位）

完成任何一段推进后自检：

- [ ] `AGENTS.md` 存在且能回答"项目约定/命令/禁忌"
- [ ] 每个阶段有独立文档 + 总索引有记录
- [ ] commit 头均符合 Conventional Commits
- [ ] 阶段完成提交带 `stage:` 标注
- [ ] CI 存在且 push 后跑通（或已配置）

## 待办与下一步

本技能只提供"稳定的壳"，具体实现（业务逻辑 / 框架选择 / 代码质量）仍需依赖：
- 各技术栈的专项 skills（typescript / python / 前端 等）
- 每阶段文档里记录的具体技术决策

工程稳定 = 流程一致 × 每阶段验证 × 可追溯，三缺一不可。