# PROGRESS.md — 总进度索引

## 当前进度

**阶段 03 完成（API-Key 鉴权 + 每 key 限流）**
> OpenAI 标准 `Authorization: Bearer <key>` 鉴权，/v1/models 与 /v1/chat/completions
> 需 key，/health 放开（监控探活）。
> 每 key 独立 rpm（固定窗口计数）：matrix:120 / dev:60 / ci:30，超限 429。
> key 存 /etc/poverty-gateway/keys.json（不进 git），systemd 环境变量注入。
> 实测：无 key 401、错误 key 401、正确 key 200、rpm=1 测试 key 第2次起 429。
> 33 测试通过 + ruff 全过。

## 阶段总览

| 编号 | 阶段 | 详细文档 | 状态 |
|---|---|---|---|
| 00 | 项目初始化 | [`00-项目初始化.md`](./00-项目初始化.md) | ✅ |
| 01 | 代理池取用模块 | [`01-代理池取用模块.md`](./01-代理池取用模块.md) | ✅ |
| 02 | 代理池状态机 + HTTPS 上线 | [`02-代理池状态机与HTTPS上线.md`](./02-代理池状态机与HTTPS上线.md) | ✅ |
| 03 | API-Key 鉴权 + 每 key 限流 | [`03-API-Key鉴权与限流.md`](./03-API-Key鉴权与限流.md) | ✅ |

---

## 待办 / 下一步

- 阶段 04：用量统计（请求/token/延迟/每 key 维度）
- 若 uvicorn 开多 worker：限流/in-flight 计数需落 Redis（当前为进程内存态）
