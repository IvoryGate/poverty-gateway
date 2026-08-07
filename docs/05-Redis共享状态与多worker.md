# 阶段 05 — Redis 共享状态 + 多 worker + 熔断降级

## 背景思考

阶段 03/04 引入的限流、in-flight、冷却计数都是**进程内存态**：

1. **限流形同虚设**：进程一重启计数清零；开多 worker 后各算各的，限额翻倍。
2. **in-flight/冷却失效**：worker A 把 IP 打进冷却，worker B 不知道，照用。
3. **上游全局限流**：免费池 429 频发时，硬顶只会打爆额度、拖垮所有 key。

目标：状态共享 + 自动降级。

## 完成内容

### 1. Redis 端口映射（proxy-pool 仓库）

docker-compose redis 服务加 `ports: "127.0.0.1:6379:6379"`（回环映射，安全），
网关连 `redis://127.0.0.1:6379/1`。

### 2. RedisStore（`app/core/redis_store.py`）

统一存储接口，**Redis 不可用时自动降级进程内存**（测试/容错）：

- `incr_window(bucket, ttl)`：窗口计数（限流用）
- `incr/decr/get`：in-flight 计数
- `setex/exists/delete`：冷却区（TTL 自动过期）
- `circuit_register_failure / circuit_open / circuit_trip`：熔断

### 3. AuthManager 改造

限流从内存 dict → `RedisStore.incr_window`，固定窗口 `rl:{key}:{window}`，
多 worker 原子共享。

### 4. ProxyPool 改造

- in-flight 计数 → store（`inflight:{proxy}`）
- 冷却区 → store（`cooldown:{proxy}`，值 `strike:quality`，TTL 指数退避）
- `_available` 池保留进程内（量小、各 worker 独立补池，成本低）
- `_sweep_cooldown` 移除：冷却过期由 Redis TTL 自然处理，靠补池重新纳入

### 5. 熔断降级（UpstreamClient）

- 429/5xx 时 `_register_failure()` 计数，窗口（30s）内超阈值（10）触发熔断
- 熔断期间 `chat/chat_stream` 快速抛 UpstreamError（503），不硬顶上游
- 熔断 TTL（15s）到期自动恢复
- 计数/熔断键存 Redis，多 worker 一致

### 6. 多 worker 部署

systemd 改 `--workers 2`，加 `REDIS_URL`。实测 6 并发打 rpm=3 的 key：
**恰好 3 过 3 限**（内存态会是 6 过）——Redis 共享限流验证通过。

## 技术决策及理由

| 决策 | 理由 |
|---|---|
| Redis 不可用降级内存态 | 单 worker 不依赖 Redis 也能跑，测试友好 |
| _available 池进程内 | 池量小（8-10），各 worker 独立补池成本低；共享反而引入分布式复杂 |
| 冷却/限流/in-flight 共享 | 三者影响跨 worker 行为，必须一致 |
| 熔断计数 + 开关分桶 | 失败计数窗口桶过期自动清，开关 TTL 到期自动恢复 |
| 熔断阈值 10/30s | 免费池 429 常态，过低会误触发，10 次/30s 合理 |

## 实际命令

```bash
# proxy-pool 侧
docker compose up -d redis   # 端口映射生效

# 网关侧
pip install redis
# systemd 加：
#   --workers 2
#   Environment=REDIS_URL=redis://127.0.0.1:6379/1
sudo systemctl restart poverty-gateway

python -m pytest   # 35 passed
ruff check .       # clean
```

## 踩坑记录

1. **熔断测试卡死**：测试用 `chat()`（非流式）调真实 `_request` 走真代理网络请求，
   卡住等超时。解法：改用 `chat_stream`（被 MockTransport 替换的路径）。
2. **多 worker stats 的 available 可能为 0**：stats 请求命中的 worker 未补池时
   `_available` 空，但共享 cooldown 有值——正常现象，非 bug。
3. **RedisStore 需兼容 dict 输入**：与 config `_parse_api_keys` 同理。

## 下一步

- 阶段 06：模型源自动治理（自动发现 free 模型 + 可用性摘除）
