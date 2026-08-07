# 阶段 02 — 代理池状态机 + HTTPS 上线

## 背景思考

阶段 01 上线后暴露三个现实问题：

1. **"可用"定义不准**：原来探活只测 `GET /models` 通不通，但限流是 **chat 端**才触发
   （`FreeUsageLimitError`）。实测"能连上 zen"的 IP 未必能出对话，导致真实请求踩雷。
2. **快照式池子不健康**：`_cache` 每 120s 一次性整批重建，期间池子静态。被限流的 IP
   只在单次请求的 `excluded` 集合里短暂避开，下个请求又可能选中，反复撞车。
3. **取号不感知并发**：多 API-key 并发时，随机挑选可能同时打同一个 IP，免费代理对
   并发极敏感，一打就 429。

用户明确要求："稳、全面、快"。重点约束是**主动探测频率要低**——探测太少没效率，
太多会自我限流打爆免费额度。

## 完成内容

### 1. 代理池状态机重构（`app/core/proxy_pool.py` 重写）

每个 IP 三态：**可用 / 冷却 / 剔除**。

- `_available: dict[proxy, {quality}]` — 可用池
- `_cooldown: dict[proxy, {until, strike, quality}]` — 冷却区（指数退避）
- `_in_flight: dict[proxy, int]` — 单 IP 在途并发数

**"可用" = 刚验证能出对话**：探活改为最小 chat 请求
（`model=deepseek-v4-flash-free, messages=[hi], max_tokens=1`），200 才算可用。

**主动探测频率控制**（影响最小化）：
- 只发生在两处：冷启动（`_warm=False`）、可用池低于低水位 `pool_low_water=4`
- 稳态探测频率 = 0，池子新鲜度全靠**真实请求回写**（隐式探测）：
  - 200 → 续用，清冷却
  - 429 → 进冷却区
  - 连接失败 → 剔除
- 探测并发硬上限 2（`min(probe_concurrency, 2)`），防止打爆额度

**冷却记忆**：429 后进入冷却区，`60s * 2^strike` 指数退避，上限 600s。
冷却到期自动回池（`_sweep_cooldown`），靠真实请求验证，不额外探测。

**in-flight 并发控制**：取号时选最空闲的 IP（`min(candidates, key=in_flight)`），
跳过超过 `max_in_flight=3` 的；全部占满则最空闲兜底（不饿死请求）。

**补池**：`_refill` 按档位降级链 stable3→2→1，只探测"不在可用池也不在冷却区"的新 IP，
达标 `pool_target=8` 即停。

### 2. 转发器回写状态（`app/services/upstream.py`）

`chat()` 重试上限 3→5，每次请求后回写：

- 成功 → `report(proxy, OK)`
- 429 / 上游5xx → `report(proxy, COOLDOWN)`，换代理
- 连接失败（`httpx.HTTPError`）→ `report(proxy, DEAD)`，剔除换代理
- 其他 4xx → 参数问题，重试无意义，标记冷却后抛

### 3. 共享请求头（`app/core/headers.py`）

`_zen_headers` 从 upstream 抽出为 `zen_headers()`，代理池探活与转发共用，
避免循环导入。

### 4. HTTPS 上线

- nginx 新建站点 `api.pixelreeds.com`（HTTP 301→HTTPS + 443 反代
  `127.0.0.1:8000`，放宽 `proxy_read_timeout=300s` 适配长推理）
- 通配证书 `*.pixelreeds.com` 已覆盖（免新申请）
- 网关改为**只监听 127.0.0.1**，仅经 nginx 出入，杜绝裸奔公网直连
- systemd 托管 `poverty-gateway.service`：开机自启 + `Restart=always` 崩溃自愈

## 技术决策及理由

| 决策 | 理由 |
|---|---|
| 探测用最小 chat 而非 /models | 限流只在 chat 端，/models 通 ≠ 能出对话 |
| 主动探测只在冷启动/低水位 | 稳态零探测，避免自我限流，成本≈0 |
| 状态回写由真实请求驱动 | 每个真实请求都是免费且准确的探测 |
| 冷却指数退避而非直接剔除 | 429 是"暂时用太多"不是"坏"，退避后还能复用 |
| in-flight 取最空闲 IP | 并发下自动分散，避免同 IP 被打爆 |
| 网关只监听 127.0.0.1 | 必须经 nginx，保证 HTTPS 与后续鉴权可控 |
| systemd 而非裸 nohup | 开机自启 + 崩溃自动拉起，运维托底 |

## 实际命令

```bash
# nginx 站点（先写 sites-available 再建软链，避免 sudo -S 与 heredoc 冲突）
sudo cp /tmp/opencode/api.pixelreeds.com /etc/nginx/sites-available/api.pixelreeds.com
sudo ln -sf /etc/nginx/sites-available/api.pixelreeds.com /etc/nginx/sites-enabled/api.pixelreeds.com
sudo nginx -t && sudo nginx -s reload

# systemd 托管
sudo cp /tmp/opencode/poverty-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable poverty-gateway
sudo systemctl start poverty-gateway

# 测试
python -m pytest   # 12 passed
ruff check .       # clean
curl https://api.pixelreeds.com/v1/models
```

## 踩坑记录

1. **`sudo -S` + heredoc 冲突**：`echo pass | sudo tee file <<EOF` 时，密码管道会被
   heredoc 抢占导致密码错误且文件写不进去。解法：先 `write` 到 `/tmp`，再 `sudo cp`。
2. **bash 工具超时误判**：`pkill` 后旧 uvicorn 是 systemd 接管前的裸进程，后台进程
   使命令挂起超时。解法：拆成多条短命令逐步确认，避免长命令含后台进程。
3. **proxy-pool `/get` 返回形态不稳定**：`count=1` 时 `data` 可能是字符串而非数组，
   `_fetch_candidates` 需兼容 `isinstance(data, str)`。
4. **DNS 核对**：`ifconfig.me` 返回 `141.11.146.69` 而域名解析 `111.228.49.57`，
   一度误判配置错误。实际 `111.228.49.57` 才是服务器公网 IP（curl 出口走的另一条路）。

## 下一步

- 阶段 03：API-Key 鉴权 + 多 key + 每 key 限流（对外提供 3-5 个 key 的前提）
- 阶段 04：用量统计（请求/token/延迟/每 key）
- 若 uvicorn 开多 worker：in-flight 计数需落 Redis（当前为进程内存态）
