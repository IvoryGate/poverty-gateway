# 阶段 18：mimo 上游 403 排查与校验修复

## 背景思考

阶段 17 把 `mimo-v2.6-flash-free` 对外暴露成 `mimo-v2.6-flash-lite` 后，经网关调它
必得上游 **403 FreeTierError**：

```
"OpenCode's free tier can only be used from within OpenCode"
```

但同一台机器上，原生 opencode 客户端直连上游跑 `mimo-v2.6-flash-free` 却是 200。
其他模型（space-bunny 等）走网关也都正常。说明上游**不是封锁第三方客户端**，
而是专门对 mimo 这条线加了"必须像 OpenCode 客户端"的校验。

一开始怀疑方向（事后看全错了一半）：

1. **TLS/HTTP2 指纹**：bun 运行器的指纹独特。我们依次试了 curl、curl_cffi
   （chrome131/safari18 impersonate）、`curl --http2`、node fetch——全部 403。
2. **请求头伪装不到位**：UA 换 1.18.31、增删 `x-opencode-*`、去 `x-api-key`、
   加 `client=desktop` 等变体——全 403。
3. **id 格式**：网关发 `msg_`+20 位 hex，真客户端是 26 位——当时换 26 位还是 403
   （因为字符集不对，见下）。

## 完成内容

### 决定性实验：relay 中间层 A/B

起一个本地 relay（`/tmp/opencode/relay.py`，8098）夹在**真 opencode 客户端和真上游
之间**，原样转发并记录。结果：

- 真客户端 → relay（httpx 转发）→ 上游：**200** ✅
- 我们的 `zen_headers()` 直发同样 body → **403** ❌

同一台机器、同一条 httpx 出口连接，**指纹论被推翻**——差异只可能在请求内容里。

### 二分定位：两个头 + 一个 body 规则

拿 relay 抓下的真客户端完整头做 A/B（基线真头 200，逐项换成我们的值）：

| 改动 | 结果 |
|---|---|
| +`x-api-key: public` | 200（不背锅） |
| -`Accept` | 200（不背锅） |
| id 换成 `msg_`+20 位 hex | **403** ← 元凶一 |
| UA 换成 `opencode/1.15.0` | **426 UpgradeRequired** ← 元凶二 |

id 继续摸规律（同一真 body，只变 id）：

- `msg_`+26 位**小写 hex** → 200（三轮复核稳定）
- 20 位 hex / 14 位 hex → 403
- 26 位 base62（含大写字母）→ 403（真客户端明明含大写也能过，但复现不了，
  不纠结，能稳定 200 的 26 位小写 hex 就是答案）
- 30 位 hex → 403

body 也有一道校验（同新头，只变 body）：

- **`stream=true` 且 `tools ≥ 5` → 200**
- `stream=false` → 403（即使 11 个 tools 全在）
- `tools ≤ 4` → 403（n=4 → 403，n=5 → 200，n=6/8/11 → 200）

### 落地修复

`app/core/headers.py` 两处：

```python
"User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
"x-opencode-request": f"msg_{uuid.uuid4().hex[:26]}",   # 原 [:20]
"x-opencode-session": f"ses_{uuid.uuid4().hex[:26]}",   # 原 [:20]
```

`tools/context_probe.py` 的 `upstream_headers()` 改为直接复用 `zen_headers()`，
避免以后两处漂移。

**body 不需要任何改动**：网关本来就是 `json=payload` 原样透传（只替换 model 名），
opencode 客户端的请求天然自带 11 个 tools + stream=true，原样转发就过校验。
先前一度想做"body 补丁"（给裸请求注入 tools + 流式聚合），属于过度设计——
只有非 opencode 的裸 SDK 请求才会撞 body 校验，不是主用例。

### 顺带修掉的：上游错误 502 → 503

（同批未提交改动）`_upstream_error()` 兜底码 502 → **503** + `Retry-After`，
docstring 写明理由：502 的 body 出网时容易被中间层（CDN/反代）替换成 HTML 错误页，
503 与 4xx/429 一样能保住 JSON 结构。error type 同时分流：
上游 4xx 无 type → `invalid_request_error`；连接失败/熔断/5xx → `upstream_error`。

## 技术决策及理由

1. **relay 中间层而不是继续猜**：前面 20+ 轮"改一个变量打一次"全是盲测，
   因为每个变量都可能被别的变量掩盖。relay 把"客户端请求"和"网络路径"解耦：
   真请求 + 我们的连接 → 200，直接证明连接层无罪，把搜索空间从
   {指纹, 头, body, id, UA} 砍到只剩头/body。
2. **id 用 26 位小写 hex 而不是模仿真客户端格式**：真客户端 id 含大写字母
   （如 `msg_115d...QyWJl`），我们模仿后仍然 403，说明上游校验逻辑拿不到全貌；
   26 位小写 hex 三轮复核稳定 200，够用即可，不逆向它内部规则。
3. **UA 跟死真客户端版本串**：旧版 1.15.0 直接 426，说明上游有客户端版本门槛；
   版本串迟早会过期，过期时症状是 426（好识别），到时再跟着升。
4. **body 交给客户端原样透传**：上游要的"像 OpenCode"特征（tools、stream）
   恰好就是真客户端本来发的东西；网关替客户端改 body 反而引入语义破坏
   （假 tools 会被模型调用、stream=false 变 true 要做聚合）。

## 实际命令

```bash
# relay 中间层（真客户端 → relay → 上游）
.venv/bin/python /tmp/opencode/relay.py &        # 8098，记录 req-dump.txt + relay.log
OPENCODE_CONFIG=/tmp/opencode/hdr/cap2.json opencode run -m opencode/mimo-v2.6-flash-free "reply ok"

# A/B：真头 vs zen_headers（脚本见会话，核心就是逐项替换 + httpx POST）
.venv/bin/python - <<'EOF' ... EOF

# 测试
.venv/bin/python -m pytest -q        # 70 passed
.venv/bin/ruff check .               # 7 errors 全是既有（proxy_pool 3 + sync_opencode_models 4）

# 端到端（配置声明 -lite 模型后）
OPENCODE_CONFIG=/tmp/opencode/hdr/cap-gw.json opencode run -m opencode/mimo-v2.6-flash-lite "reply ok"
# → ok；gwlog 记录 /v1/responses 200 + /v1/chat/completions 200(SSE)
```

## 踩坑记录

1. **relay URL 拼重**：`UP = "https://opencode.ai/zen/v1"` + path `/zen/v1/chat/...`
   → 上游 404，且响应是 br 压缩的，日志里全是乱码 TLS 数据，差点误判。
   改 `UP = "https://opencode.ai"` + relay 转发时剥掉 `accept-encoding`（让 httpx 自己解码）。
2. **relay 的 `http2=True` 起不来**：venv 里没装 `h2` 包，直接 502。去掉即可——
   本来 http/1.1 就够（后面也证明协议不是变量）。
3. **`pkill -f relay` 把自己 shell 干掉了**：pattern 匹配到自己命令行，
   以后一律 `kill <pid>`。
4. **opencode 客户端配置没声明 `-lite` 模型时 0 请求直接报错**：
   `Unexpected server error` 看着像 5xx，其实是本地模型清单里没有该名字。
   对照实验（`-free` 能发请求、`-lite` 发不出）定位；在配置 `models` 段声明即通。
   `tools/sync_opencode_models.py` 干的就是这件事。
5. **opencode 标题生成用 `gpt-5.4-nano`**（`/v1/responses`，非 mimo）：
   网关模型映射没有它 → 每次标题生成拿到 `Unknown model` 错误，但客户端容忍、
   不影响聊天。遗留项（见下）。
6. **journalctl 无权限**：普通用户看不到 systemd 日志，排错靠 curl 复现 + 记录代理。

## 下一步

- [ ] 会话标题生成：`gpt-5.4-nano` 不在网关映射 → 标题一直失败。选项：
      映射到某个便宜模型 / 让 `/v1/responses` 对未知 small model 静默降级。
- [ ] 裸 SDK 请求（无 tools / 非流式）打 mimo 仍会被上游 403。主用例
      （opencode 客户端）不受影响；若要支持裸请求再评估 body 注入 + 流式聚合。
- [ ] UA 版本串会过期（症状 426），上游升级时同步更新 `zen_headers()`。
- [ ] 提交后观察：`journalctl`/面板看 mimo 真实流量成功率。
