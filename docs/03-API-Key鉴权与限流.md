# 阶段 03 — API-Key 鉴权 + 每 key 限流

## 背景思考

阶段 02 完成 OpenAI 兼容透传后，网关已能承接 Cursor/Codex/opencode/Trae 等主流
工具。但对外提供 3-5 个 key 前必须有鉴权与隔离：

1. **无鉴权**：任何拿到域名的人都能白嫖，代理池额度会被打爆。
2. **多 key 需隔离**：一个 key 的滥用不应影响其他 key（各自限额）。
3. **兼容标准**：必须用 OpenAI 标准 `Authorization: Bearer <key>`，工具才能直接配。

用户决策：**Bearer 头 + 每 key 独立限额 + /health 不鉴权（放给监控）**。

## 完成内容

### 1. 配置（`app/config.py`）

- `AUTH_ENABLED`：鉴权总开关（默认开，测试可关）
- `API_KEYS` 环境变量：JSON 字符串，`{key: {name, rpm}}`
- `API_KEYS_FILE`：指向 JSON 文件的替代方式（生产用，避免环境变量转义地狱）
- `API_KEY_DEFAULT_RPM`：每 key 默认限流（缺省 60 次/分钟）
- `RATE_LIMIT_WINDOW`：限流窗口秒数（默认 60）

key 文件放 `/etc/poverty-gateway/keys.json`（root:matrix 640 权限，不进 git）。

### 2. 鉴权模块（`app/core/auth.py`）

`AuthManager`：
- `authenticate(auth_header)`：解析 `Bearer <key>`（兼容无 Bearer 前缀直传），
  校验存在性 → `AuthError`；校验窗口限流 → `RateLimitExceeded`
- 固定窗口计数：`{key: (window_start, count)}`，窗口过期自动重置
- `rpm=0` 表示不限额
- 线程锁保护计数（多线程/事件循环安全）

### 3. 路由接入（`app/api/routes.py`）

- `/v1/models`、`/v1/chat/completions` 加 `Depends(_require_key)`
- `_require_key`：`auth.enabled=False` 时放行（测试/内部）；否则 401（无效 key）
  / 429（超限）
- `/health` **不鉴权**（监控探活用）

### 4. 部署配置

- 生成 3 个 key（matrix: rpm120 / dev: rpm60 / ci: rpm30）
- systemd 加 `Environment=API_KEYS_FILE=/etc/poverty-gateway/keys.json`

## 技术决策及理由

| 决策 | 理由 |
|---|---|
| Bearer 标准头 | Cursor/Codex/opencode 原生支持，零配置 |
| 每 key 独立 rpm | 一个 key 滥用不影响其他，可分别管控 |
| 固定窗口计数 | 实现简单、内存态足够（单进程）；滑动窗口复杂度不值当 |
| keys 存文件不存 env | 避免 systemd 环境变量 JSON 转义问题，且权限可控 |
| /health 不鉴权 | 监控探活无需凭据，只返回状态无敏感信息 |

## 实际命令

```bash
# 生成 key 并部署
python3 - <<'EOF'
import secrets, json
keys = {
    "sk-" + secrets.token_hex(16): {"name": "matrix", "rpm": 120},
    "sk-" + secrets.token_hex(16): {"name": "dev", "rpm": 60},
    "sk-" + secrets.token_hex(16): {"name": "ci", "rpm": 30},
}
json.dump(keys, open("/tmp/api_keys.json", "w"))
EOF
sudo mkdir -p /etc/poverty-gateway
sudo cp /tmp/api_keys.json /etc/poverty-gateway/keys.json
sudo chown root:matrix /etc/poverty-gateway/keys.json && sudo chmod 640 /etc/poverty-gateway/keys.json

# systemd 更新
sudo cp poverty-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl restart poverty-gateway

# 测试
python -m pytest   # 33 passed
ruff check .       # clean

curl -s -o /dev/null -w "%{http_code}\n" https://api.pixelreeds.com/v1/models  # 401
curl -s -H "Authorization: Bearer sk-xxx" https://api.pixelreeds.com/v1/models  # 200
```

## 踩坑记录

1. **systemd EnvironmentFile 存 JSON 转义地狱**：`API_KEYS={"sk-...":...}` 在
   EnvironmentFile 里 `"`/空格都要转义，极易出错。解法：改 `API_KEYS_FILE`
   指向独立 JSON 文件，干净可靠。
2. **配置解析需兼容 dict 输入**：`_parse_api_keys` 一开始只处理 JSON 字符串，
   测试传 dict 直接 `json.loads` 崩。改成兼容 `isinstance(raw, dict)`。
3. **限流实测**：用临时 `rpm=1` 的测试 key 验证（第1次 200，后续 429），
   验证完移除恢复正式配置——避免用正式 key 打爆额度验证。

## 下一步

- 阶段 04：用量统计（请求/token/延迟/每 key 维度）
- 多 worker 部署时：限流计数需落 Redis（当前为进程内存态，单 worker 足够）
