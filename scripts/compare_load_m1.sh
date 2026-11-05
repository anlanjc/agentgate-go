#!/bin/bash
# compare_load_m1.sh — M1 同功能口径 c=1 对比：Redis 鉴权 + Lua 准入版 Go 网关
# 流程沿用 compare_load.sh（同 mock 上游、同 aiohttp 客户端、同机同尺子），新增：
#   ① 便携 Redis（.tooling/agentgate-redis，AG-1 留下）+ 预置 virtual key；
#   ② 同会话补测裸链路口径——与 11-05 存档数字相比排除"不同日期机器状态"噪声，
#     三个目标（裸 mock / 裸链路网关 / M1 网关）在同一进程批次里测完。
# 环境备注：跑前需 PATH 含 go/bin 且 GOTOOLCHAIN=local（见下两行 export）。
set -u
export GOTOOLCHAIN=local
export PATH="/c/Windows/System32:/d/1/1/1Tasks/tooling/go/bin:$PATH"
PY="D:/1/1/1Tasks/zcode/shixiproj/agentgate/.venv/Scripts/python.exe"
REDIS_HOME="D:/1/1/1Tasks/zcode/shixiproj/.tooling/agentgate-redis"
cd /d/1/1/1Tasks/zcode/agentgate-go

# 构建当前代码（不复用仓库里可能过期的 exe）
go build -o agentgate-go-bench.exe . || exit 1

# ① 便携 Redis：独立端口 6399，无持久化（纯基准用途）
"$REDIS_HOME/redis-server.exe" --port 6399 --save "" --appendonly no &
REDIS_PID=$!
for i in $(seq 1 20); do
  "$REDIS_HOME/redis-cli.exe" -p 6399 PING 2>/dev/null | grep -q PONG && break
  sleep 0.2
done

# ② 预置 virtual key（语义对照 Python 版 vk:{sha256} JSON 档，字段名逐字对齐）
RAW_KEY="ag-bench-20261106"
KEY_HASH=$("$PY" -c "import hashlib;print(hashlib.sha256(b'$RAW_KEY').hexdigest())")
"$REDIS_HOME/redis-cli.exe" -p 6399 SET "vk:$KEY_HASH" \
  '{"id":"vk-bench","tenant_id":"t-bench","model_whitelist":null,"rpm_limit":1000000,"tpm_limit":0,"budget_tokens":null,"is_active":true}' >/dev/null

# ③ mock 上游 + 两种口径的网关（8081=裸链路 8082=M1）
"$PY" scripts/mock_upstream.py --port 9301 &
MOCK_PID=$!
UPSTREAM_1_URL=http://127.0.0.1:9301 UPSTREAM_1_KEY=sk-mock RATE_CAPACITY=1000000 RATE_REFILL=1000000 PORT=8081 ./agentgate-go-bench.exe &
BARE_PID=$!
UPSTREAM_1_URL=http://127.0.0.1:9301 UPSTREAM_1_KEY=sk-mock REDIS_ADDR=127.0.0.1:6399 PORT=8082 ./agentgate-go-bench.exe &
M1_PID=$!
sleep 2

echo "=== 基线 c=1（裸 mock 直连）==="
"$PY" scripts/load_client.py --target http://127.0.0.1:9301 --mode c1 --requests 100 --label 裸mock基线
echo "=== Go 网关 c=1（裸链路口径：内存桶，同会话复测）==="
"$PY" scripts/load_client.py --target http://127.0.0.1:8081 --mode c1 --requests 100 --label Go网关裸链路
echo "=== Go 网关 c=1（M1 同功能口径：Redis 鉴权+Lua 准入）==="
"$PY" scripts/load_client.py --target http://127.0.0.1:8082 --mode c1 --requests 100 --key "$RAW_KEY" --label Go网关M1口径

kill $MOCK_PID $BARE_PID $M1_PID $REDIS_PID 2>/dev/null
echo DONE
