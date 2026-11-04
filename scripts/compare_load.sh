#!/bin/bash
# compare_load.sh — 双语言对照压测：裸 mock 基线 vs Go 网关（同 aiohttp 客户端同口径）
set -u
PY="D:/1/1/1Tasks/zcode/shixiproj/agentgate/.venv/Scripts/python.exe"
cd /d/1/1/1Tasks/zcode/agentgate-go

"$PY" scripts/mock_upstream.py --port 9301 &
MOCK_PID=$!
UPSTREAM_1_URL=http://127.0.0.1:9301 UPSTREAM_1_KEY=sk-mock RATE_CAPACITY=1000000 RATE_REFILL=1000000 PORT=8081 ./agentgate-go.exe &
GW_PID=$!
sleep 2

echo "=== 基线 c=1（裸 mock 直连）==="
"$PY" scripts/load_client.py --target http://127.0.0.1:9301 --mode c1 --requests 100 --label 裸mock基线
echo "=== Go 网关 c=1 ==="
"$PY" scripts/load_client.py --target http://127.0.0.1:8081 --mode c1 --requests 100 --label Go网关
echo "=== 基线 c=50 容量组 ==="
"$PY" scripts/load_client.py --target http://127.0.0.1:9301 --mode capacity --requests 1000 --concurrency 50 --label 裸mock基线
echo "=== Go 网关 c=50 容量组 ==="
"$PY" scripts/load_client.py --target http://127.0.0.1:8081 --mode capacity --requests 1000 --concurrency 50 --label Go网关

kill $MOCK_PID $GW_PID 2>/dev/null
echo DONE
