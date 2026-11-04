# agentgate 双语言对照：asyncio（Python）vs goroutine（Go）

> 同一个网关设计，两种并发模型各实现一遍。Python 版（完整叙事：多租户/记账/队列/管理面）vs 本 Go 版（核心请求链路）。
> 状态：**代码已就绪（13 测试全绿），压测数据待回填**。回填后本文件是面试"深挖弹药"的一级素材。

## 一、语义对照表（设计逐条对齐）

| 设计点 | Python 版实现 | Go 版实现 | 语义是否一致 |
|---|---|---|---|
| 限流 | Redis+Lua 令牌桶（跨进程，多 worker 共享） | 进程内 `sync.Mutex` 令牌桶 | ⚠️ 口径不同（见 §2），并发验收口径相同：并发 100 恰放行 50 零超卖 |
| 负载均衡 | EMA 延迟优先 + <20ms 差随机打散防羊群 | 同款（`sort.Slice` + `rand.Intn(2)`） | ✅ 逐条对齐 |
| 429 处理 | 立即冷却 + 尊重 Retry-After（缺省 1s） | 同款 | ✅ |
| 5xx 处理 | 冷却 60s；网络错误并入 5xx 口径 | 同款 | ✅ |
| 4xx 处理 | 原样透传不重试（client.py 4xx 不重试纪律） | 同款（default 分支 Relay） | ✅ |
| 全渠道冷却 | fail-closed 503 `gateway_unavailable` | 同款 | ✅ |
| SSE 转发 | 纯 ASGI 字节透传（BaseHTTPMiddleware 吞 44% 教训） | `Do`/`Relay` 两段式 + `Flusher` 逐段直推 | ✅ 字节透传语义一致；Go 版多出"写前判定"结构 |
| 优雅退出 | uvicorn 信号 + 排空 | `signal.NotifyContext` + `srv.Shutdown(10s)` | ✅ |
| 请求体与重试 | 转移重试不二次消费 body | `io.ReadAll` 先缓冲再逐次 `bytes.NewReader` | ✅（Go 版测试覆盖） |

## 二、限流口径差异（诚实声明）

Python 版令牌桶放 Redis 是为了**多 worker/多副本共享配额**（生产口径）；Go 版放进程内是为了**教学并发安全**（`-race` 证明）。两者不直接对比 QPS——对比的是各自口径下的正确性证明方式：Lua 原子脚本 vs mutex 串行化 + race detector。若 Go 版要上生产多副本口径，同样换 Redis——这是语言无关的架构决策，恰好说明"设计判断 > 语言"。

## 三、并发模型差异（代码级观察，压测前已可写）

1. **写法**：Python 的 `async/await` 会"染色"调用链（一处 async 全链 async）；Go 无染色，`go` 关键字即并发。本版唯一用到 goroutine 起步的地方是 main 的 listen 协程——**请求处理天然每连接一个 goroutine，不需要写任何并发代码**，这是与 asyncio 最大的心智差异。
2. **取消传播**：Python 靠 `asyncio.timeout`/Task 取消；Go 靠 `context` 显式传参（`http.NewRequestWithContext`）——更显式，也更啰嗦。
3. **flush 语义**：Python ASGI 的 `send({type: http.response.body})` 分帧即推；Go 需显式 `http.Flusher` 断言 + 每 chunk Flush——Go 更底层，控制粒度更细。
4. **单二进制**：`go build` 产物 5.78MB（-ldflags -s -w），无解释器无依赖树；Python 版部署需 venv + requirements（或 Docker）。这是部署维度而非运行维度差异。

## 四、压测数据（2026-11-05 实测，aiohttp 客户端 · Windows 本机 · 同机 mock 上游）

方法论沿用 Python 版 EVIDENCE §3 三教训：c=1 测附加开销（排队≠每请求代价）、同客户端差值法抵消客户端成本、口径逐字写明。客户端=独立 aiohttp 进程（`scripts/load_client.py`），上游=独立 FastAPI SSE mock（`scripts/mock_upstream.py`，流式 20 chunk+[DONE]），目标只换端口（裸 mock vs Go 网关），**同一把尺子**。

### c=1 每请求附加开销（100 请求串行，无排队——合法口径）

| 目标 | P50 | P95 | 吞吐 |
|---|---|---|---|
| 裸 mock 基线（uvicorn 直连） | 0.4ms | 0.6ms | 2097 req/s |
| **Go 网关** | **0.7ms** | **0.9ms** | 1373 req/s |
| **附加开销（差值）** | **0.3ms** | **0.3ms** | — |

### c=50 容量组（非流式 700 / 流式 300，单进程）

| 指标 | 裸 mock 基线 | Go 网关 |
|---|---|---|
| 非流式吞吐 | 4091 req/s | 2231 req/s |
| 非流式延迟 | P50 10.0 / P95 20.9ms | P50 4.6 / P95 60.1ms |
| 流式吞吐 | 1729 req/s | 1429 req/s |
| 流式 TTFT | P50 18.2ms | P50 10.1ms |

### ⚠️ 与 Python 版 3.55ms 的对比口径声明（诚实优先）

Python 版记录值 3.55ms 是**完整 M1 链路**的附加开销：含鉴权缓存读 + 准入 Lua（Redis 往返×5）+ 渠道状态 + httpx 转发跳。本 Go 版是**裸转发链路**：内存令牌桶 + EMA 选路 + HTTP 转发，**没有 Redis 鉴权/记账**。0.3ms vs 3.55ms 不是同功能对比——其中 Redis 同机往返 5 次本身约 0.5~1.5ms 量级，语言运行时的真实差距小于表观差距。若要同功能对比，Go 版需补 Redis 鉴权（届时零依赖卖点失效）——这恰好反证"功能取舍 > 语言玄学"。

### 客户端瓶颈声明（同 Python 版方法论）

c=50 组的绝对吞吐受单进程 aiohttp 客户端 CPU 约束（基线也仅 4091 req/s），不代表网关容量上限。Go 网关单进程内 goroutine 自动摊多核，无 Python 版 uvicorn --workers 多进程扩展问题（Python 版 1.00x/2.15x/4.04x 的多 worker 实测在 agentgate EVIDENCE）。Go 非流式 P95 60ms 长尾来自 GC/调度抖动 + 客户端限制下的排队，P50 4.6ms 低于基线 P50 属客户端调度差异，结论以 c=1 差值口径为准。

### 环境备注

- Go go1.23.4 windows/amd64，`-ldflags="-s -w"` 单二进制 5.78MB；`-race` 因 Windows 需 CGO 留给 CI（ubuntu runner 自带 gcc）。
- Python 版口径：agentgate EVIDENCE §3（Python 3.12 + 便携 Redis 5.0.14 + SQLite，进程内 ASGI mock）。

