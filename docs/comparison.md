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

## 四、压测数据（待回填）

方法论沿用 agentgate（Python 版）EVIDENCE §3 三教训：c=1 测附加开销（排队≠每请求代价）、客户端瓶颈差值法（httpx/aiohttp 也会冒充服务端瓶颈）、组件分解。

| 场景 | Python 版（实测口径） | Go 版（待回填） |
|---|---|---|
| c=1 附加开销 | 3.55ms（P95，全链） | 待测 |
| 多 worker 扩展 | 1.00x/2.15x/4.04x（1/2/4 worker） | goroutine 无需多进程，待测单进程多核 |
| SSE 并发流 | 待测 | 待测 |

> 回填纪律：数字必须来自最后一次代码提交之后的实测；口径逐字写明（连接数/时长/上游 mock 形态）。
