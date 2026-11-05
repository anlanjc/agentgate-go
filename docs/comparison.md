# agentgate 双语言对照：asyncio（Python）vs goroutine（Go）

> 同一个网关设计，两种并发模型各实现一遍。Python 版（完整叙事：多租户/记账/队列/管理面）vs 本 Go 版（核心请求链路 + M1 鉴权/准入）。
> 状态：**代码已就绪（29 测试全绿），双口径压测数据已回填**。本文件是面试"深挖弹药"的一级素材。
> **口径总览**：本仓有两种工作模式——裸链路口径（`REDIS_ADDR` 未设，内存令牌桶，零外部依赖）与 M1 同功能口径（`REDIS_ADDR` 设置，Redis 鉴权 + Lua 准入，对照 Python 版 M1 链路形状）。两组数据并存于 §四。

## 一、语义对照表（设计逐条对齐）

| 设计点 | Python 版实现 | Go 版实现 | 语义是否一致 |
|---|---|---|---|
| virtual key 鉴权 | SHA-256(raw)→Redis GET `vk:{hash}`（缓存优先）→miss→PG 兜底→回填 SETEX 60s | SHA-256(raw)→Redis GET `vk:{hash}`，无 PG 层（Redis 即事实来源，种子不带 TTL） | ✅ hash/键名/JSON 档字段逐字对齐（跨语言交叉验证测试钉死）；⚠️ 无 DB 兜底（见 §五） |
| 准入限流 | Redis+Lua 令牌桶（HMGET tokens/ts 惰性补充）+ tpm/预算读合并进同一脚本，单次 EVALSHA | 同款 Lua 逐行对齐（同键名 `rl:rpm:`/`tpm:`/`budget:`），go-redis NewScript = EVALSHA 优先 NOSCRIPT 回退 | ✅ 脚本语义一致（Python 版 admit.py 调优产物同思路） |
| 模型白名单 | `_admit` 第一道闸 → 403 model_not_allowed | 同款（Lua 前，403 同码） | ✅ |
| 负载均衡 | EMA 延迟优先 + <20ms 差随机打散防羊群 | 同款（`sort.Slice` + `rand.Intn(2)`） | ✅ 逐条对齐 |
| 429 处理 | 立即冷却 + 尊重 Retry-After（缺省 1s） | 同款 | ✅ |
| 5xx 处理 | 冷却 60s；网络错误并入 5xx 口径 | 同款 | ✅ |
| 4xx 处理 | 原样透传不重试（client.py 4xx 不重试纪律） | 同款（default 分支 Relay） | ✅ |
| 全渠道冷却 | fail-closed 503 `gateway_unavailable` | 同款 | ✅ |
| SSE 转发 | 纯 ASGI 字节透传（BaseHTTPMiddleware 吞 44% 教训） | `Do`/`Relay` 两段式 + `Flusher` 逐段直推 | ✅ 字节透传语义一致；Go 版多出"写前判定"结构 |
| 优雅退出 | uvicorn 信号 + 排空 | `signal.NotifyContext` + `srv.Shutdown(10s)` | ✅ |
| 请求体与重试 | 转移重试不二次消费 body | `io.ReadAll` 先缓冲再逐次 `bytes.NewReader` | ✅（Go 版测试覆盖） |
| 记账/队列/管理面 | 完整叙事（TPM 计数写入、预算扣减、admin API） | **不在 M0-M2 范围**（tpm/budget 键只读预检，无写入方） | ➖ 刻意不做，见 §四诚实声明 |

## 二、限流口径差异（诚实声明，2026-11-06 更新）

Python 版令牌桶放 Redis 是为了**多 worker/多副本共享配额**（生产口径）；Go 版原本只做进程内内存桶（教学并发安全，`-race` 证明）。**2026-11-06 起 Go 版补齐了 Redis 口径**（`REDIS_ADDR` 启用，Lua 脚本与 Python 版逐行对齐）——"若 Go 版要上生产多副本口径，同样换 Redis"这句预言已兑现，也印证"设计判断 > 语言"：换存储是架构决策，不是语言能力差异。内存桶保留为裸链路口径（零外部依赖卖点在无 Redis 场景仍成立），两种口径并存、各自有压测数据（§四）。

## 三、并发模型差异（代码级观察，压测前已可写）

1. **写法**：Python 的 `async/await` 会"染色"调用链（一处 async 全链 async）；Go 无染色，`go` 关键字即并发。本版唯一用到 goroutine 起步的地方是 main 的 listen 协程——**请求处理天然每连接一个 goroutine，不需要写任何并发代码**，这是与 asyncio 最大的心智差异。
2. **取消传播**：Python 靠 `asyncio.timeout`/Task 取消；Go 靠 `context` 显式传参（`http.NewRequestWithContext`）——更显式，也更啰嗦。
3. **flush 语义**：Python ASGI 的 `send({type: http.response.body})` 分帧即推；Go 需显式 `http.Flusher` 断言 + 每 chunk Flush——Go 更底层，控制粒度更细。
4. **单二进制**：`go build` 产物 5.78MB（-ldflags -s -w），无解释器无依赖树；Python 版部署需 venv + requirements（或 Docker）。这是部署维度而非运行维度差异。（2026-11-06 注：M1 口径引入 go-redis/v9 后"零依赖"限定为裸链路口径；miniredis 仅测试用。）

## 四、压测数据（同机同尺子，双口径并存）

方法论沿用 Python 版 EVIDENCE §3 三教训：c=1 测附加开销（排队≠每请求代价）、同客户端差值法抵消客户端成本、口径逐字写明。客户端=独立 aiohttp 进程（`scripts/load_client.py`），上游=独立 FastAPI SSE mock（`scripts/mock_upstream.py`，流式 20 chunk+[DONE]），目标只换端口（裸 mock vs Go 网关），**同一把尺子**。

### 4.1 裸链路口径（2026-11-05 存档；2026-11-06 同会话复测一致）

c=1 每请求附加开销（100 请求串行，无排队——合法口径）：

| 目标 | P50 | P95 | 吞吐 |
|---|---|---|---|
| 裸 mock 基线（uvicorn 直连） | 0.4ms | 0.6ms | 2097 req/s |
| **Go 网关（裸链路）** | **0.7ms** | **0.9ms** | 1373 req/s |
| **附加开销（差值）** | **0.3ms** | **0.3ms** | — |

2026-11-06 复测（3 批）：基线 P50 0.4~0.5，裸链路 P50 0.7~0.8，**差值恒 0.3ms**——与 11-05 存档一致，数字可复现。

c=50 容量组（非流式 700 / 流式 300，单进程）：

| 指标 | 裸 mock 基线 | Go 网关 |
|---|---|---|
| 非流式吞吐 | 4091 req/s | 2231 req/s |
| 非流式延迟 | P50 10.0 / P95 20.9ms | P50 4.6 / P95 60.1ms |
| 流式吞吐 | 1729 req/s | 1429 req/s |
| 流式 TTFT | P50 18.2ms | P50 10.1ms |

### 4.2 M1 同功能口径（2026-11-06 新增：Redis 鉴权 + Lua 准入）

网关带 `REDIS_ADDR` 启动，每请求 Redis 往返 2 次：鉴权 GET `vk:{sha256}` 1 次 + 准入 EVALSHA 1 次（tpm/预算读合并进同一脚本）。virtual key 经 `vk:{sha256}` JSON 档预置（字段名与 Python 版 `_serialize` 逐字对齐），客户端带 `Authorization: Bearer ag-…`（`load_client.py --key`）。脚本：`scripts/compare_load_m1.sh`（三目标同批次测完，排除跨日期机器状态噪声）。

3 批实测（每批 100 请求串行）：

| 批次 | 裸 mock 基线 P50/P95 | 裸链路网关 P50/P95 | **M1 网关 P50/P95** | M1 差值 P50/P95 | M1 吞吐 |
|---|---|---|---|---|---|
| 1 | 0.4 / 0.6 | 0.7 / 1.0 | 1.0 / 1.4 | **0.6 / 0.8** | 927 req/s |
| 2 | 0.4 / 0.6 | 0.7 / 0.9 | 1.1 / 1.4 | **0.7 / 0.8** | 906 req/s |
| 3 | 0.5 / 0.8 | 0.8 / 1.1 | 1.2 / 1.6 | **0.7 / 0.8** | 802 req/s |

**结论**：M1 同功能口径附加开销 **P50 0.6~0.7ms / P95 0.8ms**。其中 Redis 腿（2 次同机往返 + go-redis 连接池取还）贡献 0.3~0.4ms（= M1 1.0~1.2 减裸链路 0.7~0.8），其余 0.3ms 与裸链路口径的网关本体一致。

### 与 Python 版 3.55ms 的对比（诚实口径，2026-11-06 更新）

Python 版 3.55ms 是**完整生产链路**：鉴权缓存读 + 准入 Lua + 记账 pipeline + 路由 EMA 原子 Lua 等，**每请求 Redis 往返共 5 次**（Python EVIDENCE：优化 8→5）。本 Go 版 M1 口径只覆盖**鉴权 + 准入**（2 次往返），记账写入/路由记账 Lua 不在 M0-M2 范围。因此：

- 裸链路 0.3ms vs 3.55ms：**不同功能对比**（连鉴权都没有），仅证明转发链路本身的开销。
- M1 口径 0.6~0.7ms vs 3.55ms：**接近但非逐字节同链路**——同为"鉴权+准入"形状（且这部分 Python 自身占 2/5 往返），但 Python 数字还背着 3 次记账/路由往返 + httpx 客户端成本 + ASGI 中间件栈。**可以说"同形状链路 Go 约 0.65ms、Python 全链路 3.55ms"，不能说"同功能快 5.5 倍"**——Redis 往返数不同（2 vs 5），剩余差距里语言运行时与框架栈各占多少本实验无法切分，这是口径边界。
- 若要完全对齐 5 往返口径，Go 版需补记账写入+路由记账 Lua——属 Python 版完整叙事，刻意不重写（重写它们只证明语法不证明判断）。

### Redis 故障语义对照（三分表，2026-11-06 新增）

| 环节 | Python 版 | Go 版 | 一致性 |
|---|---|---|---|
| 鉴权 | Redis 挂 → 降级直查 DB（fail-open，网关不死） | 无 DB 层 → Redis 挂 = 鉴权无法完成 → **503 auth_unavailable（fail-closed）** | ⚠️ 语义映射差异：Go 把"无 DB 兜底"诚实为拒服务，不假装鉴权通过 |
| 准入 | Redis 挂 → **503 admission_unavailable（fail-closed）**：准入承载计费硬上界，宁可拒绝不裸放 | 同款同码 503 admission_unavailable | ✅ |
| 并发闸 | Redis 挂 → 放行（保护性优化，fail-open） | 不在 M0-M2 范围 | ➖ |

测试钉死：`TestM1AdmissionFailClosed503`（鉴权活着、准入链挂 → 503 admission_unavailable）、`TestM1AuthFailClosed503`（Redis 不可达 → 503 auth_unavailable）。

### 客户端瓶颈声明（同 Python 版方法论）

c=50 组的绝对吞吐受单进程 aiohttp 客户端 CPU 约束（基线也仅 4091 req/s），不代表网关容量上限。Go 网关单进程内 goroutine 自动摊多核，无 Python 版 uvicorn --workers 多进程扩展问题（Python 版 1.00x/2.15x/4.04x 的多 worker 实测在 agentgate EVIDENCE）。Go 非流式 P95 60ms 长尾来自 GC/调度抖动 + 客户端限制下的排队，P50 4.6ms 低于基线 P50 属客户端调度差异，结论以 c=1 差值口径为准。

### 环境备注

- Go go1.23.4 windows/amd64，`-ldflags="-s -w"` 单二进制 5.78MB；`-race` 因 Windows 需 CGO 留给 CI（ubuntu runner 自带 gcc）。
- M1 口径 Redis：便携版 5.0.14（`.tooling/agentgate-redis/`，AG-1 留下），独立端口无持久化，与网关同机。
- Python 版口径：agentgate EVIDENCE §3（Python 3.12 + 便携 Redis 5.0.14 + SQLite，进程内 ASGI mock）。
- 测试依赖 miniredis（纯 Go，测试专用，CI ubuntu 无需 Redis 服务）；运行时依赖仅 go-redis/v9（裸链路口径仍零依赖）。
