// Package admit 提供准入检查：Redis Lua 令牌桶 + TPM/预算预检，单次 EVALSHA 往返。
//
// 对照 Python 版 app/ratelimit/admit.py 的 ADMIT_LUA（M1 调优产物）：Python 版把
// "桶扣减 + tpm GET + budget GET" 合并进一个脚本，3 次往返降为 1 次 EVALSHA
// （eval_cached 缓存脚本 SHA，NACK 时回退 EVAL）。本 Go 版同思路：
//
//	每请求 Redis 往返 = 鉴权 GET 1 次（internal/auth）+ 准入 EVALSHA 1 次 = 2 次。
//	其中 tpm/预算读合并在同一次 EVALSHA 里，不额外增加往返。
//
// 与 Python 版的差异（诚实声明）：①租户预算兜底 KEYS[4] 属 Python P2 hardening，
// Go M1 不纳入对照（无记账写入方，读了恒 0）；②Go 版无记账模块，tpm/budget 键
// 只在脚本里读取返回，预置了值才生效——管道语义对齐，写侧不在 M0-M2 范围。
package admit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 键名与 Python 版 app/quota/service.py 逐字对齐（rl:rpm:{key_id} /
// tpm:{key_id} / budget:{key_id}），同一套约定便于跨语言交叉验证。
//
// KEYS[1]=限流桶 KEYS[2]=tpm窗口 KEYS[3]=key 预算
// ARGV[1]=容量 ARGV[2]=速率(tokens/s) ARGV[3]=now_ms ARGV[4]=扣减量
const admitLua = `
local key    = KEYS[1]
local cap    = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local amount = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])
if tokens == nil then
    tokens = cap
    ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(cap, tokens + elapsed / 1000.0 * rate)

local allowed = 0
local retry_after_ms = 0
if tokens >= amount then
    tokens = tokens - amount
    allowed = 1
else
    retry_after_ms = math.ceil((amount - tokens) / rate * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
local ttl_ms = math.ceil(cap / rate * 2 * 1000)
if ttl_ms < 1000 then ttl_ms = 1000 end
redis.call('PEXPIRE', key, ttl_ms)

local tpm = tonumber(redis.call('GET', KEYS[2]) or '0')
local budget = tonumber(redis.call('GET', KEYS[3]) or '0')
return {allowed, tostring(math.floor(tokens)), retry_after_ms, tpm, budget}
`

// Result 对应 Python 版 AdmitResult（去掉租户字段，见包注释差异声明）。
type Result struct {
	Allowed      bool
	TokensLeft   int
	RetryAfterMs int
	TPMWindow    int
	BudgetSpent  int
}

// Admitter 复用 go-redis 的脚本缓存：NewScript 首发 EVAL，成功后走 EVALSHA，
// 收到 NOSCRIPT 自动回退重放——与 Python 版 eval_cached 同一款省往返思路。
type Admitter struct {
	script *redis.Script
}

func New() *Admitter { return &Admitter{script: redis.NewScript(admitLua)} }

func bucketKey(keyID string) string { return "rl:rpm:" + keyID }
func tpmKey(keyID string) string    { return "tpm:" + keyID }
func budgetKey(keyID string) string { return "budget:" + keyID }

// Admit 对 keyID 做一次准入（默认扣 1 个令牌）。
// capacity<=0 → 直接拒绝（Python 版同款：60s 后重试的兜底口径，防御限速配置为 0）。
// Redis 故障原样上抛，调用方（main.go）按 fail-closed 503 处理——准入承载计费硬
// 上界，跳过意味着限流与预算全部失效，宁可拒绝也不裸放（Python 版同语义）。
func (a *Admitter) Admit(ctx context.Context, rdb *redis.Client, keyID string, capacity, amount int) (*Result, error) {
	if capacity <= 0 {
		return &Result{Allowed: false, RetryAfterMs: 60000}, nil
	}
	rate := float64(capacity) / 60.0 // Python 版同款：60s 补满
	nowMs := time.Now().UnixMilli()
	res, err := a.script.Run(ctx, rdb, []string{
		bucketKey(keyID), tpmKey(keyID), budgetKey(keyID),
	}, capacity, rate, nowMs, amount).Slice()
	if err != nil {
		// 脚本执行失败（连接/超时/只读副本等）统一上抛为 ErrUnavailable 语义。
		return nil, fmt.Errorf("admission script: %w", err)
	}
	allowed, _ := toInt(res[0])
	tokens, _ := toInt(res[1])
	retryMs, _ := toInt(res[2])
	tpm, _ := toInt(res[3])
	budget, _ := toInt(res[4])
	return &Result{
		Allowed:      allowed == 1,
		TokensLeft:   tokens,
		RetryAfterMs: retryMs,
		TPMWindow:    tpm,
		BudgetSpent:  budget,
	}, nil
}

// toInt 承接 Lua 返回的两种数值形态（GO-Redis 把整数回 int64、字符串回 string）。
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case string:
		var f float64
		if _, err := fmt.Sscanf(n, "%g", &f); err == nil {
			return int(f), true
		}
	}
	return 0, false
}
