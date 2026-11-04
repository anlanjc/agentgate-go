// Package ratelimit 提供进程内令牌桶限流。
//
// 口径说明：Python 版 agentgate M1 用 Redis+Lua 实现"跨进程共享"的令牌桶
// （多 worker 口径）；本 Go 版刻意做纯内存版——教学重点是 goroutine 并发
// 安全（sync.Mutex + go test -race 验证），对比口径差异见 docs/comparison.md。
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// Bucket 令牌桶：容量 capacity，每秒补充 refillPerSec 个。
type Bucket struct {
	mu           sync.Mutex
	tokens       float64
	capacity     float64
	refillPerSec float64
	last         time.Time
}

// New 创建令牌桶，初始满桶。
func New(capacity, refillPerSec float64) *Bucket {
	return &Bucket{
		tokens:       capacity,
		capacity:     capacity,
		refillPerSec: refillPerSec,
		last:         time.Now(),
	}
}

// Allow 尝试取 1 个令牌。
func (b *Bucket) Allow() bool { return b.AllowN(1) }

// AllowN 尝试取 n 个令牌。令牌按流逝时间惰性补充（无需后台协程）。
func (b *Bucket) AllowN(n float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.refillPerSec
	b.last = now
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// RetryAfter 返回攒够 n 个令牌需要的等待时长（0 = 现在就够）。
// refillPerSec<=0 且令牌不足时返回 math.MaxInt64（永远等不到）。
func (b *Bucket) RetryAfter(n float64) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	need := n - b.tokens
	if need <= 0 {
		return 0
	}
	if b.refillPerSec <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(need / b.refillPerSec * float64(time.Second))
}
