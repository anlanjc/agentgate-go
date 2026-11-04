package ratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 对照 Python 版 M1 的验收口径：并发 100 恰放行 50，零超卖。
// Python 版用 Redis+Lua 保证；Go 版证明 mutex 语义在本进程内同样严格。
// 运行：go test -race ./...
func TestBucketConcurrentNoOversell(t *testing.T) {
	b := New(50, 0) // 不补充，纯存量竞争
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("并发 100 应恰放行 50，实际 %d", got)
	}
}

func TestBucketRefill(t *testing.T) {
	b := New(1, 1000) // 高速补充
	if !b.Allow() {
		t.Fatal("满桶首次应放行")
	}
	// 容量 1 的桶立即第二次调用应被拒（突发上限=容量，这才是令牌桶正确语义）
	if b.Allow() {
		t.Fatal("容量 1 且未等到补充时应拒绝")
	}
	time.Sleep(2 * time.Millisecond) // 2ms × 1000/s = 2 个令牌（封顶容量 1）
	if !b.Allow() {
		t.Fatal("补充后应放行")
	}
}

func TestBucketExhausted(t *testing.T) {
	b := New(2, 0)
	if !b.Allow() || !b.Allow() {
		t.Fatal("前两次应放行")
	}
	if b.Allow() {
		t.Fatal("耗尽且零补充时应拒绝")
	}
	// 零补充 → 永远等不到 → RetryAfter 应为"无限"哨兵值而非除零溢出
	if d := b.RetryAfter(1); d != time.Duration(math.MaxInt64) {
		t.Fatalf("零补充时 RetryAfter 应为 MaxInt64 哨兵，实际 %v", d)
	}
}
