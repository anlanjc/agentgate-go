package admit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestAdmitter(t *testing.T) (*Admitter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb), mr
}

// 配额内放行 / 耗尽拒绝：并发 100 恰放行 50 的 Lua 版口径验证。
func TestAdmitAllowsWithinCapacity(t *testing.T) {
	a, _ := newTestAdmitter(t)
	ctx := context.Background()
	const cap = 3
	allowed := 0
	var last *Result
	for i := 0; i < cap+1; i++ {
		res, err := a.Admit(ctx, "vk-1", cap, 1)
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if res.Allowed {
			allowed++
		}
		last = res
	}
	if allowed != cap {
		t.Fatalf("容量 %d 应恰放行 %d，实际放行 %d", cap, cap, allowed)
	}
	if last.Allowed {
		t.Fatal("第 cap+1 次应拒绝")
	}
	if last.RetryAfterMs <= 0 {
		t.Fatalf("拒绝应带 retry_after_ms，got %d", last.RetryAfterMs)
	}
	if last.TokensLeft != 0 {
		t.Fatalf("耗尽时余量应为 0，got %d", last.TokensLeft)
	}
}

// 惰性补充：把桶回拨 1.5s（tokens=0, ts=now-1.5s），容量 60（速率 1/s）→ 补 1.5
// 个，扣 1 剩 0.5 → floor 后余量 0。不 sleep，确定性验证 elapsed/1000*rate 语义。
func TestAdmitRefillsOverTime(t *testing.T) {
	a, _ := newTestAdmitter(t)
	ctx := context.Background()
	const keyID = "vk-r"
	// 先耗尽
	for i := 0; i < 60; i++ {
		if _, err := a.Admit(ctx, keyID, 60, 1); err != nil {
			t.Fatalf("Admit: %v", err)
		}
	}
	if res, _ := a.Admit(ctx, keyID, 60, 1); res.Allowed {
		t.Fatal("耗尽后应拒绝")
	}
	// 回拨 1.5s：直接改 hash（与脚本 HMSET 的字段名一致）
	bucket := bucketKey(keyID)
	backdated := time.Now().UnixMilli() - 1500
	if err := a.rdb.Do(ctx, "HMSET", bucket, "tokens", "0", "ts", backdated).Err(); err != nil {
		t.Fatalf("HMSET: %v", err)
	}
	res, err := a.Admit(ctx, keyID, 60, 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("1.5s 应补 1.5 个令牌足以放行（rate=1/s），got %+v", res)
	}
	if res.TokensLeft != 0 {
		t.Fatalf("余量应 floor(0.5)=0，got %+v", res)
	}
}

// capacity<=0 → 拒绝 + 60s 兜底（Python 版 admit 同款防御）。
func TestAdmitCapacityZeroDenies(t *testing.T) {
	a, _ := newTestAdmitter(t)
	res, err := a.Admit(context.Background(), "vk-0", 0, 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if res.Allowed || res.RetryAfterMs != 60000 {
		t.Fatalf("capacity=0 应拒绝且 retry 60s，got %+v", res)
	}
}

// 合并读：tpm/budget 键在同一次 EVALSHA 里带出（Python 版 M1 单往返口径）。
func TestAdmitMergedReads(t *testing.T) {
	a, _ := newTestAdmitter(t)
	ctx := context.Background()
	const keyID = "vk-m"
	if err := a.rdb.Set(ctx, tpmKey(keyID), "500", 0).Err(); err != nil {
		t.Fatalf("SET tpm: %v", err)
	}
	if err := a.rdb.Set(ctx, budgetKey(keyID), "77", 0).Err(); err != nil {
		t.Fatalf("SET budget: %v", err)
	}
	res, err := a.Admit(ctx, keyID, 60, 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if res.TPMWindow != 500 || res.BudgetSpent != 77 {
		t.Fatalf("合并读应带出 tpm=500 budget=77，got %+v", res)
	}
}
