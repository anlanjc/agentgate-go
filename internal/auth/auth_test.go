package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewStore(rdb), mr
}

func sampleDoc() *Doc {
	budget := 100000
	return &Doc{
		ID:             "vk-1",
		TenantID:       "tenant-1",
		ModelWhitelist: []string{"mock-model"},
		RPMLimit:       60,
		TPMLimit:       10000,
		BudgetTokens:   &budget,
		IsActive:       true,
	}
}

// 种子 → 校验：字段完整往返；hash 语义与 Python 版 hash_key 一致（SHA-256 hex）。
func TestLookupSeededKey(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	raw := KeyPrefix + "testkey123"
	if err := s.SeedKey(ctx, raw, sampleDoc()); err != nil {
		t.Fatalf("SeedKey: %v", err)
	}
	doc, err := s.Lookup(ctx, raw)
	if err != nil {
		t.Fatalf("Lookup 应成功: %v", err)
	}
	if doc.ID != "vk-1" || doc.TenantID != "tenant-1" || doc.RPMLimit != 60 || !doc.IsActive {
		t.Fatalf("字段应完整往返，got %+v", doc)
	}
	if len(doc.ModelWhitelist) != 1 || doc.ModelWhitelist[0] != "mock-model" {
		t.Fatalf("白名单应往返，got %v", doc.ModelWhitelist)
	}
	if doc.BudgetTokens == nil || *doc.BudgetTokens != 100000 {
		t.Fatalf("预算应往返，got %v", doc.BudgetTokens)
	}
	// 对照 Python：hash_key("ag-testkey123") 的键位一致性
	want := CacheKey(HashKey(raw))
	if len(want) != 3+64 { // "vk:" + 64 位 sha256 hex
		t.Fatalf("缓存键应为 vk:{64位sha256}，got %q", want)
	}
}

func TestLookupUnknownKey(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Lookup(context.Background(), KeyPrefix+"ghost"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("未知 key 应 ErrKeyNotFound，got %v", err)
	}
}

// 吊销语义：is_active=false → ErrKeyInactive（Python 版 KeyInactive 同映射 401）。
func TestLookupRevokedKey(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	raw := KeyPrefix + "revoked"
	doc := sampleDoc()
	doc.IsActive = false
	if err := s.SeedKey(ctx, raw, doc); err != nil {
		t.Fatalf("SeedKey: %v", err)
	}
	if _, err := s.Lookup(ctx, raw); !errors.Is(err, ErrKeyInactive) {
		t.Fatalf("吊销 key 应 ErrKeyInactive，got %v", err)
	}
}

// 档损坏 = 数据事故 → ErrUnavailable（调用方 fail-closed 503，不放行不假装 401）。
func TestLookupCorruptDoc(t *testing.T) {
	s, mr := newTestStore(t)
	mr.Set(CacheKey(HashKey(KeyPrefix+"corrupt")), "{not-json")
	if _, err := s.Lookup(context.Background(), KeyPrefix+"corrupt"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("损坏档应 ErrUnavailable，got %v", err)
	}
}

// hash 语义与 Python 版逐字一致的最后防线：交叉验证自
// python -c "import hashlib;print(hashlib.sha256(b'ag-x').hexdigest())"
func TestHashKeyParity(t *testing.T) {
	const want = "64f866609bb7e0d5d64774717063036930104cef3e2c31e7cccf249b4fd9afee"
	if got := HashKey("ag-x"); got != want {
		t.Fatalf("hash 应与 Python 版 hash_key 一致，got %q want %q", got, want)
	}
}
