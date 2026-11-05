// Package auth 提供 virtual key 鉴权（对照 Python 版 app/auth/keys.py 的 key_hash 语义）。
//
// Python 版路径：SHA-256(raw) → Redis GET vk:{hash}（缓存，TTL 60s）→ miss → PG 查询 → 回填。
// 本 Go 版砍掉 PG 层（多租户/管理面是 Python 版的完整叙事，不在 M0-M2 对照范围）：
// Redis 即事实来源，SeedKey 写入不带 TTL——因此没有"缓存读失败降级直查 DB"的分支，
// Redis 不可用 = 鉴权无法完成 = fail-closed 503（Python 版鉴权可降级 DB，语义见
// docs/comparison.md 的故障语义对照）。
//
// Redis 往返次数：每请求 1 次 GET（hash 直查无热点缓存穿透问题——key 空间即 raw 的
// SHA-256，不存在可枚举性）。
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix 与 Python 版 KEY_PREFIX 一致（"ag-"）；CacheTTL 仅用于注释对照——
// Python 版缓存回填 SETEX 60s（DB 为事实来源），本版无 DB，种子不带 TTL。
const KeyPrefix = "ag-"

// 哨兵错误 → HTTP 映射在 main.go（Python 版对应 KeyNotFound/KeyInactive 异常类型）。
var (
	ErrKeyNotFound = errors.New("virtual key not found")
	ErrKeyInactive = errors.New("virtual key revoked")
	// ErrUnavailable 表示 Redis 本身不可达/超时（与 key 不存在是两回事）。
	ErrUnavailable = errors.New("auth store unavailable")
)

// Doc 是 vk:{hash} 里存的 key 文档，JSON 字段名与 Python 版 _serialize 逐字对齐
// （同一份 JSON 两边都能读，便于交叉验证/迁移）。tenant_budget_tokens 与
// concurrency_limit 是 Python P2 hardening 字段，Go M1 不消费，故不入档。
type Doc struct {
	ID             string   `json:"id"`
	TenantID       string   `json:"tenant_id"`
	ModelWhitelist []string `json:"model_whitelist"` // nil/空 = 不限
	RPMLimit       int      `json:"rpm_limit"`
	TPMLimit       int      `json:"tpm_limit"`
	BudgetTokens   *int     `json:"budget_tokens"` // nil = 不限额
	IsActive       bool     `json:"is_active"`
}

// HashKey 与 Python 版 auth.keys.hash_key 完全一致：SHA-256(raw) 的 hex。
func HashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// CacheKey 返回 Redis 键名 vk:{hash}（Python 版 _cache_key 同款）。
func CacheKey(keyHash string) string { return "vk:" + keyHash }

// Store 是鉴权存储。零字段：只包一个 client，测试可直接构造。
type Store struct{ rdb *redis.Client }

func NewStore(rdb *redis.Client) *Store { return &Store{rdb: rdb} }

// Lookup 校验 raw key：sha256 → GET vk:{hash} → 反序列化。
// 错误分类：ErrKeyNotFound / ErrKeyInactive（业务）；其余 = Redis 故障
// （调用方按 fail-closed 503 处理）。
func (s *Store) Lookup(ctx context.Context, rawKey string) (*Doc, error) {
	raw, err := s.rdb.Get(ctx, CacheKey(HashKey(rawKey))).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	var doc Doc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		// 档损坏属于数据事故而非客户端错误：按不可用口径拒绝（fail-closed）。
		return nil, fmt.Errorf("%w: vk doc corrupt: %v", ErrUnavailable, err)
	}
	if !doc.IsActive {
		return nil, ErrKeyInactive
	}
	return &doc, nil
}

// SeedKey 写入一把 key（测试与基准辅助；生产语义上这是"管理面签发"的替代——
// Go 版无管理面，Redis 即事实来源，故不带 TTL。Python 版由 admin API 写 PG +
// 失效缓存，回填走 SETEX 60s；若未来本版补 DB 双层架构，回填应对齐该 TTL）。
func (s *Store) SeedKey(ctx context.Context, rawKey string, doc *Doc) error {
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, CacheKey(HashKey(rawKey)), payload, 0).Err()
}
