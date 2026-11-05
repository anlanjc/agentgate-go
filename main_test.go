package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/anlanjc/agentgate-go/internal/admit"
	"github.com/anlanjc/agentgate-go/internal/auth"
	"github.com/anlanjc/agentgate-go/internal/channel"
	"github.com/anlanjc/agentgate-go/internal/forward"
	"github.com/anlanjc/agentgate-go/internal/ratelimit"
)

// 核心故障转移故事：主渠道 500 → 冷却 → 自动换备用渠道 → 客户端拿到 200 SSE。
func TestFailoverOn5xx(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"ok\":true}\n\ndata: [DONE]\n\n"))
	}))
	defer good.Close()

	g := &gateway{
		pool: channel.NewPool(
			&channel.Channel{Name: "bad", BaseURL: bad.URL, APIKey: "k1"},
			&channel.Channel{Name: "good", BaseURL: good.URL, APIKey: "k2"},
		),
		lim:     ratelimit.New(10, 10),
		fwd:     forward.New(5000),
		timeout: 5 * time.Second,
	}
	// 确定性铺垫：给 good 慢画像，Pick 必然先试 bad（EMA 排序打破随机打散）
	g.pool.ReportSuccess("good", 500)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"x"}`))
	g.chatCompletions(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("主渠道 500 后应故障转移到备用渠道拿到 200，实际 %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("SSE 内容应完整透传，got %q", rec.Body.String())
	}
	// 坏渠道应进入冷却：下一次请求不再碰它
	if ch, _ := g.pool.Pick(); ch.Name != "good" {
		t.Fatalf("bad 渠道应已冷却，实际选中 %s", ch.Name)
	}
}

func TestAllCoolingFailClosed503(t *testing.T) {
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer s1.Close()
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer s2.Close()

	g := &gateway{
		pool: channel.NewPool(
			&channel.Channel{Name: "c1", BaseURL: s1.URL},
			&channel.Channel{Name: "c2", BaseURL: s2.URL},
		),
		lim:     ratelimit.New(10, 10),
		fwd:     forward.New(5000),
		timeout: 5 * time.Second,
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`))
	g.chatCompletions(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("全渠道冷却应 fail-closed 503，实际 %d", rec.Code)
	}
}

func TestRateLimited429(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	g := &gateway{
		pool: channel.NewPool(&channel.Channel{Name: "c1", BaseURL: upstream.URL}),
		lim:  ratelimit.New(1, 0), fwd: forward.New(5000), timeout: 5 * time.Second,
	}
	rec := httptest.NewRecorder()
	g.chatCompletions(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("配额内请求应放行，实际 %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	g.chatCompletions(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("配额耗尽应 429，实际 %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 应带 Retry-After 头")
	}
}

func Test4xxPassthroughNoRetry(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid key"}`))
	}))
	defer upstream.Close()

	g := &gateway{
		pool: channel.NewPool(&channel.Channel{Name: "c1", BaseURL: upstream.URL}),
		lim:  ratelimit.New(10, 10), fwd: forward.New(5000), timeout: 5 * time.Second,
	}
	rec := httptest.NewRecorder()
	g.chatCompletions(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("4xx 应原样透传不重试，实际 %d", rec.Code)
	}
}

// ---------- M1 同功能口径：Redis 鉴权 + Lua 准入（对照 Python 版 v1_chat 链路） ----------

const benchKey = "ag-test-key-0000001"

// m1Gateway 组装带 Redis 的网关（miniredis）；dead 时鉴权/准入指向不可达端口。
func m1Gateway(t *testing.T, upstream *httptest.Server, mr *miniredis.Miniredis, doc *auth.Doc) *gateway {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	if doc != nil {
		if err := auth.NewStore(rdb).SeedKey(context.Background(), benchKey, doc); err != nil {
			t.Fatalf("SeedKey: %v", err)
		}
	}
	return &gateway{
		pool:    channel.NewPool(&channel.Channel{Name: "c1", BaseURL: upstream.URL, APIKey: "sk-upstream"}),
		lim:     ratelimit.New(10, 10),
		fwd:     forward.New(5000),
		timeout: 5 * time.Second,
		authz:   auth.NewStore(rdb),
		adm:     admit.New(rdb),
	}
}

func m1Doc() *auth.Doc {
	return &auth.Doc{
		ID: "vk-1", TenantID: "tenant-1",
		RPMLimit: 1000000, TPMLimit: 0, IsActive: true,
	}
}

func m1Request(body string) *http.Request {
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+benchKey)
	return req
}

// 无 Bearer / 未知 key → 401（missing_api_key / key_not_found，Python 版同码）。
func TestM1Auth401(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()
	mr := miniredis.RunT(t)

	g := m1Gateway(t, upstream, mr, m1Doc())

	rec := httptest.NewRecorder()
	g.chatCompletions(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m"}`)))
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "missing_api_key") {
		t.Fatalf("缺 Bearer 应 401 missing_api_key，实际 %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	bad := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	bad.Header.Set("Authorization", "Bearer ag-not-exist")
	g.chatCompletions(rec, bad)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "key_not_found") {
		t.Fatalf("未知 key 应 401 key_not_found，实际 %d %s", rec.Code, rec.Body.String())
	}
}

// 有效 key → 200 透传 + X-RateLimit-Remaining 头（Python 版限流响应头口径）。
func TestM1HappyPath(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data: {\"ok\":true}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()
	mr := miniredis.RunT(t)

	g := m1Gateway(t, upstream, mr, m1Doc())
	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"mock-model"}`))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("有效 key 应 200 透传，实际 %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-RateLimit-Remaining") == "" {
		t.Fatal("M1 口径应回写 X-RateLimit-Remaining")
	}
}

// 模型白名单 → 403 model_not_allowed（Python 版 _admit 第一道闸）。
func TestM1ModelWhitelist403(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("x")) }))
	defer upstream.Close()
	mr := miniredis.RunT(t)

	doc := m1Doc()
	doc.ModelWhitelist = []string{"gpt-4o"}
	g := m1Gateway(t, upstream, mr, doc)

	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"mock-model"}`))
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("白名单外模型应 403，实际 %d %s", rec.Code, rec.Body.String())
	}
}

// RPM 令牌桶：容量 1 → 第 2 发 429 rpm_exceeded + Retry-After（跨请求共享桶，
// 对照 Python 版 Redis 跨进程口径——两发请求即使换连接也共享同一桶）。
func TestM1RPMBucket429(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()
	mr := miniredis.RunT(t)

	doc := m1Doc()
	doc.RPMLimit = 1
	g := m1Gateway(t, upstream, mr, doc)

	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"m"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("配额内应放行，实际 %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"m"}`))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), "rpm_exceeded") {
		t.Fatalf("RPM 耗尽应 429 rpm_exceeded，实际 %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 应带 Retry-After")
	}
}

// 预算预检：budget 计数已超 doc 上限 → 402（Go 版无记账写入方，预置值触发）。
func TestM1BudgetExhausted402(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("x")) }))
	defer upstream.Close()
	mr := miniredis.RunT(t)

	budget := 50
	doc := m1Doc()
	doc.BudgetTokens = &budget
	g := m1Gateway(t, upstream, mr, doc)
	if err := mr.Set("budget:vk-1", "100"); err != nil {
		t.Fatalf("SET budget: %v", err)
	}

	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"m"}`))
	if rec.Code != http.StatusPaymentRequired || !strings.Contains(rec.Body.String(), "budget_exceeded") {
		t.Fatalf("预算耗尽应 402，实际 %d %s", rec.Code, rec.Body.String())
	}
}

// Redis 不可达 → 鉴权 fail-closed 503 auth_unavailable（Go 版无 DB 兜底的诚实语义；
// Python 版鉴权可降级 DB，对照表见 docs/comparison.md）。
func TestM1AuthFailClosed503(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("x")) }))
	defer upstream.Close()
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Second})
	t.Cleanup(func() { dead.Close() })

	g := &gateway{
		pool:    channel.NewPool(&channel.Channel{Name: "c1", BaseURL: upstream.URL}),
		lim:     ratelimit.New(10, 10),
		fwd:     forward.New(5000),
		timeout: 5 * time.Second,
		authz:   auth.NewStore(dead),
		adm:     admit.New(dead),
	}
	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"m"}`))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "auth_unavailable") {
		t.Fatalf("Redis 不可达应 fail-closed 503 auth_unavailable，实际 %d %s", rec.Code, rec.Body.String())
	}
}

// 准入 fail-closed 503 admission_unavailable：鉴权存活着，只挂准入链
// （对照 Python 版 _admit 的 RedisError → 503 admission_unavailable 同码）。
// 构造：auth 用活 miniredis，admit 用死端口 client。
func TestM1AdmissionFailClosed503(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("x")) }))
	defer upstream.Close()
	mr := miniredis.RunT(t)
	alive := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { alive.Close() })
	if err := auth.NewStore(alive).SeedKey(context.Background(), benchKey, m1Doc()); err != nil {
		t.Fatalf("SeedKey: %v", err)
	}
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Second})
	t.Cleanup(func() { dead.Close() })

	g := &gateway{
		pool:    channel.NewPool(&channel.Channel{Name: "c1", BaseURL: upstream.URL}),
		lim:     ratelimit.New(10, 10),
		fwd:     forward.New(5000),
		timeout: 5 * time.Second,
		authz:   auth.NewStore(alive),
		adm:     admit.New(dead),
	}
	rec := httptest.NewRecorder()
	g.chatCompletions(rec, m1Request(`{"model":"m"}`))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "admission_unavailable") {
		t.Fatalf("准入链故障应 fail-closed 503 admission_unavailable，实际 %d %s", rec.Code, rec.Body.String())
	}
}
