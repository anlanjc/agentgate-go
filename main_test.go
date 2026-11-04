package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
