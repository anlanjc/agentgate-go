// agentgate-go：agentgate（Python 版）核心链路的 Go 重写——对照实验项目。
//
// 四模块：①纯内存令牌桶限流 ②渠道池 EMA+冷却故障转移 ③SSE 流式转发
// ④context 超时 + 优雅退出。Python 版（../agentgate/）一砖不动作对照组，
// 双语言对比口径与压测数据见 docs/comparison.md。
//
// 职责边界：本版只覆盖 Python 版 M0-M2 的核心请求链路，不含多租户鉴权/
// 记账/队列/管理面（那些是 Python 版的完整叙事，重写它们只证明语法不证明判断）。
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/anlanjc/agentgate-go/internal/channel"
	"github.com/anlanjc/agentgate-go/internal/forward"
	"github.com/anlanjc/agentgate-go/internal/ratelimit"
)

type gateway struct {
	pool    *channel.Pool
	lim     *ratelimit.Bucket
	fwd     *forward.Forwarder
	timeout time.Duration // 单次上游尝试的 context 超时
}

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// ① 限流：纯内存令牌桶（并发安全见 internal/ratelimit 的 -race 测试）
	if !g.lim.Allow() {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit_error"}}`))
		return
	}
	// 请求体先读全：故障转移重试时不能二次消费 r.Body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	// ② 选渠道 + ③ 故障转移（429/5xx/网络错误 → 换渠道重试；4xx 原样透传）
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ch, err := g.pool.Pick()
		if err != nil { // 全渠道冷却：fail-closed 503
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":{"message":"all channels cooling down","type":"gateway_unavailable"}}`))
			return
		}

		h := r.Header.Clone()
		h.Set("Authorization", "Bearer "+ch.APIKey) // 租户 key → 渠道 key
		ctx, cancel := context.WithTimeout(r.Context(), g.timeout)
		t0 := time.Now()
		resp, ferr := g.fwd.Do(ctx, strings.TrimSuffix(ch.BaseURL, "/")+"/v1/chat/completions", h, body)
		elapsedMs := float64(time.Since(t0).Microseconds()) / 1000.0
		if ferr != nil { // 网络错误/超时：按 5xx 口径冷却后重试
			cancel()
			g.pool.Report5xx(ch.Name)
			log.Printf("attempt=%d channel=%s network_err=%v", attempt, ch.Name, ferr)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			g.pool.Report429(ch.Name, retryAfterOf(resp.Header))
			resp.Body.Close()
			cancel()
			log.Printf("attempt=%d channel=%s 429 cooldown", attempt, ch.Name)
			continue
		case resp.StatusCode >= 500:
			g.pool.Report5xx(ch.Name)
			resp.Body.Close()
			cancel()
			log.Printf("attempt=%d channel=%s %d cooldown", attempt, ch.Name, resp.StatusCode)
			continue
		default:
			// 2xx/4xx 原样透传；进入 Relay 后流式响应不可回滚，不再重试
			g.pool.ReportSuccess(ch.Name, elapsedMs)
			rerr := g.fwd.Relay(resp, w)
			cancel()
			if rerr != nil {
				log.Printf("channel=%s relay aborted (client gone?): %v", ch.Name, rerr)
			}
			return
		}
	}

	// 所有尝试都在 Relay 前失败——尚未向客户端写过任何字节，可安全 502
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	w.Write([]byte(`{"error":{"message":"all channels failed","type":"gateway_bad_gateway"}}`))
}

// retryAfterOf 解析上游 Retry-After（秒）；缺失返回 0（池内兜底 1s）。
func retryAfterOf(h http.Header) time.Duration {
	if s := h.Get("Retry-After"); s != "" {
		if sec, err := strconv.Atoi(s); err == nil && sec > 0 {
			return time.Duration(sec) * time.Second
		}
	}
	return 0
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envF(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func main() {
	var chans []*channel.Channel
	for i, name := range []string{"primary", "backup"} {
		url := envOr("UPSTREAM_"+strconv.Itoa(i+1)+"_URL", "")
		key := envOr("UPSTREAM_"+strconv.Itoa(i+1)+"_KEY", "")
		if url != "" {
			chans = append(chans, &channel.Channel{Name: name, BaseURL: url, APIKey: key})
		}
	}
	if len(chans) == 0 {
		log.Fatal("no upstream configured: set UPSTREAM_1_URL / UPSTREAM_1_KEY")
	}

	g := &gateway{
		pool:    channel.NewPool(chans...),
		lim:     ratelimit.New(envF("RATE_CAPACITY", 50), envF("RATE_REFILL", 10)),
		fwd:     forward.New(30000),
		timeout: 30 * time.Second,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", g.chatCompletions)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              ":" + envOr("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// ④ 优雅退出：SIGINT/SIGTERM → 停止收新连接 → 最多 10s 排空在途 SSE 流
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("agentgate-go listening on %s (channels=%d)", srv.Addr, len(chans))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down, draining in-flight streams...")
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(drain); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Printf("bye")
}
