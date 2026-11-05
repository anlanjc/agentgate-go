// agentgate-go：agentgate（Python 版）核心链路的 Go 重写——对照实验项目。
//
// 五模块：①virtual key 鉴权（Redis，M1 同功能口径）②准入（Redis Lua 令牌桶，
// M1 同功能口径）③渠道池 EMA+冷却故障转移 ④SSE 流式转发 ⑤context 超时 +
// 优雅退出。Python 版（../agentgate/）一砖不动作对照组，双语言对比口径与
// 压测数据见 docs/comparison.md。
//
// 职责边界：本版覆盖 Python 版 M0-M2 的核心请求链路 + M1 的鉴权/准入（Redis
// 口径，REDIS_ADDR 启用）；不含记账/队列/管理面（那些是 Python 版的完整叙事，
// 重写它们只证明语法不证明判断）。REDIS_ADDR 未设时保持裸链路口径（内存令牌
// 桶，零外部依赖）——双口径并存，各自数据见 comparison.md。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anlanjc/agentgate-go/internal/admit"
	"github.com/anlanjc/agentgate-go/internal/auth"
	"github.com/anlanjc/agentgate-go/internal/channel"
	"github.com/anlanjc/agentgate-go/internal/forward"
	"github.com/anlanjc/agentgate-go/internal/ratelimit"
)

type gateway struct {
	pool    *channel.Pool
	lim     *ratelimit.Bucket // 裸链路口径：进程内全局桶（REDIS_ADDR 未设时使用）
	fwd     *forward.Forwarder
	timeout time.Duration // 单次上游尝试的 context 超时
	// M1 同功能口径（REDIS_ADDR 设置时启用；nil = 裸链路）：
	authz *auth.Store
	adm   *admit.Admitter
}

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 请求体先读全：模型白名单需要 body 里的 model，故障转移重试不能二次消费 r.Body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	// ① 准入（双口径）：M1 = Redis 鉴权 + Lua 令牌桶（每请求 2 次 Redis 往返）；
	//    裸链路 = 进程内内存桶（并发安全见 internal/ratelimit 的 -race 测试）。
	if g.authz != nil {
		if !g.admitRequest(w, r, body) {
			return
		}
	} else if !g.lim.Allow() {
		writeErr(w, http.StatusTooManyRequests, "rate limited", "rate_limit_error", "rate_limited", 1)
		return
	}

	// ② 选渠道 + ③ 故障转移（429/5xx/网络错误 → 换渠道重试；4xx 原样透传）
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ch, err := g.pool.Pick()
		if err != nil { // 全渠道冷却：fail-closed 503
			writeErr(w, http.StatusServiceUnavailable, "all channels cooling down", "gateway_unavailable", "no_channel_available", 5)
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
	writeErr(w, http.StatusBadGateway, "all channels failed", "gateway_bad_gateway", "bad_gateway", 5)
}

// admitRequest 是 M1 同功能口径的准入链（对照 Python 版 v1_chat 的 _authenticate
// + _admit 顺序）：Bearer 提取 → virtual key 校验（1 次 Redis GET）→ 模型白名单
// → Lua 准入（1 次 EVALSHA：桶扣减 + tpm/预算预检合并往返）。失败时写出错误
// 响应并返回 false；通过后按 Python 版同款回写 X-RateLimit-Remaining。
func (g *gateway) admitRequest(w http.ResponseWriter, r *http.Request, body []byte) bool {
	raw := bearerOf(r.Header.Get("Authorization"))
	if raw == "" {
		writeErr(w, http.StatusUnauthorized, "missing bearer key", "invalid_request_error", "missing_api_key", 0)
		return false
	}
	doc, err := g.authz.Lookup(r.Context(), raw)
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrKeyNotFound):
			writeErr(w, http.StatusUnauthorized, "invalid api key", "authentication_error", "key_not_found", 0)
		case errors.Is(err, auth.ErrKeyInactive):
			writeErr(w, http.StatusUnauthorized, "api key revoked", "authentication_error", "key_inactive", 0)
		default:
			// Redis 故障语义对照（Python 版三分）：准入/预算承载计费硬上界，
			// 一律 fail-closed；鉴权在 Python 版可降级直查 DB（fail-open 到网关
			// 不死），本版无 DB 层，Redis 不可达 = 鉴权无法完成 → 同样 fail-closed
			// 503。完整对照表见 docs/comparison.md。
			log.Printf("auth fail-closed: %v", err)
			writeErr(w, http.StatusServiceUnavailable, "auth temporarily unavailable", "api_error", "auth_unavailable", 5)
		}
		return false
	}

	var probe struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &probe); err != nil || probe.Model == "" {
		writeErr(w, http.StatusBadRequest, "invalid request body", "invalid_request_error", "invalid_body", 0)
		return false
	}
	if len(doc.ModelWhitelist) > 0 && !slices.Contains(doc.ModelWhitelist, probe.Model) {
		writeErr(w, http.StatusForbidden, fmt.Sprintf("model '%s' not allowed for this key", probe.Model), "invalid_request_error", "model_not_allowed", 0)
		return false
	}

	res, err := g.adm.Admit(r.Context(), doc.ID, doc.RPMLimit, 1)
	if err != nil {
		// 准入 fail-closed 503（Python 版 _admit 同语义同码）：Redis 故障 =
		// 限流与预算全部失效，宁可拒绝也不裸放。
		log.Printf("admission fail-closed: %v", err)
		writeErr(w, http.StatusServiceUnavailable, "admission temporarily unavailable", "api_error", "admission_unavailable", 5)
		return false
	}
	if !res.Allowed {
		writeErr(w, http.StatusTooManyRequests, "rpm limit exceeded", "rate_limit_error", "rpm_exceeded", retrySeconds(res.RetryAfterMs))
		return false
	}
	if doc.TPMLimit > 0 && res.TPMWindow >= doc.TPMLimit {
		writeErr(w, http.StatusTooManyRequests, "tpm limit exceeded", "rate_limit_error", "tpm_exceeded", 60)
		return false
	}
	if doc.BudgetTokens != nil && res.BudgetSpent >= *doc.BudgetTokens {
		writeErr(w, http.StatusPaymentRequired, "budget exhausted", "insufficient_quota", "budget_exceeded", 0)
		return false
	}
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.TokensLeft))
	return true
}

// bearerOf 提取 "Bearer <key>"（Python 版 _bearer 同语义）。
func bearerOf(h string) string {
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(h[len("Bearer "):])
}

// retrySeconds 换算 Retry-After 秒数：max(1, ms//1000)，Python 版同款。
func retrySeconds(retryMs int) int {
	if s := retryMs / 1000; s > 1 {
		return s
	}
	return 1
}

// writeErr 按 Python 版 GatewayError 的响应体形状输出（message/type/code）；
// retryAfterS>0 时附 Retry-After 头。request_id 字段属 Python 观测面，本版不带。
func writeErr(w http.ResponseWriter, status int, msg, errType, code string, retryAfterS int) {
	w.Header().Set("Content-Type", "application/json")
	if retryAfterS > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterS))
	}
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":{"message":%q,"type":%q,"code":%q}}`, msg, errType, code)
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
	// M1 同功能口径开关：REDIS_ADDR 设置后启用 Redis 鉴权 + Lua 准入（每请求
	// 2 次往返，对照 Python 版 M1 链路形状）；不设则保持裸链路口径。超时收紧到
	// 1s：Redis 挂掉时 fail-closed 快速返回，不拖垮在途连接。
	if addr := envOr("REDIS_ADDR", ""); addr != "" {
		rdb := redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
			PoolSize:     16,
		})
		g.authz = auth.NewStore(rdb)
		g.adm = admit.New(rdb)
		log.Printf("redis auth+admission enabled (%s)", addr)
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

	// ⑤ 优雅退出：SIGINT/SIGTERM → 停止收新连接 → 最多 10s 排空在途 SSE 流
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
