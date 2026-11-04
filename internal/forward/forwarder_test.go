package forward

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSSEPassthrough(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-key" {
			t.Errorf("上游应收到网关替换后的 key，got %q", got)
		}
		if r.Header.Get("Connection") != "" {
			t.Errorf("逐跳头不应透传")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for _, chunk := range []string{"data: {\"a\":1}\n\n", "data: {\"b\":2}\n\n", "data: [DONE]\n\n"} {
			w.Write([]byte(chunk))
			w.(http.Flusher).Flush()
		}
	}))
	defer upstream.Close()

	f := New(5000)
	h := http.Header{}
	h.Set("Authorization", "Bearer upstream-key")
	h.Set("Connection", "keep-alive") // 应被剔除
	resp, err := f.Do(context.Background(), upstream.URL+"/v1/chat/completions", h, []byte(`{"model":"x"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	rec := httptest.NewRecorder()
	if err := f.Relay(resp, rec); err != nil {
		t.Fatalf("Relay: %v", err)
	}
	got := rec.Body.String()
	for _, want := range []string{"\"a\":1", "\"b\":2", "[DONE]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("SSE chunk 丢失: %q", want)
		}
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("响应 Content-Type 应透传，got %q", ct)
	}
}
