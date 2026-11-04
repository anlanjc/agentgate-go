// Package forward 实现 OpenAI 兼容上游的流式转发。
//
// 设计承接 Python 版 agentgate M0 的教训：不要让框架级中间件包一层再转发
// （BaseHTTPMiddleware 实测吞 44% 吞吐），要在 handler 里直接代理字节流。
// Go 侧对应做法是 Do/Relay 两段式：
//   - Do：发起上游请求，拿回完整响应（未读 body）——429/5xx 判定与故障转移
//     发生在此阶段，此时还**没向客户端写任何字节**，重试是安全的；
//   - Relay：判定通过后把响应逐 chunk 直写客户端（SSE 语义：每段即 flush），
//     一旦进入 Relay 就不再重试（流式响应已不可回滚）。
package forward

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

// hopHeaders 逐跳头不透传（RFC 7230 §6.1）。
var hopHeaders = map[string]bool{
	"Connection": true, "Proxy-Connection": true, "Keep-Alive": true,
	"Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
}

// Forwarder 上游 HTTP 转发器（复用连接池）。
type Forwarder struct {
	Client *http.Client
}

// New 创建转发器；timeoutMs 为单次上游请求超时（0 = 不限时）。
func New(timeoutMs int64) *Forwarder {
	return &Forwarder{
		Client: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
	}
}

// Do 发起上游 POST，返回未消费的响应。headers 中的逐跳头被剔除。
func (f *Forwarder) Do(ctx context.Context, upstreamURL string, headers http.Header, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	for k, vs := range headers {
		if hopHeaders[k] || k == "Host" || k == "Content-Length" {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return f.Client.Do(req)
}

// Relay 把上游响应逐字节转发给 w；每读到一段立即 flush（SSE 到达即推）。
// 返回前关闭 resp.Body。返回非 nil error 表示客户端侧中断（上游已正常应答）。
func (f *Forwarder) Relay(resp *http.Response, w http.ResponseWriter) error {
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if hopHeaders[k] {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
	}
}
