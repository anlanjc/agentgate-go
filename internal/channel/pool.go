// Package channel 实现渠道池：EMA 延迟优先负载均衡 + 故障冷却状态机。
//
// 语义对齐 Python 版 agentgate M2：429 尊重 Retry-After 立即冷却（缺省 1s），
// 5xx 冷却 60s，全渠道冷却时返回 ErrNoChannel（上层转 503 fail-closed）。
// Python 版单事件循环天然无锁；Go 版必须显式 mutex——两种并发模型的
// 心智差异本身就是对比叙事的一部分。
package channel

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ErrNoChannel 全渠道冷却中。
var ErrNoChannel = errors.New("all channels cooling down")

// Channel 一个上游渠道。
type Channel struct {
	Name          string
	BaseURL       string
	APIKey        string
	emaLatency    float64 // ms
	cooldownUntil time.Time
}

func (c *Channel) healthy(now time.Time) bool { return now.After(c.cooldownUntil) }

// Pool 渠道池（goroutine 安全）。
type Pool struct {
	mu       sync.Mutex
	channels []*Channel
	Cool5xx  time.Duration // 5xx 冷却时长
	emaAlpha float64       // EMA 平滑系数
	now      func() time.Time
}

func NewPool(channels ...*Channel) *Pool {
	return &Pool{
		channels: channels,
		Cool5xx:  60 * time.Second,
		emaAlpha: 0.3,
		now:      time.Now,
	}
}

// Pick 选当前健康且 EMA 延迟最低的渠道；
// 延迟差 <20ms 的前两名随机取一，避免低延迟渠道被羊群效应打爆（Python 版同款）。
func (p *Pool) Pick() (*Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	var healthy []*Channel
	for _, c := range p.channels {
		if c.healthy(now) {
			healthy = append(healthy, c)
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoChannel
	}
	sort.Slice(healthy, func(i, j int) bool { return healthy[i].emaLatency < healthy[j].emaLatency })
	if len(healthy) > 1 && healthy[1].emaLatency-healthy[0].emaLatency < 20 {
		return healthy[rand.Intn(2)], nil
	}
	return healthy[0], nil
}

// ReportSuccess 以 EMA 更新渠道延迟画像。
func (p *Pool) ReportSuccess(name string, latencyMs float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.channels {
		if c.Name == name {
			if c.emaLatency == 0 {
				c.emaLatency = latencyMs
			} else {
				c.emaLatency = p.emaAlpha*latencyMs + (1-p.emaAlpha)*c.emaLatency
			}
		}
	}
}

// Report429 立即冷却 retryAfter 时长（<=0 时兜底 1s，对齐 Python 版缺省）。
func (p *Pool) Report429(name string, retryAfter time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.channels {
		if c.Name == name {
			if retryAfter <= 0 {
				retryAfter = time.Second
			}
			c.cooldownUntil = p.now().Add(retryAfter)
		}
	}
}

// Report5xx 冷却 Cool5xx 时长（含网络错误——未拿到状态码一律按 5xx 口径）。
func (p *Pool) Report5xx(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.channels {
		if c.Name == name {
			c.cooldownUntil = p.now().Add(p.Cool5xx)
		}
	}
}
