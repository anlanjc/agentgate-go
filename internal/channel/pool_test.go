package channel

import (
	"testing"
	"time"
)

func TestPickSkipsCoolingChannel(t *testing.T) {
	a := &Channel{Name: "a"}
	b := &Channel{Name: "b"}
	p := NewPool(a, b)
	p.Report5xx("a")
	got, err := p.Pick()
	if err != nil || got.Name != "b" {
		t.Fatalf("a 冷却中应选 b，got=%v err=%v", got, err)
	}
}

func TestAllCoolingReturnsErr(t *testing.T) {
	p := NewPool(&Channel{Name: "a"})
	p.Report429("a", time.Minute)
	if _, err := p.Pick(); err != ErrNoChannel {
		t.Fatalf("全冷却应返回 ErrNoChannel，got %v", err)
	}
}

func TestEMAPrefersFastChannel(t *testing.T) {
	slow := &Channel{Name: "slow"}
	fast := &Channel{Name: "fast"}
	p := NewPool(slow, fast)
	for i := 0; i < 10; i++ {
		p.ReportSuccess("slow", 500)
		p.ReportSuccess("fast", 50)
	}
	got, err := p.Pick()
	if err != nil || got.Name != "fast" {
		t.Fatalf("EMA 应稳定选 fast，got=%v err=%v", got, err)
	}
}

func Test429RetryAfterRespected(t *testing.T) {
	a := &Channel{Name: "a"}
	b := &Channel{Name: "b"}
	p := NewPool(a, b)
	base := time.Now()
	p.now = func() time.Time { return base } // 注入时钟
	p.ReportSuccess("b", 500)                // 给 b 慢画像：恢复后 EMA 排序确定选 a（否则 EMA 相同走随机打散，断言会闪）
	p.Report429("a", 3*time.Second)

	p.now = func() time.Time { return base.Add(time.Second) }
	if got, _ := p.Pick(); got.Name != "b" {
		t.Fatalf("1s 后 a（Retry-After=3s）应仍在冷却，实际选中 %s", got.Name)
	}
	p.now = func() time.Time { return base.Add(4 * time.Second) }
	if got, _ := p.Pick(); got.Name != "a" {
		t.Fatalf("4s 后 a 应恢复，实际选中 %s", got.Name)
	}
}

func Test429DefaultCooldown(t *testing.T) {
	a := &Channel{Name: "a"}
	b := &Channel{Name: "b"}
	p := NewPool(a, b)
	p.Report429("a", 0) // 上游未带 Retry-After → 兜底 1s
	base := time.Now()
	p.now = func() time.Time { return base.Add(500 * time.Millisecond) }
	if got, _ := p.Pick(); got.Name != "b" {
		t.Fatalf("兜底冷却 1s 内不应恢复，实际选中 %s", got.Name)
	}
}
