package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sx/internal/brave"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/searxng"
)

type fakeSearxng struct {
	resp *searxng.Response
	err  error
}

func (f *fakeSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
	return f.resp, f.err
}

type fakeBrave struct {
	resp *brave.Response
	err  error
}

func (f *fakeBrave) Search(_ context.Context, _ string) (*brave.Response, error) {
	return f.resp, f.err
}

func newCfg() *config.Config {
	return &config.Config{
		SearxngBackendURL:    "http://searxng-primary:8080",
		FallbackTimeout:      30 * time.Second,
		CacheTTL:             time.Hour,
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  180 * time.Second,
	}
}

// TestSearchSearxngOK — binary fallback: 1 result (1 engine) → sufficient → SearXNG
func TestSearchSearxngOK(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "single", Engine: "wikipedia"},
	}}}
	c, _ := cache.New(10)
	p := New(newCfg(), sx, &fakeBrave{}, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (SearXNG sufficient since binary)", len(out.Results))
	}
	if out.Results[0].Engine != "wikipedia" {
		t.Errorf("Engine = %q, want wikipedia", out.Results[0].Engine)
	}
}

// TestSearchFallbackBrave — SearXNG 0 results → insufficient → Brave fallback
func TestSearchFallbackBrave(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T1", URL: "u1", Description: "d1", Age: "1d"})
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (Brave fallback)", len(out.Results))
	}
	if out.Results[0].Engine != "brave-api" {
		t.Errorf("Engine = %q, want brave-api", out.Results[0].Engine)
	}
}

// TestSearchFallbackTimeout — SearXNG timeout → Brave fallback
func TestSearchFallbackTimeout(t *testing.T) {
	sx := &fakeSearxng{err: context.DeadlineExceeded}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T", URL: "u", Description: "d"})
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (Brave on SearXNG timeout)", len(out.Results))
	}
}

// TestSearchCacheHit — cache hit, SearXNG not called
func TestSearchCacheHit(t *testing.T) {
	c, _ := cache.New(10)
	c.Set("x", &searxng.Response{Results: []searxng.Result{{Title: "cached"}}})
	sx := &fakeSearxng{} // must NOT be called
	p := New(newCfg(), sx, &fakeBrave{}, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if out.Results[0].Title != "cached" {
		t.Errorf("Title = %q, want cached", out.Results[0].Title)
	}
}

// TestSearchFallbackBraveFails — SearXNG 0 results + Brave error → error
func TestSearchFallbackBraveFails(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: nil}}
	bv := &fakeBrave{err: errors.New("upstream 500")}
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	if _, err := p.Search(context.Background(), "x"); err == nil {
		t.Error("Search expected error when both SearXNG and Brave fail")
	}
}

// --- New TDD tests for binary fallback + cooldown circuit breaker ---

// TestSearchSearxngBinaryOK — 1 result from 1 engine → sufficient (binary)
func TestSearchSearxngBinaryOK(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "single", Engine: "wikipedia"},
	}}}
	c, _ := cache.New(10)
	p := New(newCfg(), sx, &fakeBrave{}, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (SearXNG sufficient since binary)", len(out.Results))
	}
	if out.Results[0].Engine != "wikipedia" {
		t.Errorf("Engine = %q, want wikipedia", out.Results[0].Engine)
	}
}

// TestSearchSearxngBinaryFailSkipsCooldown — 5 failures < threshold 6
// SearXNG still tried before Brave fallback
func TestSearchSearxngBinaryFailSkipsCooldown(t *testing.T) {
	sx := &fakeSearxng{err: errors.New("upstream 500")}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T", URL: "u", Description: "d"})
	c, _ := cache.New(10)
	cfg := newCfg()
	p := New(cfg, sx, bv, c)

	// 5 consecutive failures
	for i := 0; i < 5; i++ {
		out, err := p.Search(context.Background(), fmt.Sprintf("query-%d", i))
		if err != nil {
			t.Fatalf("Search err %d: %v", i, err)
		}
		if len(out.Results) != 1 {
			t.Errorf("Call %d: len = %d, want 1 (Brave fallback)", i, len(out.Results))
		}
	}

	// Failure count is 5, below threshold 6 → SearXNG still tried on 6th call
	out, err := p.Search(context.Background(), "query-6")
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 1 {
		t.Errorf("Call 6: len = %d, want 1 (Brave fallback, SearXNG tried)", len(out.Results))
	}
}

// TestSearchSearxngCooldownActive — 6 failures → cooldown active
// During cooldown, SearXNG is skipped, goes directly to Brave
func TestSearchSearxngCooldownActive(t *testing.T) {
	sx := &fakeSearxng{err: errors.New("upstream 500")}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T", URL: "u", Description: "d"})
	c, _ := cache.New(10)
	cfg := newCfg()
	cfg.SearxngFailCooldown = 1 * time.Second // short for fast test
	p := New(cfg, sx, bv, c)

	// 6 warmup calls to trigger cooldown
	for i := 0; i < 6; i++ {
		_, _ = p.Search(context.Background(), fmt.Sprintf("warmup-%d", i))
	}

	// Cooldown active: new instance without cooldown checks isolation
	sxTracked := &fakeSearxng{err: errors.New("CALLED!")}
	p2 := New(cfg, sxTracked, bv, c)
	out, err := p2.Search(context.Background(), "different-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 1 {
		t.Errorf("p2 call: len = %d, want 1 (Brave)", len(out.Results))
	}

	// Same instance p still in cooldown → skips SearXNG → Brave
	out, err = p.Search(context.Background(), "post-cooldown")
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 1 {
		t.Errorf("Same instance cooldown call: len = %d, want 1", len(out.Results))
	}
}
