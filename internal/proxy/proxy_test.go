package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/searxng"
)

// --- Test doubles ---

type fakeSearxng struct {
	resp *searxng.Response
	err  error
}

func (f *fakeSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
	return f.resp, f.err
}

type fakeBackend struct {
	name    string
	results []backends.SearchResult
	err     error
	avail   bool
}

func (f *fakeBackend) Name() string              { return f.name }
func (f *fakeBackend) IsAvailable() bool         { return f.avail }
func (f *fakeBackend) Search(_ backends.SearchOptions) ([]backends.SearchResult, error) {
	return f.results, f.err
}

// --- Helpers ---

func newCfg() *config.Config {
	return &config.Config{
		SearxngBackendURL:     "http://searxng-primary:8080",
		FallbackTimeout:       30 * time.Second,
		CacheTTL:              time.Hour,
		SearxngFailThreshold:  6,
		SearxngFailCooldown:   180 * time.Second,
		SufficientMinResults:  10,
		FallbackProviders:     []string{"brave", "exa"},
		T1PremiumProviders:    []string{},
	}
}

// newTestProxy creates a Proxy with the given fallback backends and breaker.
func newTestProxy(cfg *config.Config, sx searxng.Client, c *cache.Cache, breakerMgr *breaker.Manager, fbs ...backends.SearchBackend) *Proxy {
	mgr := backends.NewManager()
	for _, fb := range fbs {
		mgr.Register(fb)
	}
	_ = mgr.SetFallbacks(cfg.FallbackProviders)
	return New(cfg, sx, c, breakerMgr, mgr)
}

// --- Tests ---

// TestSearchSearxngOK — SearXNG returns >= 10 results → sufficient, still calls 1 premium.
func TestSearchSearxngOK(t *testing.T) {
	sxRes := make([]searxng.Result, 10)
	for i := range sxRes {
		sxRes[i] = searxng.Result{Title: fmt.Sprintf("SX%d", i), URL: fmt.Sprintf("https://sx%d.com", i), Engine: "wikipedia"}
	}
	sx := &fakeSearxng{resp: &searxng.Response{Results: sxRes}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR1", URL: "https://brave1.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100)
	p := newTestProxy(newCfg(), sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) < 10 {
		t.Errorf("len = %d, want >= 10", len(out.Results))
	}
}

// TestSearchMergeDedup — SearXNG + premium results, deduplicated by URL.
func TestSearchMergeDedup(t *testing.T) {
	// Same URL in both SearXNG and Brave → dedup
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://shared.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://shared.com", Content: "d", Engine: "brave"},
		{Title: "BR2", URL: "https://brave-only.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SufficientMinResults = 5 // trigger loop to call more premiums if needed
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	// shared.com should appear only once
	sharedCount := 0
	for _, r := range out.Results {
		if r.URL == "https://shared.com" {
			sharedCount++
		}
	}
	if sharedCount != 1 {
		t.Errorf("shared.com dedup count = %d, want 1 (deduplicated)", sharedCount)
	}
}

// TestSearchPremiumLoop — SearXNG 0 results, premiums fill up to threshold.
func TestSearchPremiumLoop(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	fb1 := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	fb2 := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{
		{Title: "EX", URL: "https://ex.com", Content: "d", Engine: "exa"},
	}}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SufficientMinResults = 2 // loop will try both premiums
	p := newTestProxy(cfg, sx, c, breaker.New(), fb1, fb2)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) < 1 {
		t.Errorf("len = %d, want >= 1 (premium fallback)", len(out.Results))
	}
}

// TestSearchSearxngCooldown — SearXNG in cooldown → only premiums.
func TestSearchSearxngCooldown(t *testing.T) {
	sx := &fakeSearxng{err: errors.New("upstream 500")}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SearxngFailCooldown = 1 * time.Second
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)

	// 6 warmup calls to trigger cooldown
	for i := 0; i < 6; i++ {
		_, _ = p.Search(context.Background(), fmt.Sprintf("w%d", i))
	}

	out, err := p.Search(context.Background(), "post-cooldown")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) < 1 {
		t.Errorf("len = %d, want >= 1 (premium during cooldown)", len(out.Results))
	}
}

// TestSearchCacheHit — cache hit, no SearXNG or premium called.
func TestSearchCacheHit(t *testing.T) {
	c, _ := cache.New(100)
	c.Set("x", &searxng.Response{Results: []searxng.Result{{Title: "cached"}}})
	sx := &fakeSearxng{}
	p := newTestProxy(newCfg(), sx, c, breaker.New(), &fakeBackend{name: "brave", avail: true})
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if out.Results[0].Title != "cached" {
		t.Errorf("Title = %q, want cached", out.Results[0].Title)
	}
}

// TestSearchCircuitBreaker — premium with open circuit breaker skipped.
func TestSearchCircuitBreaker(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	fb1 := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	fb2 := &fakeBackend{name: "exa", avail: true, err: errors.New("exa down")}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	bm := breaker.New()
	bm.RecordClientError("exa", "test trip") // trip exa's breaker
	p := newTestProxy(cfg, sx, c, bm, fb1, fb2)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	// Should get brave results (exa breaker open, skipped)
	hasBrave := false
	hasExa := false
	for _, r := range out.Results {
		if r.Engine == "brave" {
			hasBrave = true
		}
		if r.Engine == "exa" {
			hasExa = true
		}
	}
	if !hasBrave {
		t.Error("expected brave results (circuit breaker should skip exa, not brave)")
	}
	if hasExa {
		t.Error("exa should be skipped (circuit breaker open)")
	}
}

// TestSearchAllFail — SearXNG 0 results, all premiums fail → error.
func TestSearchAllFail(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: nil}}
	fb := &fakeBackend{name: "brave", err: errors.New("upstream 500"), avail: true}
	c, _ := cache.New(100)
	p := newTestProxy(newCfg(), sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "x"); err == nil {
		t.Error("Search expected error when both SearXNG and all premiums fail")
	}
}

// TestSearchT1Premium — T1_PREMIUM_PROVIDERS=["brave"], brave called in parallel.
func TestSearchT1Premium(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	cfg.T1PremiumProviders = []string{"brave"}
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	// Should have both SearXNG + brave results
	hasSX := false
	hasBR := false
	for _, r := range out.Results {
		if r.Engine == "wikipedia" {
			hasSX = true
		}
		if r.Engine == "brave" {
			hasBR = true
		}
	}
	if !hasSX || !hasBR {
		t.Errorf("expected both SearXNG and brave results, got: hasSX=%v hasBR=%v", hasSX, hasBR)
	}
}

// TestSearchT1PremiumNone — T1_PREMIUM_PROVIDERS empty → no premium in hot path.
func TestSearchT1PremiumNone(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	// brave exists but T1_PREMIUM_PROVIDERS is empty → should NOT be called
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "SHOULD NOT APPEAR", URL: "https://nope.com", Engine: "brave"},
	}}
	c, _ := cache.New(100)
	cfg := newCfg()
	cfg.SufficientMinResults = 1 // SearXNG returns 1 result — sufficient, no fallback loop
	cfg.T1PremiumProviders = []string{} // empty = no T1 premium
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	// Should only have SearXNG results
	for _, r := range out.Results {
		if r.Engine == "brave" {
			t.Errorf("brave should NOT be called when T1_PREMIUM_PROVIDERS is empty")
		}
	}
}
