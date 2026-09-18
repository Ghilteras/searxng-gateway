package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
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

func (f *fakeBackend) Name() string      { return f.name }
func (f *fakeBackend) IsAvailable() bool { return f.avail }
func (f *fakeBackend) Search(_ backends.SearchOptions) ([]backends.SearchResult, error) {
	return f.results, f.err
}

// --- Helpers ---

func newCfg() *config.Config {
	return &config.Config{
		SearxngBackendURL:    "http://searxng-primary:8080",
		FallbackTimeout:      30 * time.Second,
		CacheTTL:             time.Hour,
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  180 * time.Second,
		SufficientMinResults: 10,
		FallbackProviders:    []string{"brave", "exa"},
		T1PremiumCount:       0,
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
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
	c, _ := cache.New(100, 0)
	p := newTestProxy(newCfg(), sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "x"); err == nil {
		t.Error("Search expected error when both SearXNG and all premiums fail")
	}
}

// TestSearchT1Premium — T1_PREMIUM_COUNT=1, 1 premium serially within the T1 pass, alongside SearXNG.
func TestSearchT1Premium(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	cfg.T1PremiumCount = 1
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
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

// TestSearchT1PremiumTwo — T1_PREMIUM_COUNT=2, 2 premiums serially within the T1 pass, alongside SearXNG.
func TestSearchT1PremiumTwo(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	fb1 := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	fb2 := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{
		{Title: "EX", URL: "https://ex.com", Content: "d", Engine: "exa"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	cfg.FallbackProviders = []string{"brave", "exa"}
	cfg.T1PremiumCount = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), fb1, fb2)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	hasSX := false
	hasBR := false
	hasEX := false
	for _, r := range out.Results {
		if r.Engine == "wikipedia" {
			hasSX = true
		}
		if r.Engine == "brave" {
			hasBR = true
		}
		if r.Engine == "exa" {
			hasEX = true
		}
	}
	if !hasSX || !hasBR || !hasEX {
		t.Errorf("expected all 3 engines, got: hasSX=%v hasBR=%v hasEX=%v", hasSX, hasBR, hasEX)
	}
}

// TestSearchT1PremiumNone — T1_PREMIUM_COUNT=0 → no premium in hot path.
func TestSearchT1PremiumNone(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "SHOULD NOT APPEAR", URL: "https://nope.com", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 1
	cfg.T1PremiumCount = 0
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	for _, r := range out.Results {
		if r.Engine == "brave" {
			t.Errorf("brave should NOT be called when T1_PREMIUM_COUNT=0")
		}
	}
}

func TestIsClientError(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   bool
	}{
		{"402 jina", "jina backend: HTTP 402: InsufficientBalanceError: Account balance not enough", true},
		{"429 rate limit", "brave backend: HTTP 429: too many requests", true},
		{"403 forbidden", "bing backend: HTTP 403: Forbidden", true},
		{"payment required lowercase", "account balance not enough, payment required", true},
		{"payment required capitalized", "Account balance not enough, Payment Required", true},
		{"500 server error", "backend: HTTP 500: internal server error", false},
		{"timeout", "context deadline exceeded", false},
		{"connection refused", "dial tcp: connection refused", false},
		{"empty", "", false},
		{"generic", "some random message", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isClientError(tt.reason); got != tt.want {
				t.Errorf("isClientError(%q) = %v, want %v", tt.reason, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Regression: outcome metric labels must reflect actual contribution
// ---------------------------------------------------------------------------

// outcomeCounter reads the current value of searxng_gateway_requests_total
// for the given outcome label.
func outcomeCounter(t *testing.T, outcome string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "searxng_gateway_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "outcome" && l.GetValue() == outcome {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestOutcomeLabels_SearxngErrorPremiumOnly — SearXNG errors, premium
// returns results → outcome must be "premium_ok", NOT "searxng_plus_premium_ok".
// Regression guard: the old code checked !sxSkipped (cooldown flag) instead of
// whether SearXNG actually produced results.
func TestOutcomeLabels_SearxngErrorPremiumOnly(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{err: errors.New("boom")}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR_A", URL: "https://brave-a.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	p := newTestProxy(newCfg(), sx, c, breaker.New(), fb)

	beforePremium := outcomeCounter(t, "premium_ok")
	beforeSXPlus := outcomeCounter(t, "searxng_plus_premium_ok")

	_, err := p.Search(context.Background(), "outcome_case_a_err")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	afterPremium := outcomeCounter(t, "premium_ok")
	afterSXPlus := outcomeCounter(t, "searxng_plus_premium_ok")

	if delta := afterPremium - beforePremium; delta != 1 {
		t.Errorf("premium_ok delta = %v, want 1", delta)
	}
	if delta := afterSXPlus - beforeSXPlus; delta != 0 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 0", delta)
	}
}

// TestOutcomeLabels_SearxngEmptyPremiumOnly — SearXNG returns empty result
// set, premium returns results → outcome must be "premium_ok".
func TestOutcomeLabels_SearxngEmptyPremiumOnly(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: nil}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR_B", URL: "https://brave-b.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	p := newTestProxy(newCfg(), sx, c, breaker.New(), fb)

	beforePremium := outcomeCounter(t, "premium_ok")
	beforeSXPlus := outcomeCounter(t, "searxng_plus_premium_ok")

	_, err := p.Search(context.Background(), "outcome_case_b_empty")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	afterPremium := outcomeCounter(t, "premium_ok")
	afterSXPlus := outcomeCounter(t, "searxng_plus_premium_ok")

	if delta := afterPremium - beforePremium; delta != 1 {
		t.Errorf("premium_ok delta = %v, want 1", delta)
	}
	if delta := afterSXPlus - beforeSXPlus; delta != 0 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 0", delta)
	}
}

// TestOutcomeLabels_SearxngSufficientOnly — SearXNG returns >= SufficientMinResults
// and no premium contributes → outcome must be "searxng_ok".
func TestOutcomeLabels_SearxngSufficientOnly(t *testing.T) {
	metrics.Init()
	sxRes := make([]searxng.Result, 10)
	for i := range sxRes {
		sxRes[i] = searxng.Result{
			Title:  fmt.Sprintf("SX%d", i),
			URL:    fmt.Sprintf("https://sx%d-c.com", i),
			Engine: "wikipedia",
		}
	}
	sx := &fakeSearxng{resp: &searxng.Response{Results: sxRes}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.T1PremiumCount = 0 // no premium in hot path
	p := newTestProxy(cfg, sx, c, breaker.New())

	beforeOK := outcomeCounter(t, "searxng_ok")

	_, err := p.Search(context.Background(), "outcome_case_c_sxonly")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	afterOK := outcomeCounter(t, "searxng_ok")

	if delta := afterOK - beforeOK; delta != 1 {
		t.Errorf("searxng_ok delta = %v, want 1", delta)
	}
}

// TestOutcomeLabels_PremiumDuplicatesSearxng — SearXNG returns results;
// the premium backend returns ONLY URLs that SearXNG already returned (pure
// duplicates). Premium contributed nothing new → outcome must be "searxng_ok".
func TestOutcomeLabels_PremiumDuplicatesSearxng(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX_D1", URL: "https://dup-shared-a.com", Engine: "wikipedia"},
		{Title: "SX_D2", URL: "https://dup-shared-b.com", Engine: "wikipedia"},
		{Title: "SX_D3", URL: "https://dup-shared-c.com", Engine: "wikipedia"},
	}}}
	// Premium returns ONLY URLs that SearXNG already has.
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR_D1", URL: "https://dup-shared-a.com", Content: "d", Engine: "brave"},
		{Title: "BR_D2", URL: "https://dup-shared-b.com", Content: "d", Engine: "brave"},
		{Title: "BR_D3", URL: "https://dup-shared-c.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 10 // force fallback loop to run
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)

	beforeSxOk := outcomeCounter(t, "searxng_ok")
	beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")
	beforePremOk := outcomeCounter(t, "premium_ok")

	_, err := p.Search(context.Background(), "outcome_dup_sx")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	afterSxOk := outcomeCounter(t, "searxng_ok")
	afterSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")
	afterPremOk := outcomeCounter(t, "premium_ok")

	if delta := afterSxOk - beforeSxOk; delta != 1 {
		t.Errorf("searxng_ok delta = %v, want 1 (only SearXNG contributed)", delta)
	}
	if delta := afterSxPlus - beforeSxPlus; delta != 0 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 0 (premium contributed nothing new)", delta)
	}
	if delta := afterPremOk - beforePremOk; delta != 0 {
		t.Errorf("premium_ok delta = %v, want 0", delta)
	}
}

// TestOutcomeLabels_SearxngDuplicatesPremium — the premium backend returns
// results and SearXNG returns ONLY those same URLs. Premium appended them
// first (T1 path); SearXNG added nothing new → outcome must be "premium_ok".
func TestOutcomeLabels_SearxngDuplicatesPremium(t *testing.T) {
	metrics.Init()
	// SearXNG returns URLs that are all duplicates of premium results.
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX_DUP1", URL: "https://dup-prem-a.com", Engine: "wikipedia"},
		{Title: "SX_DUP2", URL: "https://dup-prem-b.com", Engine: "wikipedia"},
		{Title: "SX_DUP3", URL: "https://dup-prem-c.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR_DUP1", URL: "https://dup-prem-a.com", Content: "d", Engine: "brave"},
		{Title: "BR_DUP2", URL: "https://dup-prem-b.com", Content: "d", Engine: "brave"},
		{Title: "BR_DUP3", URL: "https://dup-prem-c.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 10 // force fallback loop
	cfg.T1PremiumCount = 1        // premium runs in T1, before SearXNG collection
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)

	beforePremOk := outcomeCounter(t, "premium_ok")
	beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")
	beforeSxOk := outcomeCounter(t, "searxng_ok")

	_, err := p.Search(context.Background(), "outcome_dup_prem")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	afterPremOk := outcomeCounter(t, "premium_ok")
	afterSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")
	afterSxOk := outcomeCounter(t, "searxng_ok")

	if delta := afterPremOk - beforePremOk; delta != 1 {
		t.Errorf("premium_ok delta = %v, want 1 (premium contributed)", delta)
	}
	if delta := afterSxPlus - beforeSxPlus; delta != 0 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 0 (SearXNG added nothing new)", delta)
	}
	if delta := afterSxOk - beforeSxOk; delta != 0 {
		t.Errorf("searxng_ok delta = %v, want 0", delta)
	}
}

// ---------------------------------------------------------------------------
// Regression: serial premium pass — no concurrency and deterministic labels
// ---------------------------------------------------------------------------

// overlapBackend wraps results with overlap detection: on entry atomically
// increments in-flight, records the max observed, sleeps to widen any race
// window, then decrements. Also records call order.
type overlapBackend struct {
	name      string
	results   []backends.SearchResult
	inFlight  *atomic.Int64
	maxSeen   *atomic.Int64
	callOrder *[]string
	mu        *sync.Mutex
}

func (o *overlapBackend) Name() string      { return o.name }
func (o *overlapBackend) IsAvailable() bool { return true }
func (o *overlapBackend) Search(_ backends.SearchOptions) ([]backends.SearchResult, error) {
	cur := o.inFlight.Add(1)
	// Update max observed overlap.
	for {
		old := o.maxSeen.Load()
		if cur <= old || o.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	o.mu.Lock()
	*o.callOrder = append(*o.callOrder, o.name)
	o.mu.Unlock()
	o.inFlight.Add(-1)
	return o.results, nil
}

// TestT1PremiumPass_SerialNoOverlap verifies the T1 hot-path loop calls
// providers serially (max overlap == 1) and in round-robin selection order.
func TestT1PremiumPass_SerialNoOverlap(t *testing.T) {
	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	var mu sync.Mutex
	order := make([]string, 0, 2)

	brave := &overlapBackend{
		name:      "brave",
		results:   []backends.SearchResult{{Title: "B", URL: "https://b.com", Content: "d", Engine: "brave"}},
		inFlight:  &inFlight,
		maxSeen:   &maxSeen,
		callOrder: &order,
		mu:        &mu,
	}
	exa := &overlapBackend{
		name:      "exa",
		results:   []backends.SearchResult{{Title: "E", URL: "https://e.com", Content: "d", Engine: "exa"}},
		inFlight:  &inFlight,
		maxSeen:   &maxSeen,
		callOrder: &order,
		mu:        &mu,
	}

	// SearXNG returns 1 result so the fallback loop never runs.
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx.com", Engine: "wikipedia"},
	}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.T1PremiumCount = 2
	cfg.SufficientMinResults = 1
	cfg.FallbackProviders = []string{"brave", "exa"}

	// Register overlap backends into a fresh manager via newTestProxy-like construction.
	mgr := backends.NewManager()
	mgr.Register(brave)
	mgr.Register(exa)
	_ = mgr.SetFallbacks(cfg.FallbackProviders)
	p := New(cfg, sx, c, breaker.New(), mgr)

	_, err := p.Search(context.Background(), "t1_serial_overlap")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("max observed overlap = %d, want 1 (serial execution)", got)
	}
	// Round-robin with alphabetical sort: brave (idx 0), then exa (idx 1).
	mu.Lock()
	gotOrder := make([]string, len(order))
	copy(gotOrder, order)
	mu.Unlock()
	wantOrder := []string{"brave", "exa"}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("call order len = %d, want %d: got %v", len(gotOrder), len(wantOrder), gotOrder)
	}
	for i, w := range wantOrder {
		if gotOrder[i] != w {
			t.Errorf("call order[%d] = %q, want %q (got %v)", i, gotOrder[i], w, gotOrder)
		}
	}
}

// TestFallbackPremiumLoop_SerialNoOverlapAndEarlyStop verifies the fallback
// loop calls providers serially (max overlap == 1) and stops the moment the
// SufficientMinResults threshold is met — no speculative extra calls.
func TestFallbackPremiumLoop_SerialNoOverlapAndEarlyStop(t *testing.T) {
	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	var mu sync.Mutex
	order := make([]string, 0, 3)

	mkOverlap := func(name, url string) *overlapBackend {
		return &overlapBackend{
			name:      name,
			results:   []backends.SearchResult{{Title: name, URL: url, Content: "d", Engine: name}},
			inFlight:  &inFlight,
			maxSeen:   &maxSeen,
			callOrder: &order,
			mu:        &mu,
		}
	}
	brave := mkOverlap("brave", "https://b.com")
	exa := mkOverlap("exa", "https://e.com")
	jina := mkOverlap("jina", "https://j.com")

	// SearXNG returns 0 → fallback loop must run.
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	cfg.FallbackProviders = []string{"brave", "exa", "jina"}

	mgr := backends.NewManager()
	mgr.Register(brave)
	mgr.Register(exa)
	mgr.Register(jina)
	_ = mgr.SetFallbacks(cfg.FallbackProviders)
	p := New(cfg, sx, c, breaker.New(), mgr)

	_, err := p.Search(context.Background(), "fallback_serial_earlystop")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("max observed overlap = %d, want 1 (serial fallback loop)", got)
	}
	mu.Lock()
	gotOrder := make([]string, len(order))
	copy(gotOrder, order)
	mu.Unlock()
	// brave (1 result) → 1 < 2, continue; next provider (1 result) → 2 >= 2, stop.
	// Exactly 2 calls — no speculative extra calls.
	if len(gotOrder) != 2 {
		t.Fatalf("call count = %d, want 2 (early stop): got order %v", len(gotOrder), gotOrder)
	}
	// First call must be brave (alphabetically first in full set).
	if gotOrder[0] != "brave" {
		t.Errorf("first call = %q, want brave", gotOrder[0])
	}
}

// TestOutcomeLabels_DeterministicAcrossRepeats verifies that outcome metric
// labels are deterministic across repeated identical-input calls. For each
// scenario, 25 fresh iterations are run with a unique query and fresh
// cache+proxy; every iteration must produce exactly the expected outcome delta
// and the result URL order must be identical across all iterations.
func TestOutcomeLabels_DeterministicAcrossRepeats(t *testing.T) {
	metrics.Init()
	const iterations = 25

	// Shared URL set for both SearXNG and premium.
	urls := []searxng.Result{
		{Title: "D1", URL: "https://det-a.com", Engine: "wikipedia"},
		{Title: "D2", URL: "https://det-b.com", Engine: "wikipedia"},
		{Title: "D3", URL: "https://det-c.com", Engine: "wikipedia"},
	}

	// --- (i) T1 path: T1PremiumCount=1, premium and SearXNG return SAME URLs ---
	// Premium runs first (T1 loop), adds URLs. SearXNG adds nothing new.
	// outcome must be "premium_ok" every time.
	var firstURLs []string
	for i := 0; i < iterations; i++ {
		sx := &fakeSearxng{resp: &searxng.Response{Results: urls}}
		fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
			{Title: "D1", URL: "https://det-a.com", Content: "d", Engine: "brave"},
			{Title: "D2", URL: "https://det-b.com", Content: "d", Engine: "brave"},
			{Title: "D3", URL: "https://det-c.com", Content: "d", Engine: "brave"},
		}}
		c, _ := cache.New(100, 0)
		cfg := newCfg()
		cfg.T1PremiumCount = 1
		cfg.SufficientMinResults = 3
		p := newTestProxy(cfg, sx, c, breaker.New(), fb)

		beforePrem := outcomeCounter(t, "premium_ok")
		beforeSxOk := outcomeCounter(t, "searxng_ok")
		beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		query := fmt.Sprintf("det_t1_iter_%d_%d", time.Now().UnixNano(), i)
		resp, err := p.Search(context.Background(), query)
		if err != nil {
			t.Fatalf("T1 iter %d: Search error = %v", i, err)
		}

		afterPrem := outcomeCounter(t, "premium_ok")
		afterSxOk := outcomeCounter(t, "searxng_ok")
		afterSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		if delta := afterPrem - beforePrem; delta != 1 {
			t.Errorf("T1 iter %d: premium_ok delta = %v, want 1", i, delta)
		}
		if delta := afterSxOk - beforeSxOk; delta != 0 {
			t.Errorf("T1 iter %d: searxng_ok delta = %v, want 0", i, delta)
		}
		if delta := afterSxPlus - beforeSxPlus; delta != 0 {
			t.Errorf("T1 iter %d: searxng_plus_premium_ok delta = %v, want 0", i, delta)
		}

		// Record URL order for determinism check.
		iterURLs := make([]string, len(resp.Results))
		for j, r := range resp.Results {
			iterURLs[j] = r.URL
		}
		if i == 0 {
			firstURLs = iterURLs
		} else {
			if len(iterURLs) != len(firstURLs) {
				t.Errorf("T1 iter %d: URL count = %d, want %d", i, len(iterURLs), len(firstURLs))
			} else {
				for j := range firstURLs {
					if iterURLs[j] != firstURLs[j] {
						t.Errorf("T1 iter %d: URL[%d] = %q, want %q (nondeterministic)", i, j, iterURLs[j], firstURLs[j])
						break
					}
				}
			}
		}
	}

	// --- (ii) Fallback path: T1PremiumCount=0, premium merges AFTER SearXNG ---
	// SearXNG returns URLs, premium returns same URLs → all filtered → searxng_ok.
	var firstURLsFB []string
	for i := 0; i < iterations; i++ {
		sx := &fakeSearxng{resp: &searxng.Response{Results: urls}}
		fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
			{Title: "D1", URL: "https://det-a.com", Content: "d", Engine: "brave"},
			{Title: "D2", URL: "https://det-b.com", Content: "d", Engine: "brave"},
			{Title: "D3", URL: "https://det-c.com", Content: "d", Engine: "brave"},
		}}
		c, _ := cache.New(100, 0)
		cfg := newCfg()
		cfg.T1PremiumCount = 0        // no T1 — fallback loop only
		cfg.SufficientMinResults = 10 // force fallback loop
		p := newTestProxy(cfg, sx, c, breaker.New(), fb)

		beforeSxOk := outcomeCounter(t, "searxng_ok")
		beforePrem := outcomeCounter(t, "premium_ok")
		beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		query := fmt.Sprintf("det_fb_iter_%d_%d", time.Now().UnixNano(), i)
		resp, err := p.Search(context.Background(), query)
		if err != nil {
			t.Fatalf("FB iter %d: Search error = %v", i, err)
		}

		afterSxOk := outcomeCounter(t, "searxng_ok")
		afterPrem := outcomeCounter(t, "premium_ok")
		afterSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		if delta := afterSxOk - beforeSxOk; delta != 1 {
			t.Errorf("FB iter %d: searxng_ok delta = %v, want 1", i, delta)
		}
		if delta := afterPrem - beforePrem; delta != 0 {
			t.Errorf("FB iter %d: premium_ok delta = %v, want 0", i, delta)
		}
		if delta := afterSxPlus - beforeSxPlus; delta != 0 {
			t.Errorf("FB iter %d: searxng_plus_premium_ok delta = %v, want 0", i, delta)
		}

		// Record URL order for determinism check.
		iterURLs := make([]string, len(resp.Results))
		for j, r := range resp.Results {
			iterURLs[j] = r.URL
		}
		if i == 0 {
			firstURLsFB = iterURLs
		} else {
			if len(iterURLs) != len(firstURLsFB) {
				t.Errorf("FB iter %d: URL count = %d, want %d", i, len(iterURLs), len(firstURLsFB))
			} else {
				for j := range firstURLsFB {
					if iterURLs[j] != firstURLsFB[j] {
						t.Errorf("FB iter %d: URL[%d] = %q, want %q (nondeterministic)", i, j, iterURLs[j], firstURLsFB[j])
						break
					}
				}
			}
		}
	}
}
