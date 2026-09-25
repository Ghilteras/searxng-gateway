package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

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

type contextBackend struct {
	name string
	fn   func(context.Context) ([]backends.SearchResult, error)
}

func (b *contextBackend) Name() string      { return b.name }
func (b *contextBackend) IsAvailable() bool { return true }
func (b *contextBackend) Search(opts backends.SearchOptions) ([]backends.SearchResult, error) {
	return b.fn(opts.Context)
}

type contextSearxng struct {
	fn func(context.Context) (*searxng.Response, error)
}

func (s *contextSearxng) Search(ctx context.Context, _ string) (*searxng.Response, error) {
	return s.fn(ctx)
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
		SearxngTimeout:       time.Second,
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

func TestSearchReturnsT1PartialWhileSearxngChildBudgetExpires(t *testing.T) {
	metrics.Init()
	beforeTimeout := outcomeCounter(t, "timeout")
	beforePremium := outcomeCounter(t, "premium_ok")
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 500 * time.Millisecond
	cfg.SearxngTimeout = 40 * time.Millisecond
	cfg.SufficientMinResults = 10
	cfg.T1PremiumCount = 1
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "partial", URL: "https://partial.test", Engine: "brave"}}}
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	start := time.Now()
	out, err := p.Search(context.Background(), "partial-budget")
	if err != nil || len(out.Results) != 1 {
		t.Fatalf("response=%v err=%v", out, err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("search took %v", elapsed)
	}
	if got := outcomeCounter(t, "timeout") - beforeTimeout; got != 0 {
		t.Errorf("request timeout outcome delta = %v, want 0 for successful premium fallback", got)
	}
	if got := outcomeCounter(t, "premium_ok") - beforePremium; got != 1 {
		t.Errorf("premium_ok outcome delta = %v, want 1", got)
	}
}

func TestSearchFallsThroughAfterSearxngChildBudget(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 500 * time.Millisecond
	cfg.SearxngTimeout = 40 * time.Millisecond
	cfg.SufficientMinResults = 1
	cfg.T1PremiumCount = 1
	cfg.FallbackProviders = []string{"brave", "exa"}
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	fail := &fakeBackend{name: "brave", avail: true, err: errors.New("upstream failure")}
	second := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "secondary", URL: "https://secondary.test", Engine: "exa"}}}
	p := newTestProxy(cfg, sx, c, breaker.New(), fail, second)
	out, err := p.Search(context.Background(), "secondary-budget")
	if err != nil || len(out.Results) != 1 || out.Results[0].Engine != "exa" {
		t.Fatalf("response=%v err=%v", out, err)
	}
}

func TestSearchDeadlineCancelsPremiumAndReturnsPartialOrError(t *testing.T) {
	metrics.Init()
	for _, withResults := range []bool{true, false} {
		t.Run(fmt.Sprintf("results_%v", withResults), func(t *testing.T) {
			beforeTimeout := outcomeCounter(t, "timeout")
			beforePremium := outcomeCounter(t, "premium_ok")
			c, _ := cache.New(100, 0)
			cfg := newCfg()
			cfg.FallbackTimeout = 50 * time.Millisecond
			cfg.SearxngTimeout = 10 * time.Millisecond
			cfg.T1PremiumCount = 1
			cfg.SufficientMinResults = 10
			started := make(chan struct{})
			canceled := make(chan struct{})
			fb := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}}
			sx := &fakeSearxng{resp: &searxng.Response{}}
			if withResults {
				fb.fn = func(ctx context.Context) ([]backends.SearchResult, error) {
					close(started)
					<-ctx.Done()
					close(canceled)
					return []backends.SearchResult{{Title: "kept", URL: "https://kept.test", Engine: "brave"}}, nil
				}
			}
			p := newTestProxy(cfg, sx, c, breaker.New(), fb)
			out, err := p.Search(context.Background(), "premium-deadline")
			<-started
			<-canceled
			if withResults {
				if err != nil || len(out.Results) != 1 {
					t.Fatalf("response=%v err=%v", out, err)
				}
				if got := outcomeCounter(t, "timeout") - beforeTimeout; got != 0 {
					t.Errorf("request timeout outcome delta = %v, want 0 for a partial response", got)
				}
				if got := outcomeCounter(t, "premium_ok") - beforePremium; got != 1 {
					t.Errorf("premium_ok outcome delta = %v, want 1", got)
				}
			} else if err == nil {
				t.Fatal("expected error with no accumulated results")
			} else if got := outcomeCounter(t, "timeout") - beforeTimeout; got != 1 {
				t.Errorf("request timeout outcome delta = %v, want 1", got)
			}
		})
	}
}

func TestSearchDoesNotStartPremiumAfterDeadline(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 40 * time.Millisecond
	cfg.SearxngTimeout = 10 * time.Millisecond
	cfg.T1PremiumCount = 2
	cfg.FallbackProviders = []string{"brave", "exa"}
	var secondCalls atomic.Int64
	first := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) { <-ctx.Done(); return nil, ctx.Err() }}
	second := &contextBackend{name: "exa", fn: func(context.Context) ([]backends.SearchResult, error) { secondCalls.Add(1); return nil, nil }}
	p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, breaker.New(), first, second)
	if _, err := p.Search(context.Background(), "no-late-provider"); err == nil {
		t.Fatal("expected error with no results")
	}
	if got := secondCalls.Load(); got != 0 {
		t.Fatalf("second provider started %d times after deadline", got)
	}
}

func TestCallerCancellationStopsProviderWork(t *testing.T) {
	metrics.Init()
	before := outcomeCounter(t, "fallback_fail")
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = time.Second
	cfg.SearxngTimeout = time.Second
	cfg.T1PremiumCount = 2
	cfg.FallbackProviders = []string{"brave", "exa"}
	started := make(chan struct{})
	first := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	var secondCalls atomic.Int64
	second := &contextBackend{name: "exa", fn: func(context.Context) ([]backends.SearchResult, error) { secondCalls.Add(1); return nil, nil }}
	var sxCalls atomic.Int64
	sxCanceled := make(chan struct{})
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) {
		sxCalls.Add(1)
		<-ctx.Done()
		close(sxCanceled)
		return nil, ctx.Err()
	}}
	p := newTestProxy(cfg, sx, c, breaker.New(), first, second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := p.Search(ctx, "cancel-provider"); done <- err }()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected canceled request error")
	}
	if sxCalls.Load() > 0 {
		select {
		case <-sxCanceled:
		default:
			t.Fatal("in-flight SearXNG work did not observe cancellation")
		}
	}
	if got := secondCalls.Load(); got != 0 {
		t.Fatalf("second provider started after cancellation: %d", got)
	}
	if p.inCooldown() {
		t.Fatal("caller cancellation tripped SearXNG cooldown")
	}
	if got := outcomeCounter(t, "fallback_fail") - before; got != 1 {
		t.Fatalf("request outcome count delta = %v, want exactly one fallback_fail", got)
	}
}

func TestTracingSpanTreeAndSanitization(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) })

	const querySecret = "QUERY-SENTINEL-73d9"
	const titleSecret = "TITLE-SENTINEL-4c2a"
	const bodySecret = "BODY-SECRET-552a"
	var attempts atomic.Int64
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("secret error body")
		}
		return &searxng.Response{Results: []searxng.Result{{Title: titleSecret, URL: "https://private.example/path?q=hidden", Content: bodySecret, Engine: "engine"}}}, nil
	}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "fallback", URL: "https://fallback.example", Engine: "brave"}}}
	fb2 := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "fallback2", URL: "https://fallback2.example", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.T1PremiumCount = 1
	cfg.SufficientMinResults = 5
	cfg.SearxngTimeout = 5 * time.Second
	cfg.FallbackProviders = []string{"brave", "exa"}
	p := newTestProxy(cfg, sx, c, breaker.New(), fb, fb2)
	ctx, request := otel.Tracer("test").Start(context.Background(), "http.server /search")
	_, err := p.Search(ctx, querySecret)
	request.End()
	if err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	byName := make(map[string]tracetest.SpanStub)
	for _, s := range spans {
		byName[s.Name] = s
	}
	proxySpan, ok := byName["proxy.search"]
	if !ok {
		t.Fatalf("missing proxy span; got %v", spanNames(spans))
	}
	stage, ok := byName["searxng.stage"]
	if !ok {
		t.Fatalf("missing SearXNG stage; got %v", spanNames(spans))
	}
	prem, ok := byName["premium.t1"]
	if !ok {
		t.Fatalf("missing T1 provider span; got %v", spanNames(spans))
	}
	if stage.Parent.SpanID() != proxySpan.SpanContext.SpanID() || prem.Parent.SpanID() != proxySpan.SpanContext.SpanID() {
		t.Fatalf("SearXNG stage and T1 must be siblings under proxy: stage parent=%s t1 parent=%s proxy=%s", stage.Parent.SpanID(), prem.Parent.SpanID(), proxySpan.SpanContext.SpanID())
	}
	foundAttempt, foundBackoff := false, false
	for i := range spans {
		if spans[i].Name != "searxng.attempt" {
			continue
		}
		foundAttempt = true
		if spans[i].Parent.SpanID() != stage.SpanContext.SpanID() {
			t.Fatal("retry attempt is not child of SearXNG stage")
		}
		for _, event := range spans[i].Events {
			if event.Name == "retry.backoff" {
				foundBackoff = true
			}
		}
	}
	if !foundAttempt {
		t.Fatal("retry attempt span missing")
	}
	fallbackLoop, ok := byName["fallback.loop"]
	if !ok {
		t.Fatalf("missing fallback loop span; got %v", spanNames(spans))
	}
	fallbackProvider, ok := byName["premium.fallback"]
	if !ok || fallbackProvider.Parent.SpanID() != fallbackLoop.SpanContext.SpanID() {
		t.Fatal("fallback provider span is not a child of fallback loop")
	}
	if _, ok := byName["searxng.result_wait"]; !ok {
		t.Fatal("missing SearXNG result wait span")
	}
	if !foundBackoff {
		t.Fatal("retry backoff event missing")
	}
	for _, s := range spans {
		for _, a := range s.Attributes {
			if strings.Contains(a.Value.AsString(), "SENTINEL") || strings.Contains(a.Value.AsString(), "SECRET") || strings.Contains(a.Value.AsString(), "private.example") {
				t.Fatalf("sensitive attr leaked in %s: %v", s.Name, a)
			}
		}
		for _, e := range s.Events {
			for _, a := range e.Attributes {
				if strings.Contains(a.Value.AsString(), "SENTINEL") || strings.Contains(a.Value.AsString(), "SECRET") {
					t.Fatalf("sensitive event leaked in %s: %v", s.Name, a)
				}
			}
		}
	}
}

func spanNames(spans []tracetest.SpanStub) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

func TestSearxngChildTimeoutDoesNotTripCooldown(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SearxngTimeout = 15 * time.Millisecond
	cfg.FallbackTimeout = time.Second
	cfg.SearxngFailThreshold = 1
	cfg.T1PremiumCount = 1
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	fb := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
		return []backends.SearchResult{{URL: "https://partial", Engine: "brave"}}, nil
	}}
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "child-timeout"); err != nil {
		t.Fatal(err)
	}
	if p.inCooldown() {
		t.Fatal("intentional SearXNG child timeout tripped cooldown")
	}
}

func TestSearxngGenuineFailureTripsCooldown(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SearxngFailThreshold = 1
	cfg.SearxngTimeout = 80 * time.Millisecond
	cfg.SufficientMinResults = 1
	p := newTestProxy(cfg, &fakeSearxng{err: errors.New("upstream 503")}, c, breaker.New())
	_, _ = p.Search(context.Background(), "real-error")
	if !p.inCooldown() {
		t.Fatal("genuine SearXNG failure did not trip cooldown")
	}
}

func TestTracingCacheErrorAndPartialResults(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) })

	c, _ := cache.New(100, 0)
	c.Set("cached", &searxng.Response{Results: []searxng.Result{{URL: "https://cached"}}})
	cfg := newCfg()
	cfg.FallbackTimeout = 200 * time.Millisecond
	cfg.SearxngTimeout = 20 * time.Millisecond
	cfg.T1PremiumCount = 1
	cfg.SufficientMinResults = 10
	partialBackend := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		return []backends.SearchResult{{URL: "https://partial", Engine: "brave"}}, nil
	}}
	p := newTestProxy(cfg, &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}, c, breaker.New(), partialBackend)
	if _, err := p.Search(context.Background(), "cached"); err != nil {
		t.Fatal(err)
	}
	result, err := p.Search(context.Background(), "partial")
	if err != nil || len(result.Results) != 1 {
		t.Fatalf("partial response=%v err=%v", result, err)
	}
	// Cooldown makes the failing request deterministic without retry delay.
	failing := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) { return nil, errors.New("ERROR-SECRET") }}
	pErr := newTestProxy(cfg, &fakeSearxng{}, c, breaker.New(), failing)
	pErr.sxCooldownTil.Store(time.Now().Add(time.Minute).UnixNano())
	if _, err := pErr.Search(context.Background(), "error-path"); err == nil {
		t.Fatal("expected provider failure")
	}
	spans := exporter.GetSpans()
	cacheEvent, sawError, sawPartial := false, false, false
	for _, s := range spans {
		if s.Name == "proxy.search" && s.Status.Code == codes.Error {
			sawError = true
		}
		for _, event := range s.Events {
			if event.Name == "cache.hit" {
				cacheEvent = true
			}
		}
		for _, attr := range s.Attributes {
			if strings.Contains(attr.Value.AsString(), "ERROR-SECRET") {
				t.Fatalf("raw error leaked into span %s", s.Name)
			}
		}
		if s.Name == "premium.t1" {
			for _, attr := range s.Attributes {
				if attr.Key == "result_count" && attr.Value.AsInt64() == 1 {
					sawPartial = true
				}
			}
		}
	}
	if !cacheEvent || !sawError || !sawPartial {
		t.Fatalf("trace paths missing cache=%v error=%v partial=%v; spans=%v", cacheEvent, sawError, sawPartial, spanNames(spans))
	}
}

func TestLatencyHistogramsUseTruthfulStageAndProviderSamples(t *testing.T) {
	metrics.Init()
	beforeStage := histogramSampleCount(t, "searxng_gateway_searxng_stage_duration_seconds")
	beforeProvider := histogramSampleCount(t, "searxng_gateway_provider_duration_seconds")
	c, _ := cache.New(100, 0)
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{URL: "https://a", Engine: "engine-a"}, {URL: "https://b", Engine: "engine-b"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{URL: "https://c", Engine: "brave"}}}
	cfg := newCfg()
	cfg.T1PremiumCount = 1
	cfg.SufficientMinResults = 10
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "metric-contract"); err != nil {
		t.Fatal(err)
	}
	if got := histogramSampleCount(t, "searxng_gateway_searxng_stage_duration_seconds") - beforeStage; got != 1 {
		t.Fatalf("stage samples = %d, want 1 independent of 2 engines", got)
	}
	if got := histogramSampleCount(t, "searxng_gateway_provider_duration_seconds") - beforeProvider; got != 1 {
		t.Fatalf("provider samples = %d, want one actual premium call", got)
	}
	if got := histogramSampleCount(t, "searxng_gateway_request_duration_seconds"); got != 0 {
		t.Fatalf("legacy engine-labelled request histogram has %d samples", got)
	}
	for _, mf := range gatherMetricFamily(t, "searxng_gateway_provider_duration_seconds") {
		for _, metric := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range metric.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if _, exists := labels["engine"]; exists {
				t.Fatalf("provider histogram falsely has engine label: %v", labels)
			}
		}
	}
	if got := histogramSampleCountForLabels(t, "searxng_gateway_provider_duration_seconds", map[string]string{"provider": "brave", "phase": "t1"}); got < 1 {
		t.Fatalf("missing truthful T1 provider duration series: %d", got)
	}
}

func TestSearxngStageHistogramRecordsFailureAndSkipsCooldown(t *testing.T) {
	metrics.Init()
	before := histogramSampleCount(t, "searxng_gateway_searxng_stage_duration_seconds")
	cfg := newCfg()
	cfg.SearxngTimeout = 12 * time.Millisecond
	cfg.FallbackTimeout = time.Second
	cfg.SufficientMinResults = 10
	c, _ := cache.New(100, 0)
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { return nil, errors.New("upstream failed") }}
	p := newTestProxy(cfg, sx, c, breaker.New())
	_, _ = p.Search(context.Background(), "stage-failure")
	if got := histogramSampleCount(t, "searxng_gateway_searxng_stage_duration_seconds") - before; got != 1 {
		t.Fatalf("failed stage samples = %d, want 1", got)
	}
	p2 := newTestProxy(cfg, sx, c, breaker.New())
	p2.sxCooldownTil.Store(time.Now().Add(time.Minute).UnixNano())
	_, _ = p2.Search(context.Background(), "stage-skipped")
	if got := histogramSampleCount(t, "searxng_gateway_searxng_stage_duration_seconds") - before - 1; got != 0 {
		t.Fatalf("skipped stage samples = %d, want 0", got)
	}
}

func histogramSampleCount(t *testing.T, name string) uint64 {
	t.Helper()
	var total uint64
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
	}
	return total
}

func histogramSampleCountForLabels(t *testing.T, name string, wanted map[string]string) uint64 {
	t.Helper()
	var count uint64
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range wanted {
				if labels[k] != v {
					match = false
				}
			}
			if match {
				count += m.GetHistogram().GetSampleCount()
			}
		}
	}
	return count
}

func gatherMetricFamily(t *testing.T, name string) []*dto.MetricFamily {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return []*dto.MetricFamily{mf}
		}
	}
	return nil
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
