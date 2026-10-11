package proxy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

// --- helpers ---------------------------------------------------------------

type queryRecordingSearxng struct {
	mu      sync.Mutex
	queries []string
	resp    *searxng.Response
}

func (q *queryRecordingSearxng) Search(_ context.Context, query string) (*searxng.Response, error) {
	q.mu.Lock()
	q.queries = append(q.queries, query)
	q.mu.Unlock()
	return q.resp, nil
}

type queryRecordingBackend struct {
	name    string
	mu      sync.Mutex
	queries []string
	results []backends.SearchResult
}

func (b *queryRecordingBackend) Name() string      { return b.name }
func (b *queryRecordingBackend) IsAvailable() bool { return true }
func (b *queryRecordingBackend) Search(opts backends.SearchOptions) ([]backends.SearchResult, error) {
	b.mu.Lock()
	b.queries = append(b.queries, opts.Query)
	b.mu.Unlock()
	return b.results, nil
}

func histogramSumAll(t *testing.T, name string) float64 {
	t.Helper()
	var total float64
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			total += m.GetHistogram().GetSampleSum()
		}
	}
	return total
}

// --- C2: query case --------------------------------------------------------

// TestUpstreamQueryPreservesCaseWhileCacheKeyIsCaseInsensitive: providers must
// receive the caller's casing (trimmed, whitespace-collapsed) while two queries
// differing only in case still share one cache entry.
func TestUpstreamQueryPreservesCaseWhileCacheKeyIsCaseInsensitive(t *testing.T) {
	metrics.Init()
	sx := &queryRecordingSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://sx-case.test", Engine: "wikipedia"}}}}
	br := &queryRecordingBackend{name: "brave", results: []backends.SearchResult{{Title: "B", URL: "https://b-case.test", Engine: "brave"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	p := newTestProxy(cfg, sx, c, breaker.New(), br)

	if _, err := p.Search(context.Background(), "  Postgres   OR\tMariaDB tuning "); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	const want = "Postgres OR MariaDB tuning"
	if !reflect.DeepEqual(sx.queries, []string{want}) {
		t.Errorf("SearXNG received %q, want [%q]", sx.queries, want)
	}
	if !reflect.DeepEqual(br.queries, []string{want}) {
		t.Errorf("premium provider received %q, want [%q]", br.queries, want)
	}

	// Same query, different case: cache hit, no upstream traffic.
	out, err := p.Search(context.Background(), "postgres or mariadb TUNING")
	if err != nil || len(out.Results) == 0 {
		t.Fatalf("second Search = %v, %v", out, err)
	}
	if len(sx.queries) != 1 || len(br.queries) != 1 {
		t.Errorf("case-only variant missed the cache: SearXNG=%d premium=%d calls, want 1 each", len(sx.queries), len(br.queries))
	}
}

func TestNormalizeAndCollapseSpace(t *testing.T) {
	if got := normalize("  A   B\tC "); got != "a b c" {
		t.Errorf("normalize = %q, want %q", got, "a b c")
	}
	if got := collapseSpace("  A   B\tC "); got != "A B C" {
		t.Errorf("collapseSpace = %q, want %q", got, "A B C")
	}
}

// --- C1: parallel first premium call ---------------------------------------

// TestFirstPremiumCallOverlapsSearxngStage: SearXNG and the first premium call
// each wait for the other to have started. Serial execution deadlocks until the
// timeout and fails the test.
func TestFirstPremiumCallOverlapsSearxngStage(t *testing.T) {
	metrics.Init()
	sxStarted := make(chan struct{})
	premiumStarted := make(chan struct{})
	var sxSawPremium, premiumSawSx atomic.Bool

	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		close(sxStarted)
		select {
		case <-premiumStarted:
			sxSawPremium.Store(true)
		case <-time.After(2 * time.Second):
		}
		return &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://ov-sx.test", Engine: "wikipedia"}}}, nil
	}}
	fb := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		close(premiumStarted)
		select {
		case <-sxStarted:
			premiumSawSx.Store(true)
		case <-time.After(2 * time.Second):
		}
		return []backends.SearchResult{{Title: "P", URL: "https://ov-p.test", Engine: "brave"}}, nil
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	start := time.Now()
	out, err := p.Search(context.Background(), "overlap-first-premium")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if !sxSawPremium.Load() || !premiumSawSx.Load() {
		t.Fatalf("stages did not overlap: sxSawPremium=%v premiumSawSx=%v", sxSawPremium.Load(), premiumSawSx.Load())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("overlapping stages took %v, want well under the 2s barrier timeout", elapsed)
	}
	if got := resultURLs(out.Results); !reflect.DeepEqual(got, []string{"https://ov-p.test", "https://ov-sx.test"}) {
		t.Errorf("results = %v, want premium first then SearXNG-only", got)
	}
}

// TestAdmittedCallsPerRequestMatchSerialFlow pins the number of admitted
// premium calls (and the premium_calls_per_request observation) for the cases
// the previous strictly-serial flow defined.
func TestAdmittedCallsPerRequestMatchSerialFlow(t *testing.T) {
	metrics.Init()
	one := func(name, url string) []backends.SearchResult {
		return []backends.SearchResult{{Title: name, URL: url, Engine: name}}
	}
	sxN := func(n int) *fakeSearxng {
		rs := make([]searxng.Result, n)
		for i := range rs {
			rs[i] = searxng.Result{Title: "S", URL: fmt.Sprintf("https://adm-sx-%d.test", i), Engine: "wikipedia"}
		}
		return &fakeSearxng{resp: &searxng.Response{Results: rs}}
	}
	tests := []struct {
		name      string
		sx        *fakeSearxng
		target    int
		cooldown  bool
		backends  func() []*fakeBackend
		wantCalls int64
	}{
		{"searxng meets target: exactly one call", sxN(3), 3, false, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true, results: one("brave", "https://adm-b.test")},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
			}
		}, 1},
		{"searxng empty, target 2: two calls", sxN(0), 2, false, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true, results: one("brave", "https://adm-b.test")},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
				{name: "serper", avail: true, results: one("serper", "https://adm-s.test")},
			}
		}, 2},
		{"first provider errors, searxng met target: still one call", sxN(2), 2, false, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true, err: errors.New("boom")},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
			}
		}, 1},
		{"first provider empty, searxng met target: still one call", sxN(2), 2, false, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
			}
		}, 1},
		{"searxng cooldown, target 1: premium behaves identically (one call)", sxN(5), 1, true, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true, results: one("brave", "https://adm-b.test")},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
			}
		}, 1},
		{"searxng cooldown, target 2: premium tops up serially", sxN(5), 2, true, func() []*fakeBackend {
			return []*fakeBackend{
				{name: "brave", avail: true, results: one("brave", "https://adm-b.test")},
				{name: "exa", avail: true, results: one("exa", "https://adm-e.test")},
				{name: "serper", avail: true, results: one("serper", "https://adm-s.test")},
			}
		}, 2},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fbs := tc.backends()
			sbs := make([]backends.SearchBackend, len(fbs))
			for j, f := range fbs {
				sbs[j] = f
			}
			c, _ := cache.New(100, 0)
			cfg := newCfg()
			cfg.SufficientMinResults = tc.target
			p := newTestProxy(cfg, tc.sx, c, breaker.New(), sbs...)
			if tc.cooldown {
				p.sxCooldownTil.Store(time.Now().Add(time.Minute).UnixNano())
			}
			beforeSum := histogramSumAll(t, "searxng_gateway_premium_calls_per_request")
			beforeCount := histogramSampleCount(t, "searxng_gateway_premium_calls_per_request")
			if _, err := p.Search(context.Background(), fmt.Sprintf("admitted-calls-%d", i)); err != nil {
				t.Fatalf("Search error = %v", err)
			}
			var calls int64
			for _, f := range fbs {
				calls += f.calls.Load()
			}
			if calls != tc.wantCalls {
				t.Errorf("premium calls = %d, want %d", calls, tc.wantCalls)
			}
			if got := histogramSumAll(t, "searxng_gateway_premium_calls_per_request") - beforeSum; got != float64(tc.wantCalls) {
				t.Errorf("premium_calls_per_request sum delta = %v, want %d", got, tc.wantCalls)
			}
			if got := histogramSampleCount(t, "searxng_gateway_premium_calls_per_request") - beforeCount; got != 1 {
				t.Errorf("premium_calls_per_request observations = %d, want exactly 1", got)
			}
		})
	}
}

// TestMergeCountsDistinctUnionOnce: SearXNG {A,B} + first premium {B,C} is a
// union of 3. Target 3 must stop (no second call); target 4 must top up. A
// merge that double-counts (4) or drops the premium URLs (2) fails one side.
func TestMergeCountsDistinctUnionOnce(t *testing.T) {
	metrics.Init()
	for _, tc := range []struct {
		target    int
		wantExa   int64
		wantTotal int
	}{{3, 0, 3}, {4, 1, 4}} {
		t.Run(fmt.Sprintf("target_%d", tc.target), func(t *testing.T) {
			sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
				{Title: "A", URL: "https://merge-a.test", Engine: "wikipedia"},
				{Title: "B", URL: "https://merge-b.test", Engine: "wikipedia"},
			}}}
			brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
				{Title: "B", URL: "https://merge-b.test", Engine: "brave"},
				{Title: "C", URL: "https://merge-c.test", Engine: "brave"},
				{Title: "C again", URL: "https://merge-c.test", Engine: "brave"},
			}}
			exa := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{
				{Title: "D", URL: "https://merge-d.test", Engine: "exa"},
			}}
			c, _ := cache.New(100, 0)
			cfg := newCfg()
			cfg.SufficientMinResults = tc.target
			p := newTestProxy(cfg, sx, c, breaker.New(), brave, exa)
			out, err := p.Search(context.Background(), fmt.Sprintf("merge-union-%d", tc.target))
			if err != nil {
				t.Fatalf("Search error = %v", err)
			}
			if got := exa.calls.Load(); got != tc.wantExa {
				t.Errorf("second provider calls = %d, want %d", got, tc.wantExa)
			}
			seen := map[string]int{}
			for _, r := range out.Results {
				seen[r.URL]++
			}
			for u, n := range seen {
				if n != 1 {
					t.Errorf("URL %s appears %d times in output", u, n)
				}
			}
			if len(out.Results) != tc.wantTotal {
				t.Errorf("results = %v, want %d distinct", resultURLs(out.Results), tc.wantTotal)
			}
			// Premium owns B; SearXNG contributes only A.
			if got := resultURLs(out.Results)[:2]; !reflect.DeepEqual(got, []string{"https://merge-b.test", "https://merge-c.test"}) {
				t.Errorf("leading results = %v, want premium B, C first", got)
			}
		})
	}
}

// TestRoundRobinAdvancesOncePerRequest: the concurrent first call consumes
// exactly one round-robin slot, so successive sufficient requests alternate.
func TestRoundRobinAdvancesOncePerRequest(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://rr-sx.test", Engine: "wikipedia"}}}}
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "B", URL: "https://rr-b.test", Engine: "brave"}}}
	exa := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "E", URL: "https://rr-e.test", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 1
	p := newTestProxy(cfg, sx, c, breaker.New(), brave, exa)
	for i := 0; i < 4; i++ {
		if _, err := p.Search(context.Background(), fmt.Sprintf("round-robin-%d", i)); err != nil {
			t.Fatalf("Search %d: %v", i, err)
		}
	}
	if brave.calls.Load() != 2 || exa.calls.Load() != 2 {
		t.Fatalf("calls brave=%d exa=%d, want 2 each (one NextAvailable per request)", brave.calls.Load(), exa.calls.Load())
	}
}

// TestParentBudgetExpiryStopsBothStages: both concurrent stages are bound by
// the same parent budget; expiry cancels both and no further call starts.
func TestParentBudgetExpiryStopsBothStages(t *testing.T) {
	metrics.Init()
	cfg := newCfg()
	cfg.FallbackTimeout = 80 * time.Millisecond
	cfg.SearxngTimeout = 5 * time.Second
	var sxCtxErr, premiumCtxErr atomic.Value
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) {
		<-ctx.Done()
		sxCtxErr.Store(ctx.Err().Error())
		return nil, ctx.Err()
	}}
	first := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
		<-ctx.Done()
		premiumCtxErr.Store(ctx.Err().Error())
		return nil, ctx.Err()
	}}
	var secondCalls atomic.Int64
	second := &contextBackend{name: "exa", fn: func(context.Context) ([]backends.SearchResult, error) {
		secondCalls.Add(1)
		return nil, nil
	}}
	c, _ := cache.New(100, 0)
	p := newTestProxy(cfg, sx, c, breaker.New(), first, second)
	start := time.Now()
	if _, err := p.Search(context.Background(), "budget-both-stages"); err == nil {
		t.Fatal("expected error with no results")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Search took %v after the 80ms budget", elapsed)
	}
	if sxCtxErr.Load() == nil || premiumCtxErr.Load() == nil {
		t.Errorf("stages not both cancelled: searxng=%v premium=%v", sxCtxErr.Load(), premiumCtxErr.Load())
	}
	if secondCalls.Load() != 0 {
		t.Errorf("second provider started %d times after budget expiry", secondCalls.Load())
	}
}

// --- C4: metrics -----------------------------------------------------------

func stageCount(t *testing.T, outcome string) float64 {
	t.Helper()
	return counterValueForLabels(t, "searxng_gateway_searxng_stage_total", map[string]string{"outcome": outcome})
}

// TestSearxngStageTotalOutcomes: exactly one searxng_stage_total increment per
// uncached request, with the documented outcome label.
func TestSearxngStageTotalOutcomes(t *testing.T) {
	metrics.Init()
	okSx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://st-ok.test", Engine: "wikipedia"}}}}
	blockSx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	errSx := &fakeSearxng{err: errors.New("boom")}
	prem := func() backends.SearchBackend {
		return &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "P", URL: "https://st-p.test", Engine: "brave"}}}
	}
	tests := []struct {
		outcome string
		run     func(t *testing.T)
	}{
		{"success", func(t *testing.T) {
			c, _ := cache.New(10, 0)
			_, _ = newTestProxy(newCfg(), okSx, c, breaker.New(), prem()).Search(context.Background(), "stage-success")
		}},
		{"error", func(t *testing.T) {
			c, _ := cache.New(10, 0)
			_, _ = newTestProxy(newCfg(), errSx, c, breaker.New(), prem()).Search(context.Background(), "stage-error")
		}},
		{"skipped_cooldown", func(t *testing.T) {
			c, _ := cache.New(10, 0)
			p := newTestProxy(newCfg(), okSx, c, breaker.New(), prem())
			p.sxCooldownTil.Store(time.Now().Add(time.Minute).UnixNano())
			_, _ = p.Search(context.Background(), "stage-cooldown")
		}},
		{"timeout", func(t *testing.T) {
			c, _ := cache.New(10, 0)
			cfg := newCfg()
			cfg.SearxngTimeout = 30 * time.Millisecond
			cfg.FallbackTimeout = 5 * time.Second
			_, _ = newTestProxy(cfg, blockSx, c, breaker.New(), prem()).Search(context.Background(), "stage-timeout")
		}},
		{"budget_capped", func(t *testing.T) {
			c, _ := cache.New(10, 0)
			cfg := newCfg()
			cfg.SearxngTimeout = 5 * time.Second
			cfg.FallbackTimeout = 40 * time.Millisecond
			_, _ = newTestProxy(cfg, blockSx, c, breaker.New(), prem()).Search(context.Background(), "stage-budget")
		}},
		{"budget_capped", func(t *testing.T) { // stage could not start: request context already done
			c, _ := cache.New(10, 0)
			dead, cancel := context.WithCancel(context.Background())
			cancel()
			_, _ = newTestProxy(newCfg(), okSx, c, breaker.New(), prem()).Search(dead, "stage-dead-ctx")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.outcome, func(t *testing.T) {
			all := []string{"success", "timeout", "error", "skipped_cooldown", "budget_capped"}
			before := map[string]float64{}
			for _, o := range all {
				before[o] = stageCount(t, o)
			}
			tc.run(t)
			for _, o := range all {
				want := 0.0
				if o == tc.outcome {
					want = 1
				}
				if got := stageCount(t, o) - before[o]; got != want {
					t.Errorf("searxng_stage_total{outcome=%q} delta = %v, want %v", o, got, want)
				}
			}
		})
	}
}

// TestSearxngStageTotalExactlyOneSampleWhenBudgetExpiredAtEntry: a request whose
// parent budget is already exhausted when it enters Search (and a boundary
// budget that expires around the guard) must still emit exactly one stage
// sample, labelled budget_capped, never zero and never two.
func TestSearxngStageTotalExactlyOneSampleWhenBudgetExpiredAtEntry(t *testing.T) {
	metrics.Init()
	all := []string{"success", "timeout", "error", "skipped_cooldown", "budget_capped"}
	for _, budget := range []time.Duration{-time.Second, time.Nanosecond, 100 * time.Microsecond} {
		t.Run(budget.String(), func(t *testing.T) {
			sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://exp.test", Engine: "wikipedia"}}}}
			c, _ := cache.New(10, 0)
			cfg := newCfg()
			cfg.FallbackTimeout = budget
			p := newTestProxy(cfg, sx, c, breaker.New())
			before := map[string]float64{}
			var beforeTotal float64
			for _, o := range all {
				before[o] = stageCount(t, o)
				beforeTotal += before[o]
			}
			_, _ = p.Search(context.Background(), "stage-expired-"+budget.String())
			var afterTotal float64
			for _, o := range all {
				afterTotal += stageCount(t, o)
			}
			if got := afterTotal - beforeTotal; got != 1 {
				t.Fatalf("searxng_stage_total samples emitted = %v, want exactly 1", got)
			}
			if budget < 0 {
				if got := stageCount(t, "budget_capped") - before["budget_capped"]; got != 1 {
					t.Errorf("budget_capped delta = %v, want 1 for an already-expired budget", got)
				}
			}
		})
	}
}

func TestSearxngStageTotalNotCountedOnCacheHit(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "S", URL: "https://stch.test", Engine: "wikipedia"}}}}
	c, _ := cache.New(10, 0)
	p := newTestProxy(newCfg(), sx, c, breaker.New())
	if _, err := p.Search(context.Background(), "stage-cache"); err != nil {
		t.Fatal(err)
	}
	before := stageCount(t, "success")
	beforeCalls := histogramSampleCount(t, "searxng_gateway_premium_calls_per_request")
	if _, err := p.Search(context.Background(), "STAGE-cache"); err != nil {
		t.Fatal(err)
	}
	if got := stageCount(t, "success") - before; got != 0 {
		t.Errorf("cache hit incremented searxng_stage_total by %v", got)
	}
	if got := histogramSampleCount(t, "searxng_gateway_premium_calls_per_request") - beforeCalls; got != 0 {
		t.Errorf("cache hit observed premium_calls_per_request %d times", got)
	}
}

// TestSearchResultsTotalBySource: results in the final response are counted by
// source; a URL owned by premium is not also counted as searxng.
func TestSearchResultsTotalBySource(t *testing.T) {
	metrics.Init()
	src := func(s string) float64 {
		return counterValueForLabels(t, "searxng_gateway_search_results_total", map[string]string{"source": s})
	}
	names := []string{"searxng", "brave", "exa", "parallel", "serper", "tavily"}
	before := map[string]float64{}
	for _, n := range names {
		before[n] = src(n)
	}
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "A", URL: "https://src-a.test", Engine: "wikipedia"},
		{Title: "B", URL: "https://src-b.test", Engine: "wikipedia"},
	}}}
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "B", URL: "https://src-b.test", Engine: "brave"},
		{Title: "C", URL: "https://src-c.test", Engine: "brave"},
	}}
	exa := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "D", URL: "https://src-d.test", Engine: "exa"}}}
	c, _ := cache.New(10, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 4
	p := newTestProxy(cfg, sx, c, breaker.New(), brave, exa)
	out, err := p.Search(context.Background(), "results-by-source")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"searxng": 1, "brave": 2, "exa": 1}
	total := 0.0
	for _, n := range names {
		got := src(n) - before[n]
		total += got
		if got != want[n] {
			t.Errorf("search_results_total{source=%q} delta = %v, want %v", n, got, want[n])
		}
	}
	if int(total) != len(out.Results) {
		t.Errorf("counted %v results, response has %d", total, len(out.Results))
	}
}

// TestPremiumStageDurationAndCallsObservedOncePerUncachedRequest.
func TestPremiumStageDurationAndCallsObservedOncePerUncachedRequest(t *testing.T) {
	metrics.Init()
	fb := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		time.Sleep(30 * time.Millisecond)
		return []backends.SearchResult{{Title: "P", URL: "https://psd.test", Engine: "brave"}}, nil
	}}
	c, _ := cache.New(10, 0)
	p := newTestProxy(newCfg(), &fakeSearxng{resp: &searxng.Response{}}, c, breaker.New(), fb)
	beforeN := histogramSampleCount(t, "searxng_gateway_premium_stage_duration_seconds")
	beforeSum := histogramSumAll(t, "searxng_gateway_premium_stage_duration_seconds")
	if _, err := p.Search(context.Background(), "premium-stage-duration"); err != nil {
		t.Fatal(err)
	}
	if got := histogramSampleCount(t, "searxng_gateway_premium_stage_duration_seconds") - beforeN; got != 1 {
		t.Errorf("premium_stage_duration observations = %d, want 1", got)
	}
	if got := histogramSumAll(t, "searxng_gateway_premium_stage_duration_seconds") - beforeSum; got < 0.025 || got > 2 {
		t.Errorf("premium_stage_duration sum = %vs, want ~0.03s (active premium time)", got)
	}
}
