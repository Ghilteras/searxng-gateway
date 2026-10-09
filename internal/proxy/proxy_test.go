package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sony/gobreaker"
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
	calls   atomic.Int64
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
	f.calls.Add(1)
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
	}
}

// newTestProxy creates a Proxy with the given fallback backends and breaker.
func newTestProxy(cfg *config.Config, sx searxng.Client, c *cache.Cache, breakerMgr *breaker.Manager, fbs ...backends.SearchBackend) *Proxy {
	mgr := backends.NewManager()
	names := make([]string, 0, len(fbs))
	for _, fb := range fbs {
		mgr.Register(fb)
		names = append(names, fb.Name())
	}
	_ = mgr.SetFallbacks(names)
	return New(cfg, sx, c, breakerMgr, mgr)
}

// --- Tests ---

// TestSearchSearxngOK — SearXNG returns enough results without needing premium results.
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

func TestRetryFastTransientCanSucceedOnSecondAttempt(t *testing.T) {
	p := &Proxy{}
	attempt1Before := retryCounter(t, metrics.RetryAttemptsTotal, "1", "error", "network")
	attempt2Before := retryCounter(t, metrics.RetryAttemptsTotal, "2", "success", "none")
	calls := 0
	resp, err := p.retryWithBackoff(context.Background(), context.Background(), func() (*searxng.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("connection refused")
		}
		return &searxng.Response{Results: []searxng.Result{{URL: "https://ok"}}}, nil
	})
	if err != nil || resp == nil || calls != 2 {
		t.Fatalf("response=%v err=%v attempts=%d; want success on attempt 2", resp, err, calls)
	}
	if got := retryCounter(t, metrics.RetryAttemptsTotal, "1", "error", "network"); got != attempt1Before+1 {
		t.Fatalf("attempt=1 network failures counter=%v; want %v", got, attempt1Before+1)
	}
	if got := retryCounter(t, metrics.RetryAttemptsTotal, "2", "success", "none"); got != attempt2Before+1 {
		t.Fatalf("attempt=2 success counter=%v; want %v", got, attempt2Before+1)
	}
}

func TestRetryLiveTimeoutClassIsNotRetried(t *testing.T) {
	p := &Proxy{}
	calls := 0
	_, err := p.retryWithBackoff(context.Background(), context.Background(), func() (*searxng.Response, error) {
		calls++
		return nil, context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("err=%v attempts=%d; want one attempt for timeout-class error on live context", err, calls)
	}
}

func TestRetryStageTimeoutIsNotRetriedWithinParentBudget(t *testing.T) {
	p := &Proxy{}
	parent, cancelParent := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelParent()
	stage, cancelStage := context.WithTimeout(parent, 30*time.Millisecond)
	defer cancelStage()
	calls := 0
	started := time.Now()
	_, err := p.retryWithBackoff(stage, parent, func() (*searxng.Response, error) {
		calls++
		<-stage.Done()
		return nil, stage.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("err=%v attempts=%d; want one timed-out attempt", err, calls)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("stage timeout consumed parent budget: elapsed=%s", elapsed)
	}
}

func TestRetryExhaustionMakesThreeAttempts(t *testing.T) {
	p := &Proxy{}
	calls := 0
	exhaustedBefore := retryCounter(t, metrics.RetryExhaustedTotal, "network")
	_, err := p.retryWithBackoff(context.Background(), context.Background(), func() (*searxng.Response, error) {
		calls++
		return nil, errors.New("connection reset")
	})
	if err == nil || calls != 3 {
		t.Fatalf("err=%v attempts=%d; want exhausted after three attempts", err, calls)
	}
	if got := retryCounter(t, metrics.RetryExhaustedTotal, "network"); got != exhaustedBefore+1 {
		t.Fatalf("exhausted counter=%v; want %v", got, exhaustedBefore+1)
	}
}

func TestRetryOtherErrorIsNotRetried(t *testing.T) {
	p := &Proxy{}
	calls := 0
	attemptBefore := retryCounter(t, metrics.RetryAttemptsTotal, "1", "error", "other")
	_, err := p.retryWithBackoff(context.Background(), context.Background(), func() (*searxng.Response, error) {
		calls++
		return nil, errors.New("searxng: status 503")
	})
	if err == nil || calls != 1 {
		t.Fatalf("err=%v attempts=%d; want one attempt for other-class status error", err, calls)
	}
	if got := retryCounter(t, metrics.RetryAttemptsTotal, "1", "error", "other"); got != attemptBefore+1 {
		t.Fatalf("attempt counter=%v; want %v", got, attemptBefore+1)
	}
}

func retryCounter(t *testing.T, counter interface {
	WithLabelValues(...string) prometheus.Counter
}, labels ...string) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.WithLabelValues(labels...).Write(metric); err != nil {
		t.Fatalf("read retry counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// TestSearxngSufficientMakesExactlyOnePremiumCall is the money assertion for the
// A1 policy: SearXNG meets the target, yet the premium stage still makes
// exactly one admitted provider call (minAdmitted=1) and no second provider is
// tried. If the owner changes the minimum-call policy, this is the test to edit.
func TestSearxngSufficientMakesExactlyOnePremiumCall(t *testing.T) {
	metrics.Init()
	var sxCalls atomic.Int64
	results := make([]searxng.Result, 3)
	for i := range results {
		results[i] = searxng.Result{Title: fmt.Sprintf("SX%d", i), URL: fmt.Sprintf("https://sx-%d.com", i), Engine: "wikipedia"}
	}
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		sxCalls.Add(1)
		return &searxng.Response{Results: results}, nil
	}}
	first := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "P", URL: "https://premium-only.com", Engine: "brave"},
	}}
	second := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{
		{Title: "P2", URL: "https://premium-two.com", Engine: "exa"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3
	p := newTestProxy(cfg, sx, c, breaker.New(), first, second)
	if _, err := p.Search(context.Background(), "sx-sufficient"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := sxCalls.Load(); got != 1 {
		t.Fatalf("SearXNG calls = %d, want 1", got)
	}
	if got := first.calls.Load() + second.calls.Load(); got != 1 {
		t.Fatalf("premium calls = %d, want exactly 1 when SearXNG meets the target", got)
	}
}

// TestSearxngStagePrecedesPremium (a): SearXNG is invoked before any premium
// backend on a request that falls short.
func TestSearxngStagePrecedesPremium(t *testing.T) {
	metrics.Init()
	var order []string
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		order = append(order, "searxng")
		return &searxng.Response{Results: []searxng.Result{{Title: "SX", URL: "https://sx.com", Engine: "wikipedia"}}}, nil
	}}
	fb := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		order = append(order, "premium")
		return []backends.SearchResult{{Title: "P", URL: "https://p.com", Engine: "brave"}}, nil
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "order-first"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(order) != 2 || order[0] != "searxng" || order[1] != "premium" {
		t.Fatalf("call order = %v, want [searxng premium]", order)
	}
}

// TestSearxngShortfallCallsPremiumUntilTarget (c): with SearXNG short of the
// target, premium providers are called only until the target is met.
func TestSearxngShortfallCallsPremiumUntilTarget(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Title: "SX", URL: "https://sx.com", Engine: "wikipedia"}}}}
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "B", URL: "https://b.com", Engine: "brave"}}}
	exa := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "E", URL: "https://e.com", Engine: "exa"}}}
	serper := &fakeBackend{name: "serper", avail: true, results: []backends.SearchResult{{Title: "S", URL: "https://s.com", Engine: "serper"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3
	p := newTestProxy(cfg, sx, c, breaker.New(), brave, exa, serper)
	out, err := p.Search(context.Background(), "shortfall-until-target")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 3 {
		t.Fatalf("result count = %d, want 3 (SearXNG + two premium)", len(out.Results))
	}
	if got := brave.calls.Load() + exa.calls.Load() + serper.calls.Load(); got != 2 {
		t.Fatalf("premium calls = %d, want 2 (stop once the target is met)", got)
	}
}

// TestSearxngCooldownSkipsSearxngAndServesPremium (d): during cooldown SearXNG
// is not called at all and premium serves the request alone.
func TestSearxngCooldownSkipsSearxngAndServesPremium(t *testing.T) {
	metrics.Init()
	var sxCalls atomic.Int64
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		sxCalls.Add(1)
		return nil, errors.New("upstream 500")
	}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "P", URL: "https://p.com", Engine: "brave"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SearxngFailThreshold = 2
	cfg.SearxngFailCooldown = time.Minute
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	for i := 0; i < 2; i++ {
		_, _ = p.Search(context.Background(), fmt.Sprintf("cooldown-warmup-%d", i))
	}
	if !p.inCooldown() {
		t.Fatal("precondition: expected SearXNG in cooldown after threshold failures")
	}
	sxCalls.Store(0)
	out, err := p.Search(context.Background(), "cooldown-request")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := sxCalls.Load(); got != 0 {
		t.Fatalf("SearXNG calls during cooldown = %d, want 0", got)
	}
	if len(out.Results) != 1 || out.Results[0].Engine != "brave" {
		t.Fatalf("results = %+v, want premium-only during cooldown", out.Results)
	}
}

// TestSearxngErrorPremiumStillServes (e): a SearXNG error does not prevent
// premium from serving the request.
func TestSearxngErrorPremiumStillServes(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{err: errors.New("upstream down")}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "P1", URL: "https://p1.com", Engine: "brave"},
		{Title: "P2", URL: "https://p2.com", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	beforePremium := outcomeCounter(t, "premium_ok")
	out, err := p.Search(context.Background(), "sx-error-premium-serves")
	if err != nil {
		t.Fatalf("Search error = %v, want premium results", err)
	}
	if len(out.Results) != 2 {
		t.Fatalf("result count = %d, want 2 premium results", len(out.Results))
	}
	if got := outcomeCounter(t, "premium_ok") - beforePremium; got != 1 {
		t.Errorf("premium_ok delta = %v, want 1", got)
	}
}

func TestShortfallPremiumOwnsDuplicateURLs(t *testing.T) {
	shared := "https://shared.com"
	var sxCalls atomic.Int64
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		sxCalls.Add(1)
		return &searxng.Response{Results: []searxng.Result{
			{Title: "SX", URL: shared, Engine: "wikipedia"},
			{Title: "SX2", URL: "https://sx-extra.com", Engine: "bing"},
		}}, nil
	}}
	backend := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: shared, Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3
	p := newTestProxy(cfg, sx, c, breaker.New(), backend)
	out, err := p.Search(context.Background(), "shortfall-premium-owns-duplicate")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := sxCalls.Load(); got == 0 {
		t.Fatal("SearXNG stage was not called")
	}
	sharedCount := 0
	sharedEngine := ""
	for _, r := range out.Results {
		if r.URL == shared {
			sharedCount++
			sharedEngine = r.Engine
		}
	}
	if sharedCount != 1 {
		t.Fatalf("shared URL count = %d, want 1", sharedCount)
	}
	if sharedEngine != "brave" {
		t.Fatalf("surviving shared URL engine = %q, want premium (brave) to own the output duplicate", sharedEngine)
	}
	if len(out.Results) != 2 {
		t.Fatalf("result count = %d, want 2 (premium shared + sx-extra)", len(out.Results))
	}
}

func TestPremiumOneCallPerProviderAndNoCallAfterCancellation(t *testing.T) {
	sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
		return &searxng.Response{Results: []searxng.Result{}}, nil
	}}
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{URL: "https://b.com", Engine: "brave"}}}
	exa := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{URL: "https://e.com", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 10
	p := newTestProxy(cfg, sx, c, breaker.New(), brave, exa)
	if _, err := p.Search(context.Background(), "one-call-per-provider"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := brave.calls.Load(); got != 1 {
		t.Errorf("brave calls = %d, want 1", got)
	}
	if got := exa.calls.Load(); got != 1 {
		t.Errorf("exa calls = %d, want 1", got)
	}

	started := make(chan struct{})
	first := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	var secondCalls atomic.Int64
	second := &contextBackend{name: "exa", fn: func(context.Context) ([]backends.SearchResult, error) {
		secondCalls.Add(1)
		return nil, nil
	}}
	c2, _ := cache.New(100, 0)
	p2 := newTestProxy(cfg, sx, c2, breaker.New(), first, second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = p2.Search(ctx, "cancel-no-late-call"); close(done) }()
	<-started
	cancel()
	<-done
	if got := secondCalls.Load(); got != 0 {
		t.Fatalf("second provider called %d times after cancellation, want 0", got)
	}
}

func TestPrimaryLoopTerminatesEmptyAllIneligibleAndDeadline(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
		backend := &fakeBackend{name: "brave", avail: true}
		c, _ := cache.New(100, 0)
		p := newTestProxy(newCfg(), sx, c, breaker.New(), backend)
		done := make(chan struct{})
		go func() { _, _ = p.Search(context.Background(), "empty-terminate"); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Search did not terminate on empty results")
		}
	})
	t.Run("all_ineligible", func(t *testing.T) {
		sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
		unavail := &fakeBackend{name: "brave", avail: false}
		c, _ := cache.New(100, 0)
		p := newTestProxy(newCfg(), sx, c, breaker.New(), unavail)
		done := make(chan struct{})
		go func() { _, _ = p.Search(context.Background(), "ineligible-terminate"); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Search did not terminate when all providers are ineligible")
		}
	})
	t.Run("deadline", func(t *testing.T) {
		sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
		blocked := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		c, _ := cache.New(100, 0)
		cfg := newCfg()
		cfg.FallbackTimeout = 80 * time.Millisecond
		cfg.SearxngTimeout = time.Second
		p := newTestProxy(cfg, sx, c, breaker.New(), blocked)
		done := make(chan struct{})
		go func() { _, _ = p.Search(context.Background(), "deadline-terminate"); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Search did not terminate at the deadline")
		}
	})
}

func TestUnderTargetResultsAreSuccessfulAndCached(t *testing.T) {
	metrics.Init()
	beforePremium := outcomeCounter(t, "premium_ok")
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	backend := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "P", URL: "https://p.com", Engine: "brave"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 10
	p := newTestProxy(cfg, sx, c, breaker.New(), backend)
	out, err := p.Search(context.Background(), "under-target")
	if err != nil {
		t.Fatalf("Search error = %v (under-target nonempty must be success)", err)
	}
	if len(out.Results) != 1 {
		t.Fatalf("result count = %d, want 1", len(out.Results))
	}
	if delta := outcomeCounter(t, "premium_ok") - beforePremium; delta != 1 {
		t.Fatalf("premium_ok delta = %v, want 1", delta)
	}
	if _, ok := c.Get("under-target"); !ok {
		t.Fatal("under-target result was not cached")
	}
}

func TestNonPositiveThresholdDoesNotSuppressPrimary(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	backend := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "P", URL: "https://p.com", Engine: "brave"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 0
	p := newTestProxy(cfg, sx, c, breaker.New(), backend)
	out, err := p.Search(context.Background(), "zero-threshold")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := backend.calls.Load(); got != 1 {
		t.Fatalf("premium provider calls = %d, want 1 (loop must not be suppressed)", got)
	}
	if len(out.Results) != 1 {
		t.Fatalf("result count = %d, want 1", len(out.Results))
	}
}

func TestSufficientRequiresAtLeastOneDistinctURL(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	backend := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "Premium", URL: "https://premium-only.com", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 0
	p := newTestProxy(cfg, sx, c, breaker.New(), backend)

	out, err := p.Search(context.Background(), "empty-sx-zero-threshold")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	for _, result := range out.Results {
		if result.URL == "https://premium-only.com" {
			return
		}
	}
	t.Fatal("premium result URL missing")
}

func TestSufficientThresholdUsesDistinctSearxngURLs(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "S1", URL: "https://shared.com", Engine: "wikipedia"},
		{Title: "S2 duplicate", URL: "https://shared.com", Engine: "bing"},
		{Title: "S3", URL: "https://sx-only.com", Engine: "bing"},
	}}}
	backend := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "Premium", URL: "https://premium-only.com", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3
	p := newTestProxy(cfg, sx, c, breaker.New(), backend)

	out, err := p.Search(context.Background(), "distinct-sx-threshold")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 3 {
		t.Fatalf("result count = %d, want 3 distinct URLs across SearXNG and premium", len(out.Results))
	}
	if out.Results[0].URL != "https://premium-only.com" {
		t.Errorf("first URL = %q, want premium-first output order", out.Results[0].URL)
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
	bm.RecordClientError("premium:exa", "test trip") // trip exa's premium breaker
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

// TestSearchSearxngThenOnePremiumBothContribute — SearXNG runs first, then one premium provider; both SearXNG and premium results are present in the output.
func TestSearchSearxngThenOnePremiumBothContribute(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx1.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://br.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
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

// TestSearchSearxngThenTwoPremiumsAllContribute — SearXNG runs first, then two premium providers run (still below target); all three engines contribute.
func TestSearchSearxngThenTwoPremiumsAllContribute(t *testing.T) {
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

func TestClassifyPremiumOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"typed 432 plan limit", &backends.BackendError{Backend: "tavily", Code: 432, Err: errors.New("private plan body")}, "quota"},
		{"typed 401", &backends.BackendError{Code: 401}, "auth"},
		{"typed 403", &backends.BackendError{Code: 403}, "auth"},
		{"typed 402 credits", &backends.BackendError{Code: 402, Err: errors.New("payment required")}, "quota"},
		{"typed 429", &backends.BackendError{Code: 429}, "rate_limit"},
		{"typed 400 validation", &backends.BackendError{Code: 400, Err: errors.New("invalid query parameter")}, "request_error"},
		{"typed 400 credits", &backends.BackendError{Code: 400, Err: errors.New("not enough credits")}, "quota"},
		{"typed 422 validation", &backends.BackendError{Code: 422, Err: errors.New("unprocessable entity")}, "request_error"},
		{"typed 5xx", &backends.BackendError{Code: 503}, "http_5xx"},
		{"typed network", &backends.BackendError{Code: backends.ErrCodeNetwork}, "network"},
		{"typed invalid response", &backends.BackendError{Code: backends.ErrCodeInvalidResponse}, "invalid_response"},
		{"typed auth", &backends.BackendError{Code: backends.ErrCodeAuth}, "auth"},
		{"typed rate limit", &backends.BackendError{Code: backends.ErrCodeRateLimit}, "rate_limit"},
		{"typed unavailable", &backends.BackendError{Code: backends.ErrCodeUnavailable}, "not_configured"},
		{"typed degraded", &backends.BackendError{Code: backends.ErrCodeDegraded}, "degraded"},
		{"caller cancellation", context.Canceled, "cancelled"},
		{"untyped captcha", errors.New("captcha required"), "auth"},
		{"untyped blocked", errors.New("blocked by policy"), "auth"},
		{"generic", errors.New("temporary provider issue"), "other_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPremiumOutcome(context.Background(), nil, tt.err)
			if got != tt.want {
				t.Fatalf("classifyPremiumOutcome() = %q, want %q", got, tt.want)
			}
		})
	}
	if got := classifyPremiumOutcome(context.Background(), []backends.SearchResult{{URL: "https://x"}}, nil); got != "success" {
		t.Fatalf("non-empty success = %q, want success", got)
	}
	if got := classifyPremiumOutcome(context.Background(), nil, nil); got != "empty" {
		t.Fatalf("empty success = %q, want empty", got)
	}
	if got := classifyPremiumOutcome(context.Background(), nil, context.DeadlineExceeded); got != "timeout" {
		t.Fatalf("provider timeout with live parent = %q, want timeout", got)
	}
	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := classifyPremiumOutcome(deadCtx, nil, context.DeadlineExceeded); got != "cancelled" {
		t.Fatalf("deadline with done parent = %q, want cancelled", got)
	}
}

func TestTyped432TripsBreakerInPrimaryPhase(t *testing.T) {
	const querySecret = "QUERY-SECRET-432"
	const bodySecret = "PLAN-BODY-432"
	const keySecret = "API-KEY-432"
	backend := &fakeBackend{name: "tavily", avail: true, err: &backends.BackendError{
		Backend: "tavily", Code: 432, Err: errors.New(bodySecret + " " + keySecret),
	}}
	cfg := newCfg()
	bm := breaker.New()
	c, _ := cache.New(10, 0)
	p := newTestProxy(cfg, &fakeSearxng{}, c, bm, backend)
	var logs strings.Builder
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	p.premiumLoop(context.Background(), querySecret, nil, map[string]bool{}, map[string]bool{}, nil, 1)
	p.premiumLoop(context.Background(), querySecret, nil, map[string]bool{}, map[string]bool{}, nil, 1)
	if !bm.IsOpen("premium:tavily") {
		t.Fatal("typed 432 did not open Tavily breaker")
	}
	if got := bm.LastReason("premium:tavily"); got != "quota" {
		t.Fatalf("breaker reason = %q, want bounded outcome quota", got)
	}
	if strings.Contains(logs.String(), querySecret) || strings.Contains(logs.String(), bodySecret) || strings.Contains(logs.String(), keySecret) {
		t.Fatalf("sensitive text leaked in provider log: %q", logs.String())
	}
	if strings.Contains(bm.LastReason("premium:tavily"), bodySecret) || strings.Contains(bm.LastReason("premium:tavily"), keySecret) {
		t.Fatal("sensitive text leaked in breaker reason")
	}
	calls := backend.calls.Load()
	backend.err = nil
	backend.results = []backends.SearchResult{{URL: "https://recovered", Engine: "tavily"}}
	p.premiumLoop(context.Background(), querySecret, nil, map[string]bool{}, map[string]bool{}, nil, 1)
	if got := backend.calls.Load(); got != calls {
		t.Fatalf("provider call count while breaker open = %d, want %d", got, calls)
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

func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func TestSearxngCooldownMetrics(t *testing.T) {
	metrics.Init()
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	p := newTestProxy(cfg, &fakeSearxng{}, c, breaker.New())

	// The Prometheus registry is process-global, so establish the initial zero
	// state through the real Proxy success transition instead of relying on test order.
	p.recordSearxngSuccess()
	if got := gaugeValue(t, "searxng_gateway_searxng_cooldown_until_seconds"); got != 0 {
		t.Fatalf("cooldown_until_seconds after initial success = %v, want 0", got)
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_failure_streak"); got != 0 {
		t.Fatalf("failure_streak after initial success = %v, want 0", got)
	}
	for range cfg.SearxngFailThreshold {
		p.recordSearxngFailure()
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_failure_streak"); got != float64(cfg.SearxngFailThreshold) {
		t.Fatalf("failure_streak = %v, want %d", got, cfg.SearxngFailThreshold)
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_cooldown_until_seconds"); got <= float64(time.Now().Unix()) {
		t.Fatalf("cooldown_until_seconds = %v, want future unix time", got)
	}
	// Verify success clears non-zero gauges (not merely the already-expired zero state).
	p.recordSearxngSuccess()
	if got := gaugeValue(t, "searxng_gateway_searxng_failure_streak"); got != 0 {
		t.Fatalf("failure_streak after success = %v, want 0", got)
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_cooldown_until_seconds"); got != 0 {
		t.Fatalf("cooldown_until_seconds after success = %v, want 0", got)
	}
	// Re-trigger threshold independently, then verify lazy expiry clears state/gauges.
	for range cfg.SearxngFailThreshold {
		p.recordSearxngFailure()
	}
	p.sxCooldownTil.Store(time.Now().Add(-time.Second).UnixNano())
	if p.inCooldown() {
		t.Fatal("inCooldown() = true after forced expiry, want false")
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_failure_streak"); got != 0 {
		t.Fatalf("failure_streak after expiry = %v, want 0", got)
	}
	if got := gaugeValue(t, "searxng_gateway_searxng_cooldown_until_seconds"); got != 0 {
		t.Fatalf("cooldown_until_seconds after expiry = %v, want 0", got)
	}
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

// TestOutcomeLabels_PremiumDuplicatesSearxng — SearXNG returns results; the
// premium backend returns the SAME URLs. Premium owns the output duplicates and
// SearXNG contributes nothing new -> outcome must be "premium_ok".
func TestOutcomeLabels_PremiumDuplicatesSearxng(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX_D1", URL: "https://dup-shared-a.com", Engine: "wikipedia"},
		{Title: "SX_D2", URL: "https://dup-shared-b.com", Engine: "wikipedia"},
		{Title: "SX_D3", URL: "https://dup-shared-c.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR_D1", URL: "https://dup-shared-a.com", Content: "d", Engine: "brave"},
		{Title: "BR_D2", URL: "https://dup-shared-b.com", Content: "d", Engine: "brave"},
		{Title: "BR_D3", URL: "https://dup-shared-c.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 10
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)

	beforeSxOk := outcomeCounter(t, "searxng_ok")
	beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")
	beforePremOk := outcomeCounter(t, "premium_ok")

	_, err := p.Search(context.Background(), "outcome_dup_sx")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}

	if delta := outcomeCounter(t, "premium_ok") - beforePremOk; delta != 1 {
		t.Errorf("premium_ok delta = %v, want 1 (premium owns the shared URLs)", delta)
	}
	if delta := outcomeCounter(t, "searxng_ok") - beforeSxOk; delta != 0 {
		t.Errorf("searxng_ok delta = %v, want 0", delta)
	}
	if delta := outcomeCounter(t, "searxng_plus_premium_ok") - beforeSxPlus; delta != 0 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 0", delta)
	}
}

// TestOutcomeLabels_SearxngPlusPremiumComposition (f): SearXNG contributes a
// URL and the premium stage adds a distinct one -> "searxng_plus_premium_ok".
func TestOutcomeLabels_SearxngPlusPremiumComposition(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://comp-sx.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "BR", URL: "https://comp-brave.com", Content: "d", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)

	beforePlus := outcomeCounter(t, "searxng_plus_premium_ok")
	beforeSxOk := outcomeCounter(t, "searxng_ok")
	beforePremOk := outcomeCounter(t, "premium_ok")

	out, err := p.Search(context.Background(), "outcome_composition_both")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 2 {
		t.Fatalf("result count = %d, want 2", len(out.Results))
	}
	if delta := outcomeCounter(t, "searxng_plus_premium_ok") - beforePlus; delta != 1 {
		t.Errorf("searxng_plus_premium_ok delta = %v, want 1", delta)
	}
	if delta := outcomeCounter(t, "searxng_ok") - beforeSxOk; delta != 0 {
		t.Errorf("searxng_ok delta = %v, want 0", delta)
	}
	if delta := outcomeCounter(t, "premium_ok") - beforePremOk; delta != 0 {
		t.Errorf("premium_ok delta = %v, want 0", delta)
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

// TestPremiumPass_SerialNoOverlap verifies the premium-stage loop calls
// providers serially (max overlap == 1) and in round-robin selection order.
func TestPremiumPass_SerialNoOverlap(t *testing.T) {
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

	// SearXNG returns 1 result; brave's result keeps the count below 3, so the premium stage continues to exa and together they meet the threshold.
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX", URL: "https://sx.com", Engine: "wikipedia"},
	}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3

	// Register overlap backends into a fresh manager via newTestProxy-like construction.
	mgr := backends.NewManager()
	mgr.Register(brave)
	mgr.Register(exa)
	_ = mgr.SetFallbacks([]string{"brave", "exa"})
	p := New(cfg, sx, c, breaker.New(), mgr)

	_, err := p.Search(context.Background(), "premium_serial_overlap")
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

// TestPremiumLoop_SerialNoOverlapAndEarlyStop verifies the premium-stage
// loop calls providers serially (max overlap == 1) and stops the moment the
// SufficientMinResults threshold is met — no speculative extra calls.
func TestPremiumLoop_SerialNoOverlapAndEarlyStop(t *testing.T) {
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

	// SearXNG returns 0 → the premium stage keeps calling until the target is met.
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2

	mgr := backends.NewManager()
	mgr.Register(brave)
	mgr.Register(exa)
	mgr.Register(jina)
	_ = mgr.SetFallbacks([]string{"brave", "exa"})
	p := New(cfg, sx, c, breaker.New(), mgr)

	_, err := p.Search(context.Background(), "premium_serial_earlystop")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("max observed overlap = %d, want 1 (serial premium stage)", got)
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

func TestSearchReturnsPremiumPartialAfterSearxngChildBudgetExpires(t *testing.T) {
	metrics.Init()
	beforeTimeout := outcomeCounter(t, "timeout")
	beforePremium := outcomeCounter(t, "premium_ok")
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 500 * time.Millisecond
	cfg.SearxngTimeout = 40 * time.Millisecond
	cfg.SufficientMinResults = 10
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
			cfg.SearxngTimeout = 60 * time.Millisecond
			cfg.SufficientMinResults = 10
			started := make(chan struct{})
			canceled := make(chan struct{})
			fb := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}}
			// SearXNG (primary) fails fast so the premium stage owns the budget.
			sx := &fakeSearxng{err: errors.New("upstream down")}
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
	// Threshold 1: a SearXNG failure counted for the cancellation would trip the
	// cooldown immediately, so the assertions below can actually fail.
	cfg.SearxngFailThreshold = 1
	// SearXNG is the primary stage, so it is the in-flight call when the caller cancels.
	sxStarted := make(chan struct{})
	sxCanceled := make(chan struct{})
	var sxCalls atomic.Int64
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) {
		sxCalls.Add(1)
		close(sxStarted)
		<-ctx.Done()
		close(sxCanceled)
		return nil, ctx.Err()
	}}
	var premiumCalls atomic.Int64
	first := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		premiumCalls.Add(1)
		return nil, nil
	}}
	p := newTestProxy(cfg, sx, c, breaker.New(), first)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := p.Search(ctx, "cancel-provider"); done <- err }()
	<-sxStarted
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected canceled request error")
	}
	<-sxCanceled
	if got := premiumCalls.Load(); got != 0 {
		t.Fatalf("premium provider started after cancellation: %d", got)
	}
	if got := atomic.LoadInt64(&p.sxFails); got != 0 {
		t.Fatalf("SearXNG failure streak = %d, want 0 (caller cancellation is not a SearXNG failure)", got)
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
			return nil, errors.New("connection reset: secret error body")
		}
		return &searxng.Response{Results: []searxng.Result{{Title: titleSecret, URL: "https://private.example/path?q=hidden", Content: bodySecret, Engine: "engine"}}}, nil
	}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "fallback", URL: "https://fallback.example", Engine: "brave"}}}
	fb2 := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "fallback2", URL: "https://fallback2.example", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	cfg.SearxngTimeout = 5 * time.Second
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
	prem, ok := byName["premium.primary"]
	if !ok {
		t.Fatalf("missing premium provider span; got %v", spanNames(spans))
	}
	if stage.Parent.SpanID() != proxySpan.SpanContext.SpanID() || prem.Parent.SpanID() != proxySpan.SpanContext.SpanID() {
		t.Fatalf("SearXNG stage and premium provider must be siblings under proxy: stage parent=%s premium parent=%s proxy=%s", stage.Parent.SpanID(), prem.Parent.SpanID(), proxySpan.SpanContext.SpanID())
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

func TestSearxngChildBudgetExpiryTripsCooldown(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SearxngTimeout = 15 * time.Millisecond
	cfg.FallbackTimeout = time.Second
	cfg.SearxngFailThreshold = 1
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	fb := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
		return []backends.SearchResult{{URL: "https://partial", Engine: "brave"}}, nil
	}}
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	if _, err := p.Search(context.Background(), "child-timeout"); err != nil {
		t.Fatal(err)
	}
	if !p.inCooldown() {
		t.Fatal("SearXNG child budget expiry did not trip cooldown")
	}
}

func TestSearxngParentBudgetExpiryDoesNotTripCooldown(t *testing.T) {
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 15 * time.Millisecond
	cfg.SearxngTimeout = time.Second
	cfg.SearxngFailThreshold = 1
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := newTestProxy(cfg, sx, c, breaker.New())
	callerCtx := context.Background()
	started := time.Now()
	if _, err := p.Search(callerCtx, "parent-timeout"); err == nil {
		t.Fatal("expected error after parent request budget expired without results")
	}
	if time.Since(started) < cfg.FallbackTimeout {
		t.Fatal("Search returned before the parent request budget expired")
	}
	if callerCtx.Err() != nil {
		t.Fatalf("caller context unexpectedly expired: %v", callerCtx.Err())
	}
	if p.inCooldown() {
		t.Fatal("overall parent budget expiry tripped SearXNG cooldown")
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
		if s.Name == "premium.primary" {
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
	if got := histogramSampleCountForLabels(t, "searxng_gateway_provider_duration_seconds", map[string]string{"provider": "brave", "phase": "primary"}); got < 1 {
		t.Fatalf("missing truthful primary provider duration series: %d", got)
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

func labelsMatch(m *dto.Metric, wanted map[string]string) bool {
	labels := map[string]string{}
	for _, l := range m.GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	for k, v := range wanted {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// counterValueForLabels returns the value of the counter series matching all
// wanted labels, or 0 when the series does not exist.
func counterValueForLabels(t *testing.T, name string, wanted map[string]string) float64 {
	t.Helper()
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, wanted) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// counterSumForLabels sums the counter series matching all wanted labels.
func counterSumForLabels(t *testing.T, name string, wanted map[string]string) float64 {
	t.Helper()
	var total float64
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, wanted) {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// gaugeValueForLabels returns the value of the gauge series matching all wanted
// labels, or 0 when the series does not exist.
func gaugeValueForLabels(t *testing.T, name string, wanted map[string]string) float64 {
	t.Helper()
	for _, mf := range gatherMetricFamily(t, name) {
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, wanted) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
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

	// --- (i) Duplicate path: SearXNG and premium return the same URLs; premium
	// owns the output duplicates, so only premium contributes -> "premium_ok". ---
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
		cfg.SufficientMinResults = 4
		p := newTestProxy(cfg, sx, c, breaker.New(), fb)

		beforePrem := outcomeCounter(t, "premium_ok")
		beforeSxOk := outcomeCounter(t, "searxng_ok")
		beforeSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		query := fmt.Sprintf("det_dup_iter_%d_%d", time.Now().UnixNano(), i)
		resp, err := p.Search(context.Background(), query)
		if err != nil {
			t.Fatalf("duplicate-path iter %d: Search error = %v", i, err)
		}

		afterPrem := outcomeCounter(t, "premium_ok")
		afterSxOk := outcomeCounter(t, "searxng_ok")
		afterSxPlus := outcomeCounter(t, "searxng_plus_premium_ok")

		if delta := afterPrem - beforePrem; delta != 1 {
			t.Errorf("duplicate-path iter %d: premium_ok delta = %v, want 1", i, delta)
		}
		if delta := afterSxOk - beforeSxOk; delta != 0 {
			t.Errorf("duplicate-path iter %d: searxng_ok delta = %v, want 0", i, delta)
		}
		if delta := afterSxPlus - beforeSxPlus; delta != 0 {
			t.Errorf("duplicate-path iter %d: searxng_plus_premium_ok delta = %v, want 0", i, delta)
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
				t.Errorf("duplicate-path iter %d: URL count = %d, want %d", i, len(iterURLs), len(firstURLs))
			} else {
				for j := range firstURLs {
					if iterURLs[j] != firstURLs[j] {
						t.Errorf("duplicate-path iter %d: URL[%d] = %q, want %q (nondeterministic)", i, j, iterURLs[j], firstURLs[j])
						break
					}
				}
			}
		}
	}

	// --- (ii) Composition path: SearXNG supplies the URLs and premium adds one
	// new URL (still below target) -> "searxng_plus_premium_ok". ---
	var firstURLsFB []string
	for i := 0; i < iterations; i++ {
		sx := &fakeSearxng{resp: &searxng.Response{Results: urls}}
		fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
			{Title: "D1", URL: "https://det-a.com", Content: "d", Engine: "brave"},
			{Title: "D4", URL: "https://det-d.com", Content: "d", Engine: "brave"},
		}}
		c, _ := cache.New(100, 0)
		cfg := newCfg()
		cfg.SufficientMinResults = 10
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

		if delta := afterSxOk - beforeSxOk; delta != 0 {
			t.Errorf("FB iter %d: searxng_ok delta = %v, want 0", i, delta)
		}
		if delta := afterPrem - beforePrem; delta != 0 {
			t.Errorf("FB iter %d: premium_ok delta = %v, want 0", i, delta)
		}
		if delta := afterSxPlus - beforeSxPlus; delta != 1 {
			t.Errorf("FB iter %d: searxng_plus_premium_ok delta = %v, want 1", i, delta)
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

// ---------------------------------------------------------------------------
// Real circuit-breaker admission + one-terminal-outcome observability
// ---------------------------------------------------------------------------

// Requirement group 1: an open breaker excludes the provider from the
// round-robin (the Tavily rule).
func TestOpenBreakerExcludesProviderFromRoundRobin(t *testing.T) {
	metrics.Init()
	tavily := &fakeBackend{name: "tavily", avail: true, err: &backends.BackendError{
		Backend: "tavily", Code: backends.ErrCodeNetwork, Err: errors.New("connection refused"),
	}}
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{URL: "https://b", Engine: "brave"}}}
	bm := breaker.New()
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5

	p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, tavily, brave)
	_, _ = p.Search(context.Background(), "trip-tavily-with-network-fault")
	_, _ = p.Search(context.Background(), "trip-tavily-with-network-fault-again")
	if !bm.IsOpen("premium:tavily") {
		t.Fatal("real network fault did not open Tavily breaker")
	}
	tavilyCalls := tavily.calls.Load()
	braveCalls := brave.calls.Load()
	tavily.err = nil
	tavily.results = []backends.SearchResult{{URL: "https://t", Engine: "tavily"}}
	beforeAttempts := counterSumForLabels(t, "searxng_gateway_provider_attempts_total", map[string]string{"provider": "tavily"})
	beforeSkips := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "tavily", "reason": "breaker_open"})
	if _, err := p.Search(context.Background(), "open-breaker-roundrobin"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if got := tavily.calls.Load(); got != tavilyCalls {
		t.Fatalf("tavily calls = %d, want %d while breaker is open", got, tavilyCalls)
	}
	if got := brave.calls.Load(); got != braveCalls+1 {
		t.Fatalf("brave calls delta = %d, want 1 (open breaker must not suppress the others)", got-braveCalls)
	}
	if delta := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "tavily", "reason": "breaker_open"}) - beforeSkips; delta != 1 {
		t.Fatalf("tavily breaker_open skips delta = %v, want 1", delta)
	}
	if delta := counterSumForLabels(t, "searxng_gateway_provider_attempts_total", map[string]string{"provider": "tavily"}) - beforeAttempts; delta != 0 {
		t.Fatalf("tavily attempt delta while breaker open = %v, want 0", delta)
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility",
		map[string]string{"provider": "tavily", "reason": "breaker_open"}); got != 1 {
		t.Fatalf("tavily eligibility breaker_open = %v, want 1", got)
	}
}

// Requirement group 2: a credits-exhausted response trips the breaker (the
// Parallel rule), and the provider is excluded afterwards.
func TestCreditsExhaustedResponseTripsBreaker(t *testing.T) {
	metrics.Init()
	cases := []struct {
		name string
		err  error
	}{
		{"400_credits", &backends.BackendError{Backend: "parallel", Code: 400, Err: errors.New("not enough credits")}},
		{"402_payment", &backends.BackendError{Backend: "parallel", Code: 402, Err: errors.New("payment required")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeBackend{name: "parallel", avail: true, err: tc.err}
			bm := breaker.New()
			c, _ := cache.New(10, 0)
			cfg := newCfg()
			cfg.SufficientMinResults = 5
			beforeQuota := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
				map[string]string{"provider": "parallel", "phase": "primary", "outcome": "quota"})
			p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, backend)

			_, _ = p.Search(context.Background(), "credits-exhausted-"+tc.name)
			_, _ = p.Search(context.Background(), "credits-exhausted-"+tc.name+"-again")
			if !bm.IsOpen("premium:parallel") {
				t.Fatal("credits-exhausted response did not trip the Parallel breaker")
			}
			if got := gaugeValueForLabels(t, "searxng_gateway_circuit_breaker_state",
				map[string]string{"engine": "premium:parallel"}); got != 2 {
				t.Fatalf("breaker state gauge immediately after quota trip = %v, want 2", got)
			}
			if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility",
				map[string]string{"provider": "parallel", "reason": "breaker_open"}); got != 1 {
				t.Fatalf("eligibility immediately after quota trip = %v, want breaker_open one-hot", got)
			}
			if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
				map[string]string{"provider": "parallel", "phase": "primary", "outcome": "quota"}) - beforeQuota; delta != 2 {
				t.Fatalf("quota outcome delta = %v, want 2 consecutive failures", delta)
			}
			calls := backend.calls.Load()
			_, _ = p.Search(context.Background(), "credits-exhausted-"+tc.name+"-again")
			if got := backend.calls.Load(); got != calls {
				t.Fatalf("provider called while breaker open: %d, want %d", got, calls)
			}
		})
	}
}

// Requirement group 4: caller cancellation and overall request deadline never
// trip; query-validation errors do not trip.
func TestCancellationAndValidationDoNotTrip(t *testing.T) {
	metrics.Init()
	t.Run("caller_cancellation", func(t *testing.T) {
		beforeCancelled := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
			map[string]string{"provider": "brave", "phase": "primary", "outcome": "cancelled"})
		started := make(chan struct{})
		backend := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		bm := breaker.New()
		c, _ := cache.New(10, 0)
		cfg := newCfg()
		p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, backend)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _, _ = p.Search(ctx, "cancel-notrip"); close(done) }()
		<-started
		cancel()
		<-done
		if bm.IsOpen("premium:brave") {
			t.Fatal("caller cancellation tripped the breaker")
		}
		if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
			map[string]string{"provider": "brave", "phase": "primary", "outcome": "cancelled"}) - beforeCancelled; delta != 1 {
			t.Fatalf("cancelled attempt delta = %v, want exactly 1", delta)
		}
	})
	t.Run("overall_deadline", func(t *testing.T) {
		backend := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		bm := breaker.New()
		c, _ := cache.New(10, 0)
		cfg := newCfg()
		cfg.FallbackTimeout = 60 * time.Millisecond
		cfg.SearxngTimeout = time.Second
		p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, backend)
		_, _ = p.Search(context.Background(), "deadline-notrip")
		if bm.IsOpen("premium:brave") {
			t.Fatal("overall request deadline tripped the breaker")
		}
	})
	t.Run("query_validation", func(t *testing.T) {
		for _, code := range []int{400, 422} {
			backend := &fakeBackend{name: "exa", avail: true, err: &backends.BackendError{
				Backend: "exa", Code: code, Err: errors.New("invalid query parameter"),
			}}
			bm := breaker.New()
			c, _ := cache.New(10, 0)
			cfg := newCfg()
			beforeReqErr := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
				map[string]string{"provider": "exa", "phase": "primary", "outcome": "request_error"})
			p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, backend)
			_, _ = p.Search(context.Background(), fmt.Sprintf("validation-%d", code))
			if bm.IsOpen("premium:exa") {
				t.Fatalf("query-validation %d tripped the breaker", code)
			}
			if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
				map[string]string{"provider": "exa", "phase": "primary", "outcome": "request_error"}) - beforeReqErr; delta != 1 {
				t.Fatalf("validation %d request_error delta = %v, want 1", code, delta)
			}
		}
	})
}

// Requirement group 5: an empty result set is an "empty" outcome, does not
// trip, and is counted exactly once.
func TestEmptyResultOutcomeNoTrip(t *testing.T) {
	metrics.Init()
	backend := &fakeBackend{name: "exa", avail: true, results: nil}
	bm := breaker.New()
	c, _ := cache.New(10, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{URL: "https://sx", Engine: "wikipedia"}}}}
	beforeEmpty := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "exa", "phase": "primary", "outcome": "empty"})
	p := newTestProxy(cfg, sx, c, bm, backend)
	if _, err := p.Search(context.Background(), "empty-outcome"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "exa", "phase": "primary", "outcome": "empty"}) - beforeEmpty; delta != 1 {
		t.Fatalf("empty outcome delta = %v, want exactly 1", delta)
	}
	if bm.IsOpen("premium:exa") {
		t.Fatal("empty result set tripped the breaker")
	}
}

// TestProxyHalfOpenAdmitsSingleRealSearchUnderConcurrency verifies admission
// at the proxy/backend boundary (not just at Manager.Execute): one real Search
// remains in flight as the half-open probe while concurrent requests are
// skipped without invoking Search a second time.
func TestProxyHalfOpenAdmitsSingleRealSearchUnderConcurrency(t *testing.T) {
	metrics.Init()
	var calls atomic.Int64
	probeStarted := make(chan struct{})
	probeRelease := make(chan struct{})
	backend := &contextBackend{name: "brave", fn: func(context.Context) ([]backends.SearchResult, error) {
		invocation := calls.Add(1)
		if invocation <= 2 {
			return nil, &backends.BackendError{Backend: "brave", Code: backends.ErrCodeNetwork, Err: errors.New("connection refused")}
		}
		if invocation == 3 {
			close(probeStarted)
			<-probeRelease
		}
		return []backends.SearchResult{{URL: "https://probe.example", Engine: "brave"}}, nil
	}}
	bm := breaker.NewWithSettings(func(engine string) breaker.EngineSettings {
		return breaker.EngineSettings{Name: engine, MaxRequests: 1, Timeout: 30 * time.Millisecond}
	})
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 1
	sx := &fakeSearxng{resp: &searxng.Response{}}
	p := newTestProxy(cfg, sx, c, bm, backend)
	_, _ = p.Search(context.Background(), "prime-breaker-open")
	_, _ = p.Search(context.Background(), "prime-breaker-open-again")
	if !bm.IsOpen("premium:brave") {
		t.Fatal("initial network fault did not open breaker")
	}
	deadline := time.Now().Add(time.Second)
	for bm.State("premium:brave") != gobreaker.StateHalfOpen && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if bm.State("premium:brave") != gobreaker.StateHalfOpen {
		t.Fatal("breaker did not enter half-open")
	}

	const concurrentRequests = 8
	beforeSkips := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "brave", "reason": "breaker_open"})
	beforeCanary := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "brave", "phase": "canary", "outcome": "success"})
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _ = p.Search(context.Background(), fmt.Sprintf("parallel-probe-%d", i))
		}(i)
	}
	close(start)
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		close(probeRelease)
		t.Fatal("no real provider Search admitted as half-open probe")
	}

	// Keep the real probe blocked while every other concurrent request gets its
	// breaker_open skip. This proves there was no second Search call in flight.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "brave", "reason": "breaker_open"})-beforeSkips < concurrentRequests-1 {
		select {
		case <-tick.C:
		case <-time.After(time.Second):
			close(probeRelease)
			t.Fatal("concurrent requests did not all receive breaker_open skips")
		}
	}
	if got := calls.Load(); got != 3 {
		close(probeRelease)
		t.Fatalf("provider Search invocations while probe held = %d, want two initial failures + one probe", got)
	}
	close(probeRelease)
	wg.Wait()
	if got := calls.Load(); got != 3 {
		t.Fatalf("provider Search invocations = %d, want exactly 3", got)
	}
	if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "brave", "phase": "canary", "outcome": "success"}) - beforeCanary; delta != 1 {
		t.Fatalf("real half-open probe attempt delta = %v, want 1", delta)
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_circuit_breaker_state",
		map[string]string{"engine": "premium:brave"}); got != 0 {
		t.Fatalf("breaker state gauge after successful probe = %v, want closed (0)", got)
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility",
		map[string]string{"provider": "brave", "reason": "eligible"}); got != 1 {
		t.Fatalf("eligibility after successful probe = %v, want eligible one-hot", got)
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility",
		map[string]string{"provider": "brave", "reason": "breaker_open"}); got != 0 {
		t.Fatalf("breaker_open eligibility after recovery = %v, want 0", got)
	}
	if _, err := p.Search(context.Background(), "call-after-probe-recovery"); err != nil {
		t.Fatalf("subsequent request after recovered probe: %v", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("provider Search calls after recovered probe = %d, want 4 (two initial faults, probe, subsequent request)", got)
	}
}

// Requirement group 6: every actual call records exactly one terminal outcome;
// every exclusion has a reason; initialisation cannot reset an open breaker.
func TestProviderAttemptsOneOutcomeAndSkips(t *testing.T) {
	metrics.Init()
	brave := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{URL: "https://b", Engine: "brave"}}}
	unavail := &fakeBackend{name: "exa", avail: false}
	bm := breaker.New()
	c, _ := cache.New(10, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 5

	beforeSuccess := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "brave", "phase": "primary", "outcome": "success"})
	beforeExaAttempts := counterSumForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "exa"})
	beforeSkip := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "exa", "reason": "missing_key"})
	p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, brave, unavail)
	if _, err := p.Search(context.Background(), "one-outcome"); err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if delta := counterValueForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "brave", "phase": "primary", "outcome": "success"}) - beforeSuccess; delta != 1 {
		t.Fatalf("brave success attempts delta = %v, want exactly 1", delta)
	}
	if delta := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
		map[string]string{"provider": "exa", "reason": "missing_key"}) - beforeSkip; delta != 1 {
		t.Fatalf("exa missing_key skips delta = %v, want 1", delta)
	}
	if delta := counterSumForLabels(t, "searxng_gateway_provider_attempts_total",
		map[string]string{"provider": "exa"}) - beforeExaAttempts; delta != 0 {
		t.Fatalf("exa attempts delta = %v, want 0 (a skip is not an attempt)", delta)
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility",
		map[string]string{"provider": "exa", "reason": "missing_key"}); got != 1 {
		t.Fatalf("exa eligibility missing_key = %v, want 1", got)
	}

	// Initialisation must not reset an open breaker to zero.
	faulty := &fakeBackend{name: "jina", avail: true, err: &backends.BackendError{
		Backend: "jina", Code: backends.ErrCodeAuth, Err: errors.New("invalid api key"),
	}}
	cfg2 := newCfg()
	cfg2.SufficientMinResults = 5
	p2 := newTestProxy(cfg2, &fakeSearxng{resp: &searxng.Response{}}, c, bm, faulty)
	_, _ = p2.Search(context.Background(), "init-open-breaker")
	_, _ = p2.Search(context.Background(), "init-open-breaker-consecutive")
	if !bm.IsOpen("premium:jina") {
		t.Fatal("auth failure did not open the jina breaker")
	}
	if got := gaugeValueForLabels(t, "searxng_gateway_circuit_breaker_state",
		map[string]string{"engine": "premium:jina"}); got != 2 {
		t.Fatalf("jina breaker gauge = %v, want 2 (open)", got)
	}
	// A second request re-runs initProviderEligibility; the open breaker must
	// stay at 2, not be reset to 0.
	_, _ = p2.Search(context.Background(), "init-open-breaker-again")
	if got := gaugeValueForLabels(t, "searxng_gateway_circuit_breaker_state",
		map[string]string{"engine": "premium:jina"}); got != 2 {
		t.Fatalf("initialisation reset an open breaker to %v, want 2", got)
	}
}

// TestEligibilitySeededOnSearxngOnlyRequest: when SearXNG meets the target the
// premium stage makes only its single forced call, yet the eligibility gauge of
// a configured provider must still be refreshed (eligible=1) and an unconfigured
// provider's missing_key skip must be counted EXACTLY once per uncached request.
// Seeding runs once per uncached request before the SearXNG stage, so doubled
// seeding (delta 2 after one request) fails here.
func TestEligibilitySeededOnSearxngOnlyRequest(t *testing.T) {
	metrics.Init()
	metrics.SetProviderEligibility("serper", func() string { return "missing_key" })
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX1", URL: "https://elig-sx1.com", Engine: "wikipedia"},
		{Title: "SX2", URL: "https://elig-sx2.com", Engine: "wikipedia"},
	}}}
	forced := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "P", URL: "https://elig-premium.com", Engine: "brave"},
	}}
	untouched := &fakeBackend{name: "serper", avail: true, results: []backends.SearchResult{
		{Title: "S", URL: "https://elig-serper.com", Engine: "serper"},
	}}
	unconfigured := &fakeBackend{name: "tavily", avail: false}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sx, c, breaker.New(), forced, untouched, unconfigured)
	skipLabels := map[string]string{"provider": "tavily", "reason": "missing_key"}
	serperEligible := map[string]string{"provider": "serper", "reason": "eligible"}

	before := counterValueForLabels(t, "searxng_gateway_provider_skips_total", skipLabels)
	for i := 1; i <= 2; i++ {
		if _, err := p.Search(context.Background(), fmt.Sprintf("eligibility-searxng-only-%d", i)); err != nil {
			t.Fatalf("Search %d error = %v", i, err)
		}
		if got := counterValueForLabels(t, "searxng_gateway_provider_skips_total", skipLabels) - before; got != float64(i) {
			t.Fatalf("after %d uncached request(s): tavily missing_key skips delta = %v, want exactly %d (once per request)", i, got, i)
		}
		if i == 1 {
			// Request 1's single forced call goes to brave (round-robin index 0), so
			// serper is untouched and its gauge can only have been flipped by seeding.
			if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility", serperEligible); got != 1 {
				t.Fatalf("serper eligible gauge after request 1 = %v, want 1 from per-request seeding", got)
			}
		}
	}
	// Round-robin rotates across requests, so the forced call may land on either
	// premium provider; the invariant is exactly one admitted call per request.
	if got := forced.calls.Load() + untouched.calls.Load(); got != 2 {
		t.Fatalf("premium calls over 2 requests = %d, want exactly 2 (one admitted call each)", got)
	}
}

// TestEligibilitySeededWhenPremiumStageNeverRuns: seeding happens before any
// stage, so it is counted even when SearXNG consumes the whole request budget
// and the premium stage never starts. Seeding inside the premium loop would
// leave this delta at 0.
func TestEligibilitySeededWhenPremiumStageNeverRuns(t *testing.T) {
	metrics.Init()
	sx := &contextSearxng{fn: func(ctx context.Context) (*searxng.Response, error) { <-ctx.Done(); return nil, ctx.Err() }}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "P", URL: "https://never.com", Engine: "brave"}}}
	unconfigured := &fakeBackend{name: "tavily", avail: false}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.FallbackTimeout = 40 * time.Millisecond
	cfg.SearxngTimeout = time.Second // capped to the remaining parent budget
	p := newTestProxy(cfg, sx, c, breaker.New(), fb, unconfigured)
	skipLabels := map[string]string{"provider": "tavily", "reason": "missing_key"}
	before := counterValueForLabels(t, "searxng_gateway_provider_skips_total", skipLabels)
	if _, err := p.Search(context.Background(), "eligibility-premium-never-runs"); err == nil {
		t.Fatal("expected an error: SearXNG consumed the whole budget")
	}
	if got := fb.calls.Load(); got != 0 {
		t.Fatalf("premium calls = %d, want 0 (budget exhausted before the premium stage)", got)
	}
	if got := counterValueForLabels(t, "searxng_gateway_provider_skips_total", skipLabels) - before; got != 1 {
		t.Fatalf("tavily missing_key skips delta = %v, want exactly 1 even though premium never ran", got)
	}
}

// sufficientSearxng returns a fake SearXNG that alone meets a target of 2.
func sufficientSearxng() *fakeSearxng {
	return &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX1", URL: "https://forced-sx1.com", Engine: "wikipedia"},
		{Title: "SX2", URL: "https://forced-sx2.com", Engine: "wikipedia"},
	}}}
}

// TestForcedPremiumCallFailureDoesNotTriggerSecondCall: SearXNG met the target
// and the one forced premium call errors -> stop, no second provider.
func TestForcedPremiumCallFailureDoesNotTriggerSecondCall(t *testing.T) {
	metrics.Init()
	first := &fakeBackend{name: "brave", avail: true, err: errors.New("upstream failure")}
	second := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "E", URL: "https://forced-e.com", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sufficientSearxng(), c, breaker.New(), first, second)
	out, err := p.Search(context.Background(), "forced-call-error")
	if err != nil {
		t.Fatalf("Search error = %v, want SearXNG results served", err)
	}
	if got := first.calls.Load(); got != 1 {
		t.Fatalf("first provider calls = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("second provider calls = %d, want 0 after a failed forced call", got)
	}
	if len(out.Results) != 2 {
		t.Fatalf("result count = %d, want 2 SearXNG results", len(out.Results))
	}
}

// TestForcedPremiumCallEmptyDoesNotTriggerSecondCall: same as above, but the
// forced premium call returns an empty result set.
func TestForcedPremiumCallEmptyDoesNotTriggerSecondCall(t *testing.T) {
	metrics.Init()
	first := &fakeBackend{name: "brave", avail: true, results: nil}
	second := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "E", URL: "https://forced-e2.com", Engine: "exa"}}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 2
	p := newTestProxy(cfg, sufficientSearxng(), c, breaker.New(), first, second)
	out, err := p.Search(context.Background(), "forced-call-empty")
	if err != nil {
		t.Fatalf("Search error = %v, want SearXNG results served", err)
	}
	if got := first.calls.Load(); got != 1 {
		t.Fatalf("first provider calls = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("second provider calls = %d, want 0 after an empty forced call", got)
	}
	if len(out.Results) != 2 {
		t.Fatalf("result count = %d, want 2 SearXNG results", len(out.Results))
	}
}

// TestSkippedProviderDoesNotCountAsForcedCall: a provider that is skipped AFTER
// selection (open breaker; admission refused) is not an admitted call, so the
// round-robin moves to the next provider and exactly one admitted call happens.
// (An unconfigured provider cannot reach this branch: NextAvailable filters it
// out before selection; its missing_key accounting is pinned in
// TestEligibilitySeededOnSearxngOnlyRequest.)
func TestSkippedProviderDoesNotCountAsForcedCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		// prep readies the "premium:brave" breaker and returns a cleanup.
		prep func(t *testing.T, bm *breaker.Manager) (cleanup func())
	}{
		// IsOpen(id) is true: skipped before admission.
		{"breaker_open", func(t *testing.T, bm *breaker.Manager) func() {
			for i := 0; i < 2; i++ {
				_, _ = bm.Execute("premium:brave", func() (interface{}, error) { return nil, errors.New("fault") })
			}
			if !bm.IsOpen("premium:brave") {
				t.Fatal("precondition: expected the brave breaker to be open")
			}
			return func() {}
		}},
		// Half-open with its single probe slot held: IsOpen is false, so the
		// provider is selected and sent to admission, which refuses it
		// (admitted==false): skipped AFTER selection and admission.
		{"admission_refused", func(t *testing.T, bm *breaker.Manager) func() {
			for i := 0; i < 2; i++ {
				_, _ = bm.Execute("premium:brave", func() (interface{}, error) { return nil, errors.New("fault") })
			}
			deadline := time.Now().Add(time.Second)
			for bm.State("premium:brave") != gobreaker.StateHalfOpen && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if bm.State("premium:brave") != gobreaker.StateHalfOpen {
				t.Fatal("precondition: breaker did not enter half-open")
			}
			held := make(chan struct{})
			release := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = bm.Execute("premium:brave", func() (interface{}, error) {
					close(held)
					<-release
					return nil, nil
				})
			}()
			<-held
			if bm.IsOpen("premium:brave") {
				t.Fatal("precondition: half-open breaker must not report open")
			}
			return func() { close(release); <-done }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.Init()
			first := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{{Title: "B", URL: "https://skip-b.com", Engine: "brave"}}}
			second := &fakeBackend{name: "exa", avail: true, results: []backends.SearchResult{{Title: "E", URL: "https://skip-e.com", Engine: "exa"}}}
			third := &fakeBackend{name: "serper", avail: true, results: []backends.SearchResult{{Title: "S", URL: "https://skip-s.com", Engine: "serper"}}}
			bm := breaker.NewWithSettings(func(engine string) breaker.EngineSettings {
				return breaker.EngineSettings{Name: engine, MaxRequests: 1, Timeout: 30 * time.Millisecond}
			})
			if tc.name == "breaker_open" {
				// Keep the breaker open for the whole request.
				bm = breaker.New()
			}
			cleanup := tc.prep(t, bm)
			defer cleanup()
			c, _ := cache.New(100, 0)
			cfg := newCfg()
			cfg.SufficientMinResults = 2
			p := newTestProxy(cfg, sufficientSearxng(), c, bm, first, second, third)
			beforeSkips := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
				map[string]string{"provider": "brave", "reason": "breaker_open"})
			if _, err := p.Search(context.Background(), "skipped-"+tc.name); err != nil {
				t.Fatalf("Search error = %v", err)
			}
			if got := first.calls.Load(); got != 0 {
				t.Fatalf("skipped provider calls = %d, want 0", got)
			}
			if delta := counterValueForLabels(t, "searxng_gateway_provider_skips_total",
				map[string]string{"provider": "brave", "reason": "breaker_open"}) - beforeSkips; delta != 1 {
				t.Fatalf("brave breaker_open skip delta = %v, want 1 (the skip branch must be exercised)", delta)
			}
			// Round-robin picks the next non-skipped provider; which of the two
			// remaining is chosen depends on the rotating index, so assert the
			// invariant: exactly one admitted call across them.
			if got := second.calls.Load() + third.calls.Load(); got != 1 {
				t.Fatalf("admitted calls after the skipped provider = %d, want exactly 1", got)
			}
		})
	}
}

// TestOutputOrderingPremiumFirst: premium results come first in arrival order,
// then SearXNG results; a URL both stages returned appears once, at the premium
// position.
func TestOutputOrderingPremiumFirst(t *testing.T) {
	metrics.Init()
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "SX-shared", URL: "https://order-shared.com", Engine: "wikipedia"},
		{Title: "SX-only", URL: "https://order-sx-only.com", Engine: "wikipedia"},
	}}}
	fb := &fakeBackend{name: "brave", avail: true, results: []backends.SearchResult{
		{Title: "P1", URL: "https://order-p1.com", Engine: "brave"},
		{Title: "P-shared", URL: "https://order-shared.com", Engine: "brave"},
	}}
	c, _ := cache.New(100, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 3
	p := newTestProxy(cfg, sx, c, breaker.New(), fb)
	out, err := p.Search(context.Background(), "ordering-premium-first")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	want := []string{"https://order-p1.com", "https://order-shared.com", "https://order-sx-only.com"}
	if len(out.Results) != len(want) {
		t.Fatalf("result count = %d, want %d", len(out.Results), len(want))
	}
	for i, u := range want {
		if out.Results[i].URL != u {
			t.Fatalf("result[%d] = %q, want %q (full order %v)", i, out.Results[i].URL, u, resultURLs(out.Results))
		}
	}
	if out.Results[1].Engine != "brave" {
		t.Fatalf("shared URL engine = %q, want premium (brave) at the premium position", out.Results[1].Engine)
	}
}

func resultURLs(rs []searxng.Result) []string {
	urls := make([]string, len(rs))
	for i, r := range rs {
		urls[i] = r.URL
	}
	return urls
}
