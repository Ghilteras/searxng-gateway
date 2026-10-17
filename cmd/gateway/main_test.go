package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/proxy"
	"sx/internal/searxng"
)

type stubSearxng struct {
	resp  *searxng.Response
	err   error
	calls atomic.Int64
}

func (s *stubSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
	s.calls.Add(1)
	return s.resp, s.err
}

// stubBackend implements backends.SearchBackend for testing.
type stubBackend struct {
	name    string
	results []backends.SearchResult
	err     error
	avail   bool
	calls   atomic.Int64
}

func (s *stubBackend) Name() string      { return s.name }
func (s *stubBackend) IsAvailable() bool { return s.avail }
func (s *stubBackend) Search(_ backends.SearchOptions) ([]backends.SearchResult, error) {
	s.calls.Add(1)
	return s.results, s.err
}

func setupRouter(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{
		FallbackTimeout:      5 * time.Second,
		MetricsPath:          "/metrics",
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  180 * time.Second,
		SufficientMinResults: 2,
	}
	c, _ := cache.New(10, 0)
	metrics.Init()

	sx := &stubSearxng{resp: &searxng.Response{Results: []searxng.Result{{Engine: "wikipedia"}}}}
	fb := &stubBackend{
		name: "brave",
		results: []backends.SearchResult{
			{Title: "T", URL: "u", Content: "d", Engine: "brave"},
		},
		avail: true,
	}
	mgr := backends.NewManager()
	mgr.Register(fb)
	_ = mgr.SetFallbacks([]string{"brave"})

	return newRouter(proxy.New(cfg, sx, c, breaker.New(), mgr), cfg)
}

func TestHealthz(t *testing.T) {
	r := setupRouter(t)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest("GET", "/healthz", nil))
	if rr.Code != 200 {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestSearchEndpointFallback(t *testing.T) {
	r := setupRouter(t)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest("GET", "/search?q=hello&format=json", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Query   string           `json:"query"`
		Results []searxng.Result `json:"results"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode error = %v", err)
	}
	if len(body.Results) == 0 {
		t.Error("expected non-empty results (Brave fallback)")
	}
	if body.Query != "hello" {
		t.Errorf("query = %q, want effective query hello", body.Query)
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "json") {
		t.Errorf("Content-Type = %q, want json", rr.Header().Get("Content-Type"))
	}
}

func TestSearchFailureDoesNotDiscloseProviderError(t *testing.T) {
	const sentinel = "SENTINEL-PROVIDER-BODY-DO-NOT-LEAK"
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })

	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: 20 * time.Millisecond, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 1}
	c, err := cache.New(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	mgr := backends.NewManager()
	_ = mgr.SetFallbacks(nil)
	r := newRouter(proxy.New(cfg, &stubSearxng{err: errors.New("all backends failed: provider HTTP 432: " + sentinel)}, c, breaker.New(), mgr), cfg)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=disclosure", nil))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, sentinel) || strings.Contains(body, "context deadline exceeded") || strings.Contains(body, "all backends failed") {
		t.Fatalf("provider/internal error disclosed in body: %q", body)
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", rr.Header().Get("Content-Type"))
	}
	if !strings.Contains(body, `"error":"search_failed"`) {
		t.Fatalf("body = %q, want sanitized JSON error", body)
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatalf("server log disclosed provider sentinel: %q", logs.String())
	}
}

func TestSearchEndpointCleanNoResults(t *testing.T) {
	metrics.Init()
	c, err := cache.New(10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: time.Second, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 2}
	mgr := backends.NewManager()
	mgr.Register(&stubBackend{name: "brave", avail: true})
	_ = mgr.SetFallbacks([]string{"brave"})
	r := newRouter(proxy.New(cfg, &stubSearxng{resp: &searxng.Response{}}, c, breaker.New(), mgr), cfg)
	before := requestOutcomeValue(t, "no_results")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=clean-empty", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if strings.TrimSpace(rr.Body.String()) != `{"query":"clean-empty","results":[]}` {
		t.Fatalf("body = %q, want query metadata and empty result set", rr.Body.String())
	}
	if got := requestOutcomeValue(t, "no_results") - before; got != 1 {
		t.Fatalf("no_results delta = %v, want 1", got)
	}
}

func TestSearchEndpointCAPTCHADegradedNoResults(t *testing.T) {
	metrics.Init()
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	c, _ := cache.New(10, 0)
	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: time.Second, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 10}
	mgr := backends.NewManager()
	for _, name := range []string{"brave", "exa", "parallel"} {
		mgr.Register(&stubBackend{name: name, avail: true})
	}
	_ = mgr.SetFallbacks([]string{"brave", "exa", "parallel"})
	sx := &stubSearxng{resp: &searxng.Response{UnresponsiveEngines: [][]string{{"duckduckgo", "CAPTCHA"}}}}
	r := newRouter(proxy.New(cfg, sx, c, breaker.New(), mgr), cfg)
	before := requestOutcomeValue(t, "no_results")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=captcha-degraded", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if strings.TrimSpace(rr.Body.String()) != `{"query":"captcha-degraded","results":[]}` {
		t.Fatalf("body = %q, want query and empty results", rr.Body.String())
	}
	if got := requestOutcomeValue(t, "no_results") - before; got != 1 {
		t.Fatalf("no_results delta = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), `"reason":"exhausted_with_engine_failures"`) {
		t.Fatalf("diagnostic omitted degraded-exhaustion reason: %s", logs.String())
	}
}

func TestSearchEndpointProductionShapedOptionalProviders(t *testing.T) {
	metrics.Init()
	c, _ := cache.New(10, 0)
	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: time.Second, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 10}
	mgr := backends.NewManager()
	configured := []*stubBackend{{name: "brave", avail: true}, {name: "exa", avail: true}, {name: "parallel", avail: true}}
	optional := []*stubBackend{{name: "serper", avail: false}, {name: "tavily", avail: false}}
	names := make([]string, 0, 5)
	for _, backend := range append(configured, optional...) {
		mgr.Register(backend)
		names = append(names, backend.name)
	}
	_ = mgr.SetFallbacks(names)
	r := newRouter(proxy.New(cfg, &stubSearxng{resp: &searxng.Response{}}, c, breaker.New(), mgr), cfg)
	before := requestOutcomeValue(t, "no_results")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=production-shaped", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if strings.TrimSpace(rr.Body.String()) != `{"query":"production-shaped","results":[]}` {
		t.Fatalf("body = %q, want query and empty results", rr.Body.String())
	}
	if got := requestOutcomeValue(t, "no_results") - before; got != 1 {
		t.Fatalf("no_results delta = %v, want 1", got)
	}
	var attempts int64
	for _, backend := range configured {
		if got := backend.calls.Load(); got != 1 {
			t.Errorf("%s admitted calls = %d, want 1", backend.name, got)
		}
		attempts += backend.calls.Load()
	}
	for _, backend := range optional {
		if got := backend.calls.Load(); got != 0 {
			t.Errorf("unavailable %s calls = %d, want 0", backend.name, got)
		}
	}
	if attempts != 3 {
		t.Fatalf("configured premium attempts = %d, want 3", attempts)
	}
}

func TestSearchRejectsWhitespaceQueriesWithoutUpstreamWork(t *testing.T) {
	metrics.Init()
	sx := &stubSearxng{resp: &searxng.Response{Results: []searxng.Result{{URL: "https://must-not-run.example"}}}}
	backend := &stubBackend{name: "brave", avail: true, results: []backends.SearchResult{{URL: "https://must-not-run.example", Engine: "brave"}}}
	c, _ := cache.New(10, time.Hour)
	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: time.Second, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 2}
	mgr := backends.NewManager()
	mgr.Register(backend)
	_ = mgr.SetFallbacks([]string{"brave"})
	r := newRouter(proxy.New(cfg, sx, c, breaker.New(), mgr), cfg)
	for _, target := range []string{"/search?q=%20", "/search?q=%09"} {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400; body=%q", target, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "q must not be blank") {
			t.Errorf("%s body = %q, want clear blank-query message", target, rr.Body.String())
		}
	}
	if got := sx.calls.Load(); got != 0 {
		t.Fatalf("SearXNG calls = %d, want 0", got)
	}
	if got := backend.calls.Load(); got != 0 {
		t.Fatalf("provider calls = %d, want 0", got)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("cache entries = %d, want 0", got)
	}
}

func TestSearchCacheHitCarriesCurrentQuery(t *testing.T) {
	r := setupRouter(t)
	for _, tc := range []struct{ target, query string }{
		{"/search?q=Fresh+Query", "Fresh Query"},
		{"/search?q=fresh+QUERY", "fresh QUERY"},
	} {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
		if rr.Code < 200 || rr.Code >= 300 {
			t.Fatalf("%s status = %d, want 2xx", tc.target, rr.Code)
		}
		var body struct {
			Query   string           `json:"query"`
			Results []searxng.Result `json:"results"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("%s JSON decode: %v", tc.target, err)
		}
		if body.Query != tc.query || len(body.Results) == 0 {
			t.Fatalf("%s body query=%q results=%d; want query=%q and nonempty results", tc.target, body.Query, len(body.Results), tc.query)
		}
	}
}

func requestOutcomeValue(t *testing.T, outcome string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "searxng_gateway_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) == 1 && metric.GetLabel()[0].GetValue() == outcome {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestSearchEndpointTavilyStructuralZeroResultHTTP(t *testing.T) {
	for _, tc := range []struct {
		status int
		reason string
	}{
		{status: http.StatusUnauthorized, reason: "credentials_rejected"},
		{status: 432, reason: "quota_exhausted"},
		{status: 433, reason: "quota_exhausted"},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			metrics.Init()
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			t.Cleanup(func() { log.SetOutput(previous) })
			r, calls := newHermeticTavilyRouter(t, tc.status, "account issue")
			beforeNoResults := requestOutcomeValue(t, "no_results")
			beforeFail := requestOutcomeValue(t, "fallback_fail")
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=structural-"+strings.ReplaceAll(http.StatusText(tc.status), " ", "-"), nil))
			assertZeroResultHTTP(t, rr, http.StatusOK)
			if got := requestOutcomeValue(t, "no_results") - beforeNoResults; got != 1 {
				t.Fatalf("no_results delta = %v, want 1", got)
			}
			if got := requestOutcomeValue(t, "fallback_fail") - beforeFail; got != 0 {
				t.Fatalf("fallback_fail delta = %v, want 0", got)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("Tavily HTTP calls = %d, want 1", got)
			}
			if !strings.Contains(logs.String(), `"eligibility_reason":"`+tc.reason+`"`) {
				t.Fatalf("Tavily structural eligibility reason %q missing from diagnostics: %s", tc.reason, logs.String())
			}
		})
	}
}

func TestSearchEndpointTavilyStructuralOpenBreakerZeroResultHTTP(t *testing.T) {
	metrics.Init()
	r, calls := newHermeticTavilyRouter(t, 432, "quota exhausted")
	beforeNoResults := requestOutcomeValue(t, "no_results")
	beforeFail := requestOutcomeValue(t, "fallback_fail")
	for _, query := range []string{"breaker-trip-one", "breaker-trip-two"} {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q="+query, nil))
		assertZeroResultHTTP(t, rr, http.StatusOK)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Tavily HTTP calls before open-breaker request = %d, want 2", got)
	}
	if got := providerEligibilityValue(t, "tavily", "quota_exhausted"); got != 1 {
		t.Fatalf("Tavily quota_exhausted eligibility = %v, want 1 after structural breaker trip", got)
	}
	beforeSkipped := providerSkipValue(t, "tavily", "breaker_open")
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=breaker-open-followup", nil))
	// no_results requires admittedTotal > 0: the gateway must have admitted a
	// premium call and observed it return empty. With every provider skipped,
	// even for a frozen structural breaker cause, emptiness was not tested, so
	// fallback_fail/502 is the honest outcome.
	assertZeroResultHTTP(t, rr, http.StatusBadGateway)
	if got := calls.Load(); got != 2 {
		t.Fatalf("Tavily HTTP calls after breaker-open request = %d, want unchanged at 2", got)
	}
	if got := providerSkipValue(t, "tavily", "breaker_open") - beforeSkipped; got != 1 {
		t.Fatalf("breaker_open provider-skip delta = %v, want 1", got)
	}
	if got := requestOutcomeValue(t, "no_results") - beforeNoResults; got != 2 {
		t.Fatalf("no_results delta = %v, want 2", got)
	}
	if got := requestOutcomeValue(t, "fallback_fail") - beforeFail; got != 1 {
		t.Fatalf("fallback_fail delta = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), `"skip":"breaker_open","eligibility_reason":"quota_exhausted"`) {
		t.Fatalf("fallback diagnostic omitted frozen structural breaker reason: %s", logs.String())
	}
}

func TestSearchEndpointStructuralBreakerWithAdmittedEmptyProviderReturnsNoResults(t *testing.T) {
	metrics.Init()
	r, tavilyCalls, braveCalls := newHermeticTwoProviderRouter(t)
	beforeNoResults := requestOutcomeValue(t, "no_results")
	beforeFail := requestOutcomeValue(t, "fallback_fail")
	for _, query := range []string{"reachable-trip-one", "reachable-trip-two"} {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q="+query, nil))
		assertZeroResultHTTP(t, rr, http.StatusOK)
	}
	if got := tavilyCalls.Load(); got != 2 {
		t.Fatalf("Tavily calls before open-breaker request = %d, want 2", got)
	}
	if got := braveCalls.Load(); got != 2 {
		t.Fatalf("Brave calls before open-breaker request = %d, want 2", got)
	}
	if got := providerEligibilityValue(t, "tavily", "quota_exhausted"); got != 1 {
		t.Fatalf("Tavily quota_exhausted eligibility = %v, want 1", got)
	}
	beforeSkipped := providerSkipValue(t, "tavily", "breaker_open")
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=reachable-open-followup", nil))
	assertZeroResultHTTP(t, rr, http.StatusOK)
	if got := tavilyCalls.Load(); got != 2 {
		t.Fatalf("Tavily calls after breaker-open request = %d, want unchanged at 2", got)
	}
	if got := braveCalls.Load(); got != 3 {
		t.Fatalf("Brave calls after breaker-open request = %d, want 3 admitted empty calls", got)
	}
	if got := providerSkipValue(t, "tavily", "breaker_open") - beforeSkipped; got != 1 {
		t.Fatalf("breaker_open provider-skip delta = %v, want 1", got)
	}
	if got := requestOutcomeValue(t, "no_results") - beforeNoResults; got != 3 {
		t.Fatalf("no_results delta = %v, want 3", got)
	}
	if got := requestOutcomeValue(t, "fallback_fail") - beforeFail; got != 0 {
		t.Fatalf("fallback_fail delta = %v, want 0", got)
	}
	if !strings.Contains(logs.String(), `"provider":"brave","outcome":"empty"`) || !strings.Contains(logs.String(), `"skip":"breaker_open","eligibility_reason":"quota_exhausted"`) {
		t.Fatalf("reachable zero-result diagnostic omitted the admitted empty provider or frozen structural skip: %s", logs.String())
	}
}

func TestSearchEndpointTavilyUnprovenZeroResultFailuresVetoHTTP(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "403 forbidden", status: http.StatusForbidden, body: "forbidden"},
		{name: "429 rate limited", status: http.StatusTooManyRequests, body: "rate limited"},
		{name: "503 unavailable", status: http.StatusServiceUnavailable, body: "unavailable"},
		{name: "400 misleading quota text", status: http.StatusBadRequest, body: "credits and quota are mentioned but not annotated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.Init()
			r, calls := newHermeticTavilyRouter(t, tc.status, tc.body)
			beforeNoResults := requestOutcomeValue(t, "no_results")
			beforeFail := requestOutcomeValue(t, "fallback_fail")
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=unproven-"+strings.ReplaceAll(tc.name, " ", "-"), nil))
			assertZeroResultHTTP(t, rr, http.StatusBadGateway)
			if got := requestOutcomeValue(t, "fallback_fail") - beforeFail; got != 1 {
				t.Fatalf("fallback_fail delta = %v, want 1", got)
			}
			if got := requestOutcomeValue(t, "no_results") - beforeNoResults; got != 0 {
				t.Fatalf("no_results delta = %v, want 0", got)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("Tavily HTTP calls = %d, want 1", got)
			}
		})
	}
}

// newHermeticTavilyRouter preserves the gateway HTTP harness while using the
// production SearXNG HTTP client and the real Tavily backend. Both upstreams
// terminate at httptest servers; the key is a non-secret test marker.
func newHermeticTavilyRouter(t *testing.T, status int, body string) (http.Handler, *atomic.Int64) {
	t.Helper()
	searxngServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("format") != "json" {
			http.Error(w, "unexpected SearXNG request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	t.Cleanup(searxngServer.Close)

	var calls atomic.Int64
	tavilyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/search" {
			http.Error(w, "unexpected Tavily request", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer hermetic-test-key" {
			http.Error(w, "unexpected test authorization", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(tavilyServer.Close)

	tavily := backends.NewTavilyBackend("hermetic-test-key", time.Second, "basic", false, false)
	tavily.BaseURL = tavilyServer.URL + "/search"
	mgr := backends.NewManager()
	mgr.Register(tavily)
	if err := mgr.SetFallbacks([]string{"tavily"}); err != nil {
		t.Fatal(err)
	}
	c, err := cache.New(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		FallbackTimeout:      3 * time.Second,
		SearxngTimeout:       time.Second,
		MetricsPath:          "/metrics",
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  time.Minute,
		SufficientMinResults: 10,
	}
	return newRouter(proxy.New(cfg, searxng.New(searxngServer.URL, time.Second), c, breaker.New(), mgr), cfg), &calls
}

// newHermeticTwoProviderRouter uses real backends whose BaseURL fields are
// injectable. Brave proves an admitted empty result while Tavily's breaker is
// open with a frozen structural quota cause, exercising the reachable v3 shape
// through the gateway HTTP handler.
func newHermeticTwoProviderRouter(t *testing.T) (http.Handler, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	searxngServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("format") != "json" {
			http.Error(w, "unexpected SearXNG request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	t.Cleanup(searxngServer.Close)

	var tavilyCalls, braveCalls atomic.Int64
	tavilyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tavilyCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/search" {
			http.Error(w, "unexpected Tavily request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(432)
		_, _ = w.Write([]byte("quota exhausted"))
	}))
	t.Cleanup(tavilyServer.Close)
	braveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		braveCalls.Add(1)
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected Brave request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"web":{"results":[]}}`))
	}))
	t.Cleanup(braveServer.Close)

	tavily := backends.NewTavilyBackend("hermetic-test-key", time.Second, "basic", false, false)
	tavily.BaseURL = tavilyServer.URL + "/search"
	brave := backends.NewBraveBackend("hermetic-test-key", time.Second)
	brave.BaseURL = braveServer.URL
	mgr := backends.NewManager()
	mgr.Register(brave)
	mgr.Register(tavily)
	if err := mgr.SetFallbacks([]string{"brave", "tavily"}); err != nil {
		t.Fatal(err)
	}
	c, err := cache.New(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		FallbackTimeout:      3 * time.Second,
		SearxngTimeout:       time.Second,
		MetricsPath:          "/metrics",
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  time.Minute,
		SufficientMinResults: 10,
	}
	r := newRouter(proxy.New(cfg, searxng.New(searxngServer.URL, time.Second), c, breaker.New(), mgr), cfg)
	return r, &tavilyCalls, &braveCalls
}

func assertZeroResultHTTP(t *testing.T, rr *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if rr.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, wantStatus, rr.Body.String())
	}
	if wantStatus == http.StatusOK {
		var body struct {
			Query   string           `json:"query"`
			Results []searxng.Result `json:"results"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if body.Results == nil || len(body.Results) != 0 {
			t.Fatalf("results = %#v, want []", body.Results)
		}
		return
	}
	if !strings.Contains(rr.Body.String(), `"error":"search_failed"`) {
		t.Fatalf("502 body = %q, want sanitized search_failed error", rr.Body.String())
	}
}

func providerEligibilityValue(t *testing.T, provider, reason string) float64 {
	t.Helper()
	return gaugeValue(t, "searxng_gateway_provider_eligibility", map[string]string{"provider": provider, "reason": reason})
}

func providerSkipValue(t *testing.T, provider, reason string) float64 {
	t.Helper()
	return counterValue(t, "searxng_gateway_provider_skips_total", map[string]string{"provider": provider, "reason": reason})
}

func gaugeValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	return gatheredMetricValue(t, name, labels, false)
}

func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	return gatheredMetricValue(t, name, labels, true)
}

func gatheredMetricValue(t *testing.T, name string, labels map[string]string, counter bool) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matches := len(metric.GetLabel()) == len(labels)
			for _, label := range metric.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matches = false
					break
				}
			}
			if matches {
				if counter {
					return metric.GetCounter().GetValue()
				}
				return metric.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func TestSearchHandlerCreatesSanitizedServerSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) })
	r := setupRouter(t)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/search?q=QUERY-SENTINEL-39fa", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	found := false
	for _, s := range exporter.GetSpans() {
		if s.Name != "http.server /search" {
			continue
		}
		found = true
		if s.Status.Code != codes.Ok {
			t.Fatalf("server span status = %v", s.Status.Code)
		}
		for _, a := range s.Attributes {
			if strings.Contains(a.Value.AsString(), "QUERY-SENTINEL") {
				t.Fatalf("query leaked in server span: %v", a)
			}
		}
		for _, e := range s.Events {
			for _, a := range e.Attributes {
				if strings.Contains(a.Value.AsString(), "QUERY-SENTINEL") {
					t.Fatalf("query leaked in span event: %v", a)
				}
			}
		}
	}
	if !found {
		t.Fatal("/search server span missing")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	r := setupRouter(t)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != 200 {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "searxng_gateway_requests_total") {
		t.Error("metrics body missing searxng_gateway_requests_total")
	}
}

func TestSearchRequestDurationIncludesInvalidSuccessCacheAndError(t *testing.T) {
	metrics.Init()
	beforeRequest := histogramCount(t, "searxng_gateway_search_request_duration_seconds")
	beforeStage := histogramCount(t, "searxng_gateway_searxng_stage_duration_seconds")
	r := setupRouter(t)
	for _, target := range []string{"/search", "/search?q=handler-cache", "/search?q=handler-cache"} {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, httptest.NewRequest("GET", target, nil))
	}
	if got := histogramCount(t, "searxng_gateway_search_request_duration_seconds") - beforeRequest; got != 3 {
		t.Fatalf("handler duration samples = %d, want 3 (invalid, success, cache hit)", got)
	}
	if got := histogramCount(t, "searxng_gateway_searxng_stage_duration_seconds") - beforeStage; got != 1 {
		t.Fatalf("stage samples = %d, want 1 (no sample on cache hit/invalid)", got)
	}
	// The error handler path still records exactly one complete request sample.
	cfg := &config.Config{FallbackTimeout: time.Second, SearxngTimeout: 20 * time.Millisecond, MetricsPath: "/metrics", SearxngFailThreshold: 6, SearxngFailCooldown: time.Minute, SufficientMinResults: 1}
	c, _ := cache.New(10, 0)
	mgr := backends.NewManager()
	_ = mgr.SetFallbacks(nil)
	errRouter := newRouter(proxy.New(cfg, &stubSearxng{err: context.DeadlineExceeded}, c, breaker.New(), mgr), cfg)
	rr := httptest.NewRecorder()
	errRouter.ServeHTTP(rr, httptest.NewRequest("GET", "/search?q=handler-error", nil))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("error status = %d, want 502", rr.Code)
	}
	if got := histogramCount(t, "searxng_gateway_search_request_duration_seconds") - beforeRequest; got != 4 {
		t.Fatalf("handler duration samples after error = %d, want 4", got)
	}
	if got := histogramCount(t, "searxng_gateway_searxng_stage_duration_seconds") - beforeStage; got != 2 {
		t.Fatalf("stage samples after failure = %d, want 2", got)
	}
}

func histogramCount(t *testing.T, name string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, metric := range mf.GetMetric() {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}
