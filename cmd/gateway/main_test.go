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
	resp *searxng.Response
	err  error
}

func (s *stubSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
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
