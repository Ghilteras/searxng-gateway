package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
}

func (s *stubBackend) Name() string      { return s.name }
func (s *stubBackend) IsAvailable() bool { return s.avail }
func (s *stubBackend) Search(_ backends.SearchOptions) ([]backends.SearchResult, error) {
	return s.results, s.err
}

func setupRouter(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{
		FallbackTimeout:      5 * time.Second,
		MetricsPath:          "/metrics",
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  180 * time.Second,
		SufficientMinResults: 1,
		FallbackProviders:    []string{"brave"},
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
	_ = mgr.SetFallbacks(cfg.FallbackProviders)

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
		Results []searxng.Result `json:"results"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode error = %v", err)
	}
	if len(body.Results) == 0 {
		t.Error("expected non-empty results (Brave fallback)")
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "json") {
		t.Errorf("Content-Type = %q, want json", rr.Header().Get("Content-Type"))
	}
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
