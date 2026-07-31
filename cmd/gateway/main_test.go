package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
