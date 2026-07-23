package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sx/internal/brave"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/proxy"
	"sx/internal/searxng"
)

type stubSearxng struct{ resp *searxng.Response; err error }

func (s *stubSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
	return s.resp, s.err
}

type stubBrave struct{ resp *brave.Response; err error }

func (s *stubBrave) Search(_ context.Context, _ string) (*brave.Response, error) {
	return s.resp, s.err
}

func setupRouter(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{
		FallbackTimeout:      5 * time.Second,
		MetricsPath:          "/metrics",
		SearxngFailThreshold: 6,
		SearxngFailCooldown:  180 * time.Second,
	}
	c, _ := cache.New(10)
	metrics.Init()

	sx := &stubSearxng{resp: &searxng.Response{Results: []searxng.Result{{Engine: "wikipedia"}}}}
	bv := &stubBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T", URL: "u", Description: "d"})

	return newRouter(proxy.New(cfg, sx, bv, c), cfg)
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
