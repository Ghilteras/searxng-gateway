package backends

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sx/internal/metrics"
)

func TestBraveBackend_Name(t *testing.T) {
	b := NewBraveBackend("key", 10*time.Second)
	if b.Name() != "brave" {
		t.Errorf("expected 'brave', got %q", b.Name())
	}
}

func TestBraveBackend_IsAvailable(t *testing.T) {
	tests := []struct {
		apiKey string
		want   bool
	}{
		{"", false},
		{"some-key", true},
	}
	for _, tt := range tests {
		b := NewBraveBackend(tt.apiKey, 10*time.Second)
		if got := b.IsAvailable(); got != tt.want {
			t.Errorf("IsAvailable(%q) = %v, want %v", tt.apiKey, got, tt.want)
		}
	}
}

func TestBraveBackend_Search_Unavailable(t *testing.T) {
	b := NewBraveBackend("", 10*time.Second)
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for unavailable backend")
	}
	backendErr, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected BackendError, got %T", err)
	}
	if backendErr.Code != ErrCodeUnavailable {
		t.Errorf("expected ErrCodeUnavailable, got %d", backendErr.Code)
	}
}

func newTestBraveBackend(serverURL, apiKey string) *BraveBackend {
	return &BraveBackend{
		APIKey:  apiKey,
		Timeout: 10 * time.Second,
		BaseURL: serverURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func TestBraveBackend_Search_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify method and auth header
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.Header.Get("X-Subscription-Token") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("q") != "golang" {
			t.Errorf("expected query 'golang', got %q", r.URL.Query().Get("q"))
		}

		resp := braveSearchResponse{
			Query: braveQuery{Original: "golang"},
			Web: braveWebResults{
				Results: []braveResult{
					{Title: "Go Lang", URL: "https://go.dev", Description: "Official Go site"},
					{Title: "Go Wikipedia", URL: "https://en.wikipedia.org/wiki/Go", Description: "Wiki article"},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "test-key")
	results, err := b.Search(SearchOptions{Query: "golang", NumResults: 5})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Title != "Go Lang" {
		t.Errorf("expected 'Go Lang', got %q", results[0].Title)
	}
	if results[0].URL != "https://go.dev" {
		t.Errorf("expected 'https://go.dev', got %q", results[0].URL)
	}
	if results[0].Content != "Official Go site" {
		t.Errorf("expected 'Official Go site', got %q", results[0].Content)
	}
	if results[0].Engine != "brave" {
		t.Errorf("expected engine 'brave', got %q", results[0].Engine)
	}
}

func TestBraveBackend_Search_AuthError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "invalid key"}`))
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "bad-key")
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for auth failure")
	}
	backendErr, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected BackendError, got %T", err)
	}
	if backendErr.Code != ErrCodeAuth {
		t.Errorf("expected ErrCodeAuth, got %d", backendErr.Code)
	}
}

func TestBraveBackend_Search_RateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error": "rate limited"}`))
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for rate limit")
	}
	backendErr, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected BackendError, got %T", err)
	}
	if backendErr.Code != ErrCodeRateLimit {
		t.Errorf("expected ErrCodeRateLimit, got %d", backendErr.Code)
	}
}

func TestBraveBackend_Search_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{invalid json}`))
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	backendErr, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected BackendError, got %T", err)
	}
	if backendErr.Code != ErrCodeInvalidResponse {
		t.Errorf("expected ErrCodeInvalidResponse, got %d", backendErr.Code)
	}
}

func TestBraveBackend_Search_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`internal server error`))
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for server error")
	}
}

func TestBraveBackend_Search_SafeSearch(t *testing.T) {
	var capturedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query().Get("safesearch")
		resp := braveSearchResponse{Web: braveWebResults{Results: []braveResult{}}}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	tests := []struct {
		safeSearch string
		want       string
	}{
		{"none", "off"},
		{"strict", "strict"},
		{"moderate", "moderate"},
		{"", "moderate"}, // default
	}

	for _, tt := range tests {
		b := newTestBraveBackend(server.URL, "key")
		b.Search(SearchOptions{Query: "test", SafeSearch: tt.safeSearch})
		if capturedQuery != tt.want {
			t.Errorf("SafeSearch(%q): expected safesearch=%q, got %q", tt.safeSearch, tt.want, capturedQuery)
		}
	}
}

func TestBraveBackend_Search_Pagination(t *testing.T) {
	var capturedOffset string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedOffset = r.URL.Query().Get("offset")
		resp := braveSearchResponse{Web: braveWebResults{Results: []braveResult{}}}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	b.Search(SearchOptions{Query: "test", PageNo: 3, NumResults: 10})
	if capturedOffset != "20" {
		t.Errorf("expected offset=20 for page 3, got %q", capturedOffset)
	}
}

// ---------------------------------------------------------------------------
// Rate-limit gauge helpers (mirrored from internal/brave/client_test.go)
// ---------------------------------------------------------------------------

// resetBraveRateLimitGauges removes the "month" label from all three Brave
// rate-limit GaugeVecs so tests don't leak state to each other via global
// Prometheus collectors.
func resetBraveRateLimitGauges() {
	metrics.BraveRateLimitRemaining.DeleteLabelValues("month")
	metrics.BraveRateLimitLimit.DeleteLabelValues("month")
	metrics.BraveRateLimitResetSeconds.DeleteLabelValues("month")
}

// gaugeValue gathers the Prometheus default gatherer and returns the Gauge
// value for metric <name> with label period="month".  Returns (value, true)
// when found, (0, false) when the label combination is absent.
func gaugeValue(t *testing.T, name string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "period" && l.GetValue() == "month" {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Active-backend rate-limit observation tests
// ---------------------------------------------------------------------------

// TestBraveBackend_Search_Success_RateLimitHeaders verifies that a successful
// Brave search response carrying a complete set of X-RateLimit-* headers
// updates the three metrics.BraveRateLimit* gauges with the expected monthly
// values.
func TestBraveBackend_Search_Success_RateLimitHeaders(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "1, 15000")
		w.Header().Set("X-RateLimit-Remaining", "1, 1000")
		w.Header().Set("X-RateLimit-Reset", "1, 1419704")
		resp := braveSearchResponse{
			Query: braveQuery{Original: "test"},
			Web: braveWebResults{
				Results: []braveResult{
					{Title: "Result", URL: "https://example.com", Description: "Desc"},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	results, err := b.Search(SearchOptions{Query: "test"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Verify all three rate-limit gauges were updated with monthly values.
	for _, tt := range []struct {
		name   string
		metric string
		want   float64
	}{
		{"limit", "searxng_gateway_brave_rate_limit_limit", 15000},
		{"remaining", "searxng_gateway_brave_rate_limit_remaining", 1000},
		{"reset", "searxng_gateway_brave_rate_limit_reset_seconds", 1419704},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, found := gaugeValue(t, tt.metric)
			if !found {
				t.Fatalf("metric %s not found after successful Search with rate-limit headers", tt.metric)
			}
			if got != tt.want {
				t.Errorf("metric %s = %v, want %v", tt.metric, got, tt.want)
			}
		})
	}
}

// TestBraveBackend_Search_RateLimit_UpdatesGauges verifies that a 429 response
// carrying X-RateLimit-* headers still returns the rate-limit error AND updates
// all three month gauges from the response headers.
func TestBraveBackend_Search_RateLimit_UpdatesGauges(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "1, 15000")
		w.Header().Set("X-RateLimit-Remaining", "1, 1000")
		w.Header().Set("X-RateLimit-Reset", "1, 1419704")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": "rate limited"}`))
	}))
	defer server.Close()

	b := newTestBraveBackend(server.URL, "key")
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for 429 response")
	}

	// Must still return ErrCodeRateLimit.
	backendErr, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected BackendError, got %T", err)
	}
	if backendErr.Code != ErrCodeRateLimit {
		t.Errorf("expected ErrCodeRateLimit, got %d", backendErr.Code)
	}

	// The 429 headers must update all three rate-limit gauges.
	for _, tt := range []struct {
		name   string
		metric string
		want   float64
	}{
		{"limit", "searxng_gateway_brave_rate_limit_limit", 15000},
		{"remaining", "searxng_gateway_brave_rate_limit_remaining", 1000},
		{"reset", "searxng_gateway_brave_rate_limit_reset_seconds", 1419704},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, found := gaugeValue(t, tt.metric)
			if !found {
				t.Fatalf("metric %s not found after 429 response with rate-limit headers", tt.metric)
			}
			if got != tt.want {
				t.Errorf("metric %s = %v, want %v", tt.metric, got, tt.want)
			}
		})
	}
}
