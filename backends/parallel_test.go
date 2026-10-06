package backends

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type parallelCapturedSettings struct {
	MaxResults int `json:"max_results"`
}

type parallelCapturedRequest struct {
	Objective     string                   `json:"objective"`
	SearchQueries []string                 `json:"search_queries"`
	Mode          string                   `json:"mode"`
	Advanced      parallelCapturedSettings `json:"advanced_settings"`
}

func assertBackendCode(t *testing.T, err error, want int) {
	t.Helper()
	var be *BackendError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BackendError, got %T: %v", err, err)
	}
	if be.Code != want {
		t.Fatalf("expected backend code %d, got %d (%v)", want, be.Code, be.Err)
	}
}

func TestParallelBackend_SearchSuccess(t *testing.T) {
	var got parallelCapturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if key := r.Header.Get("x-api-key"); key != "test-key" {
			t.Errorf("expected x-api-key 'test-key', got %q", key)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"search_id":"search_test","results":[{"url":"https://a.example/1","title":"Alpha","publish_date":null,"excerpts":["Alpha excerpt 1","Alpha excerpt 2"]},{"url":"https://b.example/2","title":"Beta","publish_date":"2024-01-15","excerpts":["","   ","Beta fallback excerpt"]},{"url":"https://c.example/3","title":null,"publish_date":null,"excerpts":[]}],"warnings":null,"usage":[{"name":"sku_search","count":1}],"session_id":"session_test"}`))
	}))
	defer server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL

	results, err := b.Search(SearchOptions{Query: "golang", NumResults: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[0].Title != "Alpha" || results[0].URL != "https://a.example/1" || results[0].Content != "Alpha excerpt 1" {
		t.Errorf("unexpected first result: %#v", results[0])
	}
	if results[0].Engine != "parallel" || len(results[0].Engines) != 1 || results[0].Engines[0] != "parallel" {
		t.Errorf("unexpected engine fields: %#v", results[0])
	}
	if results[1].Content != "Beta fallback excerpt" {
		t.Errorf("expected first non-empty excerpt, got %q", results[1].Content)
	}
	if results[2].Title != "" || results[2].Content != "" {
		t.Errorf("expected empty nullable fields, got %#v", results[2])
	}
	if got.Objective != "golang" {
		t.Errorf("expected objective 'golang', got %q", got.Objective)
	}
	if len(got.SearchQueries) != 1 || got.SearchQueries[0] != "golang" {
		t.Errorf("expected search_queries [golang], got %#v", got.SearchQueries)
	}
	if got.Mode != "fast" {
		t.Errorf("expected mode 'fast', got %q", got.Mode)
	}
	if got.Advanced.MaxResults != 3 {
		t.Errorf("expected max_results 3, got %d", got.Advanced.MaxResults)
	}
}

func TestParallelBackend_NumResultsDefault(t *testing.T) {
	var got parallelCapturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"search_id":"s","results":[],"session_id":"x"}`))
	}))
	defer server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	if _, err := b.Search(SearchOptions{Query: "q"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Advanced.MaxResults != 10 {
		t.Errorf("expected default max_results 10, got %d", got.Advanced.MaxResults)
	}
}

func TestParallelBackend_SitePrefix(t *testing.T) {
	var got parallelCapturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"search_id":"s","results":[],"session_id":"x"}`))
	}))
	defer server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	if _, err := b.Search(SearchOptions{Query: "test", Site: "example.com"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "site:example.com test"
	if got.Objective != want {
		t.Errorf("expected objective %q, got %q", want, got.Objective)
	}
	if len(got.SearchQueries) != 1 || got.SearchQueries[0] != want {
		t.Errorf("expected search_queries [%q], got %#v", want, got.SearchQueries)
	}
}

func TestParallelBackend_AuthErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"type":"error","error":{"ref_id":"r","message":"bad key"}}`))
		}))
		b := NewParallelBackend("bad-key", 2*time.Second)
		b.BaseURL = server.URL
		_, err := b.Search(SearchOptions{Query: "q"})
		assertBackendCode(t, err, ErrCodeAuth)
		server.Close()
	}
}

func TestParallelBackend_RateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeRateLimit)
}

func TestParallelBackendErrorDoesNotExposeVendorBody(t *testing.T) {
	const vendorBody = "VENDOR_PRIVATE_ERROR_BODY_91d7"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(vendorBody))
	}))
	defer server.Close()
	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	if err == nil || strings.Contains(err.Error(), vendorBody) {
		t.Fatalf("error = %v; vendor response body must not be exposed", err)
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("error = %v; want safe HTTP status", err)
	}
}

func TestParallelBackend_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeInvalidResponse)
}

func TestParallelBackend_NetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = url
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeNetwork)
}

func TestParallelBackend_NotConfigured(t *testing.T) {
	b := NewParallelBackend("", 2*time.Second)
	if b.IsAvailable() {
		t.Fatal("expected IsAvailable false without an API key")
	}
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeUnavailable)
}

func TestParallelBackend_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"search_id":"s","results":[{"url":"https://x","title":"X","excerpts":["should not be returned"]}],"session_id":"y"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := NewParallelBackend("test-key", 2*time.Second)
	b.BaseURL = server.URL
	results, err := b.Search(SearchOptions{Query: "q", Context: ctx})
	if err == nil {
		t.Fatal("expected an error from a cancelled context")
	}
	if len(results) != 0 {
		t.Fatalf("expected no fabricated results, got %#v", results)
	}
}

func TestParallelBackend_FactoryAndEligibility(t *testing.T) {
	t.Setenv("PARALLEL_API_KEY", "test-key")
	b, err := NewFromEnv("parallel", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Name() != "parallel" {
		t.Errorf("expected backend name 'parallel', got %q", b.Name())
	}
	if !b.IsAvailable() {
		t.Error("expected parallel backend to be available with a key")
	}

	t.Setenv("PARALLEL_API_KEY", "")
	keyless, err := NewFromEnv("parallel", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keyless.IsAvailable() {
		t.Fatal("expected keyless parallel backend to report unavailable")
	}
	m := NewManager()
	m.Register(keyless)
	if got := m.GetAvailable(); len(got) != 0 {
		t.Fatalf("expected no eligible backends, got %d", len(got))
	}
	if sel := m.NextAvailable(nil); sel != nil {
		t.Fatalf("expected no selectable backend, got %v", sel)
	}
}
