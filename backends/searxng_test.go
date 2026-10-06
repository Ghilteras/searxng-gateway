package backends

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSearxngBackend_Name(t *testing.T) {
	b := NewSearxngBackend("http://localhost", "", "", "GET", 10*time.Second, false, false)
	if b.Name() != "searxng" {
		t.Errorf("expected 'searxng', got %q", b.Name())
	}
}

func TestSearxngBackend_IsAvailable(t *testing.T) {
	tests := []struct {
		baseURL string
		want    bool
	}{
		{"", false},
		{"not-a-url", false},
		{"http://localhost:8888", true},
		{"https://searx.example.com", true},
	}
	for _, tt := range tests {
		b := NewSearxngBackend(tt.baseURL, "", "", "GET", 10*time.Second, false, false)
		if got := b.IsAvailable(); got != tt.want {
			t.Errorf("IsAvailable(%q) = %v, want %v", tt.baseURL, got, tt.want)
		}
	}
}

func TestSearxngBackend_Search_Unavailable(t *testing.T) {
	b := NewSearxngBackend("", "", "", "GET", 10*time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for unavailable backend")
	}
}

func TestSearxngErrorDoesNotExposeBody(t *testing.T) {
	const upstreamBody = "SEARXNG_PRIVATE_BODY_91d7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer srv.Close()
	b := NewSearxngBackend(srv.URL, "", "", "GET", time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "q"})
	if err == nil || strings.Contains(err.Error(), upstreamBody) {
		t.Fatalf("error = %v; upstream response body must not be exposed", err)
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("error = %v; want safe HTTP status", err)
	}
}

func TestSearxngBackend_Search_GET(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Query().Get("q") != "golang" {
			t.Errorf("expected query 'golang', got %q", r.URL.Query().Get("q"))
		}
		if r.URL.Query().Get("format") != "json" {
			t.Errorf("expected format 'json', got %q", r.URL.Query().Get("format"))
		}

		resp := SearxngResponse{
			Results: []searxngResult{
				{
					Title:   "Go Dev",
					URL:     "https://go.dev",
					Content: "Official Go site",
					Engines: []string{"google", "duckduckgo"},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// The server URL includes no /search path, so we remove the trailing slash
	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	results, err := b.Search(SearchOptions{Query: "golang"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Go Dev" {
		t.Errorf("expected 'Go Dev', got %q", results[0].Title)
	}
}

func TestSearxngBackend_Search_EmptyWithUnresponsiveEngines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results": [], "unresponsive_engines": [["PRIVATE_ENGINE_91d7", "Suspended: too many requests"], ["startpage", "PRIVATE_REASON_91d7"]]}`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "golang"})
	if err == nil {
		t.Fatal("expected degraded-instance error for empty results with unresponsive engines")
	}
	be, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected *BackendError, got %T", err)
	}
	if be.Code != ErrCodeDegraded {
		t.Errorf("expected ErrCodeDegraded, got %d", be.Code)
	}
	if !strings.Contains(err.Error(), "count=2") {
		t.Errorf("error should include unresponsive engine count, got: %v", err)
	}
	for _, marker := range []string{"PRIVATE_ENGINE_91d7", "PRIVATE_REASON_91d7", "startpage", "Suspended: too many requests"} {
		if strings.Contains(err.Error(), marker) {
			t.Errorf("error must not expose %q, got: %v", marker, err)
		}
	}
}

func TestSearxngBackend_Search_EmptyUnresponsiveTupleDoesNotDegrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[[""]]}`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	results, err := b.Search(SearchOptions{Query: "golang"})
	if err != nil {
		t.Fatalf("empty unresponsive tuple should not degrade: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestSearxngBackend_Search_MultipleEmptyUnresponsiveTuplesPreserveDegraded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[[""],[""]]}`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "golang"})
	be, ok := err.(*BackendError)
	if !ok {
		t.Fatalf("expected *BackendError, got %T: %v", err, err)
	}
	if be.Code != ErrCodeDegraded {
		t.Errorf("expected ErrCodeDegraded, got %d", be.Code)
	}
	if got, want := formatUnresponsiveEngines(json.RawMessage(`[[""],[""]]`)), "count=2"; got != want {
		t.Errorf("sanitized summary = %q, want %q", got, want)
	}
	if !strings.Contains(err.Error(), "count=2") {
		t.Errorf("error should include safe tuple count, got: %v", err)
	}
	if got, want := be.Err.Error(), "no results, upstream engines unresponsive: count=2"; got != want {
		t.Errorf("error must contain only safe summary text: got %q, want %q", got, want)
	}
}

func TestSearxngBackend_Search_EmptyWithoutUnresponsiveEngines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results": [], "unresponsive_engines": []}`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	results, err := b.Search(SearchOptions{Query: "golang"})
	if err != nil {
		t.Fatalf("genuinely empty result set should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestSearxngBackend_Search_EmptyLaterPageWithUnresponsiveEngines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results": [], "unresponsive_engines": [["brave", "Suspended: too many requests"]]}`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	results, err := b.Search(SearchOptions{Query: "golang", PageNo: 3})
	if err != nil {
		t.Fatalf("empty later page should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestFormatUnresponsiveEngines(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"empty field", ``, ""},
		{"empty list", `[]`, ""},
		{"name and reason", `[["brave", "Suspended: too many requests"]]`, "count=1"},
		{"name only", `[["brave"]]`, "count=1"},
		{"extra fields", `[["brave", "rate limited", true]]`, "count=1"},
		{"empty tuples", `[[], null]`, ""},
		{"one empty part", `[[""]]`, ""},
		{"two empty parts", `[[""], [""]]`, "count=2"},
		{"empty fields with delimiters", `[["", ""]]`, "count=1"},
		{"empty and nonempty parts", `[[""], ["brave"]]`, "count=2"},
		{"multiple engines", `[["brave"], [], ["startpage", "down"]]`, "count=2"},
		{"unexpected shape", `{"brave": "down"}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatUnresponsiveEngines(json.RawMessage(tt.raw))
			if got != tt.want {
				t.Errorf("formatUnresponsiveEngines(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestSearxngBackend_Search_POST(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("expected form content-type, got %q", r.Header.Get("Content-Type"))
		}

		_ = r.ParseForm()
		if r.FormValue("q") != "test" {
			t.Errorf("expected query 'test', got %q", r.FormValue("q"))
		}

		resp := SearxngResponse{
			Results: []searxngResult{
				{Title: "POST Result", URL: "https://post.com"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "POST", 10*time.Second, false, false)
	results, err := b.Search(SearchOptions{Query: "test"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 || results[0].Title != "POST Result" {
		t.Errorf("unexpected results: %v", results)
	}
}

func TestSearxngBackend_Search_WithBasicAuth(t *testing.T) {
	var capturedUser, capturedPass string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUser, capturedPass, _ = r.BasicAuth()

		resp := SearxngResponse{Results: []searxngResult{}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "user", "pass", "GET", 10*time.Second, false, false)
	_, _ = b.Search(SearchOptions{Query: "test"})

	if capturedUser != "user" || capturedPass != "pass" {
		t.Errorf("expected user/pass, got %q/%q", capturedUser, capturedPass)
	}
}

func TestSearxngBackend_Search_WithSiteFilter(t *testing.T) {
	var capturedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query().Get("q")
		resp := SearxngResponse{Results: []searxngResult{}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, _ = b.Search(SearchOptions{Query: "test", Site: "example.com"})

	if capturedQuery != "site:example.com test" {
		t.Errorf("expected 'site:example.com test', got %q", capturedQuery)
	}
}

func TestSearxngBackend_Search_WithCategories(t *testing.T) {
	var capturedCategories string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCategories = r.URL.Query().Get("categories")
		resp := SearxngResponse{Results: []searxngResult{}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, _ = b.Search(SearchOptions{Query: "test", Categories: []string{"news", "social-media"}})

	if capturedCategories != "news,social media" {
		t.Errorf("expected 'news,social media', got %q", capturedCategories)
	}
}

func TestSearxngBackend_Search_WithTimeRange(t *testing.T) {
	var capturedTimeRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTimeRange = r.URL.Query().Get("time_range")
		resp := SearxngResponse{Results: []searxngResult{}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, _ = b.Search(SearchOptions{Query: "test", TimeRange: "week"})

	if capturedTimeRange != "week" {
		t.Errorf("expected 'week', got %q", capturedTimeRange)
	}
}

func TestSearxngBackend_Search_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for server error")
	}
}

func TestSearxngBackend_Search_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()

	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSearxngBackend_Search_UserAgent(t *testing.T) {
	var capturedUA string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUA = r.Header.Get("User-Agent")
		resp := SearxngResponse{Results: []searxngResult{}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// With user agent
	b := NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, false)
	_, _ = b.Search(SearchOptions{Query: "test"})
	if capturedUA != "sx/2.0" {
		t.Errorf("expected 'sx/2.0', got %q", capturedUA)
	}

	// Without user agent
	b = NewSearxngBackend(server.URL, "", "", "GET", 10*time.Second, false, true)
	_, _ = b.Search(SearchOptions{Query: "test"})
	if capturedUA == "sx/2.0" {
		t.Error("expected no user agent when NoUserAgent=true")
	}
}

func TestNormalizeCategory(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"social-media", "social media"},
		{"social+media", "social media"},
		{"social_media", "social media"},
		{"socialmedia", "social media"},
		{"news", "news"},
		{"general", "general"},
	}
	for _, tt := range tests {
		if got := normalizeCategory(tt.input); got != tt.want {
			t.Errorf("normalizeCategory(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
