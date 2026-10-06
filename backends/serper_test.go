package backends

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSerperSearchMapsHomepageSchema(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-API-KEY") != "test-key" {
			t.Errorf("unexpected request method/key")
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"organic":[{"title":"Title","link":"https://example.test","snippet":"Snippet","position":1}]}`))
	}))
	defer srv.Close()
	b := NewSerperBackend("test-key", time.Second)
	b.BaseURL = srv.URL
	got, err := b.Search(SearchOptions{Query: "q", NumResults: 4})
	if err != nil {
		t.Fatal(err)
	}
	if body["q"] != "q" || body["num"] != float64(4) {
		t.Fatalf("request body=%v", body)
	}
	if len(got) != 1 || got[0].Title != "Title" || got[0].URL != "https://example.test" || got[0].Content != "Snippet" || got[0].Engine != "serper" || len(got[0].Engines) != 1 || got[0].Engines[0] != "serper" {
		t.Fatalf("mapped results=%#v", got)
	}
}

func contains(names []string, target string) bool {
	for _, name := range names {
		if name == target {
			return true
		}
	}
	return false
}

func TestSerperTypedFailures(t *testing.T) {
	cases := []struct{ status, code int }{{401, ErrCodeAuth}, {403, ErrCodeAuth}, {429, ErrCodeRateLimit}, {503, 503}}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tc.status) }))
			defer srv.Close()
			b := NewSerperBackend("key", time.Second)
			b.BaseURL = srv.URL
			_, err := b.Search(SearchOptions{Query: "q"})
			assertBackendCode(t, err, tc.code)
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not-json")) }))
	defer srv.Close()
	b := NewSerperBackend("key", time.Second)
	b.BaseURL = srv.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeInvalidResponse)
}

func TestSerperErrorDoesNotExposeVendorBody(t *testing.T) {
	const vendorBody = "VENDOR_PRIVATE_ERROR_BODY_91d7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(vendorBody))
	}))
	defer srv.Close()
	b := NewSerperBackend("key", time.Second)
	b.BaseURL = srv.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	if err == nil || strings.Contains(err.Error(), vendorBody) {
		t.Fatalf("error = %v; vendor response body must not be exposed", err)
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("error = %v; want safe HTTP status", err)
	}
}

func TestSerperNetworkAndCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	b := NewSerperBackend("key", time.Second)
	b.BaseURL = url
	_, err := b.Search(SearchOptions{Query: "q"})
	assertBackendCode(t, err, ErrCodeNetwork)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Search(SearchOptions{Query: "q", Context: ctx})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	var be *BackendError
	if !errors.As(err, &be) || be.Code == ErrCodeUnavailable {
		t.Fatalf("unexpected cancellation error: %v", err)
	}
}

func TestConfiguredPoolOrderAndKeylessExclusion(t *testing.T) {
	for _, name := range PremiumPoolOrder {
		t.Setenv(strings.ToUpper(name)+"_API_KEY", "")
	}
	t.Setenv("BRAVE_API_KEY", "brave")
	t.Setenv("EXA_API_KEY", "exa")
	_, got, err := ConfiguredPool(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"brave", "exa"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enrolled order=%v, want %v", got, want)
	}
	if contains(got, "serper") || contains(got, "jina") {
		t.Fatalf("keyless provider enrolled: %v", got)
	}
	m := NewManager()
	m.Register(&fakeAvailabilityBackend{name: "exa", available: true})
	m.Register(&fakeAvailabilityBackend{name: "brave", available: true})
	for _, name := range want {
		if selected := m.NextAvailable(nil); selected.Name() != name {
			t.Fatalf("round-robin selected %s, want %s", selected.Name(), name)
		}
	}
}

type fakeAvailabilityBackend struct {
	name      string
	available bool
}

func (b *fakeAvailabilityBackend) Name() string                                 { return b.name }
func (b *fakeAvailabilityBackend) IsAvailable() bool                            { return b.available }
func (b *fakeAvailabilityBackend) Search(SearchOptions) ([]SearchResult, error) { return nil, nil }
