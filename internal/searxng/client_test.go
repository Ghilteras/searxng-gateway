package searxng

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSearchOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") != "hello" {
			t.Errorf("query = %q, want %q", r.URL.Query().Get("q"), "hello")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[{"title":"t1","url":"u1","content":"c1","engine":"brave","score":0.9},{"title":"t2","url":"u2","content":"c2","engine":"ddg","score":0.8}]}`))
	}))
	defer srv.Close()
	c := New(srv.URL, 5*time.Second)
	resp, err := c.Search(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(resp.Results) != 2 {
		t.Errorf("len(Results) = %d, want 2", len(resp.Results))
	}
	if resp.Results[0].Engine != "brave" {
		t.Errorf("Results[0].Engine = %q, want %q", resp.Results[0].Engine, "brave")
	}
}
