package brave

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSearchOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Subscription-Token") != "test-key" {
			t.Errorf("X-Subscription-Token = %q, want test-key", r.Header.Get("X-Subscription-Token"))
		}
		if r.URL.Query().Get("q") != "k8s" {
			t.Errorf("q = %q, want k8s", r.URL.Query().Get("q"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"web":{"results":[{"title":"T","url":"https://x","description":"D","age":"2 days ago"}]}}`))
	}))
	defer srv.Close()

	c := New("test-key", 5*time.Second)
	c = newAtURL(c, srv.URL)
	resp, err := c.Search(context.Background(), "k8s")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(resp.Web.Results) != 1 {
		t.Errorf("len(Web.Results) = %d, want 1", len(resp.Web.Results))
	}
}
