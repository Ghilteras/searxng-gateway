package backends

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestBraveRequestInheritsCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	observed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(observed)
	}))
	defer server.Close()
	b := NewBraveBackend("test", time.Second)
	b.BaseURL = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = b.Search(SearchOptions{Context: ctx, Query: "cancel"}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("HTTP handler was not reached")
	}
	cancel()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("handler context not canceled")
	}
	<-done
}

func TestExaMCPInitializeAndToolShareCallerContext(t *testing.T) {
	var calls atomic.Int32
	toolCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
			ID     int    `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		calls.Add(1)
		if request.Method == "initialize" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
			return
		}
		<-r.Context().Done()
		close(toolCanceled)
	}))
	defer server.Close()
	b := NewExaBackend(ExaModeMCP, "", time.Second, server.URL, "exa-web-search", 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = b.Search(SearchOptions{Context: ctx, Query: "mcp"}) }()
	deadline := time.After(time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("expected initialize and tool requests")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-toolCanceled:
	case <-time.After(time.Second):
		t.Fatal("tool request did not observe caller cancellation")
	}
	<-done
}
