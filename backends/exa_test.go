package backends

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExaBackend_API(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("missing key"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]string{
				{"title": "Exa A", "url": "https://exa.example/a", "text": "A content"},
			},
		})
	}))
	defer server.Close()

	b := NewExaBackend(ExaModeAPI, "test-key", 2*time.Second, "", "", 10)
	b.BaseURL = server.URL

	results, err := b.Search(SearchOptions{Query: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Exa A" {
		t.Fatalf("unexpected title: %s", results[0].Title)
	}
}

func TestExaInvalidJSONDoesNotExposePayload(t *testing.T) {
	const marker = "9182736450918273645"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{"title":` + marker + `}]}`))
	}))
	defer srv.Close()
	b := NewExaBackend(ExaModeAPI, "key", time.Second, "", "", 10)
	b.BaseURL = srv.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	be, ok := err.(*BackendError)
	if !ok || be.Code != ErrCodeInvalidResponse {
		t.Fatalf("error = %v; want ErrCodeInvalidResponse", err)
	}
	if strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), "invalid JSON response") {
		t.Fatalf("error = %v; want safe JSON error without payload", err)
	}
}

func TestExaErrorDoesNotExposeVendorBody(t *testing.T) {
	const vendorBody = "VENDOR_PRIVATE_EXA_BODY_91d7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(vendorBody))
	}))
	defer srv.Close()
	b := NewExaBackend(ExaModeAPI, "key", time.Second, "", "", 10)
	b.BaseURL = srv.URL
	_, err := b.Search(SearchOptions{Query: "q"})
	if err == nil || strings.Contains(err.Error(), vendorBody) {
		t.Fatalf("error = %v; vendor response body must not be exposed", err)
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("error = %v; want safe HTTP status", err)
	}
}

func TestExaBackend_AutoAPIFailureIsNotReportedAsUnconfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	b := NewExaBackend(ExaModeAuto, "test-key", 2*time.Second, "", "", 10)
	b.BaseURL = server.URL
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected API failure")
	}
	if strings.Contains(err.Error(), "not configured") {
		t.Fatalf("API failure was misreported as configuration failure: %v", err)
	}
	var backendErr *BackendError
	if !errors.As(err, &backendErr) || !strings.Contains(backendErr.Err.Error(), "exa api search failed") {
		t.Fatalf("expected wrapped API search failure, got %v", err)
	}
}

func TestExaBackend_AutoWithoutCredentialsIsNotConfigured(t *testing.T) {
	b := NewExaBackend(ExaModeAuto, "", 2*time.Second, "", "", 10)
	_, err := b.Search(SearchOptions{Query: "test"})
	if err == nil || !strings.Contains(err.Error(), "exa not configured (need API key or MCP URL)") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
}

func TestExaBackend_MCP(t *testing.T) {
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)

		if method == "initialize" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"protocolVersion": "2024-11-05",
				},
			})
			return
		}
		if method == "tools/call" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"structuredContent": map[string]interface{}{
						"results": []map[string]string{{
							"title": "Exa MCP",
							"url":   "https://exa.example/mcp",
							"text":  "from mcp",
						}},
					},
				},
			})
			return
		}

		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	b := NewExaBackend(ExaModeMCP, "", 2*time.Second, server.URL, "exa-web-search", 10)
	results, err := b.Search(SearchOptions{Query: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Exa MCP" {
		t.Fatalf("unexpected result title: %s", results[0].Title)
	}
	if callCount < 1 {
		t.Fatal("expected at least one MCP call")
	}
}

func TestExaBackend_AutoFallsBackToMCP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]interface{}{
				"content": []map[string]string{{
					"type": "text",
					"text": "- [Fallback](https://exa.example/fallback)",
				}},
			},
		})
	}))
	defer server.Close()

	b := NewExaBackend(ExaModeAuto, "", 2*time.Second, server.URL, "exa-web-search", 10)
	results, err := b.Search(SearchOptions{Query: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || results[0].URL != "https://exa.example/fallback" {
		t.Fatalf("unexpected fallback results: %#v", results)
	}
}
