package backends

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMCPHTTPErrorDoesNotExposeRPCMessage(t *testing.T) {
	const marker = "MCP_PRIVATE_RPC_MESSAGE_91d7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"` + marker + `"}}`))
	}))
	defer srv.Close()
	err := NewMCPHTTPClient(srv.URL, time.Second).Initialize()
	if err == nil || err.Error() != "MCP error -32603" {
		t.Fatalf("error = %v; want safe numeric RPC code without vendor message", err)
	}
}

func TestMCPHTTPInvalidJSONDoesNotExposePayload(t *testing.T) {
	const marker = "9182736450918273645"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":{"code":` + marker + `999}}`))
	}))
	defer srv.Close()
	err := NewMCPHTTPClient(srv.URL, time.Second).Initialize()
	if err == nil || strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), "invalid JSON response") {
		t.Fatalf("error = %v; want safe JSON error without payload", err)
	}
}

func TestMCPHTTPErrorDoesNotExposeBody(t *testing.T) {
	const upstreamBody = "MCP_PRIVATE_BODY_91d7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer srv.Close()

	c := NewMCPHTTPClient(srv.URL, time.Second)
	if err := c.Initialize(); err == nil {
		t.Fatal("expected error on non-2xx MCP response")
	} else if strings.Contains(err.Error(), upstreamBody) {
		t.Fatalf("error = %v; upstream response body must not be exposed", err)
	} else if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("error = %v; want safe HTTP status", err)
	}
}
