package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setTracingEnv(t *testing.T, values map[string]string) {
	t.Helper()
	keys := []string{"OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_TRACES_SAMPLER_ARG"}
	for _, k := range keys {
		t.Setenv(k, "")
	}
	for k, v := range values {
		t.Setenv(k, v)
	}
}

func TestInitDisabledNoExportAndTraceContextOnly(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	setTracingEnv(t, map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": srv.URL})
	shutdown, err := Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "disabled")
	if span.IsRecording() {
		t.Fatal("disabled exporter produced recording span")
	}
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits != 0 {
		t.Fatalf("disabled mode made %d network calls", hits)
	}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier{"baggage": "secret=value"})
	if baggage.FromContext(ctx).Len() != 0 {
		t.Fatal("baggage was extracted")
	}
}

func TestInitExporterValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"missing endpoint", map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}},
		{"invalid endpoint", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_ENDPOINT": "file:///tmp/x"}},
		{"invalid sampling", map[string]string{"OTEL_TRACES_EXPORTER": "console", "OTEL_TRACES_SAMPLER_ARG": "1.1"}},
		{"unknown exporter", map[string]string{"OTEL_TRACES_EXPORTER": "nope"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setTracingEnv(t, tc.env)
			if _, err := Init(context.Background()); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestRemoteSampledTraceparentDoesNotOverrideSampler(t *testing.T) {
	setTracingEnv(t, map[string]string{"OTEL_TRACES_EXPORTER": "console", "OTEL_TRACES_SAMPLER_ARG": "0"})
	shutdown, err := Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled, Remote: true})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	_, span := otel.Tracer("test").Start(ctx, "sampling")
	if span.IsRecording() {
		t.Fatal("incoming sampled traceparent forced recording despite zero ratio")
	}
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsoleExporterStructuredAndAllowListed(t *testing.T) {
	var output bytes.Buffer
	exporter := &safeConsoleExporter{w: &output}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	_, span := provider.Tracer("test").Start(context.Background(), "proxy.search")
	span.SetAttributes(attribute.String("http.route", "/search"), attribute.String("query", "QUERY-SECRET"))
	span.AddEvent("cache.hit", trace.WithAttributes(attribute.String("outcome", "cache_hit"), attribute.String("result", "RESULT-SECRET")))
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	line := output.String()
	for _, required := range []string{"trace_id", "span_id", "parent_span_id", "proxy.search", "http.route", "cache.hit", "duration_ns"} {
		if !strings.Contains(line, required) {
			t.Errorf("structured output missing %q: %s", required, line)
		}
	}
	for _, secret := range []string{"QUERY-SECRET", "RESULT-SECRET", "query", "result"} {
		if strings.Contains(line, secret) {
			t.Errorf("console output leaked %q: %s", secret, line)
		}
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("invalid structured JSON: %v", err)
	}
}

func TestGenericOTLPEndpointAppendsTracePath(t *testing.T) {
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotPath = r.URL.Path; w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	setTracingEnv(t, map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_ENDPOINT": srv.URL, "OTEL_TRACES_SAMPLER_ARG": "1"})
	shutdown, err := Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "otlp")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/traces" {
		t.Fatalf("OTLP request path = %q, want /v1/traces", gotPath)
	}
}

func TestTraceSpecificOTLPEndpointUsedAsIs(t *testing.T) {
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotPath = r.URL.Path; w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	setTracingEnv(t, map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": srv.URL + "/custom/traces", "OTEL_TRACES_SAMPLER_ARG": "1"})
	shutdown, err := Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "otlp")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/custom/traces" {
		t.Fatalf("OTLP request path = %q, want /custom/traces", gotPath)
	}
}
