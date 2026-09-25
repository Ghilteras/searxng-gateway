package tracing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.26.0"
)

const shutdownTimeout = 5 * time.Second

// Init configures tracing without enabling ambient baggage propagation. The
// ratio sampler deliberately ignores an incoming sampled bit.
func Init(ctx context.Context) (func(context.Context) error, error) {
	mode := os.Getenv("OTEL_TRACES_EXPORTER")
	if mode == "" || mode == "none" {
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample())))
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return func(context.Context) error { return nil }, nil
	}
	rate := 0.1
	if raw := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v < 0 || v > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG must be between 0 and 1")
		}
		rate = v
	}
	var exp sdktrace.SpanExporter
	switch mode {
	case "console":
		exp = &safeConsoleExporter{w: os.Stdout}
	case "otlp":
		endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
		traceSpecific := endpoint != ""
		if endpoint == "" {
			endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		}
		if endpoint == "" {
			return nil, fmt.Errorf("OTEL_TRACES_EXPORTER=otlp requires OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
		}
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("invalid explicit OTLP endpoint")
		}
		if !traceSpecific {
			u.Path = strings.TrimRight(u.Path, "/") + "/v1/traces"
			endpoint = u.String()
		}
		e, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithTimeout(shutdownTimeout))
		if err != nil {
			return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
		}
		exp = e
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER %q (expected none, console, or otlp)", mode)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(256), sdktrace.WithMaxExportBatchSize(64), sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(shutdownTimeout)),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(rate)),
		sdktrace.WithResource(gatewayResource()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func(ctx context.Context) error {
		c, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
		return tp.Shutdown(c)
	}, nil
}

func gatewayResource() *resource.Resource {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		if value, ok := resource.Environment().Set().Value("service.name"); ok {
			serviceName = value.AsString()
		}
	}
	if serviceName == "" {
		serviceName = "searxng-gateway"
	}
	defaults := resource.Default()
	environment, err := resource.Merge(defaults, resource.Environment())
	if err != nil {
		otel.Handle(err)
	}
	merged, err := resource.Merge(environment, resource.NewWithAttributes("", semconv.ServiceName(serviceName)))
	if err != nil {
		otel.Handle(err)
	}
	return merged
}

// Console exporter emits structured, allow-listed span records only.
type safeConsoleExporter struct{ w io.Writer }

func (*safeConsoleExporter) Shutdown(context.Context) error { return nil }
func (e *safeConsoleExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, s := range spans {
		record := map[string]any{
			"trace_id": s.SpanContext().TraceID().String(), "span_id": s.SpanContext().SpanID().String(),
			"parent_span_id": s.Parent().SpanID().String(), "name": s.Name(),
			"start": s.StartTime().UTC().Format(time.RFC3339Nano), "end": s.EndTime().UTC().Format(time.RFC3339Nano),
			"duration_ns": s.EndTime().Sub(s.StartTime()).Nanoseconds(), "status": s.Status().Code.String(),
			"attributes": allowAttributes(s.Attributes()),
		}
		events := make([]map[string]any, 0, len(s.Events()))
		for _, event := range s.Events() {
			if event.Name != "cache.hit" && event.Name != "cache.miss" && event.Name != "retry.backoff" {
				continue
			}
			events = append(events, map[string]any{"name": event.Name, "time": event.Time.UTC().Format(time.RFC3339Nano), "attributes": allowAttributes(event.Attributes)})
		}
		record["events"] = events
		if err := json.NewEncoder(e.w).Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func allowAttributes(attrs []attribute.KeyValue) map[string]any {
	allowed := map[string]bool{"http.route": true, "http.status_code": true, "phase": true, "provider": true, "attempt": true, "outcome": true, "result_count": true}
	out := make(map[string]any)
	for _, attr := range attrs {
		if !allowed[string(attr.Key)] {
			continue
		}
		switch attr.Value.Type() {
		case attribute.STRING:
			out[string(attr.Key)] = attr.Value.AsString()
		case attribute.INT64:
			out[string(attr.Key)] = attr.Value.AsInt64()
		case attribute.BOOL:
			out[string(attr.Key)] = attr.Value.AsBool()
		}
	}
	return out
}
