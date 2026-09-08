package brave

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sx/internal/metrics"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resetBraveRateLimitGauges removes the "month" label from all three Brave
// rate-limit GaugeVecs so tests don't leak state to each other via global
// Prometheus collectors.
func resetBraveRateLimitGauges() {
	metrics.BraveRateLimitRemaining.DeleteLabelValues("month")
	metrics.BraveRateLimitLimit.DeleteLabelValues("month")
	metrics.BraveRateLimitResetSeconds.DeleteLabelValues("month")
}

// gaugeValue gathers the Prometheus default gatherer and returns the Gauge
// value for metric <name> with label period="month".  Returns (value, true)
// when found, (0, false) when the label combination is absent.
func gaugeValue(t *testing.T, name string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "period" && l.GetValue() == "month" {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Existing test (unchanged)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Rate-limit header observation tests
// ---------------------------------------------------------------------------

// TestObserveRateLimitHeaders_CompleteValidSet verifies that a full set of
// Brave X-RateLimit-* headers populates every matching gauge with the
// correct monthly value under the "month" period label.
func TestObserveRateLimitHeaders_CompleteValidSet(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, 15000")
	h.Set("X-RateLimit-Remaining", "1, 1000")
	h.Set("X-RateLimit-Reset", "1, 1419704")

	ObserveRateLimitHeaders(h)

	for _, tt := range []struct {
		name   string
		metric string
		want   float64
	}{
		{"limit_month", "searxng_gateway_brave_rate_limit_limit", 15000},
		{"remaining_month", "searxng_gateway_brave_rate_limit_remaining", 1000},
		{"reset_seconds_month", "searxng_gateway_brave_rate_limit_reset_seconds", 1419704},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, found := gaugeValue(t, tt.metric)
			if !found {
				t.Fatalf("metric %s not found after ObserveRateLimitHeaders", tt.metric)
			}
			if got != tt.want {
				t.Errorf("metric %s = %v, want %v", tt.metric, got, tt.want)
			}
		})
	}
}

// TestObserveRateLimitHeaders_PartialHeaders_NoInventedZero ensures that when
// only a subset of headers is present, ObserveRateLimitHeaders sets only the
// corresponding gauges and does NOT create an invented zero for a missing
// gauge.
func TestObserveRateLimitHeaders_PartialHeaders_NoInventedZero(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	// Only send Limit header.
	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, 15000")

	ObserveRateLimitHeaders(h)

	// Limit should be set.
	if got, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_limit"); !found {
		t.Error("limit gauge not found after providing Limit header")
	} else if got != 15000 {
		t.Errorf("limit = %v, want 15000", got)
	}

	// Remaining must NOT appear (no invented zero for a missing header).
	if _, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_remaining"); found {
		t.Error("remaining gauge must NOT be present when header is absent (no invented zero)")
	}

	// Reset must NOT appear (no invented zero for a missing header).
	if _, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_reset_seconds"); found {
		t.Error("reset gauge must NOT be present when header is absent (no invented zero)")
	}
}

// TestObserveRateLimitHeaders_InvalidValue_NoGauge ensures that a header
// with a non-parseable second value does not create any gauge.
func TestObserveRateLimitHeaders_InvalidValue_NoGauge(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, not-a-number")

	ObserveRateLimitHeaders(h)

	if _, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_limit"); found {
		t.Error("limit gauge must NOT be present for non-numeric header value")
	}
}

// TestObserveRateLimitHeaders_EmptyHeaders_NoOp verifies that empty headers
// produce no gauge updates at all.
func TestObserveRateLimitHeaders_EmptyHeaders_NoOp(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	ObserveRateLimitHeaders(http.Header{})

	for _, metric := range []string{
		"searxng_gateway_brave_rate_limit_limit",
		"searxng_gateway_brave_rate_limit_remaining",
		"searxng_gateway_brave_rate_limit_reset_seconds",
	} {
		if _, found := gaugeValue(t, metric); found {
			t.Errorf("metric %s must NOT be present for empty headers", metric)
		}
	}
}

// TestObserveRateLimitHeaders_SingleValue_NoGauge ensures that a header
// containing only the per-second value (no comma-separated monthly value)
// is not parsed into any gauge.
func TestObserveRateLimitHeaders_SingleValue_NoGauge(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1")
	h.Set("X-RateLimit-Remaining", "1")
	h.Set("X-RateLimit-Reset", "1")

	ObserveRateLimitHeaders(h)

	for _, metric := range []string{
		"searxng_gateway_brave_rate_limit_limit",
		"searxng_gateway_brave_rate_limit_remaining",
		"searxng_gateway_brave_rate_limit_reset_seconds",
	} {
		if _, found := gaugeValue(t, metric); found {
			t.Errorf("metric %s must NOT be present when only per-second value is given", metric)
		}
	}
}
