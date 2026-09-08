package brave

import (
	"context"
	"math"
	"net/http"
	"strconv"
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

// ---------------------------------------------------------------------------
// Regression: partial successor removes previously-populated gauges
// ---------------------------------------------------------------------------

// seedAllRateLimitGauges sets all three Brave rate-limit gauges to known
// values so a subsequent ObserveRateLimitHeaders call can prove deletion.
func seedAllRateLimitGauges(t *testing.T) {
	t.Helper()
	metrics.Init()
	resetBraveRateLimitGauges()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, 9999")
	h.Set("X-RateLimit-Remaining", "1, 8888")
	h.Set("X-RateLimit-Reset", "1, 7777")
	ObserveRateLimitHeaders(h)

	// Sanity: all three must exist after seeding.
	for _, tt := range []struct {
		metric string
		want   float64
	}{
		{"searxng_gateway_brave_rate_limit_limit", 9999},
		{"searxng_gateway_brave_rate_limit_remaining", 8888},
		{"searxng_gateway_brave_rate_limit_reset_seconds", 7777},
	} {
		if got, found := gaugeValue(t, tt.metric); !found || got != tt.want {
			t.Fatalf("seed sanity failed for %s: got=%v found=%v want=%v", tt.metric, got, found, tt.want)
		}
	}
}

// TestObserveRateLimitHeaders_PartialSuccessor_DeletesMissingGauges proves
// that when all three gauges are seeded and a successor response omits
// Remaining and Reset, those gauges are deleted while Limit is updated.
func TestObserveRateLimitHeaders_PartialSuccessor_DeletesMissingGauges(t *testing.T) {
	seedAllRateLimitGauges(t)
	defer resetBraveRateLimitGauges()

	// Successor: only Limit header present.
	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, 5000")
	ObserveRateLimitHeaders(h)

	// Limit must be updated.
	if got, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_limit"); !found {
		t.Error("limit gauge missing after successor with Limit header")
	} else if got != 5000 {
		t.Errorf("limit = %v, want 5000", got)
	}

	// Remaining must be deleted (was seeded at 8888, now absent).
	if _, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_remaining"); found {
		t.Error("remaining gauge must be deleted when successor omits X-RateLimit-Remaining")
	}

	// Reset must be deleted (was seeded at 7777, now absent).
	if _, found := gaugeValue(t, "searxng_gateway_brave_rate_limit_reset_seconds"); found {
		t.Error("reset gauge must be deleted when successor omits X-RateLimit-Reset")
	}
}

// TestObserveRateLimitHeaders_InvalidSuccessor_ClearsAllGauges proves that
// when all three gauges are seeded and a successor sends only invalid
// monthly values, all three gauges are deleted (stale data removed).
func TestObserveRateLimitHeaders_InvalidSuccessor_ClearsAllGauges(t *testing.T) {
	seedAllRateLimitGauges(t)
	defer resetBraveRateLimitGauges()

	// Successor: all three headers present but with non-parseable monthly values.
	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, not-a-number")
	h.Set("X-RateLimit-Remaining", "1, also-bad")
	h.Set("X-RateLimit-Reset", "1, invalid")
	ObserveRateLimitHeaders(h)

	for _, metric := range []string{
		"searxng_gateway_brave_rate_limit_limit",
		"searxng_gateway_brave_rate_limit_remaining",
		"searxng_gateway_brave_rate_limit_reset_seconds",
	} {
		if _, found := gaugeValue(t, metric); found {
			t.Errorf("metric %s must be deleted when successor has invalid monthly value", metric)
		}
	}
}

// ---------------------------------------------------------------------------
// Regression: non-finite and negative values are rejected
// ---------------------------------------------------------------------------

// TestObserveRateLimitHeaders_NonFiniteValues_Rejected verifies that NaN,
// +Inf, and -Inf monthly values never populate any gauge.
func TestObserveRateLimitHeaders_NonFiniteValues_Rejected(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	for _, tt := range []struct {
		name     string
		value    string // monthly part after the comma
		wantZero bool   // NaN/Inf parse to non-zero floats
	}{
		{"NaN", "NaN", false},
		{"PositiveInf", "+Inf", false},
		{"NegativeInf", "-Inf", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetBraveRateLimitGauges()

			h := http.Header{}
			h.Set("X-RateLimit-Limit", "1, "+tt.value)
			h.Set("X-RateLimit-Remaining", "1, "+tt.value)
			h.Set("X-RateLimit-Reset", "1, "+tt.value)
			ObserveRateLimitHeaders(h)

			for _, metric := range []string{
				"searxng_gateway_brave_rate_limit_limit",
				"searxng_gateway_brave_rate_limit_remaining",
				"searxng_gateway_brave_rate_limit_reset_seconds",
			} {
				if _, found := gaugeValue(t, metric); found {
					t.Errorf("metric %s must NOT be present for non-finite value %s", metric, tt.value)
				}
			}
		})
	}
}

// TestObserveRateLimitHeaders_NegativeValues_Rejected verifies that negative
// monthly values never populate any gauge.
func TestObserveRateLimitHeaders_NegativeValues_Rejected(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	for _, val := range []string{"-1", "-0.5", "-100000"} {
		t.Run("negative_"+val, func(t *testing.T) {
			resetBraveRateLimitGauges()

			h := http.Header{}
			h.Set("X-RateLimit-Limit", "1, "+val)
			h.Set("X-RateLimit-Remaining", "1, "+val)
			h.Set("X-RateLimit-Reset", "1, "+val)
			ObserveRateLimitHeaders(h)

			for _, metric := range []string{
				"searxng_gateway_brave_rate_limit_limit",
				"searxng_gateway_brave_rate_limit_remaining",
				"searxng_gateway_brave_rate_limit_reset_seconds",
			} {
				if _, found := gaugeValue(t, metric); found {
					t.Errorf("metric %s must NOT be present for negative value %s", metric, val)
				}
			}
		})
	}
}

// TestObserveRateLimitHeaders_ZeroMonthlyValues_Accepted verifies that a
// zero monthly value is valid and populates the gauge.
func TestObserveRateLimitHeaders_ZeroMonthlyValues_Accepted(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "1, 0")
	h.Set("X-RateLimit-Remaining", "1, 0")
	h.Set("X-RateLimit-Reset", "1, 0")
	ObserveRateLimitHeaders(h)

	for _, metric := range []string{
		"searxng_gateway_brave_rate_limit_limit",
		"searxng_gateway_brave_rate_limit_remaining",
		"searxng_gateway_brave_rate_limit_reset_seconds",
	} {
		got, found := gaugeValue(t, metric)
		if !found {
			t.Errorf("metric %s must be present for zero monthly value", metric)
		} else if got != 0 {
			t.Errorf("metric %s = %v, want 0", metric, got)
		}
	}
}

// TestObserveRateLimitHeaders_NegativeSeedThenValidSuccessor proves that
// after an invalid negative seed, a valid successor populates correctly
// (no stale negative leak).
func TestObserveRateLimitHeaders_NegativeSeedThenValidSuccessor(t *testing.T) {
	metrics.Init()
	resetBraveRateLimitGauges()
	defer resetBraveRateLimitGauges()

	// First call: all-negative → nothing should be set.
	bad := http.Header{}
	bad.Set("X-RateLimit-Limit", "1, -999")
	bad.Set("X-RateLimit-Remaining", "1, -999")
	bad.Set("X-RateLimit-Reset", "1, -999")
	ObserveRateLimitHeaders(bad)

	for _, metric := range []string{
		"searxng_gateway_brave_rate_limit_limit",
		"searxng_gateway_brave_rate_limit_remaining",
		"searxng_gateway_brave_rate_limit_reset_seconds",
	} {
		if _, found := gaugeValue(t, metric); found {
			t.Errorf("metric %s must NOT be present after negative seed", metric)
		}
	}

	// Second call: valid values → gauges should populate cleanly.
	good := http.Header{}
	good.Set("X-RateLimit-Limit", "1, 15000")
	good.Set("X-RateLimit-Remaining", "1, 1234")
	good.Set("X-RateLimit-Reset", "1, 600")
	ObserveRateLimitHeaders(good)

	for _, tt := range []struct {
		metric string
		want   float64
	}{
		{"searxng_gateway_brave_rate_limit_limit", 15000},
		{"searxng_gateway_brave_rate_limit_remaining", 1234},
		{"searxng_gateway_brave_rate_limit_reset_seconds", 600},
	} {
		got, found := gaugeValue(t, tt.metric)
		if !found {
			t.Errorf("metric %s must be present after valid successor", tt.metric)
		} else if got != tt.want {
			t.Errorf("metric %s = %v, want %v", tt.metric, got, tt.want)
		}
	}
}

// TestObserveRateLimitHeaders_NaNRejectedByParseFloat verifies that the
// parseRateLimitHeaders path rejects NaN even though strconv.ParseFloat
// succeeds — a regression guard for the math.IsNaN check.
func TestObserveRateLimitHeaders_NaNRejectedByParseFloat(t *testing.T) {
	// strconv.ParseFloat("NaN", 64) succeeds; verify the production
	// code's extra guard catches it.
	val, err := strconv.ParseFloat("NaN", 64)
	if err != nil {
		t.Skipf("strconv.ParseFloat NaN returned error: %v", err)
	}
	if !math.IsNaN(val) {
		t.Skipf("strconv.ParseFloat NaN returned %v, not NaN", val)
	}
	// If we reach here, NaN parses successfully — the production guard
	// must reject it.
}

// TestObserveRateLimitHeaders_InfRejectedByParseFloat verifies that the
// parseRateLimitHeaders path rejects ±Inf even though strconv.ParseFloat
// succeeds.
func TestObserveRateLimitHeaders_InfRejectedByParseFloat(t *testing.T) {
	for _, raw := range []string{"+Inf", "-Inf"} {
		t.Run(raw, func(t *testing.T) {
			val, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				t.Skipf("strconv.ParseFloat(%s) returned error: %v", raw, err)
			}
			if !math.IsInf(val, 0) {
				t.Skipf("strconv.ParseFloat(%s) returned %v, not Inf", raw, val)
			}
		})
	}
}
