package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

func TestMonitoringContractStructuralBreakerTransitions(t *testing.T) {
	metrics.Init()
	for _, tc := range []struct {
		name, reason string
		code         int
	}{
		{"quota", backends.UnavailabilityQuotaExhausted, 402},
		{"credentials", backends.UnavailabilityCredentialsRejected, backends.ErrCodeAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := "monitoring-contract-" + tc.name
			backend := &fakeBackend{name: provider, avail: true, err: (&backends.BackendError{
				Backend: provider, Code: tc.code, Err: errors.New("structural provider failure"),
			}).WithUnavailabilityReason(tc.reason)}
			bm := breaker.NewWithSettings(func(engine string) breaker.EngineSettings {
				s := breaker.DefaultSettings()
				s.Timeout = 20 * time.Millisecond
				return s
			})
			c, _ := cache.New(20, 0)
			cfg := newCfg()
			cfg.SufficientMinResults = 5
			p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, backend)
			attemptLabels := map[string]string{"provider": provider, "phase": "primary", "outcome": map[string]string{"quota": "quota", "credentials": "auth"}[tc.name]}
			beforeAttempts := counterValueForLabels(t, "searxng_gateway_provider_attempts_total", attemptLabels)
			beforeSkips := counterValueForLabels(t, "searxng_gateway_provider_skips_total", map[string]string{"provider": provider, "reason": "breaker_open"})
			for _, query := range []string{"first", "second"} {
				_, _ = p.Search(context.Background(), provider+"-"+query)
			}
			if !bm.IsOpen("premium:" + provider) {
				t.Fatal("two structural failures did not trip breaker")
			}
			_, _ = p.Search(context.Background(), provider+"-open-skip")
			if delta := counterValueForLabels(t, "searxng_gateway_provider_skips_total", map[string]string{"provider": provider, "reason": "breaker_open"}) - beforeSkips; delta != 1 {
				t.Fatalf("breaker skip delta = %v, want 1", delta)
			}
			if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility", map[string]string{"provider": provider, "reason": tc.reason}); got != 1 {
				t.Fatalf("structural eligibility %q = %v, want 1", tc.reason, got)
			}
			if got := counterValueForLabels(t, "searxng_gateway_provider_attempts_total", attemptLabels) - beforeAttempts; got != 2 {
				t.Fatalf("historical %s attempt delta = %v, want 2", attemptLabels["outcome"], got)
			}

			deadline := time.Now().Add(time.Second)
			for bm.State("premium:"+provider) != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if bm.State("premium:"+provider) != 1 {
				t.Fatal("breaker did not enter half-open for recovery probe")
			}
			backend.err = nil
			backend.results = []backends.SearchResult{{URL: "https://recovered.example", Engine: provider}}
			if _, err := p.Search(context.Background(), provider+"-recovery"); err != nil {
				t.Fatal(err)
			}
			if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility", map[string]string{"provider": provider, "reason": "eligible"}); got != 1 {
				t.Fatalf("recovered eligibility = %v, want 1", got)
			}
			if got := counterValueForLabels(t, "searxng_gateway_provider_attempts_total", attemptLabels) - beforeAttempts; got != 2 {
				t.Fatalf("historical %s attempts changed during skip/recovery: %v, want 2", attemptLabels["outcome"], got)
			}
		})
	}
}

func TestMonitoringContractRemovedProviderIsMissingKey(t *testing.T) {
	metrics.Init()
	const provider = "monitoring-contract-removed"
	backend := &fakeBackend{name: provider, avail: false}
	c, _ := cache.New(10, 0)
	cfg := newCfg()
	cfg.SufficientMinResults = 1
	p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, breaker.New(), backend)
	before := counterValueForLabels(t, "searxng_gateway_provider_skips_total", map[string]string{"provider": provider, "reason": "missing_key"})
	_, _ = p.Search(context.Background(), provider+"-removed")
	if got := gaugeValueForLabels(t, "searxng_gateway_provider_eligibility", map[string]string{"provider": provider, "reason": "missing_key"}); got != 1 {
		t.Fatalf("missing-key eligibility = %v, want 1", got)
	}
	if got := counterValueForLabels(t, "searxng_gateway_provider_skips_total", map[string]string{"provider": provider, "reason": "missing_key"}) - before; got != 1 {
		t.Fatalf("missing-key skip delta = %v, want 1", got)
	}
}
