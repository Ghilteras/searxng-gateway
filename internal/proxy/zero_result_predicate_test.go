package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

func structuralTavilyFailure(code int, reason string, message string) error {
	return (&backends.BackendError{Backend: "tavily", Code: code, Err: errors.New(message)}).WithUnavailabilityReason(reason)
}

func searchZeroResult(t *testing.T, p *Proxy, query string, wantSuccess bool) {
	t.Helper()
	response, err := p.Search(context.Background(), query)
	if wantSuccess {
		if err != nil || response == nil || len(response.Results) != 0 {
			t.Fatalf("Search() = %#v, %v; want successful empty response", response, err)
		}
		return
	}
	if err == nil || response != nil {
		t.Fatalf("Search() = %#v, %v; want fallback failure", response, err)
	}
}

func newStructuralPredicateProxy(t *testing.T, bm *breaker.Manager, tavilyErr error) (*Proxy, *fakeBackend) {
	t.Helper()
	metrics.Init()
	brave := &fakeBackend{name: "brave", avail: true}
	tavily := &fakeBackend{name: "tavily", avail: true, err: tavilyErr}
	c, err := cache.New(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := newCfg()
	cfg.SufficientMinResults = 10
	p := newTestProxy(cfg, &fakeSearxng{resp: &searxng.Response{}}, c, bm, brave, tavily)
	return p, tavily
}

func TestV3StructuralProviderFailureAllowsNoResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int
		reason string
	}{
		{name: "quota exhaustion 432", code: 432, reason: backends.UnavailabilityQuotaExhausted},
		{name: "credentials rejected 433", code: 433, reason: backends.UnavailabilityCredentialsRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			previous := log.Writer()
			log.SetOutput(&logs)
			t.Cleanup(func() { log.SetOutput(previous) })
			p, _ := newStructuralPredicateProxy(t, breaker.New(), structuralTavilyFailure(tc.code, tc.reason, "provider rejected account"))
			searchZeroResult(t, p, fmt.Sprintf("structural-%d", tc.code), true)
			for _, field := range []string{`"provider":"tavily"`, `"outcome":"quota"`, fmt.Sprintf(`"eligibility_reason":"%s"`, tc.reason)} {
				if !strings.Contains(logs.String(), field) {
					t.Errorf("diagnostic missing %q: %s", field, logs.String())
				}
			}
		})
	}
}

func TestV3StructuralBreakerRejectionUsesFrozenReason(t *testing.T) {
	p, tavily := newStructuralPredicateProxy(t, breaker.New(), structuralTavilyFailure(432, backends.UnavailabilityQuotaExhausted, "plan limit"))
	searchZeroResult(t, p, "structural-trip-one", true)
	searchZeroResult(t, p, "structural-trip-two", true)
	if !p.breakerMgr.IsOpen(premiumBreakerID("tavily")) {
		t.Fatal("structural failures did not open Tavily breaker")
	}
	searchZeroResult(t, p, "structural-open-skip", true)
	if got := tavily.calls.Load(); got != 2 {
		t.Fatalf("Tavily calls = %d, want 2 (third request should be breaker-skipped)", got)
	}
}

func TestV3TransientAndUnprovenFailuresVetoNoResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "network", err: &backends.BackendError{Backend: "tavily", Code: backends.ErrCodeNetwork}},
		{name: "server error", err: &backends.BackendError{Backend: "tavily", Code: 503}},
		{name: "provider timeout", err: context.DeadlineExceeded},
		{name: "malformed response", err: &backends.BackendError{Backend: "tavily", Code: backends.ErrCodeInvalidResponse}},
		{name: "rate limit", err: &backends.BackendError{Backend: "tavily", Code: 429}},
		{name: "generic 403", err: &backends.BackendError{Backend: "tavily", Code: 403}},
		{name: "unknown 4xx", err: &backends.BackendError{Backend: "tavily", Code: 418}},
		{name: "misleading credit text", err: &backends.BackendError{Backend: "tavily", Code: 400, Err: errors.New("credit field is invalid")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newStructuralPredicateProxy(t, breaker.New(), tc.err)
			searchZeroResult(t, p, "transient-"+tc.name, false)
		})
	}
}

func TestV3AllPremiumsSkippedWithNoAdmissionVetoesNoResults(t *testing.T) {
	bm := breaker.New()
	for _, name := range []string{"brave", "tavily"} {
		for range 2 {
			_, _ = bm.Execute(premiumBreakerID(name), func() (interface{}, error) { return nil, errors.New("unknown fault") })
		}
	}
	p, _ := newStructuralPredicateProxy(t, bm, nil)
	searchZeroResult(t, p, "all-skipped-zero-admitted", false)
}

func TestV3CleanEmptyRejectsContradictoryOrUnknownEvidence(t *testing.T) {
	for _, event := range []providerDiagnostic{
		{Provider: "brave", Outcome: "empty", EligibilityReason: "mystery"},
		{Provider: "brave", Outcome: "empty", EligibilityReason: backends.UnavailabilityQuotaExhausted},
		{Provider: "brave", Skip: "breaker_open"},
		{Provider: "brave", Skip: "not_attempted"},
		{Provider: "brave", Outcome: "quota"},
	} {
		d := &requestDiagnostics{providerEvents: []providerDiagnostic{event}}
		if d.cleanEmpty() {
			t.Errorf("cleanEmpty accepted contradictory/unproven event: %+v", event)
		}
	}
}
