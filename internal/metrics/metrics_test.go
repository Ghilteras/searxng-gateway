package metrics

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestProviderEligibilityContractVocabularyAndOneHot(t *testing.T) {
	Init()
	var contract struct {
		Eligibility struct {
			Reasons []struct {
				Reason   string `json:"reason"`
				Class    string `json:"class"`
				Produced bool   `json:"produced"`
			} `json:"reasons"`
			AlertMaskExpected []string                  `json:"alert_mask_expected"`
			Snapshots         map[string]map[string]int `json:"snapshots"`
		} `json:"eligibility"`
	}
	data, err := os.ReadFile("../../testdata/monitoring/provider_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	fixtureReasons := make([]string, 0, len(contract.Eligibility.Reasons))
	derivedMask := make([]string, 0, len(contract.Eligibility.Reasons))
	for _, reason := range contract.Eligibility.Reasons {
		fixtureReasons = append(fixtureReasons, reason.Reason)
		switch reason.Class {
		case "healthy", "runtime_failure":
			derivedMask = append(derivedMask, reason.Reason)
		case "configuration":
		default:
			t.Errorf("reason %q has unknown class %q", reason.Reason, reason.Class)
		}
		if reason.Class == "runtime_failure" && !containsString(contract.Eligibility.AlertMaskExpected, reason.Reason) {
			t.Errorf("runtime-failure reason %q missing from alert_mask_expected", reason.Reason)
		}
		if reason.Class == "configuration" && containsString(contract.Eligibility.AlertMaskExpected, reason.Reason) {
			t.Errorf("configuration reason %q must not appear in alert_mask_expected", reason.Reason)
		}
	}
	if !reflect.DeepEqual(fixtureReasons, ProviderEligibilityReasons) {
		t.Fatalf("fixture eligibility reasons = %v, code vocabulary = %v", fixtureReasons, ProviderEligibilityReasons)
	}
	if !reflect.DeepEqual(contract.Eligibility.AlertMaskExpected, derivedMask) {
		t.Fatalf("alert_mask_expected = %v, reasons with non-configuration classes = %v", contract.Eligibility.AlertMaskExpected, derivedMask)
	}
	for _, activeReason := range ProviderEligibilityReasons {
		provider := "contract-reason-" + activeReason
		SetProviderEligibility(provider, func() string { return activeReason })
		families, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			t.Fatal(err)
		}
		active := 0
		for _, family := range families {
			if family.GetName() != "searxng_gateway_provider_eligibility" {
				continue
			}
			for _, metric := range family.GetMetric() {
				labels := map[string]string{}
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				if labels["provider"] != provider {
					continue
				}
				value := metric.GetGauge().GetValue()
				if value == 1 {
					active++
					if labels["reason"] != activeReason {
						t.Errorf("active reason = %q, want %q", labels["reason"], activeReason)
					}
				} else if value != 0 {
					t.Errorf("reason %q = %v, want 0 or 1", labels["reason"], value)
				}
			}
		}
		if active != 1 {
			t.Errorf("reason %q active eligibility series = %d, want 1", activeReason, active)
		}
	}
	for snapshot, values := range contract.Eligibility.Snapshots {
		t.Run(snapshot, func(t *testing.T) {
			provider := "contract-" + snapshot
			SetProviderEligibility(provider, func() string {
				for _, reason := range ProviderEligibilityReasons {
					if values[reason] == 1 {
						return reason
					}
				}
				return ""
			})
			families, err := prometheus.DefaultGatherer.Gather()
			if err != nil {
				t.Fatal(err)
			}
			active := 0
			for _, family := range families {
				if family.GetName() != "searxng_gateway_provider_eligibility" {
					continue
				}
				for _, metric := range family.GetMetric() {
					labels := map[string]string{}
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if labels["provider"] != provider {
						continue
					}
					got := int(metric.GetGauge().GetValue())
					if got != values[labels["reason"]] {
						t.Errorf("reason %s = %d, want %d", labels["reason"], got, values[labels["reason"]])
					}
					active += got
				}
			}
			if active != 1 {
				t.Fatalf("active eligibility series = %d, want 1", active)
			}
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestInitIdempotent(t *testing.T) {
	Init() // first
	// Second call must not panic, even if collectors are already registered.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Init() panicked on second call: %v", r)
		}
	}()
	Init() // must not panic on re-register
}

func TestRequestsTotalNotNil(t *testing.T) {
	Init()
	if RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil after Init")
	}
	RequestsTotal.WithLabelValues("no_results").Inc()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "searxng_gateway_requests_total" {
			for _, metric := range family.GetMetric() {
				if len(metric.GetLabel()) == 1 && metric.GetLabel()[0].GetValue() == "no_results" && metric.GetCounter().GetValue() > 0 {
					return
				}
			}
		}
	}
	t.Fatal("RequestsTotal did not expose dynamic no_results outcome label")
}

func TestLatencyBucketBoundaries(t *testing.T) {
	want := []float64{0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 8, 10, 15, 20, 30}
	if !reflect.DeepEqual(durationBuckets, want) {
		t.Fatalf("latency buckets = %v, want %v", durationBuckets, want)
	}
}

func TestSetProviderEligibilityConcurrentOneHotFinalState(t *testing.T) {
	Init()
	const provider = "eligibility-concurrency-test"
	var current atomic.Int32
	resolve := func() string {
		if current.Load() == 1 {
			return "breaker_open"
		}
		return "eligible"
	}

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				current.Store(int32((i + j) % 2))
				SetProviderEligibility(provider, resolve)
			}
		}(i)
	}
	wg.Wait()

	// Pin a deterministic authoritative state after all contending writers have
	// joined; the final one-hot publication must match it exactly.
	current.Store(0)
	SetProviderEligibility(provider, resolve)

	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var active int
	for _, family := range metrics {
		if family.GetName() != "searxng_gateway_provider_eligibility" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["provider"] != provider {
				continue
			}
			value := metric.GetGauge().GetValue()
			if value == 1 {
				active++
				if labels["reason"] != "eligible" {
					t.Errorf("active eligibility reason = %q, want eligible", labels["reason"])
				}
			} else if value != 0 {
				t.Errorf("eligibility reason %q gauge = %v, want 0 or 1", labels["reason"], value)
			}
		}
	}
	if active != 1 {
		t.Fatalf("active eligibility series = %d, want one", active)
	}
}

// TestV310SeriesRegisteredWithExpectedLabels asserts that each new series is
// registered on the default registry under its exposed name and that the
// labelled counters carry exactly the documented label sets and values.
func TestV310SeriesRegisteredWithExpectedLabels(t *testing.T) {
	Init()
	for _, outcome := range []string{"success", "timeout", "error", "skipped_cooldown", "budget_capped"} {
		SearxngStageTotal.WithLabelValues(outcome).Add(0)
	}
	for _, source := range []string{"searxng", "brave", "exa", "parallel", "serper", "tavily"} {
		SearchResultsTotal.WithLabelValues(source).Add(0)
	}
	PremiumStageDuration.Observe(0.2)
	PremiumCallsPerRequest.Observe(1)

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}
	labelValues := func(name, label string) []string {
		f := byName[name]
		if f == nil {
			t.Fatalf("metric family %s not exposed", name)
		}
		var got []string
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != 1 || m.GetLabel()[0].GetName() != label {
				t.Fatalf("%s: labels = %v, want exactly [%s]", name, m.GetLabel(), label)
			}
			got = append(got, m.GetLabel()[0].GetValue())
		}
		sort.Strings(got)
		return got
	}
	wantOutcomes := []string{"budget_capped", "error", "skipped_cooldown", "success", "timeout"}
	if got := labelValues("searxng_gateway_searxng_stage_total", "outcome"); !reflect.DeepEqual(got, wantOutcomes) {
		t.Errorf("searxng_stage_total outcomes = %v, want %v", got, wantOutcomes)
	}
	wantSources := []string{"brave", "exa", "parallel", "searxng", "serper", "tavily"}
	if got := labelValues("searxng_gateway_search_results_total", "source"); !reflect.DeepEqual(got, wantSources) {
		t.Errorf("search_results_total sources = %v, want %v", got, wantSources)
	}
	for _, name := range []string{"searxng_gateway_premium_stage_duration_seconds", "searxng_gateway_premium_calls_per_request"} {
		f := byName[name]
		if f == nil || f.GetType() != dto.MetricType_HISTOGRAM {
			t.Fatalf("%s not exposed as a histogram", name)
		}
	}
	// premium_stage_duration_seconds uses the shared latency buckets (incl. sub-second).
	var bounds []float64
	for _, b := range byName["searxng_gateway_premium_stage_duration_seconds"].GetMetric()[0].GetHistogram().GetBucket() {
		bounds = append(bounds, b.GetUpperBound())
	}
	if !reflect.DeepEqual(bounds, durationBuckets) {
		t.Errorf("premium_stage_duration buckets = %v, want %v", bounds, durationBuckets)
	}
}
