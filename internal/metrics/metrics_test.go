package metrics

import (
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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
