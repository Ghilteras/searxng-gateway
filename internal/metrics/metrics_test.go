package metrics

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
	want := []float64{1, 2, 3, 4, 5, 8, 10, 15, 20, 30}
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
