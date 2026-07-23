// Package metrics provides Prometheus collectors for gateway observability.
//
// Idempotent initialisation: Init() uses sync.Once so it is safe to call
// from multiple goroutines or repeatedly (e.g. in tests). The var block
// defines five metric families specified in §4.7 of the design doc.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RequestsTotal counts gateway requests by outcome label.
	// outcome ∈ {searxng_ok, fallback_brave_ok, fallback_brave_fail, cache_hit, timeout}
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_requests_total",
			Help: "Total gateway requests by outcome",
		},
		[]string{"outcome"},
	)

	// RequestDuration tracks request latency in seconds, labelled by source backend.
	// source ∈ {searxng, brave}
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "searxng_gateway_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"source"},
	)

	// ResultsCount is a histogram of the number of results returned per request.
	ResultsCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "searxng_gateway_results_count",
		Help:    "Number of results returned per request",
		Buckets: []float64{0, 1, 3, 5, 10, 20, 50},
	})

	// EnginesCount reports the number of distinct engines in the last response.
	EnginesCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "searxng_gateway_engines_count",
		Help: "Distinct engines in last response",
	})

	// CacheSize reports the current LRU cache size (number of entries).
	CacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "searxng_gateway_cache_size",
		Help: "Current LRU cache size (entries)",
	})

	// EngineResultsTotal counts results contributed per SearXNG engine.
	EngineResultsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_engine_results_total",
			Help: "Total results contributed by each SearXNG engine",
		},
		[]string{"engine"},
	)

	// EngineUnresponsiveTotal counts how often each SearXNG engine was
	// reported as unresponsive, labelled by reason (CAPTCHA, rate limit, …).
	EngineUnresponsiveTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_engine_unresponsive_total",
			Help: "Total times an engine was reported as unresponsive by SearXNG",
		},
		[]string{"engine", "reason"},
	)

	// EngineStatus is 1 if the engine responded in the last request, 0 otherwise.
	EngineStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_engine_status",
			Help: "Last-seen status per engine (1=responded, 0=unresponsive/absent)",
		},
		[]string{"engine"},
	)
)

var initOnce sync.Once

// Init registers all Prometheus collectors with the default registerer.
// It is safe to call multiple times — subsequent calls are no-ops.
func Init() {
	initOnce.Do(func() {
		prometheus.MustRegister(RequestsTotal, RequestDuration, ResultsCount, EnginesCount, CacheSize, EngineResultsTotal, EngineUnresponsiveTotal, EngineStatus)
	})
}
