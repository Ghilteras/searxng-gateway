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
	// outcome ∈ {cache_hit, searxng_ok, premium_ok, searxng_plus_premium_ok, fallback_fail, timeout}
	// Exactly one outcome is recorded per request; a SearXNG stage timeout is
	// not a request timeout when fallback returns results before the parent deadline.
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_requests_total",
			Help: "Total gateway requests by outcome",
		},
		[]string{"outcome"},
	)

	// RequestDuration tracks actual premium-provider calls, labeled by provider and phase (primary|canary).
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "searxng_gateway_provider_duration_seconds",
			Help:    "Duration in seconds for an actual premium-provider call",
			Buckets: durationBuckets,
		},
		[]string{"provider", "phase"},
	)

	// ProviderAttemptsTotal counts exactly one terminal outcome per actual
	// premium-provider Search invocation. Skips are not attempts (see
	// ProviderSkipsTotal). outcome ∈ {success, empty, auth, quota, rate_limit,
	// network, timeout, http_5xx, degraded, invalid_response, not_configured,
	// request_error, cancelled, panic, other_error}; phase ∈ {primary,
	// canary}.
	ProviderAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_provider_attempts_total",
			Help: "Terminal outcome of each actual premium-provider call",
		},
		[]string{"provider", "phase", "outcome"},
	)

	// ProviderSkipsTotal counts provider exclusions from the premium round-robin
	// by reason. A skip is not an attempt: no provider call was made.
	ProviderSkipsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_provider_skips_total",
			Help: "Provider exclusions from the premium round-robin by reason",
		},
		[]string{"provider", "reason"},
	)

	// ProviderEligibility is a bounded one-hot gauge of the current eligibility
	// reason for each intended provider. Exactly one reason per provider is 1.
	ProviderEligibility = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_provider_eligibility",
			Help: "One-hot current eligibility reason per provider (1 for the active reason)",
		},
		[]string{"provider", "reason"},
	)

	SearxngStageDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "searxng_gateway_searxng_stage_duration_seconds",
		Help:    "Duration of a complete SearXNG HTTP retry stage",
		Buckets: durationBuckets,
	})
	SearchRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "searxng_gateway_search_request_duration_seconds",
		Help:    "Duration of the complete /search handler including response writing",
		Buckets: durationBuckets,
	})
	SearxngFailureStreak = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "searxng_gateway_searxng_failure_streak",
		Help: "Current consecutive SearXNG failures",
	})
	SearxngCooldownUntilSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "searxng_gateway_searxng_cooldown_until_seconds",
		Help: "Unix time when the current SearXNG cooldown expires, or 0 when not in cooldown",
	})

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

	// RetryAttemptsTotal counts every retry attempt (including the first).
	//   attempt:      1, 2, 3, final
	//   outcome:      success, error, exhausted, cancelled
	//   error_class:  none, 5xx, timeout, network, 4xx, cancelled, other
	// Without per-attempt instrumentation, the retry path is invisible when
	// SearXNG succeeds on the first try (no .Inc() ever fires).
	RetryAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_retry_attempts_total",
			Help: "Total retry attempts on SearXNG errors by attempt, outcome, error_class",
		},
		[]string{"attempt", "outcome", "error_class"},
	)

	// RetryExhaustedTotal counts requests where all retry attempts failed.
	//   error_class:  network
	RetryExhaustedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_retry_exhausted_total",
			Help: "Total times all retry attempts were exhausted",
		},
		[]string{"error_class"},
	)

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

	// BraveRateLimitRemaining exposes the per-period remaining request count
	// parsed from the Brave X-RateLimit-Remaining response header.
	// This reflects the request-window allowance, not account credits or
	// dollar balance.
	BraveRateLimitRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_brave_rate_limit_remaining",
			Help: "Remaining Brave API requests in the current window, parsed from X-RateLimit-Remaining header",
		},
		[]string{"period"},
	)

	// BraveRateLimitLimit exposes the per-period maximum request count
	// parsed from the Brave X-RateLimit-Limit response header.
	// This reflects the request-window allowance, not account credits or
	// dollar balance.
	BraveRateLimitLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_brave_rate_limit_limit",
			Help: "Maximum Brave API requests allowed in the current window, parsed from X-RateLimit-Limit header",
		},
		[]string{"period"},
	)

	// BraveRateLimitResetSeconds exposes seconds until the Brave API
	// request window resets, parsed from the X-RateLimit-Reset header.
	BraveRateLimitResetSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_brave_rate_limit_reset_seconds",
			Help: "Seconds until the Brave API request window resets, parsed from X-RateLimit-Reset header",
		},
		[]string{"period"},
	)
)

var durationBuckets = []float64{1, 2, 3, 4, 5, 8, 10, 15, 20, 30}

// ProviderEligibilityReasons is the bounded set of eligibility/exclusion
// reasons exposed by ProviderEligibility. Keeping the set closed prevents
// label-cardinality drift.
var ProviderEligibilityReasons = []string{
	"eligible",
	"disabled",
	"missing_key",
	"invalid_config",
	"breaker_open",
	"quota_exhausted",
}

var providerEligibilityMu sync.Mutex

// SetProviderEligibility records the active eligibility reason for a provider
// as a one-hot series: the active reason is set to 1 and every other bounded
// reason to 0. resolveReason runs while updates are serialized, so callers read
// authoritative state at write time; a delayed writer cannot publish a stale
// eligibility snapshot after a newer breaker transition.
func SetProviderEligibility(provider string, resolveReason func() string) {
	if provider == "" {
		return
	}
	providerEligibilityMu.Lock()
	defer providerEligibilityMu.Unlock()
	reason := resolveReason()
	if !containsProviderEligibilityReason(reason) {
		reason = "invalid_config"
	}
	for _, r := range ProviderEligibilityReasons {
		v := 0.0
		if r == reason {
			v = 1
		}
		ProviderEligibility.WithLabelValues(provider, r).Set(v)
	}
}

func containsProviderEligibilityReason(reason string) bool {
	for _, allowed := range ProviderEligibilityReasons {
		if reason == allowed {
			return true
		}
	}
	return false
}

var initOnce sync.Once

// Init registers all Prometheus collectors with the default registerer.
// It is safe to call multiple times — subsequent calls are no-ops.
func Init() {
	initOnce.Do(func() {
		prometheus.MustRegister(RequestsTotal, RequestDuration, SearxngStageDuration, SearchRequestDuration, SearxngFailureStreak, SearxngCooldownUntilSeconds, ResultsCount, EnginesCount, CacheSize, RetryAttemptsTotal, RetryExhaustedTotal, EngineResultsTotal, EngineUnresponsiveTotal, EngineStatus, BraveRateLimitRemaining, BraveRateLimitLimit, BraveRateLimitResetSeconds, ProviderAttemptsTotal, ProviderSkipsTotal, ProviderEligibility)
	})
}
