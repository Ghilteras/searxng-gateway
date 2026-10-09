// Package proxy implements the core gateway orchestration:
// cache check → bounded SearXNG stage (first; skipped during cooldown) →
// premium stage (always at least one admitted provider call, further providers
// round-robin only while the distinct-URL count is below the target) → output
// with premium results first (premium owns duplicate URLs), returning
// accumulated nonempty results even below target.
//
// The Proxy.Search method orchestrates the stages:
//  1. Normalise the query and check the LRU cache.
//  2. Seed provider eligibility, then consult the SearXNG cooldown; while in
//     cooldown the SearXNG stage is skipped entirely and the request is served
//     by the premium stage alone.
//  3. Run the bounded SearXNG stage (with retry+backoff) from the parent's
//     remaining budget capped at SEARXNG_TIMEOUT_SECONDS. Its distinct URLs
//     count toward the target before the premium stage starts.
//  4. Run the premium stage on every uncached request whose context is still
//     live: round-robin, each provider at most once, until at least one
//     provider call has been admitted (minAdmitted=1) AND the distinct-URL
//     count (SearXNG + premium) has reached the target, all providers are
//     exhausted, or the request deadline expires. Skips (missing key, open
//     breaker, refused admission) are not admitted calls. If SearXNG already
//     met the target and the single admitted call errors or returns empty, the
//     stage stops without trying a second provider.
//  5. Assemble the output: premium results in arrival order, then SearXNG
//     results whose URL premium did not return. Return accumulated nonempty
//     results even below the target; an empty result set is a timeout at the
//     parent deadline, otherwise a failure.
//  6. Circuit breaker: premium providers whose breaker is open are skipped.
//     Each actual premium call runs inside gobreaker.Execute (real admission)
//     and records exactly one terminal outcome; genuine provider faults trip
//     the breaker, non-provider failures (caller cancellation, overall
//     deadline, query validation, empty results) do not.
//
// Community-aligned behaviour (2026):
//   - SearXNG is queried first; the premium stage guarantees at least one
//     admitted provider call per uncached request and continues only while the
//     distinct-URL count is below the target.
//   - Round-robin premium selection distributes load evenly; premium calls are
//     serial within the premium stage.
//   - Cooldown circuit breaker for SearXNG: after SEARXNG_FAIL_THRESHOLD
//     consecutive failures, SearXNG is skipped entirely until cooldown expires.
//   - Retry network-class SearXNG failures up to 3 attempts with 250ms/500ms
//     backoff; stage timeouts and other errors are not retried,
//     bounded by the SearXNG child timeout.
//   - URL deduplication across all providers, with premium results listed first
//     and taking precedence on duplicate URLs.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

// Proxy orchestrates SearXNG-first search followed by a premium stage (at least one admitted provider call) with pluggable premium providers.
type Proxy struct {
	cfg        *config.Config
	sx         searxng.Client
	c          *cache.Cache
	breakerMgr *breaker.Manager

	// Premium providers, tried in round-robin order: at least one admitted call per
	// uncached request, further providers only while below the distinct-URL target.
	fallbackMgr *backends.Manager

	// Cooldown circuit breaker (community pattern: searxng-resilient-router)
	sxFails       int64        // atomic counter of consecutive failures
	sxCooldownTil atomic.Int64 // unix nano; 0 = no cooldown
	mu            sync.Mutex   // guards sxFails/sxCooldownTil updates
}

// New creates a Proxy with the given config, backends and cache.
func New(cfg *config.Config, sx searxng.Client, c *cache.Cache, breakerMgr *breaker.Manager, fallbackMgr *backends.Manager) *Proxy {
	return &Proxy{cfg: cfg, sx: sx, c: c, breakerMgr: breakerMgr, fallbackMgr: fallbackMgr}
}

// Search runs the full orchestration pipeline for a raw query string:
// cache, then SearXNG first (skipped while in cooldown), then the premium stage
// (at least one admitted provider call, further providers only while below the
// distinct-URL target). The output lists premium results first.
//
// Outcome counters (all via RequestsTotal):
//   - cache_hit:                  entry found in cache, no provider called.
//   - searxng_ok:                 only SearXNG contributed results (premium errored, returned empty, or had no provider to call).
//   - premium_ok:                 only premium providers contributed results (SearXNG skipped, errored, returned empty, or returned only URLs premium also returned).
//   - searxng_plus_premium_ok:    both SearXNG and premium providers contributed results.
//   - fallback_fail:              all providers exhausted with no results.
//   - timeout:                    The overall request budget expired before any results were collected.
func (p *Proxy) Search(ctx context.Context, raw string) (*searxng.Response, error) {
	ctx, span := otel.Tracer("sx/internal/proxy").Start(ctx, "proxy.search")
	defer span.End()
	key := normalize(raw)

	// 1. Cache check.
	if v, ok := p.c.Get(key); ok {
		span.AddEvent("cache.hit", trace.WithAttributes(traceAttrs("cache_hit")...))
		span.SetAttributes(attribute.String("outcome", "cache_hit"), attribute.Int("result_count", resultCount(v.(*searxng.Response))))
		span.SetStatus(codes.Ok, "")
		metrics.RequestsTotal.WithLabelValues("cache_hit").Inc()
		return v.(*searxng.Response), nil
	}
	span.AddEvent("cache.miss", trace.WithAttributes(traceAttrs("cache_miss")...))

	// Seed and refresh provider eligibility once per uncached request, before
	// any stage runs, so a request fully served by SearXNG still keeps the
	// eligibility gauges and missing_key skip counter current.
	p.initProviderEligibility()

	timeoutCtx, cancel := context.WithTimeout(ctx, p.cfg.FallbackTimeout)
	defer cancel()
	parentDeadline, _ := timeoutCtx.Deadline()

	// 2. SearXNG cooldown gates the SearXNG stage unconditionally: when in
	// cooldown the request is served by the premium stage alone.
	sxSkipped := p.inCooldown()

	// Per-call accumulators. seenURLs is the distinct-URL union of both stages
	// and only drives the target count; the output is assembled at the end,
	// premium results first.
	sxResults := make([]searxng.Result, 0)
	seenURLs := make(map[string]bool)
	usedPremiums := make(map[string]bool)
	var premiumResults []searxng.Result
	var premiumErrMsgs []string

	// 3. SearXNG stage (first): bounded, skipped only while in cooldown or when
	// the request context is already done.
	if !sxSkipped && timeoutCtx.Err() == nil && time.Until(parentDeadline) > 0 {
		budget := p.cfg.SearxngTimeout
		budgetCapped := false
		if remaining := time.Until(parentDeadline); remaining < budget {
			budget = remaining
			budgetCapped = true
		}
		sxCtx, cancelSX := context.WithTimeout(timeoutCtx, budget)

		start := time.Now()
		stageCtx, stageSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "searxng.stage")
		resp, err := p.retryWithBackoff(sxCtx, stageCtx, func() (*searxng.Response, error) {
			return p.sx.Search(sxCtx, key)
		})
		elapsed := time.Since(start)
		stageSpan.SetAttributes(attribute.String("phase", "primary"), attribute.String("outcome", searxngOutcome(resp, err)), attribute.Int("result_count", resultCount(resp)))
		stageSpan.End()
		metrics.SearxngStageDuration.Observe(elapsed.Seconds())
		cancelSX()

		if err == nil && resp != nil {
			// Per-engine metrics from the SearXNG response.
			seenEngines := make(map[string]struct{})
			for _, result := range resp.Results {
				if result.Engine != "" {
					metrics.EngineResultsTotal.WithLabelValues(result.Engine).Inc()
					metrics.EngineStatus.WithLabelValues(result.Engine).Set(1)
					seenEngines[result.Engine] = struct{}{}
				}
				for _, eng := range result.Engines {
					if eng != "" {
						metrics.EngineResultsTotal.WithLabelValues(eng).Inc()
						metrics.EngineStatus.WithLabelValues(eng).Set(1)
						seenEngines[eng] = struct{}{}
					}
				}
			}
			for eng := range seenEngines {
				p.breakerMgr.RecordEngineSeen(eng)
				p.breakerMgr.RecordSuccess(eng)
			}
			// Handle unresponsive engines.
			unresponsiveSet := make(map[string]string)
			for _, ue := range resp.UnresponsiveEngines {
				if len(ue) >= 2 {
					unresponsiveSet[ue[0]] = ue[1]
				}
			}
			for engine, reason := range unresponsiveSet {
				metrics.EngineUnresponsiveTotal.WithLabelValues(engine, reason).Inc()
				metrics.EngineStatus.WithLabelValues(engine).Set(0)
				p.breakerMgr.RecordEngineSeen(engine)
				if isClientError(reason) {
					p.breakerMgr.RecordClientError(engine, reason)
				}
			}

			p.recordSearxngSuccess()

			// Collect SearXNG's distinct URLs. They count toward the target before
			// premium starts; in the output they follow the premium results.
			for _, r := range resp.Results {
				if !seenURLs[r.URL] {
					seenURLs[r.URL] = true
					sxResults = append(sxResults, r)
				}
			}
		} else {
			// SearXNG failed. A child deadline capped by the parent's remaining
			// budget coincides with the parent deadline and says nothing about
			// SearXNG's health, so it must not count as a SearXNG failure.
			failureCounts := false
			if errors.Is(err, context.DeadlineExceeded) {
				failureCounts = !budgetCapped
			} else {
				failureCounts = timeoutCtx.Err() == nil
			}
			if failureCounts {
				p.recordSearxngFailure()
			}
			if err != nil {
				premiumErrMsgs = append(premiumErrMsgs, fmt.Sprintf("searxng: %v", err))
			}
		}
	}

	// 4. Premium stage (second): round-robin. It runs on EVERY uncached request
	// whose context is still live: at least one admitted provider call is
	// always made (minAdmitted=1), and further providers are tried only while
	// the distinct-URL union (SearXNG + premium) is below the target. Premium is
	// the last stage, so it runs directly under timeoutCtx.
	if timeoutCtx.Err() == nil {
		premiumResults, _, _, premiumErrMsgs =
			p.premiumLoop(timeoutCtx, key, premiumResults, seenURLs, usedPremiums, premiumErrMsgs, 1)
	}

	// Assemble the output: premium results in arrival order, then SearXNG
	// results whose URL premium did not already return, so premium owns duplicate
	// URLs in the output. The outcome label is derived from this composition.
	allResults := make([]searxng.Result, 0, len(premiumResults)+len(sxResults))
	allResults = append(allResults, premiumResults...)
	premiumURLs := make(map[string]bool, len(premiumResults))
	for _, r := range premiumResults {
		premiumURLs[r.URL] = true
	}
	premiumHadResults := len(premiumResults) > 0
	searxngHadResults := false
	for _, r := range sxResults {
		if !premiumURLs[r.URL] {
			allResults = append(allResults, r)
			searxngHadResults = true
		}
	}

	// 5. Outcome.
	if len(allResults) == 0 {
		outcome := "fallback_fail"
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			outcome = "timeout"
		}
		metrics.RequestsTotal.WithLabelValues(outcome).Inc()
		span.SetAttributes(attribute.String("outcome", outcome), attribute.Int("result_count", 0))
		span.SetStatus(codes.Error, "")
		errDetail := "all fallbacks failed"
		if len(premiumErrMsgs) > 0 {
			errDetail = strings.Join(premiumErrMsgs, "; ")
		}
		return nil, fmt.Errorf("%s", errDetail)
	}

	outcome := "searxng_ok"
	if searxngHadResults && premiumHadResults {
		outcome = "searxng_plus_premium_ok"
	} else if premiumHadResults {
		outcome = "premium_ok"
	}
	metrics.RequestsTotal.WithLabelValues(outcome).Inc()
	span.SetAttributes(attribute.String("outcome", outcome), attribute.Int("result_count", len(allResults)))
	span.SetStatus(codes.Ok, "")

	mapped := &searxng.Response{Results: allResults}
	p.observe(mapped)
	p.c.Set(key, mapped)
	return mapped, nil
}

// targetResults is the distinct-URL target: once SearXNG + premium distinct URLs
// reach it, the premium stage stops after its guaranteed minimum of admitted
// calls. A non-positive configured threshold is treated as 1 so the target is
// always reachable by a single result.
func (p *Proxy) targetResults() int {
	if p.cfg.SufficientMinResults < 1 {
		return 1
	}
	return p.cfg.SufficientMinResults
}

// premiumLoop is the premium stage: it iterates through premium backends
// round-robin and keeps going while EITHER
//   - the distinct-URL union (len(seenURLs): SearXNG + premium) is below the
//     target, OR
//   - fewer than minAdmitted provider calls have been admitted,
//
// and stops when all available premiums have been tried or the deadline expires.
// admittedCalls counts only calls the breaker admitted (whatever the outcome:
// success, empty or error); skips (missing_key, breaker_open, refused
// admission) do not count, so the round-robin moves on to the next provider.
// Each premium is called at most once per request (tracked via usedPremiums).
// Premium results are appended to premiumResults in arrival order; a URL
// already returned by premium is not repeated, but a URL that only SearXNG
// returned is still appended (premium owns duplicate URLs in the output).
// Returns the updated premium results, accumulators and error messages.
func (p *Proxy) premiumLoop(
	ctx context.Context,
	key string,
	premiumResults []searxng.Result,
	seenURLs map[string]bool,
	usedPremiums map[string]bool,
	errMsgs []string,
	minAdmitted int,
) ([]searxng.Result, map[string]bool, map[string]bool, []string) {
	premiumURLs := make(map[string]bool, len(premiumResults))
	for _, r := range premiumResults {
		premiumURLs[r.URL] = true
	}
	admittedCalls := 0

	for len(seenURLs) < p.targetResults() || admittedCalls < minAdmitted {
		// Stop conditions.
		if ctx.Err() != nil {
			break
		}

		premium := p.fallbackMgr.NextAvailable(usedPremiums)
		if premium == nil {
			break // all tried or none available
		}
		usedPremiums[premium.Name()] = true

		if !premium.IsAvailable() {
			p.recordProviderSkip(premium.Name(), "missing_key")
			continue
		}
		id := premiumBreakerID(premium.Name())
		if p.breakerMgr.IsOpen(id) {
			p.recordProviderSkip(premium.Name(), "breaker_open")
			continue
		}

		phase := "primary"
		if p.breakerMgr.State(id) == gobreaker.StateHalfOpen {
			phase = "canary"
		}

		// Call the premium backend under real circuit-breaker admission.
		providerCtx, providerSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "premium.primary")
		providerSpan.SetAttributes(attribute.String("provider", premium.Name()), attribute.String("phase", phase))
		if ctx.Err() != nil {
			providerSpan.End()
			break
		}
		results, outcome, admitted, callErr := p.admitPremiumCall(providerCtx, key, premium, id, phase)
		providerSpan.SetAttributes(attribute.String("outcome", outcome), attribute.Int("result_count", len(results)))
		providerSpan.End()

		if !admitted {
			// Not admitted: either the breaker opened between the IsOpen check and
			// admission, or it is half-open and the single probe slot is already
			// taken (gobreaker returns ErrTooManyRequests and fn never runs).
			p.recordProviderSkip(premium.Name(), "breaker_open")
			continue
		}
		admittedCalls++
		// Execute has now observed the terminal call outcome and completed any
		// trip/recovery transition. Keep the one-hot eligibility gauge current
		// in the same request, without waiting for a later refresh.
		p.refreshProviderEligibility(premium)
		if callErr != nil {
			logPremiumFailure(premium.Name(), phase, outcome)
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", premium.Name(), callErr))
			continue
		}
		if len(results) == 0 {
			continue // reachable, empty result set: no trip
		}

		metrics.EngineResultsTotal.WithLabelValues(premium.Name()).Add(float64(len(results)))

		// Collect premium results (dedup among premium only); the union set
		// keeps the target count distinct across both stages.
		for _, r := range results {
			if premiumURLs[r.URL] {
				continue
			}
			premiumURLs[r.URL] = true
			seenURLs[r.URL] = true
			premiumResults = append(premiumResults, searxng.Result{
				Title:   r.Title,
				URL:     r.URL,
				Content: r.Content,
				Engine:  r.Engine,
				Engines: []string{r.Engine},
			})
		}
	}

	return premiumResults, seenURLs, usedPremiums, errMsgs
}

// premiumBreakerID namespaces premium-provider breakers so a SearXNG engine
// sighting with the same name can never reset a premium API breaker.
func premiumBreakerID(provider string) string {
	return "premium:" + provider
}

// initProviderEligibility seeds eligibility and breaker-state visibility for
// every registered provider before the first call.
func (p *Proxy) initProviderEligibility() {
	for _, name := range p.fallbackMgr.AvailableBackends() {
		backend, ok := p.fallbackMgr.GetBackend(name)
		if !ok {
			continue
		}
		p.refreshProviderEligibility(backend)
		// NextAvailable deliberately filters unconfigured backends out before
		// selection; account for that exclusion here rather than calling it an
		// attempt or silently losing the skip reason.
		if !backend.IsAvailable() {
			p.recordProviderSkip(name, "missing_key")
		}
	}
}

// refreshProviderEligibility records the current one-hot eligibility reason for
// a provider. It also ensures the breaker-state gauge series exists without
// ever resetting an open breaker.
func (p *Proxy) refreshProviderEligibility(provider backends.SearchBackend) {
	name := provider.Name()
	id := premiumBreakerID(name)
	p.breakerMgr.RecordEngineSeen(id)
	metrics.SetProviderEligibility(name, func() string {
		return p.providerEligibilityReason(provider)
	})
}

// providerEligibilityReason reads authoritative backend and breaker state at
// the point the metrics package serializes and applies the one-hot update.
func (p *Proxy) providerEligibilityReason(provider backends.SearchBackend) string {
	if !provider.IsAvailable() {
		return "missing_key"
	}
	if p.breakerMgr.IsOpen(premiumBreakerID(provider.Name())) {
		return "breaker_open"
	}
	return "eligible"
}

// recordProviderSkip counts an exclusion and updates the provider's eligibility.
// Skips are not attempts: no provider call was made.
func (p *Proxy) recordProviderSkip(name, reason string) {
	metrics.ProviderSkipsTotal.WithLabelValues(name, reason).Inc()
	backend, ok := p.fallbackMgr.GetBackend(name)
	if !ok {
		metrics.SetProviderEligibility(name, func() string { return "invalid_config" })
		return
	}
	p.refreshProviderEligibility(backend)
}

// admitPremiumCall runs one premium provider call inside the real
// circuit-breaker admission. It records exactly one terminal outcome and the
// call duration, stores the breaker reason on a trip, and reports whether the
// breaker admitted the call (admitted=false means no provider call was made).
func (p *Proxy) admitPremiumCall(
	ctx context.Context,
	key string,
	provider backends.SearchBackend,
	id string,
	phase string,
) (results []backends.SearchResult, outcome string, admitted bool, callErr error) {
	_, _ = p.breakerMgr.Execute(id, func() (res interface{}, err error) {
		admitted = true
		start := time.Now()
		defer func() {
			metrics.RequestDuration.WithLabelValues(provider.Name(), phase).Observe(time.Since(start).Seconds())
		}()
		defer func() {
			if r := recover(); r != nil {
				outcome = "panic"
				metrics.ProviderAttemptsTotal.WithLabelValues(provider.Name(), phase, outcome).Inc()
				p.breakerMgr.RecordReason(id, outcome)
				callErr = fmt.Errorf("provider %s panicked", provider.Name())
				res, err = nil, callErr
			}
		}()
		results, callErr = provider.Search(backends.SearchOptions{
			Context:    ctx,
			Query:      key,
			NumResults: 10,
		})
		outcome = classifyPremiumOutcome(ctx, results, callErr)
		metrics.ProviderAttemptsTotal.WithLabelValues(provider.Name(), phase, outcome).Inc()
		switch {
		case outcome == "success" || outcome == "empty":
			return results, nil
		case !premiumTrips(outcome):
			// Caller cancellation, overall deadline, query validation or an
			// unconfigured provider must never trip the breaker.
			return results, breaker.ErrNonProviderFault
		default:
			p.breakerMgr.RecordReason(id, outcome)
			return results, callErr
		}
	})
	return results, outcome, admitted, callErr
}

// recordSearxngSuccess resets the failure counter and clears any active cooldown.
func (p *Proxy) recordSearxngSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt64(&p.sxFails, 0)
	p.sxCooldownTil.Store(0)
	metrics.SearxngFailureStreak.Set(0)
	metrics.SearxngCooldownUntilSeconds.Set(0)
}

// recordSearxngFailure increments the failure counter and starts a cooldown
// if the threshold is reached.
func (p *Proxy) recordSearxngFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	fails := atomic.AddInt64(&p.sxFails, 1)
	metrics.SearxngFailureStreak.Set(float64(fails))
	if int(fails) >= p.cfg.SearxngFailThreshold {
		until := time.Now().Add(p.cfg.SearxngFailCooldown).UnixNano()
		p.sxCooldownTil.Store(until)
		metrics.SearxngCooldownUntilSeconds.Set(float64(until / int64(time.Second)))
	}
}

// inCooldown reports whether SearXNG is currently in cooldown. If the
// cooldown period has expired, it is automatically cleared.
func (p *Proxy) inCooldown() bool {
	until := p.sxCooldownTil.Load()
	if until == 0 {
		return false
	}
	if time.Now().UnixNano() >= until {
		// Recheck under the same lock used by success/failure transitions. A
		// concurrent failure may have restarted or extended the cooldown.
		p.mu.Lock()
		defer p.mu.Unlock()
		until = p.sxCooldownTil.Load()
		if until == 0 {
			return false
		}
		if time.Now().UnixNano() >= until {
			// Cooldown expired — reset state.
			p.sxCooldownTil.Store(0)
			atomic.StoreInt64(&p.sxFails, 0)
			metrics.SearxngFailureStreak.Set(0)
			metrics.SearxngCooldownUntilSeconds.Set(0)
			return false
		}
		return true
	}
	return true
}

// observe records Prometheus metrics for the given response.
func (p *Proxy) observe(r *searxng.Response) {
	metrics.ResultsCount.Observe(float64(len(r.Results)))

	distinct := make(map[string]struct{}, len(r.Results))
	for _, res := range r.Results {
		distinct[res.Engine] = struct{}{}
	}
	metrics.EnginesCount.Set(float64(len(distinct)))

	metrics.CacheSize.Set(float64(p.c.Len()))
}

// retryWithBackoff calls fn up to maxAttempts with exponential backoff.
//   - attempt 1: immediate
//   - attempt 2: after 250ms
//   - attempt 3: after 500ms (final)
//
// Returns the last error if all retries fail.
// Only network-class failures are retried. Stage timeouts and other errors are
// returned immediately so the premium stage retains the parent request budget.
//
// Metrics (v0.8.1): every attempt is instrumented with attempt, outcome,
// and error_class. Without per-attempt metrics, the retry path is invisible
// to monitoring when SearXNG succeeds on the first try.
func (p *Proxy) retryWithBackoff(ctx context.Context, parent context.Context, fn func() (*searxng.Response, error)) (*searxng.Response, error) {
	backoff := 250 * time.Millisecond
	const maxAttempts = 3

	var lastErr error
	var lastResp *searxng.Response

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			if lastErr != nil {
				return lastResp, lastErr
			}
			return lastResp, ctx.Err()
		}
		attemptCtx, attemptSpan := otel.Tracer("sx/internal/proxy").Start(parent, "searxng.attempt")
		attemptSpan.SetAttributes(attribute.Int("attempt", attempt), attribute.String("phase", "primary"))
		if attempt > 1 {
			attemptSpan.AddEvent("retry.backoff", trace.WithAttributes(traceAttrs("backoff")...))
			select {
			case <-ctx.Done():
				attemptSpan.SetAttributes(attribute.String("outcome", "cancelled"))
				attemptSpan.End()
				// Context cancelled while waiting between attempts.
				metrics.RetryAttemptsTotal.WithLabelValues(
					fmt.Sprintf("%d", attempt), "cancelled", "cancelled",
				).Inc()
				if lastErr != nil {
					return lastResp, lastErr
				}
				return lastResp, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2 // exponential: 250ms -> 500ms
		}

		_, callSpan := otel.Tracer("sx/internal/proxy").Start(attemptCtx, "searxng.http")
		resp, err := fn()
		callSpan.SetAttributes(attribute.String("phase", "primary"), attribute.Int("attempt", attempt), attribute.String("outcome", spanOutcome(err)), attribute.Int("result_count", resultCount(resp)))
		callSpan.End()
		outcome := "success"
		errClass := "none"
		if err != nil {
			outcome = "error"
			errClass = classifyError(err)
		}
		metrics.RetryAttemptsTotal.WithLabelValues(
			fmt.Sprintf("%d", attempt), outcome, errClass,
		).Inc()

		if err == nil {
			attemptSpan.SetAttributes(attribute.String("outcome", "success"))
			attemptSpan.End()
			return resp, nil
		}
		attemptSpan.SetAttributes(attribute.String("outcome", "error"))
		attemptSpan.End()
		lastErr = err
		lastResp = resp
		if errClass != "network" {
			return lastResp, lastErr
		}
	}

	// All retry attempts exhausted.
	errClass := classifyError(lastErr)
	metrics.RetryAttemptsTotal.WithLabelValues("final", "exhausted", errClass).Inc()
	metrics.RetryExhaustedTotal.WithLabelValues(errClass).Inc()
	return lastResp, lastErr
}

func traceAttrs(value string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("outcome", value)}
}
func spanOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
func searxngOutcome(resp *searxng.Response, err error) string {
	if err != nil {
		return spanOutcome(err)
	}
	if resp == nil {
		return "error"
	}
	return "success"
}
func resultCount(r *searxng.Response) int {
	if r == nil {
		return 0
	}
	return len(r.Results)
}

// classifyError maps an error to a Prometheus label value for retry metrics.
//   - context.DeadlineExceeded         -> "timeout"
//   - context.Canceled                 -> "cancelled"
//   - HTTP 5xx (500/502/503/504/...)   -> "5xx"
//   - network errors                   -> "network"
//   - HTTP 4xx (rare)                  -> "4xx"
//   - default                          -> "other"
func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	msg := err.Error()
	low := strings.ToLower(msg)
	if strings.Contains(low, "5xx") ||
		strings.Contains(low, "http error 5") ||
		strings.Contains(msg, "500 ") || strings.Contains(msg, "502 ") ||
		strings.Contains(msg, "503 ") || strings.Contains(msg, "504 ") ||
		strings.Contains(low, "internal server error") ||
		strings.Contains(low, "bad gateway") ||
		strings.Contains(low, "service unavailable") ||
		strings.Contains(low, "gateway timeout") {
		return "5xx"
	}
	if strings.Contains(low, "4xx") || strings.Contains(low, "http error 4") {
		return "4xx"
	}
	if strings.Contains(low, "connection refused") ||
		strings.Contains(low, "no such host") ||
		strings.Contains(low, "dial tcp") ||
		strings.Contains(low, "i/o timeout") ||
		strings.Contains(low, "connection reset") ||
		strings.Contains(low, "no route to host") ||
		strings.Contains(low, "network is unreachable") {
		return "network"
	}
	return "other"
}

// normalize lower-cases a query, trims spaces and collapses whitespace runs.
func normalize(q string) string {
	return strings.Join(strings.Fields(strings.ToLower(q)), " ")
}

// isClientError returns true if reason indicates a 4xx client error
// (server is blocking us: 402, 403, 429, access denied, forbidden, etc.)
//
// Pattern coverage:
//
//	4xx HTTP codes:           "HTTP error 4", "HTTP 402", "HTTP 403", "HTTP 429"
//	Cloudflare-style blocks:  "blocked", "blocked by"
//	Rate limiting:            "too many requests", "rate limited"
//	Auth/access:              "access denied", "forbidden", "unauthorized", "not found"
//	Billing/quota:            "payment required"
//	Bot detection:            "captcha"
//
// 5xx, timeout, HTTP error (5xx), connection refused = server error (retry).
func isClientError(reason string) bool {
	if strings.Contains(reason, "access denied") {
		return true
	}
	if strings.Contains(reason, "forbidden") {
		return true
	}
	if strings.Contains(reason, "too many requests") {
		return true
	}
	if strings.Contains(reason, "rate limited") {
		return true
	}
	if strings.Contains(reason, "not found") {
		return true
	}
	if strings.Contains(reason, "unauthorized") {
		return true
	}
	if strings.Contains(reason, "HTTP error 4") {
		return true
	}
	if strings.Contains(reason, "HTTP 402") {
		return true
	}
	if strings.Contains(reason, "HTTP 403") {
		return true
	}
	if strings.Contains(reason, "HTTP 429") {
		return true
	}
	if strings.Contains(strings.ToLower(reason), "payment required") {
		return true
	}
	if strings.Contains(reason, "blocked by") {
		return true
	}
	if strings.Contains(reason, "blocked") {
		return true
	}
	if strings.Contains(reason, "banned") {
		return true
	}
	if strings.Contains(reason, "captcha") {
		return true
	}
	// 5xx, timeout, HTTP error (5xx), connection refused = server error
	return false
}

// classifyPremiumOutcome maps a premium provider call to exactly one bounded
// terminal outcome. Context errors are inspected before BackendError.Code, so a
// caller cancellation or an overall request deadline never trips the breaker
// while a provider timeout with a live parent request does.
func classifyPremiumOutcome(parent context.Context, results []backends.SearchResult, err error) string {
	if err == nil {
		if len(results) == 0 {
			return "empty"
		}
		return "success"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if parent != nil && parent.Err() != nil {
			return "cancelled" // overall request deadline, not a provider fault
		}
		return "timeout" // provider timeout while the parent request is live
	}
	var backendErr *backends.BackendError
	if errors.As(err, &backendErr) {
		switch {
		case backendErr.Code == backends.ErrCodeUnavailable:
			return "not_configured"
		case backendErr.Code == backends.ErrCodeDegraded:
			return "degraded"
		case backendErr.Code == backends.ErrCodeAuth:
			return "auth"
		case backendErr.Code == backends.ErrCodeRateLimit:
			return "rate_limit"
		case backendErr.Code == backends.ErrCodeNetwork:
			return "network"
		case backendErr.Code == backends.ErrCodeInvalidResponse:
			return "invalid_response"
		case backendErr.Code == 400 || backendErr.Code == 422:
			// 400/422 are query-validation errors unless the provider says the
			// failure is about credits/quota.
			if isQuotaText(err.Error()) {
				return "quota"
			}
			return "request_error"
		case backendErr.Code == 401 || backendErr.Code == 403:
			return "auth"
		case backendErr.Code == 402:
			return "quota"
		case backendErr.Code == 429:
			return "rate_limit"
		case backendErr.Code >= 500 && backendErr.Code <= 599:
			return "http_5xx"
		case backendErr.Code >= 400 && backendErr.Code <= 499:
			// Non-standard 4xx (e.g. Tavily 432 plan limit) is an admission
			// failure, not a query-validation error.
			return "quota"
		default:
			return "other_error"
		}
	}
	if isQuotaText(err.Error()) {
		return "quota"
	}
	if isClientError(err.Error()) {
		return "auth"
	}
	return "other_error"
}

// premiumTrips reports whether a terminal outcome is a provider fault that must
// trip the circuit breaker.
func premiumTrips(outcome string) bool {
	switch outcome {
	case "auth", "quota", "rate_limit", "network", "timeout", "http_5xx", "degraded", "invalid_response", "panic", "other_error":
		return true
	default:
		return false
	}
}

// isQuotaText reports whether an error message describes a credits/quota
// failure rather than a malformed request.
func isQuotaText(msg string) bool {
	low := strings.ToLower(msg)
	for _, needle := range []string{"credit", "quota", "balance", "insufficient", "payment required", "billing"} {
		if strings.Contains(low, needle) {
			return true
		}
	}
	return false
}

func logPremiumFailure(provider, phase, outcome string) {
	if premiumTrips(outcome) {
		log.Printf("[WARN] provider_failure provider=%s phase=%s outcome=%s", provider, phase, outcome)
		return
	}
	log.Printf("[INFO] provider_failure provider=%s phase=%s outcome=%s", provider, phase, outcome)
}
