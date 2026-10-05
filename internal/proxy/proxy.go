// Package proxy implements the core gateway orchestration:
// cache check → sequential premium round-robin (primary) until the target
// result count is reached → bounded SearXNG secondary stage on shortfall →
// merge, dedupe by URL (premium owns duplicates), and return accumulated
// nonempty results even below target.
//
// The Proxy.Search method orchestrates the stages:
//  1. Normalise the query and check the LRU cache.
//  2. Run the premium providers via round-robin (primary), each at most once,
//     until the target distinct-URL count is reached, all providers are
//     exhausted, or the premium-stage deadline (parent budget minus a reserve
//     for the secondary stage) expires.
//  3. If the target was reached, return immediately without calling SearXNG.
//  4. Otherwise run the bounded SearXNG secondary stage (with retry+backoff)
//     from the parent's remaining budget capped at SEARXNG_TIMEOUT_SECONDS,
//     honoring the existing cooldown circuit breaker, and merge its results
//     after the premium results.
//  5. Return accumulated nonempty results even below the target; an empty
//     result set is a timeout at the parent deadline, otherwise a failure.
//  6. Circuit breaker: premiums where breakerMgr.IsOpen(name) are skipped.
//     After success: RecordSuccess. After failure: RecordClientError.
//
// Community-aligned behaviour (2026):
//   - Premium providers are the primary source; SearXNG is a bounded secondary
//     reached only when the primary falls short of the target.
//   - Round-robin premium selection distributes load evenly; premium calls are
//     serial within the primary stage.
//   - Cooldown circuit breaker for SearXNG: after SEARXNG_FAIL_THRESHOLD
//     consecutive failures, SearXNG is skipped entirely until cooldown expires.
//   - Retry network-class SearXNG failures up to 3 attempts with 250ms/500ms
//     backoff; stage timeouts and other errors are not retried,
//     bounded by the SearXNG child timeout.
//   - URL deduplication across all providers, with premium results taking
//     precedence on duplicate URLs.
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

// Proxy orchestrates premium-first search with a bounded SearXNG secondary and pluggable fallback providers.
type Proxy struct {
	cfg        *config.Config
	sx         searxng.Client
	c          *cache.Cache
	breakerMgr *breaker.Manager

	// Fallback chain: premium providers, tried in the primary stage order.
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

// Search runs the full orchestration pipeline for a raw query string.
//
// Outcome counters (all via RequestsTotal):
//   - cache_hit:                  entry found in cache, no provider called.
//   - searxng_ok:                 only SearXNG contributed results (no premium results).
//   - premium_ok:                 only premium providers contributed results (SearXNG skipped, errored, or returned empty).
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

	timeoutCtx, cancel := context.WithTimeout(ctx, p.cfg.FallbackTimeout)
	defer cancel()
	parentDeadline, _ := timeoutCtx.Deadline()

	// 2. SearXNG cooldown is consulted only when the secondary stage is reached.
	sxSkipped := p.inCooldown()

	// Per-call accumulators.
	allResults := make([]searxng.Result, 0)
	seenURLs := make(map[string]bool)
	usedPremiums := make(map[string]bool)
	premiumHadResults := false
	searxngHadResults := false
	var premiumErrMsgs []string

	target := p.targetResults()

	// 3. PRIMARY stage: sequential premium round-robin until the target is met.
	// Reserve the tail of the request budget for the bounded SearXNG secondary
	// stage so the premium stage cannot consume the whole deadline.
	reserve := time.Second
	if half := p.cfg.FallbackTimeout / 2; reserve > half {
		reserve = half
	}
	premiumCtx, cancelPremium := context.WithDeadline(timeoutCtx, parentDeadline.Add(-reserve))
	defer cancelPremium()

	allResults, seenURLs, _, premiumHadResults, premiumErrMsgs =
		p.premiumLoop(premiumCtx, key, allResults, seenURLs, usedPremiums, premiumHadResults, premiumErrMsgs)

	// 4. SECONDARY stage: bounded SearXNG, only when the primary fell short and
	// the request context is still live.
	if len(allResults) < target && !sxSkipped && timeoutCtx.Err() == nil && time.Until(parentDeadline) > 0 {
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
		stageSpan.SetAttributes(attribute.String("phase", "continuation"), attribute.String("outcome", searxngOutcome(resp, err)), attribute.Int("result_count", resultCount(resp)))
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

			// Merge SearXNG results after the premium results (dedup by URL), so
			// premium results own duplicate URLs.
			for _, r := range resp.Results {
				if !seenURLs[r.URL] {
					seenURLs[r.URL] = true
					allResults = append(allResults, r)
					searxngHadResults = true
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

// targetResults is the distinct-URL target for the primary premium stage. A
// non-positive configured threshold is treated as 1 so it cannot silently
// suppress the primary loop and turn a recoverable search into a failure.
func (p *Proxy) targetResults() int {
	if p.cfg.SufficientMinResults < 1 {
		return 1
	}
	return p.cfg.SufficientMinResults
}

// premiumLoop is the primary premium stage: it iterates through premium backends
//   - merged results reach SufficientMinResults, OR
//   - all available premiums have been tried, OR
//   - the deadline expires.
//
// Each premium is called at most once per request (tracked via usedPremiums).
// Premiums where the circuit breaker is open are skipped.
// Returns the updated accumulators.
func (p *Proxy) premiumLoop(
	ctx context.Context,
	key string,
	allResults []searxng.Result,
	seenURLs map[string]bool,
	usedPremiums map[string]bool,
	premiumHadResults bool,
	errMsgs []string,
) ([]searxng.Result, map[string]bool, map[string]bool, bool, []string) {
	for len(allResults) < p.targetResults() {
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
			continue
		}
		if p.breakerMgr.IsOpen(premium.Name()) {
			continue
		}

		// Call the premium backend.
		providerCtx, providerSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "premium.primary")
		providerSpan.SetAttributes(attribute.String("provider", premium.Name()), attribute.String("phase", "primary"))
		if ctx.Err() != nil {
			providerSpan.End()
			break
		}
		start := time.Now()
		results, err := premium.Search(backends.SearchOptions{
			Context:    providerCtx,
			Query:      key,
			NumResults: 10,
		})
		providerSpan.SetAttributes(attribute.String("outcome", spanOutcome(err)), attribute.Int("result_count", len(results)))
		providerSpan.End()
		elapsed := time.Since(start)
		metrics.RequestDuration.WithLabelValues(premium.Name(), "primary").Observe(elapsed.Seconds())

		if err != nil {
			clientError, category := classifyPremiumError(err)
			before := p.breakerMgr.State(premium.Name())
			if clientError {
				p.breakerMgr.RecordClientError(premium.Name(), category)
			}
			logPremiumFailure(premium.Name(), "primary", category, clientError, before, p.breakerMgr.State(premium.Name()))
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", premium.Name(), err))
			continue
		}
		if len(results) == 0 {
			continue
		}

		p.breakerMgr.RecordSuccess(premium.Name())
		metrics.EngineResultsTotal.WithLabelValues(premium.Name()).Add(float64(len(results)))

		// Merge and deduplicate by URL.
		for _, r := range results {
			if !seenURLs[r.URL] {
				seenURLs[r.URL] = true
				premiumHadResults = true
				allResults = append(allResults, searxng.Result{
					Title:   r.Title,
					URL:     r.URL,
					Content: r.Content,
					Engine:  r.Engine,
					Engines: []string{r.Engine},
				})
			}
		}
	}

	return allResults, seenURLs, usedPremiums, premiumHadResults, errMsgs
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
		attemptSpan.SetAttributes(attribute.Int("attempt", attempt), attribute.String("phase", "continuation"))
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
		callSpan.SetAttributes(attribute.String("phase", "continuation"), attribute.Int("attempt", attempt), attribute.String("outcome", spanOutcome(err)), attribute.Int("result_count", resultCount(resp)))
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

// classifyPremiumError uses backend status/type data where available. Text is
// consulted only for unstructured errors; returned categories are bounded.
func classifyPremiumError(err error) (bool, string) {
	var backendErr *backends.BackendError
	if errors.As(err, &backendErr) {
		switch {
		case backendErr.Code >= 400 && backendErr.Code <= 499:
			return true, "http_4xx"
		case backendErr.Code == backends.ErrCodeAuth:
			return true, "auth"
		case backendErr.Code == backends.ErrCodeRateLimit:
			return true, "rate_limit"
		case backendErr.Code >= 500 && backendErr.Code <= 599:
			return false, "http_5xx"
		case backendErr.Code == backends.ErrCodeNetwork:
			return false, "network"
		case backendErr.Code == backends.ErrCodeInvalidResponse:
			return false, "invalid_response"
		case backendErr.Code == backends.ErrCodeUnavailable || backendErr.Code == backends.ErrCodeDegraded:
			return false, "unavailable"
		default:
			return false, "backend_error"
		}
	}
	if isClientError(err.Error()) {
		return true, "unstructured_client_error"
	}
	return false, "unstructured_error"
}

func logPremiumFailure(provider, phase, category string, clientError bool, before, after gobreaker.State) {
	action := "continue"
	if after == gobreaker.StateOpen && before != gobreaker.StateOpen {
		action = "breaker_open"
		log.Printf("[WARN] provider_failure provider=%s phase=%s class=%s breaker=%s", provider, phase, category, action)
		return
	}
	if clientError {
		action = "breaker_recorded"
	}
	log.Printf("[INFO] provider_failure provider=%s phase=%s class=%s breaker=%s", provider, phase, category, action)
}
