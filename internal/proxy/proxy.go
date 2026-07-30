// Package proxy implements the core gateway orchestration:
// cache check → parallel SearXNG + premium → round-robin premium loop until threshold.
//
// The Proxy.Search method orchestrates the stages:
//  1. Normalise the query and check the LRU cache.
//  2. Check SearXNG cooldown (binary fallback circuit breaker).
//  3. Call SearXNG (with retry+backoff) AND first premium (round-robin) in parallel.
//  4. Merge SearXNG + premium results, deduplicating by URL.
//  5. If merged results < SufficientMinResults:
//     call additional premiums (round-robin, non-repeating) until threshold met,
//     all premiums exhausted, or FallbackTimeout expires.
//  6. Circuit breaker: premiums where breakerMgr.IsOpen(name) are skipped.
//     After success: RecordSuccess. After failure: RecordClientError.
//
// Community-aligned behaviour (2026):
//   - Always call at least one premium alongside SearXNG (not just on failure).
//   - Round-robin premium selection distributes load evenly.
//   - Cooldown circuit breaker for SearXNG: after SEARXNG_FAIL_THRESHOLD
//     consecutive failures, SearXNG is skipped entirely until cooldown expires.
//   - Retry with exponential backoff (3 attempts: 1s/2s/4s) for SearXNG errors.
//   - URL deduplication across SearXNG and all premium providers.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

// Proxy orchestrates SearXNG-first search with pluggable fallback providers.
type Proxy struct {
	cfg        *config.Config
	sx         searxng.Client
	c          *cache.Cache
	breakerMgr *breaker.Manager

	// Fallback chain: tried in order when SearXNG is insufficient.
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
//   - cache_hit:           entry found in cache, SearXNG not called.
//   - searxng_ok:          SearXNG returned a sufficient response.
//   - timeout:             SearXNG returned context.DeadlineExceeded.
//   - fallback_brave_ok:   SearXNG insufficient/failed/cooldown, Brave OK.
//   - fallback_brave_fail: SearXNG insufficient/failed/cooldown, Brave also failed.
func (p *Proxy) Search(ctx context.Context, raw string) (*searxng.Response, error) {
	key := normalize(raw)

	// 1. Cache check.
	if v, ok := p.c.Get(key); ok {
		metrics.RequestsTotal.WithLabelValues("cache_hit").Inc()
		return v.(*searxng.Response), nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, p.cfg.FallbackTimeout)
	defer cancel()

	// 2. Check if SearXNG is in cooldown (skip SearXNG, go premium-only).
	sxSkipped := p.inCooldown()
	deadline := time.Now().Add(p.cfg.FallbackTimeout)

	// Per-call state
	allResults := make([]searxng.Result, 0)
	seenURLs := make(map[string]bool)
	usedPremiums := make(map[string]bool)
	premiumHadResults := false
	var premiumErrMsgs []string

	// 3. Channel to collect SearXNG result from goroutine.
	type sxResult struct {
		resp *searxng.Response
		err  error
		elapsed time.Duration
	}
	sxCh := make(chan sxResult, 1)

	if !sxSkipped {
		go func() {
			start := time.Now()
			resp, err := p.retryWithBackoff(timeoutCtx, func() (*searxng.Response, error) {
				return p.sx.Search(timeoutCtx, key)
			})
			sxCh <- sxResult{resp: resp, err: err, elapsed: time.Since(start)}
		}()
	} else {
		// Cooldown active: signal SearXNG skipped.
		close(sxCh)
	}

	// 4. T1 premium: if T1_PREMIUM_COUNT > 0, pick that many distinct providers
	// via round-robin and call each one. Runs in parallel with the SearXNG goroutine.
	for i := 0; i < p.cfg.T1PremiumCount; i++ {
		if time.Now().After(deadline) {
			break
		}
		premium := p.fallbackMgr.NextAvailable(usedPremiums)
		if premium == nil || !premium.IsAvailable() || p.breakerMgr.IsOpen(premium.Name()) {
			continue
		}
		usedPremiums[premium.Name()] = true
		start := time.Now()
		results, err := premium.Search(backends.SearchOptions{
			Query:      key,
			NumResults: 10,
		})
		elapsed := time.Since(start)
		metrics.RequestDuration.WithLabelValues(premium.Name(), premium.Name()).Observe(elapsed.Seconds())

		if err != nil {
			p.breakerMgr.RecordClientError(premium.Name(), err.Error())
			premiumErrMsgs = append(premiumErrMsgs, fmt.Sprintf("%s: %v", premium.Name(), err))
		} else if len(results) > 0 {
			p.breakerMgr.RecordSuccess(premium.Name())
			metrics.EngineResultsTotal.WithLabelValues(premium.Name()).Add(float64(len(results)))
			premiumHadResults = true
			for _, r := range results {
				if !seenURLs[r.URL] {
					seenURLs[r.URL] = true
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
	}

	// 5. Collect SearXNG result.
	if sxResult, ok := <-sxCh; ok {
		if sxResult.err == nil && sxResult.resp != nil {
			// Per-engine metrics from SearXNG response.
			seenEngines := make(map[string]struct{})
			for _, result := range sxResult.resp.Results {
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
				metrics.RequestDuration.WithLabelValues("searxng", eng).Observe(sxResult.elapsed.Seconds())
				p.breakerMgr.RecordEngineSeen(eng)
				p.breakerMgr.RecordSuccess(eng)
			}
			// Handle unresponsive engines.
			unresponsiveSet := make(map[string]string)
			for _, ue := range sxResult.resp.UnresponsiveEngines {
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

			// Merge SearXNG results into allResults (dedup by URL).
			// Outcome label is set once at the end (step 7).
			for _, r := range sxResult.resp.Results {
				if !seenURLs[r.URL] {
					seenURLs[r.URL] = true
					allResults = append(allResults, r)
				}
			}
		} else {
			// SearXNG failed.
			p.recordSearxngFailure()
			if errors.Is(sxResult.err, context.DeadlineExceeded) {
				metrics.RequestsTotal.WithLabelValues("timeout").Inc()
			}
			if sxResult.err != nil {
				premiumErrMsgs = append(premiumErrMsgs, fmt.Sprintf("searxng: %v", sxResult.err))
			}
		}
	}

	// 6. Fallback loop: all remaining FALLBACK_PROVIDERS (skips already-used).
	if len(allResults) < p.cfg.SufficientMinResults {
		allResults, seenURLs, usedPremiums, premiumHadResults, premiumErrMsgs =
			p.premiumLoop(timeoutCtx, key, deadline, allResults, seenURLs, usedPremiums, premiumHadResults, premiumErrMsgs)
	}

	// 7. Outcome.
	if len(allResults) == 0 {
		metrics.RequestsTotal.WithLabelValues("fallback_fail").Inc()
		errDetail := "all fallbacks failed"
		if len(premiumErrMsgs) > 0 {
			errDetail = strings.Join(premiumErrMsgs, "; ")
		}
		return nil, fmt.Errorf("%s", errDetail)
	}

	outcome := "searxng_ok"
	if premiumHadResults {
		outcome = "premium_ok"
		// Check if SearXNG also contributed.
		if !sxSkipped {
			outcome = "searxng_plus_premium_ok"
		}
	}
	metrics.RequestsTotal.WithLabelValues(outcome).Inc()

	mapped := &searxng.Response{Results: allResults}
	p.observe(mapped)
	p.c.Set(key, mapped)
	return mapped, nil
}

// sufficient returns true when the SearXNG response has at least SufficientMinResults.
func (p *Proxy) sufficient(r *searxng.Response) bool {
	return len(r.Results) >= p.cfg.SufficientMinResults
}

// premiumLoop iterates through premium backends via round-robin until:
//   - merged results reach SufficientMinResults, OR
//   - all available premiums have been tried, OR
//   - the deadline expires.
// Each premium is called at most once per request (tracked via usedPremiums).
// Premiums where the circuit breaker is open are skipped.
// Returns the updated accumulators.
func (p *Proxy) premiumLoop(
	ctx context.Context,
	key string,
	deadline time.Time,
	allResults []searxng.Result,
	seenURLs map[string]bool,
	usedPremiums map[string]bool,
	premiumHadResults bool,
	errMsgs []string,
) ([]searxng.Result, map[string]bool, map[string]bool, bool, []string) {
	for {
		// Stop conditions.
		if len(allResults) >= p.cfg.SufficientMinResults {
			break
		}
		if time.Now().After(deadline) {
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
		start := time.Now()
		results, err := premium.Search(backends.SearchOptions{
			Query:      key,
			NumResults: 10,
		})
		elapsed := time.Since(start)
		metrics.RequestDuration.WithLabelValues(premium.Name(), premium.Name()).Observe(elapsed.Seconds())

		if err != nil {
			p.breakerMgr.RecordClientError(premium.Name(), err.Error())
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", premium.Name(), err))
			continue
		}
		if len(results) == 0 {
			continue
		}

		p.breakerMgr.RecordSuccess(premium.Name())
		metrics.EngineResultsTotal.WithLabelValues(premium.Name()).Add(float64(len(results)))
		premiumHadResults = true

		// Merge and deduplicate by URL.
		for _, r := range results {
			if !seenURLs[r.URL] {
				seenURLs[r.URL] = true
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
}

// recordSearxngFailure increments the failure counter and starts a cooldown
// if the threshold is reached.
func (p *Proxy) recordSearxngFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	fails := atomic.AddInt64(&p.sxFails, 1)
	if int(fails) >= p.cfg.SearxngFailThreshold {
		until := time.Now().Add(p.cfg.SearxngFailCooldown).UnixNano()
		p.sxCooldownTil.Store(until)
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
		// Cooldown expired — reset state.
		p.sxCooldownTil.Store(0)
		atomic.StoreInt64(&p.sxFails, 0)
		return false
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

// retryWithBackoff retries fn up to maxAttempts with exponential backoff.
//   - attempt 1: immediate
//   - attempt 2: after 1s
//   - attempt 3: after 2s
//   - attempt 4: after 4s (final)
// Returns the last error if all retries fail.
// All errors are retried — no 4xx/5xx distinction, no circuit breaker.
//
// Metrics (v0.8.1): every attempt is instrumented with attempt, outcome,
// and error_class. Without per-attempt metrics, the retry path is invisible
// to monitoring when SearXNG succeeds on the first try.
func (p *Proxy) retryWithBackoff(ctx context.Context, fn func() (*searxng.Response, error)) (*searxng.Response, error) {
	backoff := 1 * time.Second
	const maxAttempts = 3

	var lastErr error
	var lastResp *searxng.Response

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				// Context cancelled while waiting between attempts.
				metrics.RetryAttemptsTotal.WithLabelValues(
					fmt.Sprintf("%d", attempt), "cancelled", "cancelled",
				).Inc()
				return lastResp, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2 // exponential: 1s -> 2s -> 4s
		}

		resp, err := fn()
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
			return resp, nil
		}
		lastErr = err
		lastResp = resp
	}

	// All retry attempts exhausted.
	errClass := "other"
	if lastErr != nil {
		errClass = classifyError(lastErr)
	}
	metrics.RetryAttemptsTotal.WithLabelValues("final", "exhausted", errClass).Inc()
	metrics.RetryExhaustedTotal.WithLabelValues(errClass).Inc()
	return lastResp, lastErr
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
// (server is blocking us: 403, 429, access denied, forbidden, etc.)
//
// Pattern coverage:
//   4xx HTTP codes:           "HTTP error 4"
//   Cloudflare-style blocks:  "blocked", "blocked by"
//   Rate limiting:            "too many requests", "rate limited"
//   Auth/access:              "access denied", "forbidden", "unauthorized", "not found"
//   Bot detection:            "captcha"
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
