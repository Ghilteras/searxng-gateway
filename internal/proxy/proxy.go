// Package proxy implements the core gateway orchestration:
// cache check → parallel SearXNG + speculative premium → early return if SearXNG
// meets the result threshold → round-robin premium loop otherwise.
//
// The Proxy.Search method orchestrates the stages:
//  1. Normalise the query and check the LRU cache.
//  2. Check SearXNG cooldown (binary fallback circuit breaker).
//  3. Call SearXNG (with retry+backoff) AND the T1 premium pass in parallel.
//  4. If distinct SearXNG URLs meet SufficientMinResults, return them without
//     waiting for or merging T1 results; otherwise merge T1 first, then SearXNG.
//  5. If merged results < SufficientMinResults:
//     call additional premiums (round-robin, non-repeating) until threshold met,
//     all premiums exhausted, or FallbackTimeout expires.
//  6. Circuit breaker: premiums where breakerMgr.IsOpen(name) are skipped.
//     After success: RecordSuccess. After failure: RecordClientError.
//
// Community-aligned behaviour (2026):
//   - Speculatively call configured T1 premiums alongside SearXNG; sufficient
//     SearXNG results do not wait for or include premium-only results.
//   - Round-robin premium selection distributes load evenly.
//   - Cooldown circuit breaker for SearXNG: after SEARXNG_FAIL_THRESHOLD
//     consecutive failures, SearXNG is skipped entirely until cooldown expires.
//   - Retry with up to 3 attempts and 1s/2s backoff for SearXNG errors,
//     bounded by the SearXNG child timeout.
//   - URL deduplication across SearXNG and all premium providers.
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

type t1PassResult struct {
	results           []searxng.Result
	usedPremiums      map[string]bool
	premiumHadResults bool
	errMsgs           []string
}

// New creates a Proxy with the given config, backends and cache.
func New(cfg *config.Config, sx searxng.Client, c *cache.Cache, breakerMgr *breaker.Manager, fallbackMgr *backends.Manager) *Proxy {
	return &Proxy{cfg: cfg, sx: sx, c: c, breakerMgr: breakerMgr, fallbackMgr: fallbackMgr}
}

// Search runs the full orchestration pipeline for a raw query string.
//
// Outcome counters (all via RequestsTotal):
//   - cache_hit:                  entry found in cache, SearXNG not called.
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
	sxCtx, cancelSX := context.WithTimeout(timeoutCtx, p.cfg.SearxngTimeout)
	defer cancelSX()

	// 2. Check if SearXNG is in cooldown (skip SearXNG, go premium-only).
	sxSkipped := p.inCooldown()

	// Per-call state. T1 owns its own maps and returns them over a buffered
	// channel, so a sufficient SearXNG response can cancel and ignore T1 without
	// racing with the worker or blocking its eventual send.
	allResults := make([]searxng.Result, 0)
	seenURLs := make(map[string]bool)
	usedPremiums := make(map[string]bool)
	premiumHadResults := false
	searxngHadResults := false
	var premiumErrMsgs []string

	// 3. Channels for the concurrent SearXNG stage and speculative T1 pass.
	type sxResult struct {
		resp *searxng.Response
		err  error
	}
	sxCh := make(chan sxResult, 1)
	t1Ctx, cancelT1 := context.WithCancel(timeoutCtx)
	defer cancelT1()
	t1Ch := make(chan t1PassResult, 1)

	if !sxSkipped {
		go func() {
			start := time.Now()
			stageCtx, stageSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "searxng.stage")
			resp, err := p.retryWithBackoff(sxCtx, stageCtx, func() (*searxng.Response, error) {
				return p.sx.Search(sxCtx, key)
			})
			elapsed := time.Since(start)
			stageSpan.SetAttributes(attribute.String("phase", "searxng"), attribute.String("outcome", searxngOutcome(resp, err)), attribute.Int("result_count", resultCount(resp)))
			stageSpan.End()
			metrics.SearxngStageDuration.Observe(elapsed.Seconds())
			sxCh <- sxResult{resp: resp, err: err}
		}()
	} else {
		// Cooldown active: signal SearXNG skipped.
		close(sxCh)
	}

	// Start T1 concurrently with SearXNG, but do not make its completion a
	// prerequisite when SearXNG alone already supplies enough distinct URLs.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// net/http recovery does not cover goroutines started here.
				t1Ch <- t1PassResult{
					usedPremiums: make(map[string]bool),
					errMsgs:      []string{fmt.Sprintf("t1 pass panic: %v", r)},
				}
			}
		}()
		t1Ch <- p.runT1Pass(t1Ctx, key)
	}()

	// 4. Collect SearXNG first. If it is sufficient by itself, cancel the
	// speculative T1 pass and return SearXNG-only results. Otherwise, wait for
	// T1 and preserve the existing T1-before-SearXNG merge order.
	_, waitSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "searxng.result_wait")
	sxRes, sxAvailable := <-sxCh
	waitSpan.SetAttributes(attribute.String("phase", "searxng"), attribute.String("outcome", map[bool]string{true: "completed", false: "skipped"}[sxAvailable]))
	waitSpan.End()
	sxSucceeded := sxAvailable && sxRes.err == nil && sxRes.resp != nil
	if sxSucceeded && p.sufficient(sxRes.resp) {
		cancelT1()
	} else {
		t1Res := <-t1Ch
		usedPremiums = t1Res.usedPremiums
		premiumHadResults = t1Res.premiumHadResults
		premiumErrMsgs = append(premiumErrMsgs, t1Res.errMsgs...)
		for _, r := range t1Res.results {
			if !seenURLs[r.URL] {
				seenURLs[r.URL] = true
				allResults = append(allResults, r)
			}
		}
	}

	if sxAvailable {
		if sxSucceeded {
			// Per-engine metrics from SearXNG response.
			seenEngines := make(map[string]struct{})
			for _, result := range sxRes.resp.Results {
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
			for _, ue := range sxRes.resp.UnresponsiveEngines {
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
			for _, r := range sxRes.resp.Results {
				if !seenURLs[r.URL] {
					seenURLs[r.URL] = true
					allResults = append(allResults, r)
					searxngHadResults = true
				}
			}
		} else {
			// SearXNG failed.
			if timeoutCtx.Err() == nil {
				p.recordSearxngFailure()
			}
			if sxRes.err != nil {
				premiumErrMsgs = append(premiumErrMsgs, fmt.Sprintf("searxng: %v", sxRes.err))
			}
		}
	}

	// 6. Fallback loop: all remaining FALLBACK_PROVIDERS (skips already-used).
	if len(allResults) < p.cfg.SufficientMinResults {
		fallbackCtx, fallbackSpan := otel.Tracer("sx/internal/proxy").Start(timeoutCtx, "fallback.loop")
		allResults, _, _, premiumHadResults, premiumErrMsgs =
			p.premiumLoop(fallbackCtx, key, allResults, seenURLs, usedPremiums, premiumHadResults, premiumErrMsgs)
		fallbackSpan.SetAttributes(attribute.String("phase", "fallback"), attribute.Int("result_count", len(allResults)), attribute.String("outcome", "complete"))
		fallbackSpan.End()
	}

	// 7. Outcome.
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

// sufficient returns true when the SearXNG response has at least SufficientMinResults.
func (p *Proxy) sufficient(r *searxng.Response) bool {
	if r == nil {
		return false
	}
	seenURLs := make(map[string]struct{}, len(r.Results))
	for _, result := range r.Results {
		seenURLs[result.URL] = struct{}{}
	}
	if len(seenURLs) == 0 {
		// With SUFFICIENT_MIN_RESULTS=0, 0 >= 0 would cancel T1 and skip
		// fallback, turning a recoverable search into fallback_fail.
		return false
	}
	return len(seenURLs) >= p.cfg.SufficientMinResults
}

// runT1Pass speculatively queries the configured T1 premium providers in
// serial round-robin order. Its maps and results are worker-owned; Search only
// merges the returned value when SearXNG alone is insufficient.
func (p *Proxy) runT1Pass(ctx context.Context, key string) t1PassResult {
	pass := t1PassResult{
		results:      make([]searxng.Result, 0),
		usedPremiums: make(map[string]bool),
	}
	seenURLs := make(map[string]bool)
	for i := 0; i < p.cfg.T1PremiumCount; i++ {
		if ctx.Err() != nil {
			break
		}
		premium := p.fallbackMgr.NextAvailable(pass.usedPremiums)
		if premium == nil || !premium.IsAvailable() || p.breakerMgr.IsOpen(premium.Name()) {
			continue
		}
		pass.usedPremiums[premium.Name()] = true
		start := time.Now()
		providerCtx, providerSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "premium.t1")
		providerSpan.SetAttributes(attribute.String("provider", premium.Name()), attribute.String("phase", "t1"))
		if ctx.Err() != nil {
			providerSpan.End()
			break
		}
		var results []backends.SearchResult
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// net/http recovery does not cover goroutines started here.
					err = fmt.Errorf("panic: %v", r)
					results = nil
				}
			}()
			results, err = premium.Search(backends.SearchOptions{
				Context:    providerCtx,
				Query:      key,
				NumResults: 10,
			})
		}()
		providerSpan.SetAttributes(attribute.String("outcome", spanOutcome(err)), attribute.Int("result_count", len(results)))
		providerSpan.End()
		elapsed := time.Since(start)
		metrics.RequestDuration.WithLabelValues(premium.Name(), "t1").Observe(elapsed.Seconds())

		if err != nil {
			clientError, category := classifyPremiumError(err)
			before := p.breakerMgr.State(premium.Name())
			if clientError {
				p.breakerMgr.RecordClientError(premium.Name(), category)
			}
			logPremiumFailure(premium.Name(), "t1", category, clientError, before, p.breakerMgr.State(premium.Name()))
			pass.errMsgs = append(pass.errMsgs, fmt.Sprintf("%s: %v", premium.Name(), err))
			continue
		}
		if len(results) == 0 {
			continue
		}

		p.breakerMgr.RecordSuccess(premium.Name())
		metrics.EngineResultsTotal.WithLabelValues(premium.Name()).Add(float64(len(results)))
		for _, r := range results {
			if !seenURLs[r.URL] {
				seenURLs[r.URL] = true
				pass.premiumHadResults = true
				pass.results = append(pass.results, searxng.Result{
					Title:   r.Title,
					URL:     r.URL,
					Content: r.Content,
					Engine:  r.Engine,
					Engines: []string{r.Engine},
				})
			}
		}
	}
	return pass
}

// premiumLoop iterates through premium backends via round-robin until:
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
	for len(allResults) < p.cfg.SufficientMinResults {
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
		providerCtx, providerSpan := otel.Tracer("sx/internal/proxy").Start(ctx, "premium.fallback")
		providerSpan.SetAttributes(attribute.String("provider", premium.Name()), attribute.String("phase", "fallback"))
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
		metrics.RequestDuration.WithLabelValues(premium.Name(), "fallback").Observe(elapsed.Seconds())

		if err != nil {
			clientError, category := classifyPremiumError(err)
			before := p.breakerMgr.State(premium.Name())
			if clientError {
				p.breakerMgr.RecordClientError(premium.Name(), category)
			}
			logPremiumFailure(premium.Name(), "fallback", category, clientError, before, p.breakerMgr.State(premium.Name()))
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
// All errors are retried — no 4xx/5xx distinction, no circuit breaker.
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
		attemptSpan.SetAttributes(attribute.Int("attempt", attempt), attribute.String("phase", "searxng"))
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
		callSpan.SetAttributes(attribute.String("phase", "searxng"), attribute.Int("attempt", attempt), attribute.String("outcome", spanOutcome(err)), attribute.Int("result_count", resultCount(resp)))
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
