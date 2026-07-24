// Package proxy implements the core gateway fallback logic:
// cache check → SearXNG call → fallback decision → Brave fallback → metrics.
//
// The Proxy.Search method orchestrates the four stages:
//  1. Normalise the query and check the LRU cache.
//  2. Call SearXNG with a FallbackTimeout context.
//  3. If the response is insufficient (len(Results)==0), call Brave.
//  4. Map Brave results to the SearXNG shape and cache them.
//
// Community-aligned behaviour (2026):
//   - Binary fallback: a SearXNG response is sufficient if it has ≥1 result.
//   - Cooldown circuit breaker: after SEARXNG_FAIL_THRESHOLD consecutive
//     failures, SearXNG is skipped entirely for SEARXNG_FAIL_COOLDOWN_SECONDS,
//     going directly to Brave.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sx/internal/brave"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/mapper"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

// engineState tracks consecutive failures and cooldown for a single SearXNG
// engine. Used by the per-engine circuit breaker to avoid repeatedly hitting
// engines known to be broken (e.g., Mojeek "access denied", Wikipedia 429).
type engineState struct {
	fails       int64
	cooldownTil atomic.Int64
}

// Proxy orchestrates SearXNG-first search with a Brave fallback.
type Proxy struct {
	cfg *config.Config
	sx  searxng.Client
	bv  brave.Client
	c   *cache.Cache

	// Cooldown circuit breaker (community pattern: searxng-resilient-router)
	sxFails       int64        // atomic counter of consecutive failures
	sxCooldownTil atomic.Int64 // unix nano; 0 = no cooldown
	mu            sync.Mutex   // guards sxFails/sxCooldownTil updates

	// Brave circuit breaker (mirrors the SearXNG pattern)
	bvFails       int64        // atomic counter of consecutive failures
	bvCooldownTil atomic.Int64 // unix nano; 0 = no cooldown

	// Per-engine circuit breaker: engine name → *engineState
	engineCB sync.Map
}

// New creates a Proxy with the given config, backends and cache.
func New(cfg *config.Config, sx searxng.Client, bv brave.Client, c *cache.Cache) *Proxy {
	return &Proxy{cfg: cfg, sx: sx, bv: bv, c: c}
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

	// 2. Check if SearXNG is in cooldown (binary fallback during outage).
	if p.inCooldown() {
		// Skip SearXNG entirely, go direct to Brave.
		return p.braveOnlySearch(ctx, key)
	}

	// 3. SearXNG call with timeout.
	start := time.Now()
	sxResp, sxErr := p.sx.Search(timeoutCtx, key)
	sxElapsed := time.Since(start)

	// 4. If SearXNG succeeded and response is sufficient → done.
	if sxErr == nil && p.sufficient(sxResp) {
		// Per-engine metrics from SearXNG response.
		seenEngines := make(map[string]struct{})
		for _, result := range sxResp.Results {
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
		// Record duration per distinct engine contributing results.
		for eng := range seenEngines {
			metrics.RequestDuration.WithLabelValues("searxng", eng).Observe(sxElapsed.Seconds())
		}
		unresponsiveSet := make(map[string]string)
		for _, ue := range sxResp.UnresponsiveEngines {
			if len(ue) >= 2 {
				unresponsiveSet[ue[0]] = ue[1]
			}
		}
		for engine, reason := range unresponsiveSet {
			metrics.EngineUnresponsiveTotal.WithLabelValues(engine, reason).Inc()
			metrics.EngineStatus.WithLabelValues(engine).Set(0)
			p.recordEngineFailure(engine, reason)
		}
		// Record success for every engine that contributed results.
		for eng := range seenEngines {
			p.recordEngineSuccess(eng)
		}

		p.recordSearxngSuccess()
		metrics.RequestsTotal.WithLabelValues("searxng_ok").Inc()
		p.observe(sxResp)
		p.c.Set(key, sxResp)
		return sxResp, nil
	}

	// 5. Record failure (timeout counts as a consecutive failure).
	p.recordSearxngFailure()
	if errors.Is(sxErr, context.DeadlineExceeded) {
		metrics.RequestsTotal.WithLabelValues("timeout").Inc()
	}

	// 6. Fallback to Brave (with the original ctx, not the expired timeoutCtx).
	return p.braveSearch(ctx, key, sxResp, sxErr)
}

// sufficient returns true when the SearXNG response has at least one result
// (community-aligned binary fallback, original byteowlz/sx pattern).
func (p *Proxy) sufficient(r *searxng.Response) bool {
	return len(r.Results) > 0
}

// braveOnlySearch is called when SearXNG is in cooldown — searches Brave only.
// Uses context.Background() with BraveTimeout so the call survives upstream
// client disconnections (which would cancel r.Context()).
func (p *Proxy) braveOnlySearch(_ context.Context, key string) (*searxng.Response, error) {
	if p.braveInCooldown() {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		return nil, fmt.Errorf("brave is in cooldown (previous failures)")
	}
	braveCtx, cancel := context.WithTimeout(context.Background(), p.cfg.BraveTimeout)
	defer cancel()

	start := time.Now()
	bvResp, bvErr := p.bv.Search(braveCtx, key)
	metrics.RequestDuration.WithLabelValues("brave", "brave").Observe(time.Since(start).Seconds())

	if bvErr != nil {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		p.recordBraveFailure()
		return nil, fmt.Errorf("searxng in cooldown, brave failed: %w", bvErr)
	}

	metrics.RequestsTotal.WithLabelValues("fallback_brave_ok").Inc()
	p.recordBraveSuccess()
	mapped := &searxng.Response{Results: mapper.ToSearxngResults(bvResp.Web.Results)}
	p.observe(mapped)
	p.c.Set(key, mapped)
	return mapped, nil
}

// braveSearch is called when SearXNG was attempted but failed or was
// insufficient — searches Brave as fallback.
// Uses context.Background() with BraveTimeout so the call survives upstream
// client disconnections (which would cancel r.Context()).
func (p *Proxy) braveSearch(_ context.Context, key string, sxResp *searxng.Response, sxErr error) (*searxng.Response, error) {
	if p.braveInCooldown() {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		return nil, fmt.Errorf("brave is in cooldown (previous failures); searxng=%v", sxErr)
	}
	braveCtx, cancel := context.WithTimeout(context.Background(), p.cfg.BraveTimeout)
	defer cancel()

	start := time.Now()
	bvResp, bvErr := p.bv.Search(braveCtx, key)
	metrics.RequestDuration.WithLabelValues("brave", "brave").Observe(time.Since(start).Seconds())

	if bvErr != nil {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		p.recordBraveFailure()
		return nil, fmt.Errorf("searxng insufficient (err=%v) and brave failed: %w", sxErr, bvErr)
	}

	metrics.RequestsTotal.WithLabelValues("fallback_brave_ok").Inc()
	p.recordBraveSuccess()
	mapped := &searxng.Response{Results: mapper.ToSearxngResults(bvResp.Web.Results)}
	p.observe(mapped)
	p.c.Set(key, mapped)
	return mapped, nil
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

// recordBraveSuccess resets the failure counter and clears any active cooldown.
func (p *Proxy) recordBraveSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt64(&p.bvFails, 0)
	p.bvCooldownTil.Store(0)
}

// recordBraveFailure increments the failure counter and starts a cooldown
// if the threshold is reached.
func (p *Proxy) recordBraveFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	fails := atomic.AddInt64(&p.bvFails, 1)
	if int(fails) >= p.cfg.BraveFailThreshold {
		until := time.Now().Add(p.cfg.BraveFailCooldown).UnixNano()
		p.bvCooldownTil.Store(until)
	}
}

// braveInCooldown reports whether Brave is currently in cooldown. If the
// cooldown period has expired, it is automatically cleared.
func (p *Proxy) braveInCooldown() bool {
	until := p.bvCooldownTil.Load()
	if until == 0 {
		return false
	}
	if time.Now().UnixNano() >= until {
		// Cooldown expired — reset state.
		p.bvCooldownTil.Store(0)
		atomic.StoreInt64(&p.bvFails, 0)
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

// normalize lower-cases a query, trims spaces and collapses whitespace runs.
func normalize(q string) string {
	return strings.Join(strings.Fields(strings.ToLower(q)), " ")
}

// Engine circuit breaker — per-engine failure tracking.

// engineNameForMetric sanitises an engine name for use in the cooldown map.
// If the name contains spaces, returns only the part before the first space.
func engineNameForMetric(name string) string {
	if idx := strings.IndexByte(name, ' '); idx >= 0 {
		return name[:idx]
	}
	return name
}

// recordEngineFailure increments the per-engine failure counter and starts a
// 1h cooldown after 3 consecutive failures for "access denied" or
// "too many requests" (or their "Suspended:" variants).
func (p *Proxy) recordEngineFailure(engine, reason string) {
	if engine == "" {
		return
	}
	key := engineNameForMetric(engine)
	val, _ := p.engineCB.LoadOrStore(key, &engineState{})
	es := val.(*engineState)
	p.mu.Lock()
	defer p.mu.Unlock()
	fails := atomic.AddInt64(&es.fails, 1)
	if (reason == "access denied" || reason == "Suspended: access denied" ||
		reason == "too many requests" || reason == "Suspended: too many requests") &&
		fails >= 3 {
		cooldown := 1 * time.Hour
		until := time.Now().Add(cooldown).UnixNano()
		es.cooldownTil.Store(until)
	}
}

// engineInCooldown reports whether the given engine is currently in cooldown.
// If the cooldown period has expired, it is automatically cleared and the
// failure counter is reset.
func (p *Proxy) engineInCooldown(engine string) bool {
	key := engineNameForMetric(engine)
	val, ok := p.engineCB.Load(key)
	if !ok {
		return false
	}
	es := val.(*engineState)
	until := es.cooldownTil.Load()
	if until == 0 {
		return false
	}
	if time.Now().UnixNano() >= until {
		// Cooldown expired — reset state.
		es.cooldownTil.Store(0)
		atomic.StoreInt64(&es.fails, 0)
		return false
	}
	return true
}

// recordEngineSuccess resets the failure counter and clears any active cooldown
// for the given engine.
func (p *Proxy) recordEngineSuccess(engine string) {
	if engine == "" {
		return
	}
	key := engineNameForMetric(engine)
	val, ok := p.engineCB.Load(key)
	if !ok {
		return
	}
	es := val.(*engineState)
	atomic.StoreInt64(&es.fails, 0)
	es.cooldownTil.Store(0)
}

// isEngineSkipped returns true if the engine is currently in cooldown and
// should be skipped. Callers can use this to avoid calling engines that are
// known to be broken (e.g., Mojeek blocked by IP, Wikipedia rate-limited).
// When an engine is skipped, a warning is logged.
func (p *Proxy) isEngineSkipped(engine string) bool {
	if p.engineInCooldown(engine) {
		return true
	}
	return false
}
