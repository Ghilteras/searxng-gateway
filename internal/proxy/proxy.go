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
	metrics.RequestDuration.WithLabelValues("searxng").Observe(time.Since(start).Seconds())

	// 4. If SearXNG succeeded and response is sufficient → done.
	if sxErr == nil && p.sufficient(sxResp) {
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
	braveCtx, cancel := context.WithTimeout(context.Background(), p.cfg.BraveTimeout)
	defer cancel()

	start := time.Now()
	bvResp, bvErr := p.bv.Search(braveCtx, key)
	metrics.RequestDuration.WithLabelValues("brave").Observe(time.Since(start).Seconds())

	if bvErr != nil {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		return nil, fmt.Errorf("searxng in cooldown, brave failed: %w", bvErr)
	}

	metrics.RequestsTotal.WithLabelValues("fallback_brave_ok").Inc()
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
	braveCtx, cancel := context.WithTimeout(context.Background(), p.cfg.BraveTimeout)
	defer cancel()

	start := time.Now()
	bvResp, bvErr := p.bv.Search(braveCtx, key)
	metrics.RequestDuration.WithLabelValues("brave").Observe(time.Since(start).Seconds())

	if bvErr != nil {
		metrics.RequestsTotal.WithLabelValues("fallback_brave_fail").Inc()
		return nil, fmt.Errorf("searxng insufficient (err=%v) and brave failed: %w", sxErr, bvErr)
	}

	metrics.RequestsTotal.WithLabelValues("fallback_brave_ok").Inc()
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
