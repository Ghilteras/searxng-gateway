// Package proxy implements the core gateway fallback logic:
// cache check → SearXNG call → fallback decision → Brave fallback → metrics.
//
// The Proxy.Search method orchestrates the four stages:
//  1. Normalise the query and check the LRU cache.
//  2. Call SearXNG with a FallbackTimeout context.
//  3. If the response is insufficient (too few results / engines), call Brave.
//  4. Map Brave results to the SearXNG shape and cache them.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
}

// New creates a Proxy with the given config, backends and cache.
func New(cfg *config.Config, sx searxng.Client, bv brave.Client, c *cache.Cache) *Proxy {
	return &Proxy{cfg: cfg, sx: sx, bv: bv, c: c}
}

// Search runs the full orchestration pipeline for a raw query string.
//
// Outcome counters (all via RequestsTotal):
//   - cache_hit:          entry found in cache, SearXNG not called.
//   - searxng_ok:         SearXNG returned a sufficient response.
//   - timeout:            SearXNG returned context.DeadlineExceeded.
//   - fallback_brave_ok:  SearXNG insufficient/failed, Brave returned OK.
//   - fallback_brave_fail:SearXNG insufficient/failed, Brave also failed.
func (p *Proxy) Search(ctx context.Context, raw string) (*searxng.Response, error) {
	key := normalize(raw)

	// 1. Cache check.
	if v, ok := p.c.Get(key); ok {
		metrics.RequestsTotal.WithLabelValues("cache_hit").Inc()
		return v.(*searxng.Response), nil
	}

	// 2. SearXNG call with timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, p.cfg.FallbackTimeout)
	defer cancel()

	start := time.Now()
	sxResp, sxErr := p.sx.Search(timeoutCtx, key)
	metrics.RequestDuration.WithLabelValues("searxng").Observe(time.Since(start).Seconds())

	// 3. If SearXNG succeeded and response is sufficient → done.
	if sxErr == nil && p.sufficient(sxResp) {
		metrics.RequestsTotal.WithLabelValues("searxng_ok").Inc()
		p.observe(sxResp)
		p.c.Set(key, sxResp)
		return sxResp, nil
	}

	// 4. Record timeout independently (regardless of Brave outcome).
	if errors.Is(sxErr, context.DeadlineExceeded) {
		metrics.RequestsTotal.WithLabelValues("timeout").Inc()
	}

	// 5. Fallback to Brave (with the original ctx, not the expired timeoutCtx).
	startB := time.Now()
	bvResp, bvErr := p.bv.Search(ctx, key)
	metrics.RequestDuration.WithLabelValues("brave").Observe(time.Since(startB).Seconds())

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

// sufficient returns true when the SearXNG response meets the configured
// minimum result count and minimum distinct engine count.
func (p *Proxy) sufficient(r *searxng.Response) bool {
	if len(r.Results) < p.cfg.FallbackMinResults {
		return false
	}
	distinct := make(map[string]struct{}, len(r.Results))
	for _, res := range r.Results {
		distinct[res.Engine] = struct{}{}
	}
	return len(distinct) >= p.cfg.FallbackMinEngines
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
