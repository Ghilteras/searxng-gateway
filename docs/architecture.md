# Architecture

## Overview

`searxng-gateway` is a thin decision proxy that sits in front of a SearXNG instance. It forwards queries to SearXNG as the primary source and falls back to the Brave Search API when SearXNG returns too few results or all engines are degraded.

The client never talks to SearXNG directly — all traffic flows through the gateway.

```
Client ───▶ searxng-gateway ───▶ SearXNG (primary)
                    │
                    └──▶ Brave Search API (fallback)
```

## Decision flow (F3)

1. **Forward to SearXNG** with 3 retries (exponential backoff: 1s, 2s, 4s on 5xx/timeout; no retry on 4xx)
2. **Parse `unresponsive_engines`** from SearXNG response
3. **Circuit breaker per engine**: 4xx on an engine → circuit opens (5 min cooldown) → engine skipped on subsequent requests → auto-probe after 5 min
4. **Fallback trigger**: if `len(results) < SUFFICIENT_MIN_RESULTS` (default 25) → call Brave Search API
5. **Cache**: in-memory LRU (1000 entries, 1h TTL). Cache hit skips both SearXNG and Brave.

## Multi-tier engine posture

The reference SearXNG config (`examples/searxng/`) implements three tiers:

| Tier | Source | Cost | Description |
|------|--------|------|-------------|
| T1 | Serper API | Free (2,500/mo) | Google results via API. Most relevant results. |
| T2 | Bing, Wikipedia, GitHub, StackOverflow, ArXiv, PyPI, Docker Hub, Mwmbl, Marginalia | Free (keyless) | Broad coverage, no API key needed. |
| T3 | Brave Search API | Free ($5 credit = 1,000/mo) | Paid fallback, triggered by gateway, not by SearXNG itself. |

Tier 1 is optional. The gateway works with T2 + T3 only. For a fully keyless setup, see [keyless.md](keyless.md).

## Circuit breaker (per-engine)

Uses `sony/gobreaker`. The gateway parses SearXNG's `unresponsive_engines` response field:

- **4xx on engine** → circuit opens immediately (threshold = 1)
- **Open state**: engine excluded from SearXNG requests for 5 minutes
- **Half-open probe**: after 5 min, one test request
  - Success → circuit closes, `recovery_total` counter increments
  - Failure → circuit re-opens for another 5 min

## Prometheus metrics

All metrics exposed at `:8080/metrics`, prefix `searxng_gateway_`:

| Metric | Type | Description |
|--------|------|-------------|
| `circuit_breaker_state{engine}` | Gauge | 0=closed, 1=half-open, 2=open |
| `circuit_breaker_trips_total{engine,reason}` | Counter | Cumulative CB trips |
| `circuit_breaker_recovery_total{engine}` | Counter | Auto-recovery events |
| `circuit_breaker_requests_total{engine,state}` | Counter | Requests per engine per CB state |
| `retry_attempts_total{attempt,outcome,error_class}` | Counter | Retry attempts |
| `retry_exhausted_total{error_class}` | Counter | Retries exhausted |
| `engine_results_total{engine}` | Counter | Results per engine (including `brave-premium`) |
| `request_duration_seconds{engine}` | Histogram | Latency per engine |
| `fallback_triggered_total` | Counter | Brave API fallback calls |

## Configuration

All configuration via environment variables. See [README](../README.md) for the full list.

## Response shape

The gateway returns JSON in SearXNG format regardless of source (SearXNG or Brave), so existing clients work without modification.

| SearXNG field | Brave source |
|---------------|-------------|
| `title` | Brave `title` |
| `url` | Brave `url` |
| `content` | Brave `description` |
| `engine` | constant `"brave-api"` |
| `score` | constant `1.0` |
