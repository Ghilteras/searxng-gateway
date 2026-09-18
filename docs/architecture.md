# Architecture

## Overview

`searxng-gateway` is an HTTP search gateway that sits in front of a SearXNG instance and a configurable pool of premium search providers. It uses **speculative execution** — starting SearXNG and the configured premium-provider pass concurrently, selecting providers via round-robin, and invoking premium providers **serially within each pass** (a deliberate design choice; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)) — then merges results with URL deduplication. If the merged count is insufficient, it loops through remaining providers until a target threshold is met or a timeout expires. The client never talks to SearXNG directly.

```
Client ───▶ searxng-gateway (:8080) ───▶ SearXNG (primary)
                    │                         │
                    │  ┌──────────────────────┤
                    │  │ Speculative execution │
                    │  │ (parallel round-robin)│
                    │  └──────────────────────┤
                    │                         │
                    └──▶ T1 premium pool (T1_PREMIUM_COUNT providers)
                         ├── Brave ──┐
                         ├── Exa     ├── round-robin alongside SearXNG;
                         ├── Jina    │   serial within the premium pass.
                         └── Tavily ─┘
                         │
                         └──▶ Fallback loop (if merged < SUFFICIENT_MIN_RESULTS)
                              Round-robin through remaining providers
                              until threshold, exhaustion, or FALLBACK_TIMEOUT
```

## Request flow

1. **Normalise** the query (lowercase, collapse whitespace) and check the **LRU cache**. Cache hit returns immediately without calling any backend.
2. **SearXNG cooldown check**: if SearXNG has hit `SEARXNG_FAIL_THRESHOLD` consecutive failures (default 6), it is skipped entirely for `SEARXNG_FAIL_COOLDOWN_SECONDS` (default 180s). Success resets the counter.
3. **Speculative call**: SearXNG starts concurrently with the `T1_PREMIUM_COUNT` premium-provider pass. Providers are selected via atomic round-robin from `FALLBACK_PROVIDERS` and invoked **serially within that pass** (by design; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)).
   - SearXNG is called with retry+backoff (3 attempts: 1s, 2s, 4s exponential — all error classes retried).
   - Each premium provider is called once; circuit-breaker-open providers are skipped.
4. **Merge and deduplicate** all results by URL. Record per-engine metrics from SearXNG's `unresponsive_engines` field.
5. **Bounded fallback loop**: if merged results < `SUFFICIENT_MIN_RESULTS` (default 1), remaining providers (those not already called this request) are tried via round-robin until the threshold is met, all providers are exhausted, or `FALLBACK_TIMEOUT_SECONDS` (default 30) expires.
6. **Outcome**: cache_hit, searxng_ok, premium_ok, searxng_plus_premium_ok, or fallback_fail.

## Why the premium pass is serial (deliberate)

Both premium execution paths — the T1 hot-path loop (`proxy.go:118-157`) and the fallback `premiumLoop` (`proxy.go:265-337`) — invoke providers serially. This is a deliberate design choice, not a TODO to be fixed. The trade-offs behind the choice, and why concurrency is not the fix, are:

**1. Quota and rate-limit control.** The two paths differ here, and the argument is strongest for the fallback loop. In the fallback loop, concurrency defeats early stop: calls already launched cannot be un-launched even when the first provider alone would have sufficed, so it is strictly more billed calls (Brave ~1,000/mo, Exa ~2,800/mo, Tavily credits, Jina RPM/tokens). In the T1 hot path `T1_PREMIUM_COUNT` is an attempt budget rather than a guaranteed call count: the loop breaks once the deadline has passed and skips providers that are unavailable or circuit-broken (`proxy.go:118-125`), so concurrency would not generally bill more T1 calls; what it would remove is the inter-call spacing that provides best-effort pacing against per-second and per-minute caps (Brave QPS, Jina 500 RPM) — response-time spacing, not an explicit rate limiter — and under a tight deadline a fan-out could instead launch more calls before the deadline is observed. A single 4xx from any provider trips that engine's circuit breaker for 5 minutes — an availability cost, not just a metric blip.

**2. Attribution determinism.** Merge order is load-bearing and deliberately order-dependent. In the T1 path, premium results merge *before* SearXNG, so premium owns duplicate URLs. In the fallback loop, premium merges *after* SearXNG, so SearXNG owns them. Two existing tests (`TestOutcomeLabels_SearxngDuplicatesPremium`, `TestOutcomeLabels_PremiumDuplicatesSearxng`) pin the two opposite outcomes for identical-URL inputs. Concurrency would make merge order nondeterministic, so both labels and result ordering become nondeterministic.

**3. Latency — the correct fix is context propagation, not parallelism.** Concurrency would cap wall-clock latency, but the budget is currently advisory because `backends.SearchBackend.Search` takes no context (`backends/interface.go`), so the deadline is only checked between calls. The correct future fix is context propagation into each backend's `Search` method. Also, at `T1_PREMIUM_COUNT=1` there is nothing to overlap.

**Non-goal:** Do not convert these passes to a concurrent fan-out.

**Known limitation:** `FALLBACK_TIMEOUT_SECONDS` is not enforced as a hard bound inside a single premium call today — the deadline is only checked between calls. Context propagation (passing the request deadline into `backends.SearchBackend.Search`) is the correct fix for this, not parallelism.

## Per-engine circuit breaker

Uses `sony/gobreaker`. Each premium provider (and each SearXNG engine) gets its own breaker instance.

- **Trip trigger**: a single 4xx client error (403, 429, rate limit, captcha, access denied) trips the circuit immediately (`ReadyToTrip`: `ConsecutiveFailures >= 1`).
- **Open state**: the engine is excluded from subsequent requests.
- **Timeout** (default 5 min): circuit enters half-open, sends one probe request.
  - Success → circuit closes; `recovery_total` counter increments.
  - Failure → circuit re-opens for another 5 min.
- **SearXNG is handled separately** via a binary cooldown counter (not `gobreaker`): after N consecutive SearXNG failures, the entire SearXNG call is skipped for the cooldown duration.

## Supported backends

Set `FALLBACK_PROVIDERS` to a comma-separated list. Each needs its `<NAME>_API_KEY` env var (except keyless backends). `T1_PREMIUM_COUNT` controls how many are called in the T1 hot path alongside SearXNG; the rest are used in the fallback loop.

| Backend | Factory name | Key env var | Keyless? | Notes |
|---------|-------------|-------------|----------|-------|
| Brave Search API | `brave` | `BRAVE_API_KEY` | No | `$5 credit = ~1,000 queries/mo` |
| Exa | `exa` | `EXA_API_KEY` | No | Also supports MCP mode (`EXA_MCP_URL`) |
| Jina | `jina` | `JINA_API_KEY` | Optional | `JINA_ALLOW_KEYLESS=true` by default |
| Tavily | `tavily` | `TAVILY_API_KEY` | No | `TAVILY_SEARCH_DEPTH=basic\|advanced` |
| Bing (HTML scrape) | `bing` | — | Yes | Parses Bing HTML; bot-challenge detection |
| Brave Web (HTML scrape) | `brave-web` | — | Yes | Parses Brave Search HTML |
| SearXNG instance | `searxng` | — | Yes | For multi-instance or remote SearXNG backends |

All backends implement the `SearchBackend` interface in `backends/interface.go`. The `backends/factory.go` factory instantiates them from env vars. See [README](../README.md) for the full env var table and [keyless.md](keyless.md) for a zero-API-key setup.

## Response shape

The gateway returns JSON in SearXNG format regardless of which backend provided the results. Each result carries:

| Field | Source | Notes |
|-------|--------|-------|
| `title` | Backend-specific title | — |
| `url` | Backend-specific URL | Deduplicated across all backends |
| `content` | Backend-specific snippet | `description` from Brave, `content`/`summary` from Exa/Tavily, `content` from Jina |
| `engine` | Backend name | e.g. `"brave"`, `"exa"`, `"jina"`, `"tavily"`, `"bing"`, or a SearXNG engine name |
| `engines` | `[]string{engine}` | List format for compatibility |
| `score` | — | Not set by premium providers (omitted) |

Premium provider results are normalised to `searxng.Result` at merge time in `proxy.go`. SearXNG results are passed through as-is.

## Retry and error classification

SearXNG requests are retried up to 3 times with exponential backoff (1s → 2s → 4s). All error classes are retried — there is no 4xx/5xx distinction for the retry path. Errors are classified for metrics:

| Error class | Trigger |
|-------------|---------|
| `timeout` | `context.DeadlineExceeded` |
| `cancelled` | `context.Canceled` |
| `5xx` | HTTP 500–599, "internal server error", "bad gateway", etc. |
| `4xx` | HTTP 400–499 (rare in SearXNG, more common in premium providers) |
| `network` | Connection refused, DNS failure, dial timeout, connection reset |
| `other` | Anything else |

## Prometheus metrics

All metrics are exposed at `:8080/metrics` (configurable via `METRICS_PATH`), prefixed `searxng_gateway_`.

### Request metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `requests_total` | Counter | `outcome` | Requests by outcome: `cache_hit`, `searxng_ok`, `premium_ok`, `searxng_plus_premium_ok`, `fallback_fail`, `timeout` |
| `request_duration_seconds` | Histogram | `source`, `engine` | Latency per source backend and engine |
| `results_count` | Histogram | — | Number of results returned per request |
| `engines_count` | Gauge | — | Distinct engines in last response |

### Engine metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `engine_results_total` | Counter | `engine` | Results contributed per engine (SearXNG engines + premium backends) |
| `engine_unresponsive_total` | Counter | `engine`, `reason` | Times an engine was reported unresponsive by SearXNG |
| `engine_status` | Gauge | `engine` | 1 if the engine responded in the last request, 0 otherwise |

### Circuit breaker metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `circuit_breaker_state` | Gauge | `engine` | 0=closed, 1=half-open, 2=open |
| `circuit_breaker_triggered_at` | Gauge | `engine`, `reason` | Unix timestamp when breaker went open |
| `circuit_breaker_trips_total` | Counter | `engine`, `reason` | Cumulative CB trips |
| `circuit_breaker_requests_total` | Counter | `engine`, `state` | Requests per engine per CB state |
| `circuit_breaker_rejections_total` | Counter | `engine` | Requests rejected (open state) |
| `circuit_breaker_recovery_total` | Counter | `engine` | Auto-recovery events |

### Retry metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `retry_attempts_total` | Counter | `attempt`, `outcome`, `error_class` | Every retry attempt (including first) |
| `retry_exhausted_total` | Counter | `error_class` | Requests where all retries failed |

### Cache metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cache_size` | Gauge | Current LRU cache entry count |

### Quota metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `searxng_gateway_brave_rate_limit_remaining` | Gauge | `period` | Brave API requests remaining in the current request window (from `X-RateLimit-Remaining` header) |
| `searxng_gateway_brave_rate_limit_limit` | Gauge | `period` | Brave API requests allowed in the current request window (from `X-RateLimit-Limit` header) |
| `searxng_gateway_brave_rate_limit_reset_seconds` | Gauge | `period` | Seconds until the Brave API request window resets (from `X-RateLimit-Reset` header) |
| `serper_searches_remaining` | Gauge | `period` | Serper searches remaining (no public API yet) |
| `serper_searches_limit` | Gauge | `period` | Serper searches limit |

## Configuration

All configuration is via environment variables. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `SEARXNG_BACKEND_URL` | `http://searxng-primary:8080` | SearXNG instance URL |
| `FALLBACK_PROVIDERS` | `brave` | Comma-separated premium provider names |
| `T1_PREMIUM_COUNT` | `0` | Number of providers to call in the hot path while SearXNG runs (serial by design; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)) |
| `SUFFICIENT_MIN_RESULTS` | `1` | Target merged result count before fallback loop stops |
| `FALLBACK_TIMEOUT_SECONDS` | `30` | Max time for speculative execution + fallback loop |
| `SEARXNG_TIMEOUT_SECONDS` | `25` | Per-request timeout for SearXNG |
| `SEARXNG_FAIL_THRESHOLD` | `6` | Consecutive SearXNG failures before cooldown |
| `SEARXNG_FAIL_COOLDOWN_SECONDS` | `180` | Cooldown duration for SearXNG |
| `CACHE_SIZE` | `1000` | LRU cache entries |
| `CACHE_TTL_SECONDS` | `3600` | Cache entry TTL |
| `LOG_LEVEL` | `info` | Log level |
| `METRICS_PATH` | `/metrics` | Prometheus endpoint path |

See [README](../README.md) for the complete env var table including per-provider keys.

## Deployment

- **Docker**: `docker compose -f docker-compose.example.yml up -d` starts SearXNG + gateway. See [keyless.md](keyless.md) for a zero-key setup.
- **Endpoints**: `GET /search?q=<query>&format=json`, `GET /healthz`, `GET /metrics`
- The gateway image is built and pushed automatically by GitHub Actions on push to `main` and `v*` tags (`.github/workflows/build.yml`). See [README](../README.md) for build details.
