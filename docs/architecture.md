# Architecture

## Overview

`searxng-gateway` is an HTTP search gateway that sits in front of a SearXNG instance and a configurable pool of premium search providers. It uses **speculative execution** — starting SearXNG and the configured premium-provider pass concurrently, selecting providers via round-robin, and invoking premium providers **serially within each pass** (a deliberate design choice; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)). If SearXNG alone returns enough distinct URLs, the gateway returns without waiting for or merging premium results. Otherwise, it merges T1 results with URL deduplication and loops through remaining providers until a target threshold is met or a timeout expires. The client never talks to SearXNG directly.

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
                         └── Tavily ─┘   ignored if SearXNG alone meets threshold
                         │
                         └──▶ Fallback loop (if merged < SUFFICIENT_MIN_RESULTS)
                              Round-robin through remaining providers
                              until threshold, exhaustion, or FALLBACK_TIMEOUT
```

## Request flow

1. **Normalise** the query (lowercase, collapse whitespace) and check the **LRU cache**. Cache hit returns immediately without calling any backend.
2. **SearXNG cooldown check**: if SearXNG has hit `SEARXNG_FAIL_THRESHOLD` consecutive failures (default 6), it is skipped entirely for `SEARXNG_FAIL_COOLDOWN_SECONDS` (default 180s). Success resets the counter.
3. **Speculative call**: SearXNG starts concurrently with the `T1_PREMIUM_COUNT` premium-provider pass. Providers are selected via atomic round-robin from `FALLBACK_PROVIDERS` and invoked **serially within that pass** (by design; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)).
     - SearXNG is called with up to 3 attempts (250ms/500ms between attempts); the complete retry sequence shares the `SEARXNG_TIMEOUT_SECONDS` child budget, bounded by the overall request context.
     - Each premium provider is called at most once; circuit-breaker-open providers are skipped.
4. **SearXNG fast path / merge**: if SearXNG alone returns at least `SUFFICIENT_MIN_RESULTS` distinct URLs, cancel the speculative T1 context and return SearXNG results without waiting for or merging T1 output. Otherwise, wait for T1, merge T1 results before SearXNG results, and deduplicate by URL. Record per-engine metrics from SearXNG's `unresponsive_engines` field.
5. **Bounded fallback loop**: if merged distinct URLs < `SUFFICIENT_MIN_RESULTS` (default 1), remaining providers (those not already called this request) are tried via round-robin until the threshold is met, all providers are exhausted, or the overall `FALLBACK_TIMEOUT_SECONDS` budget (default 8) expires. At expiry, accumulated nonempty results are returned; an empty result set remains an error. Under-threshold results returned at the deadline are cached with the normal cache TTL.
6. **Outcome**: cache_hit, searxng_ok, premium_ok, searxng_plus_premium_ok, or fallback_fail.

## Why the premium pass is serial (deliberate)

Both premium execution paths — `runT1Pass` and the fallback `premiumLoop` — invoke providers serially. This is a deliberate design choice, not a TODO to be fixed. The trade-offs behind the choice, and why concurrency is not the fix, are:

**1. Quota and rate-limit control.** The two paths differ here, and the argument is strongest for the fallback loop. In the fallback loop, concurrency defeats early stop: calls already launched cannot be un-launched even when the first provider alone would have sufficed, so it is strictly more billed calls (Brave ~1,000/mo, Exa ~2,800/mo, Tavily credits, Jina RPM/tokens). In the T1 pass, `T1_PREMIUM_COUNT` is an attempt budget rather than a guaranteed call count: the loop stops at the request deadline and skips providers that are unavailable or circuit-broken. Serial execution also preserves best-effort pacing against per-second and per-minute caps (Brave QPS, Jina 500 RPM) — response-time spacing, not an explicit rate limiter. A single 4xx from any provider trips that engine's circuit breaker for 5 minutes — an availability cost, not just a metric blip.

**2. Attribution determinism.** Merge order is load-bearing and deliberately order-dependent. When SearXNG is insufficient, T1 premium results merge *before* SearXNG, so premium owns duplicate URLs; in the fallback loop, premium merges *after* SearXNG, so SearXNG owns them. The SearXNG-sufficient fast path is deliberately SearXNG-only, even if T1 has already completed, so response contents and ordering do not depend on goroutine scheduling. Tests pin these cases.

**3. Latency — do not wait for T1 when it is unnecessary.** `SearchOptions.Context` carries the T1 context through each built-in backend to its outbound HTTP requests (including both Exa MCP calls). If SearXNG alone meets the distinct-URL threshold, the gateway cancels T1 and returns immediately; any already-issued provider request may still have consumed quota, and cancellation is cooperative. Provider duration/result metrics and breaker state still record speculative T1 work even when its results are omitted; treat them as provider activity/quota signals, not a count of results returned to the caller. Insufficient-result searches retain speculative parallelism and wait for T1 before starting the bounded fallback loop.

**Non-goal:** Do not convert these passes to a concurrent fan-out.

**Budget and degraded-result trade-offs:** `FALLBACK_TIMEOUT_SECONDS` (default 8s) is the hard parent request budget; `SEARXNG_TIMEOUT_SECONDS` (default 3s) bounds the entire SearXNG retry stage; the legacy `BRAVE_TIMEOUT_SECONDS` variable (default 3s) sets the HTTP timeout for every premium provider. Premium fallback remains serial. If the parent budget expires after some results have accumulated, those partial results are returned and cached with the configured cache TTL; if no result exists, the search fails. Only one case is exempt from the SearXNG cooldown counter: when the request's own parent budget has already expired (or the caller cancelled), the outcome says nothing about SearXNG's health. A stage that cannot answer within `SEARXNG_TIMEOUT_SECONDS` while the parent budget is still alive **is** a failure and counts toward the consecutive-failure cooldown — otherwise a hung upstream could never be skipped and every request would pay the full stage budget forever. Residual: when premium calls consume the entire parent budget before the SearXNG result is collected, a genuine stage expiry can go uncounted and the cooldown under-trips; the request is already degraded in that case.

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

SearXNG requests use up to 3 attempts total with 250ms then 500ms backoff between attempts, all within the `SEARXNG_TIMEOUT_SECONDS` child budget — the short ladder is deliberate so three attempts can fit inside the default 3s stage budget. All error classes are retried — there is no 4xx/5xx distinction for the retry path. Errors are classified for metrics:

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
| `requests_total` | Counter | `outcome` | Exactly one final outcome per request: `cache_hit`, `searxng_ok`, `premium_ok`, `searxng_plus_premium_ok`, `fallback_fail`, or `timeout` (overall budget expired with no results). A SearXNG child-stage timeout that falls back to a result is not a `timeout` request outcome; inspect `retry_attempts_total` for attempt-level timeout/cancellation. |
| `search_request_duration_seconds` | Histogram | — | Complete `/search` handler duration, including cache hits, validation errors and failures |
| `searxng_stage_duration_seconds` | Histogram | — | Complete SearXNG retry stage: exactly one sample per attempted stage, including failures and timeouts, independent of how many engines responded |
| `provider_duration_seconds` | Histogram | `provider`, `phase` | Duration of an actual premium-provider call, labelled by provider and `phase` (`t1`/`fallback`) |
| `results_count` | Histogram | — | Number of results returned per request |
| `engines_count` | Gauge | — | Distinct engines in last response |

Latency histograms use second buckets `1, 2, 3, 4, 5, 8, 10, 15, 20, 30`. There is deliberately **no per-engine duration series**: the gateway can only time the whole SearXNG call, not the individual engines inside it, so attributing that duration to each engine that responded was misleading. Use `retry_attempts_total` and `engine_unresponsive_total` for per-engine signals, and instrument SearXNG itself for true per-engine timing.

### Tracing (OpenTelemetry)

Tracing is **disabled by default**. Set `OTEL_TRACES_EXPORTER` to `console` (structured, allow-listed span records on stdout) or `otlp` (OTLP/HTTP) to enable it. `otlp` requires `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, used verbatim); a generic endpoint gets `/v1/traces` appended. Sampling is a trace-ID ratio (`OTEL_TRACES_SAMPLER_ARG`, default `0.1`) and deliberately ignores an incoming sampled flag, so an external caller cannot force full sampling. Only W3C `tracecontext` is propagated; baggage is never extracted. Spans cover the `/search` handler, the cache lookup, the SearXNG stage with its attempts and backoff, the wait for the SearXNG result, and each premium call (`t1`/`fallback`). Attributes are restricted to fixed low-cardinality keys — query text, URLs, provider bodies and raw error strings are never recorded.

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
| `circuit_breaker_recovery_total` | Counter | `engine` | Auto-recovery events |

### SearXNG cooldown metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `searxng_failure_streak` | Gauge | — | Current consecutive SearXNG failures; reset to 0 on success or when an expired cooldown is cleared |
| `searxng_cooldown_until_seconds` | Gauge | — | Unix timestamp (seconds) when the current SearXNG cooldown expires; 0 when not in cooldown |

The cooldown gauge is cleared lazily: it keeps the expired timestamp until `inCooldown()` runs on the next request that checks it, so an alert must compare against the current time (`> time()`), not against zero (`> 0`).

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
| `T1_PREMIUM_COUNT` | `0` | Number of providers to call speculatively while SearXNG runs; results are ignored on the SearXNG-sufficient fast path (serial within T1; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)) |
| `SUFFICIENT_MIN_RESULTS` | `1` | Target distinct-URL result count before fallback loop stops |
| `FALLBACK_TIMEOUT_SECONDS` | `8` | Hard total budget for speculative execution + fallback loop; returns accumulated results on expiry |
| `SEARXNG_TIMEOUT_SECONDS` | `3` | Total SearXNG stage budget shared across the request and retries/backoff |
| `BRAVE_TIMEOUT_SECONDS` | `3` | HTTP timeout applied to every premium provider (legacy name) |
| `OTEL_TRACES_EXPORTER` | *(empty)* | Tracing off. `console` or `otlp` to enable |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(empty)* | OTLP/HTTP base endpoint; `/v1/traces` is appended |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | *(empty)* | Trace-specific endpoint, used verbatim; wins over the generic one |
| `OTEL_TRACES_SAMPLER_ARG` | `0.1` | Trace-ID ratio sampler, `0`–`1` |
| `OTEL_SERVICE_NAME` | `searxng-gateway` | Resource `service.name`; resolved via `resource.Default()` |
| `OTEL_RESOURCE_ATTRIBUTES` | *(empty)* | Standard extra resource attributes (merged from the environment) |
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
