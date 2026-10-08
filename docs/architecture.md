# Architecture

## Overview

`searxng-gateway` is an HTTP search gateway that sits in front of a SearXNG instance and a configurable pool of premium search providers. It runs a **SearXNG-first** flow: a bounded SearXNG stage first (skipped during cooldown), then a premium stage that always makes at least one admitted provider call and continues serially (round-robin) only while the distinct-URL count is below the target (a deliberate design choice; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)). The response lists premium results first, then SearXNG-only results; premium owns duplicate URLs. Accumulated non-empty results are returned even below target; the client never talks to SearXNG directly.

```
Client ───▶ searxng-gateway (:8080)
                    │
                    ├──▶ 1. SearXNG stage (first; bounded; skipped during cooldown)
                    │       Retry (network-class) up to 3 attempts within
                    │       SEARXNG_TIMEOUT_SECONDS; cooldown after repeated
                    │       failures; its distinct URLs count toward the target.
                    │
                    ├──▶ 2. Premium stage (configured pool, fixed order)
                    │       ├── Brave ──┐
                    │       ├── Exa     ├── round-robin, serial; always at least
                    │       ├── Parallel│   one admitted call, then continues only
                    │       ├── Tavily  │   while distinct URLs < target.
                    │       └── Serper ─┘
                    │
                    └──▶ 3. Response: premium results first, then SearXNG-only
                            results; a URL both stages returned appears once, at
                            the premium position.
```

## Request flow

1. **Normalise** the query (lowercase, collapse whitespace) and check the **LRU cache**. Cache hit returns immediately without calling any backend.
2. **SearXNG cooldown check**: if SearXNG has hit `SEARXNG_FAIL_THRESHOLD` consecutive failures (default 6), it is skipped entirely for `SEARXNG_FAIL_COOLDOWN_SECONDS` (default 180s). Success resets the counter.
3. **SearXNG stage (first)**: unless SearXNG is in cooldown, it is called with up to 3 attempts (250ms/500ms between attempts) within the parent's remaining budget capped at `SEARXNG_TIMEOUT_SECONDS`. Its distinct URLs count toward `SUFFICIENT_MIN_RESULTS` before the premium stage starts, and per-engine metrics are recorded from its `unresponsive_engines` field. Its stage span carries `phase=primary`. The search cooldown after repeated failures is unchanged.
4. **Premium stage**: keyed providers are selected via atomic round-robin in fixed order (brave, exa, parallel, tavily, serper) and invoked **serially** (by design; see [Why the premium pass is serial](#why-the-premium-pass-is-serial-deliberate)). The stage runs on every uncached request whose context is still live and always makes at least one *admitted* provider call; it continues to further providers only while the distinct-URL count (SearXNG + premium) is below `SUFFICIENT_MIN_RESULTS`, until all providers have been tried or the request deadline expires.
     - Each premium provider is called at most once; skips (missing key, open circuit breaker, refused admission) are not admitted calls, so the round-robin moves to the next provider.
     - If the one admitted call errors or returns empty, the stage stops there (no second provider) when SearXNG already met the target. On cooldown the stage runs the same way and continues to the target. The response lists premium results first, then SearXNG-only results; a URL returned by both stages appears once, at the premium position.
5. **Return**: accumulated non-empty results are returned even below `SUFFICIENT_MIN_RESULTS` (an under-target response is a success, cached with the normal TTL). At the parent deadline an empty result set is a `timeout`; an empty result set otherwise is a failure.
6. **Outcome**: cache_hit, searxng_ok, premium_ok, searxng_plus_premium_ok, fallback_fail, or timeout.

## Why the premium pass is serial (deliberate)

The premium stage — `premiumLoop` — invokes providers serially. This is a deliberate design choice, not a TODO to be fixed. The trade-offs behind the choice, and why concurrency is not the fix, are:

**1. Quota and rate-limit control.** Concurrency defeats early stop: calls already launched cannot be un-launched even when the first provider alone would have sufficed, so it is strictly more billed calls (Brave ~1,000/mo, Exa ~2,800/mo, Tavily credits, Jina RPM/tokens). The loop stops as soon as the target is reached (and the one guaranteed admitted call is done) and skips providers that are unavailable or circuit-broken. Serial execution also preserves best-effort pacing against per-second and per-minute caps (Brave QPS, Jina 500 RPM) — response-time spacing, not an explicit rate limiter. A single 4xx from any provider trips that engine's circuit breaker for 5 minutes — an availability cost, not just a metric blip.

**2. Attribution determinism.** Output order is load-bearing and deliberately order-dependent. Premium results are listed *before* the SearXNG-only results, so premium owns duplicate URLs and stays visible to clients that keep only the first results, and response contents/ordering do not depend on goroutine scheduling. Tests pin these cases.

**3. Latency — extra providers only when needed.** `SearchOptions.Context` carries the premium-stage context through each built-in backend to its outbound HTTP requests (including both Exa MCP calls). SearXNG runs first and is bounded by its own timeout, so the premium stage runs directly under the request deadline; once the target is met and the one guaranteed admitted call is done, no further provider is called.

**Non-goal:** Do not convert these passes to a concurrent fan-out.

**Budget and degraded-result trade-offs:** `FALLBACK_TIMEOUT_SECONDS` (default 8s) is the hard parent request budget; `SEARXNG_TIMEOUT_SECONDS` (default 3s) bounds the entire SearXNG retry stage; the legacy `BRAVE_TIMEOUT_SECONDS` variable (default 3s) sets the HTTP timeout for every premium provider. Premium fallback remains serial. If the parent budget expires after some results have accumulated, those partial results are returned and cached with the configured cache TTL; if no result exists, the search fails. Only one case is exempt from the SearXNG cooldown counter: when the request's own parent budget has already expired (or the caller cancelled), the outcome says nothing about SearXNG's health. A stage that cannot answer within `SEARXNG_TIMEOUT_SECONDS` while the parent budget is still alive **is** a failure and counts toward the consecutive-failure cooldown — otherwise a hung upstream could never be skipped and every request would pay the full stage budget forever. Residual: SearXNG runs first and its child budget is `min(SEARXNG_TIMEOUT_SECONDS, remaining parent budget)`. When the remaining parent budget is the smaller value (only when `FALLBACK_TIMEOUT_SECONDS` is configured tighter than the SearXNG timeout) the child deadline coincides with the parent deadline, so a stage expiry there is not counted as a SearXNG failure and the cooldown can under-trip; that request is already degraded. Conversely, the premium stage no longer reserves a tail of the budget: a SearXNG stage that runs to its full timeout leaves the premium stage `FALLBACK_TIMEOUT_SECONDS − SEARXNG_TIMEOUT_SECONDS` (by default 8s − 3s).

## Per-engine circuit breaker

Uses `sony/gobreaker`. Each premium provider (and each SearXNG engine) gets its own breaker instance.

- **Trip trigger**: a single genuine provider fault trips the premium circuit immediately (`ReadyToTrip`: `ConsecutiveFailures >= 1`): auth, quota, rate limit, network, timeout, 5xx, degraded, invalid response, or unexpected provider error. Caller cancellation, overall deadline, query validation, empty results, and missing configuration do not trip. SearXNG engine breakers still use reported client errors. Premium breaker identities are namespaced as `premium:<provider>`.
- **Open state**: the engine is excluded from subsequent requests.
- **Timeout** (default 5 min): circuit enters half-open, sends one probe request.
  - Success → circuit closes; `recovery_total` counter increments.
  - Failure → circuit re-opens for another 5 min.
- **SearXNG is handled separately** via a binary cooldown counter (not `gobreaker`): after N consecutive SearXNG failures, the entire SearXNG call is skipped for the cooldown duration.

## Supported backends

The premium pool is derived from configured `<NAME>_API_KEY` variables; missing-key providers are not enrolled. Round-robin order is brave, exa, parallel, tavily, serper. The premium stage always makes at least one admitted call, then runs further enrolled providers serially only until the distinct-URL target is reached.

| Backend | Factory name | Key env var | Keyless? | Notes |
|---------|-------------|-------------|----------|-------|
| Brave Search API | `brave` | `BRAVE_API_KEY` | No | `$5 credit = ~1,000 queries/mo` |
| Exa | `exa` | `EXA_API_KEY` | No | Also supports MCP mode (`EXA_MCP_URL`) |
| Parallel | `parallel` | `PARALLEL_API_KEY` | No | Auto-enrolled when key is configured |
| Tavily | `tavily` | `TAVILY_API_KEY` | No | `TAVILY_SEARCH_DEPTH=basic\|advanced` |
| Serper | `serper` | `SERPER_API_KEY` | No | Homepage example; docs page returned 404; free-query recurrence unverified |
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

SearXNG requests retry only `network`-class failures, up to 3 attempts total with 250ms then 500ms backoff between attempts, all within the `SEARXNG_TIMEOUT_SECONDS` child budget. `5xx`, `4xx`, `timeout`, and `other` errors are not retried and cannot reach exhaustion, but are still emitted per attempt in `retry_attempts_total`. Errors are classified for metrics:

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
| `provider_duration_seconds` | Histogram | `provider`, `phase` | Duration of an actual premium-provider call, labelled by provider and phase (`primary`, `canary`) |
| `provider_attempts_total` | Counter | `provider`, `phase`, `outcome` | Exactly one terminal outcome per actual premium Search call; phases `primary`, `canary`; outcomes are bounded provider results/errors |
| `provider_skips_total` | Counter | `provider`, `reason` | Provider exclusions, not attempts; reason is bounded (`missing_key`, `breaker_open`, etc.) |
| `provider_eligibility` | Gauge | `provider`, `reason` | Bounded one-hot current eligibility state per intended provider |
| `results_count` | Histogram | — | Number of results returned per request |
| `engines_count` | Gauge | — | Distinct engines in last response |

Latency histograms use second buckets `1, 2, 3, 4, 5, 8, 10, 15, 20, 30`. There is deliberately **no per-engine duration series**: the gateway can only time the whole SearXNG call, not the individual engines inside it, so attributing that duration to each engine that responded was misleading. Use `retry_attempts_total` and `engine_unresponsive_total` for per-engine signals, and instrument SearXNG itself for true per-engine timing.

### Tracing (OpenTelemetry)

Tracing is **disabled by default**. Set `OTEL_TRACES_EXPORTER` to `console` (structured, allow-listed span records on stdout) or `otlp` (OTLP/HTTP) to enable it. `otlp` requires `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, used verbatim); a generic endpoint gets `/v1/traces` appended. Sampling is a trace-ID ratio (`OTEL_TRACES_SAMPLER_ARG`, default `0.1`) and deliberately ignores an incoming sampled flag, so an external caller cannot force full sampling. Only W3C `tracecontext` is propagated; baggage is never extracted. Spans cover the `/search` handler, the cache lookup, each premium call (`primary`), and the SearXNG stage (`phase=primary`) with its attempts and backoff (the per-attempt child spans carry the same `phase=primary`). Attributes are restricted to fixed low-cardinality keys — query text, URLs, provider bodies and raw error strings are never recorded.

### Engine metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `engine_results_total` | Counter | `engine` | Results contributed per engine (SearXNG engines + premium backends) |
| `engine_unresponsive_total` | Counter | `engine`, `reason` | Times an engine was reported unresponsive by SearXNG |
| `engine_status` | Gauge | `engine` | 1 if the engine responded in the last request, 0 otherwise |

### Circuit breaker metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `circuit_breaker_state` | Gauge | `engine` | 0=closed, 1=half-open, 2=open; premium providers use `premium:<provider>` identities |
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
| `retry_exhausted_total` | Counter | `error_class` | Requests where all network-class retry attempts failed (`network` only) |

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
| `SUFFICIENT_MIN_RESULTS` | `1` | Target distinct-URL result count before the premium stage stops (values < 1 are treated as 1) |
| `FALLBACK_TIMEOUT_SECONDS` | `8` | Hard total budget for the bounded SearXNG stage + premium stage; returns accumulated results on expiry |
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
