# Prometheus metrics

The gateway exposes metrics at `/metrics` with the `searxng_gateway_` prefix.
This inventory describes the metrics emitted by the source; it does not assert
what is present in any deployed instance.

## Request and provider metrics

- `searxng_gateway_requests_total{outcome}` — gateway request outcomes.
- `searxng_gateway_provider_duration_seconds{provider,phase}` — duration of each actual premium-provider call; `phase` is `primary` or `canary`.
- `searxng_gateway_provider_attempts_total{provider,phase,outcome}` — one terminal outcome per actual premium `Search` call.
- `searxng_gateway_provider_skips_total{provider,reason}` — provider exclusions; skips are not attempts.
- `searxng_gateway_provider_eligibility{provider,reason}` — bounded one-hot eligibility reason for each intended provider.
- `searxng_gateway_searxng_stage_duration_seconds` — duration of the complete SearXNG retry stage.
- `searxng_gateway_searxng_stage_total{outcome}` — SearXNG stage outcome per uncached request.
- `searxng_gateway_premium_stage_duration_seconds` — active premium-stage duration for an uncached request.
- `searxng_gateway_search_results_total{source}` — results included in a freshly assembled response by source; cache hits are not counted.
- `searxng_gateway_premium_calls_per_request` — admitted premium calls per uncached request.
- `searxng_gateway_search_request_duration_seconds` — complete `/search` handler duration, including response writing.
- `searxng_gateway_results_count` — histogram of results returned per request.
- `searxng_gateway_cache_size` — current number of entries in the in-memory LRU cache, updated on each SearXNG response.
- `searxng_gateway_low_results_engine_results_total{engine}` — result contributions by primary engine for responses with at most five results. It counts results, not responses; do not compare it as a share of the `searxng_gateway_results_count_bucket{le="5"}` thin-response count.

## SearXNG and engine metrics

- `searxng_gateway_searxng_failure_streak` — consecutive SearXNG failures.
- `searxng_gateway_searxng_cooldown_until_seconds` — Unix timestamp when the current SearXNG cooldown expires, or zero outside cooldown. Cooldown clears lazily on a request after expiry; alert on a value greater than `time()`, not merely greater than zero.
- `searxng_gateway_engines_count` — distinct engines in the last response.
- `searxng_gateway_engine_results_total{engine}` — result contributions keyed by `engine`. The label is ambiguous: premium providers write their own names into the same series (`brave`, `exa`, `parallel`, `serper`, `tavily`), so a value may belong to a premium provider or to an upstream SearXNG engine of the same name. Do not read it as SearXNG-only.
- `searxng_gateway_engine_unresponsive_total{engine,reason}` — times an engine was reported unresponsive.
- `searxng_gateway_engine_status{engine}` — whether an engine responded in the last request, based on SearXNG's unresponsive-engine list.
- `searxng_gateway_retry_attempts_total{attempt,outcome,error_class}` — SearXNG retry attempts, including the first.
- `searxng_gateway_retry_exhausted_total{error_class}` — requests where all SearXNG retries failed.

## Circuit breaker metrics

- `searxng_gateway_circuit_breaker_state{engine}` — 0 closed, 1 half-open, 2 open; premium breakers use `engine="premium:<provider>"`.
- `searxng_gateway_circuit_breaker_triggered_at{engine,reason}` — Unix timestamp when the breaker opened.
- `searxng_gateway_circuit_breaker_trips_total{engine,reason}` — breaker trips.
- `searxng_gateway_circuit_breaker_recovery_total{engine}` — automatic recoveries to closed state.

## Brave request-window metrics

- `searxng_gateway_brave_rate_limit_remaining{period}` — remaining Brave API requests in the current request window.
- `searxng_gateway_brave_rate_limit_limit{period}` — maximum Brave API requests in the current request window.
- `searxng_gateway_brave_rate_limit_reset_seconds{period}` — seconds until the Brave request window resets.

These Brave gauges are parsed from response headers and describe request-window
allowance, not account credits or dollar balance.

## Source pointers

- `internal/metrics/metrics.go` — gateway metric definitions and registration.
- `internal/brave/client.go` — Brave rate-limit header parsing and gauges.
- `internal/breaker/breaker.go` — circuit breaker gauges and counters.
- `internal/proxy/proxy.go` — request, stage, provider, engine, and response observations.
- `backends/*.go` — provider implementations and provider-related observations.
- The previously cited `internal/quota/quota.go` path is not present at this repository base; Brave gauges are defined in `internal/metrics/metrics.go` and observed from `internal/brave/client.go`.
