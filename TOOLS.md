---
description: Premium-provider metrics and quota diagnostic inventory.
---

# Gateway operational inventory

## Premium and engine metrics (2026-08-28)

Per-engine metrics are:

- `searxng_gateway_engine_results_total{engine}`
- `searxng_gateway_circuit_breaker_state{engine}`
- `searxng_gateway_circuit_breaker_trips_total{engine,reason}`
- `searxng_gateway_circuit_breaker_recovery_total{engine}`
- `searxng_gateway_request_duration_seconds{source,engine}`
- `searxng_gateway_engine_status{engine}` — 0/1, derived from `UnresponsiveEngines`.

Implementation pointers: `internal/brave/client.go`, `internal/quota/quota.go`, and `backends/*.go`.

Only Brave parses `X-RateLimit-Remaining`, `X-RateLimit-Limit`, and `X-RateLimit-Reset`, exporting `searxng_gateway_brave_credits_remaining` and `searxng_gateway_brave_credits_limit`. Tavily, Exa, and Jina discard 429 headers. The 2026-08-28 live probe found that none of those three sends `X-RateLimit-*`: Tavily exposes `GET /usage` (Bearer; `plan_usage`/`plan_limit`), Exa exposes `costDollars` plus binary `PAYMENT-REQUIRED`, and Jina exposes binary HTTP 402. A remaining-credits alarm is therefore structurally Brave-only; the other providers require separate polling or response-body mechanisms. Jina's dashboard alert is an RPM/token proxy, not a quota countdown.
