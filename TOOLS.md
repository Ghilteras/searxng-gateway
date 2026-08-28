# TOOLS — homelab-gateway

Machine facts for SearXNG Gateway development.

## Metrics inventory (per-engine)

All metrics are Prometheus, emitted by the gateway at /metrics:

- `searxng_gateway_engine_results_total{engine="..."}` — results per engine (brave, exa, tavily, jina, plus SearXNG engines)
- `searxng_gateway_circuit_breaker_state{engine="..."}` — 0 closed, 1 open
- `searxng_gateway_circuit_breaker_trips_total{engine="...",reason="..."}` — trips
- `searxng_gateway_circuit_breaker_recovery_total{engine="..."}` — half-open recoveries
- `searxng_gateway_request_duration_seconds{source="...",engine="..."}` — latency histogram
- `searxng_gateway_engine_status{engine="..."}` — 0/1 from SearXNG UnresponsiveEngines list (gauge)

Source pointers: `internal/brave/client.go` (X-RateLimit parsing), `internal/quota/quota.go` (Brave remaining/limit gauges; Serper scaffold at 0), `backends/*.go` (SearchBackend), `internal/proxy/proxy.go` (engine_status).

## Premium quota caveat (2026-08-28 live probe)

Only Brave sends `X-RateLimit-Remaining/Limit/Reset` in search responses (→ `searxng_gateway_brave_credits_remaining{period}`). Tavily/Exa/Jina throw away 429 headers:

- Tavily: GET /usage (Bearer) → {key:{usage,limit}, account:{plan_usage,plan_limit}}
- Exa: costDollars in body + PAYMENT-REQUIRED header (binary, not countdown)
- Jina: HTTP 402 InsufficientBalanceError (binary)

Remaining-credits alarm is structurally Brave-only. Other providers need separate polling/body mechanisms.
