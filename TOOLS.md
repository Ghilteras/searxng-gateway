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

Only Brave sends `X-RateLimit-Remaining/Limit/Reset` in search responses; the gateway exposes these as `searxng_gateway_brave_rate_limit_remaining`, `searxng_gateway_brave_rate_limit_limit`, and `searxng_gateway_brave_rate_limit_reset_seconds` (request-window metrics, not account credits). Tavily/Exa/Jina throw away 429 headers:

- Tavily: GET /usage (Bearer) → {key:{usage,limit}, account:{plan_usage,plan_limit}}
- Exa: costDollars in body + PAYMENT-REQUIRED header (binary, not countdown)
- Jina: HTTP 402 InsufficientBalanceError (binary)

Request-window rate-limit alarm is structurally Brave-only. Other providers need separate polling/body mechanisms.

## Alerting snapshot (2026-08-28)

Source of truth is `homelab-config:configs/grafana/provisioning/alerting/rules.yml` (not this repo) — this is a cached snapshot:

- 4 base + 4 per-engine premium-spike rules. UIDs: `searxng-bravespikeburst` (renamed to PremiumFallbackSpike 2026-08-28; UID preserved — renaming without `deleteRules:` orphans), `searxng-premiumspike-brave/exa/tavily/jina` (thresholds 3/8/3/5, `for: 10m`, `alert_type: billing`), `searxng-cbstuckopen`, `searxng-cbrecovered`, `searxng-retryexhausted`.
- BraveSpikeBurst was premium-wide, not Brave-only (corrected 2026-08-28). Jina rule is RPM/token proxy, not quota countdown.
