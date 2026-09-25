# TOOLS — homelab-gateway

Machine facts for SearXNG Gateway development.

## Metrics inventory

All metrics are Prometheus, emitted by the gateway at /metrics:

- `searxng_gateway_engine_results_total{engine="..."}` — results per engine (brave, exa, tavily, jina, plus SearXNG engines)
- `searxng_gateway_circuit_breaker_state{engine="..."}` — 0 closed, 1 open
- `searxng_gateway_circuit_breaker_trips_total{engine="...",reason="..."}` — trips
- `searxng_gateway_circuit_breaker_recovery_total{engine="..."}` — half-open recoveries
- `searxng_gateway_search_request_duration_seconds` — whole `/search` handler latency histogram
- `searxng_gateway_searxng_stage_duration_seconds` — one sample per attempted SearXNG retry stage, no sample when the stage is skipped (histogram)
- `searxng_gateway_provider_duration_seconds{provider="...",phase="t1|fallback"}` — premium call latency histogram
- `searxng_gateway_engine_status{engine="..."}` — 0/1 from SearXNG UnresponsiveEngines list (gauge)

Source pointers: `internal/brave/client.go` (X-RateLimit parsing), `internal/quota/quota.go` (Brave remaining/limit gauges; Serper scaffold at 0), `backends/*.go` (SearchBackend), `internal/proxy/proxy.go` (engine_status).

## Retired metrics

- `searxng_gateway_request_duration_seconds{source="...",engine="..."}` — retired 2026-09-25: it recorded one whole-SearXNG-call duration once per engine that responded, so co-responding engines got identical deltas (misleading as per-engine latency). Replaced by the three histograms above; no longer emitted.

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

## Deploy env verification (2026-09-18)

The deployed gateway environment (`T1_PREMIUM_COUNT`, `SUFFICIENT_MIN_RESULTS`, API keys, ...) lives only in Portainer stack 31 (id: `ai`) Env. `homelab-config:stacks/31-ai.yml` is SOPS-encrypted, so deployed values can be verified from neither repo — do not assert production values from repo contents.

## Request-budget and tracing env (2026-09-25)

- `FALLBACK_TIMEOUT_SECONDS=8` — parent request budget; must stay below the tighter caller (OpenCode `fetch` 10s, OpenClaw 20s — both route to `searxng-fallback`).
- `SEARXNG_TIMEOUT_SECONDS=3` — whole SearXNG retry stage (250ms/500ms backoff, up to 3 attempts).
- `BRAVE_TIMEOUT_SECONDS=3` — legacy name; sets the HTTP timeout for **every** premium provider, not just Brave.
- **Stack 31 must be updated to these values.** Tracing is off by default; optional `OTEL_TRACES_EXPORTER` (`console`/`otlp`), `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`), `OTEL_TRACES_SAMPLER_ARG` (default 0.1), `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`. Full table in `README.md`; spans never record query/URL/result/error text.
