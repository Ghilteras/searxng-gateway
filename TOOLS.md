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
- `searxng_gateway_searxng_failure_streak` — current consecutive SearXNG failures, reset to 0 on success or when an expired cooldown is cleared (gauge, no labels)
- `searxng_gateway_searxng_cooldown_until_seconds` — Unix timestamp when the current SearXNG cooldown expires, 0 when not in cooldown (gauge, no labels). Cleared lazily by `inCooldown()` on the next request after expiry, so alert with `> time()`, never `> 0`

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

## Deployed image (verified 2026-09-25)

Live gateway (stack 31, consumer-facing route `searxng-fallback`): revision `9343e13bfcdd0337b74a2ec05cb6ba4575ac4d71`, version `main`, built 2026-09-25T18:32:53Z. GHCR manifest digest `sha256:9d849dca6c5a14d840a3df95515710826878f4b8e8328b85b3fcd68662cb0a72`; the container's `com.docker.compose.image` reports amd64 config digest `sha256:56b54b147cb9e6069df522374e348dbf8ed119e7ed94ece0af1c1e774a2aa6e6`.

## Request-budget and tracing env (verified live 2026-09-25)

- `FALLBACK_TIMEOUT_SECONDS=8` — parent request budget; must stay below the tighter caller (OpenCode `fetch` 10s, OpenClaw 20s — both route to `searxng-fallback`).
- `SEARXNG_TIMEOUT_SECONDS=3` — whole SearXNG retry stage (250ms/500ms backoff, up to 3 attempts).
- `BRAVE_TIMEOUT_SECONDS=3` — legacy name; sets the HTTP timeout for **every** premium provider, not just Brave.
- Other live values: `SUFFICIENT_MIN_RESULTS=25`, `T1_PREMIUM_COUNT=1`, `FALLBACK_PROVIDERS=brave,exa,tavily`, `SEARXNG_FAIL_THRESHOLD=6`, `SEARXNG_FAIL_COOLDOWN_SECONDS=180`.
- Tracing is off by default; optional `OTEL_TRACES_EXPORTER` (`console`/`otlp`), `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`), `OTEL_TRACES_SAMPLER_ARG` (default 0.1), `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`. Full table in `README.md`; spans never record query/URL/result/error text.

## SearXNG settings (verified 2026-09-25)

Container `searxng-primary`; settings from `homelab-config:configs/searxng/settings.yml`. Live settings md5 `b8927bc0e1dcf57c8aac16462ac3eceb`. `mwmbl` carries per-engine `timeout: 2.5`; `dictzone` removed via `use_default_settings.engines.remove`. SearXNG's own `outgoing.request_timeout` is 60 and `max_request_timeout` 120 — an uncapped hung engine can hold a search ~45s, which is why the gateway must bound the stage. The SearXNG image is a rolling `latest` with no GitHub releases/tags: compare image build dates, not tags, to reason about version drift.

## Latency observability live (2026-09-25)

- Grafana dashboard `searxng-curated`: live `sourceChecksum` `7481ea6d0571086ebe906188e04801fa`, equal to the homelab-config dashboard file md5 (no drift). `gcx dashboards get <uid>` 404s — fetch by resource name.
- Alerts in group `searxng-gateway`, `severity: warning` → routed to `telegram-loud`: `SearxngSearchLatencyHigh` (whole-request p95 > 5s over 10m) and `SearxngStageLatencyHigh` (stage p95 > 3s over 10m); both carry a `>= 20 requests / 10m` guard implemented as a second `classic_conditions` term.
