# searxng-gateway v3.2.5

Decision proxy in front of SearXNG: **SearXNG plus a guaranteed premium call in parallel** — runs a bounded SearXNG stage (skipped during cooldown) concurrently with the first admitted premium provider call, then continues the premium round-robin serially only while the distinct-URL count is below a configurable target; the response lists premium results first (premium wins duplicate URLs), followed by the SearXNG-only results, and accumulated non-empty results are returned even below target. Prometheus /metrics, in-memory LRU cache.

The gateway returns a reduced SearXNG-compatible web-results representation. It guarantees `query` and `results`; it does not transparently forward the full SearXNG envelope: `answers`, `corrections`, and `infoboxes` are not modelled, and modelled `suggestions` and `unresponsive_engines` are not forwarded in the assembled output.

`GET /search` rejects a missing or whitespace-only `q` with HTTP 400 before any upstream search or provider work.

🚀 **Works with zero API keys in keyless mode.** See [docs/keyless.md](docs/keyless.md).

Originally derived from [sx](https://github.com/byteowlz/sx); adds HTTP server, per-engine circuit breaker, Prometheus metrics, cache, and Docker packaging.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Ghilteras/searxng-gateway.git
cd searxng-gateway

# 2. (Optional) Create .env with API keys — or skip for keyless mode
echo 'BRAVE_API_KEY=your_key' > .env
echo 'SERPER_API_KEY=your_key' >> .env

# 3. Start
docker compose -f docker-compose.example.yml up -d

# 4. Test
curl 'http://localhost:8080/search?q=hello+world&format=json'
curl 'http://localhost:8080/metrics'
```

## Architecture

```
Client ───▶ searxng-gateway (:8080)
                    │
                    ├──▶ 0. LRU cache (key = lower-cased query; upstream keeps case)
                    │
                    ├──▶ 1a. SearXNG stage (bounded; skipped during cooldown) ─┐
                    │    ├── Serper (Google via API)                          │  run
                    │    ├── Bing, Wikipedia, GitHub...                       │  in
                    │    └── Circuit breaker per engine + cooldown            │  parallel
                    │    Retry (network-class) up to 3 attempts within        │
                    │    SEARXNG_TIMEOUT_SECONDS.                             │
                    │                                                         │
                    ├──▶ 1b. FIRST premium call (exactly one admitted call) ──┘
                    │    next provider in round-robin: brave, exa, parallel,
                    │    tavily, serper (skips are not admitted calls)
                    │
                    ├──▶ 2. Serial premium top-up: round-robin, one provider
                    │    at a time, ONLY while distinct URLs
                    │    (SearXNG + premium) < SUFFICIENT_MIN_RESULTS.
                    │
                    └──▶ 3. Response: premium results first, then SearXNG-only
                         results (a URL both stages returned appears once, at
                         the premium position).

Latency (typical): p50 ~1.0-1.7s, p95 ~3.5-5s; worst case bounded by
FALLBACK_TIMEOUT_SECONDS (default 8s).
```

See [docs/architecture.md](docs/architecture.md) for the full design.

## Supported fallback providers

The premium pool is automatically derived from configured provider API keys, in fixed round-robin order: brave, exa, parallel, tavily, serper. Providers without keys are not enrolled. The first admitted premium call runs in parallel with the bounded SearXNG stage (skipped during cooldown); further providers run serially only until the accumulated distinct-URL count reaches `SUFFICIENT_MIN_RESULTS` (or all providers are tried, or the request budget expires).

| Provider | Env var | Free tier | Production |
|----------|---------|-----------|------------|
| Brave | `BRAVE_API_KEY` | $5 credit (1,000/mo) | ✅ Yes |
| Exa | `EXA_API_KEY` | $20 + $10/mo (~2,800 searches) | ✅ Yes |
| Parallel | `PARALLEL_API_KEY` | Check current plan | No |
| Tavily | `TAVILY_API_KEY` | 1,000 credits/mo | ✅ Yes |
| Serper | `SERPER_API_KEY` | Homepage advertises 2,500 free queries; recurrence unverified | Pending key |

Example:
```bash
BRAVE_API_KEY=xxx
EXA_API_KEY=xxx
PARALLEL_API_KEY=xxx
TAVILY_API_KEY=xxx
SERPER_API_KEY=xxx
```

Keyless mode (no API keys) works out of the box using SearXNG's free engines (Bing, Wikipedia, GitHub, etc.). Premium providers require their respective API keys.

## Features

- **SearXNG stage** — the SearXNG stage runs concurrently with the first premium call, bounded by `SEARXNG_TIMEOUT_SECONDS` (capped by the remaining `FALLBACK_TIMEOUT_SECONDS` budget) with retry, and is skipped entirely while the SearXNG cooldown is active. Its distinct URLs count toward `SUFFICIENT_MIN_RESULTS` before the serial premium top-up starts.
- **Premium stage, at least one call** — the configured premium providers are selected via atomic round-robin. The first *admitted* provider call (skips such as a missing key or an open breaker do not count) runs in parallel with SearXNG; any further providers are invoked **serially** (by deliberate design; see [docs/architecture.md](docs/architecture.md#why-the-premium-top-up-is-serial-deliberate)) and only while the distinct-URL count is below `SUFFICIENT_MIN_RESULTS`. The response lists premium results first, then SearXNG-only results; a URL returned by both appears once, at the premium position.
- **Circuit breaker per provider/engine** — premium provider faults and SearXNG engine client errors open their circuit for 5 min; auto-recovers
- **Exponential backoff retry** — up to 3 attempts (250ms/500ms between attempts), bounded by the SearXNG stage budget
- **Prometheus /metrics** — 20+ gauges, counters and histograms prefixed `searxng_gateway_`, including `searxng_stage_total{outcome}`, `premium_stage_duration_seconds`, `search_results_total{source}` and `premium_calls_per_request`; latency buckets start at 0.1s
- **LRU cache** — 1000 entries, 1h TTL, in-memory
- **SearXNG config tuning** — reference `examples/searxng/` with engine selection, `suspended_times` tuning, custom User-Agent, and custom Python engines (Serper, Mojeek)
- **Fallback billing alert** — `engine_results_total` tracks premium API usage so you can alert before hitting quota limits

## Observability

The gateway exposes Prometheus metrics at `:8080/metrics`.

### Grafana dashboard

![Full dashboard](examples/grafana/screenshots/searxng-dashboard-full.png)

*A reference dashboard is included at `examples/grafana/searxng-gateway-dashboard.json`. Import it into Grafana, select your Prometheus datasource, and you'll see:*

#### Circuit Breaker State (per engine)

![CB State Timeline](examples/grafana/screenshots/cb-state-timeline.png)

*Each circuit gets its own row. Premium circuits use `premium:<provider>` identities and open on genuine provider faults; SearXNG engine circuits open on reported client errors. 🟢 Closed → 🟡 Half-Open → 🔴 Open. After 5 minutes, exactly one real provider probe is admitted; success closes the circuit and failure reopens it.*

#### Circuit Breaker Trips (cumulative)

![CB Trips](examples/grafana/screenshots/cb-trips.png)

*Each trip means the gateway caught a 4xx and opened the circuit before the engine could degrade further queries. Colored by reason: `rate_limited`, `access_denied`, `captcha`.*

#### Circuit Breaker Recoveries (auto-healing)

![CB Recoveries](examples/grafana/screenshots/cb-recoveries.png)

*After 5 minutes of cooldown, the gateway probes the engine. If it responds, the circuit closes and a recovery is recorded.*

#### Cache hit rate & Cache size

![Cache panels](examples/grafana/screenshots/cache-hit-rate.png)

*Repeated queries are served from the in-memory LRU cache (1000 entries, 1h TTL). "Cache hit rate" is the percentage of requests answered from cache (`100 * rate(cache_hit) / rate(total)`); "Cache size" tracks the current entry count. A low hit rate means every query is hitting SearXNG and premium providers — tune `CACHE_SIZE`/`CACHE_TTL_SECONDS` accordingly.*

### Alerting

Example Prometheus alert rules (vmalert/Mimir compatible):

```yaml
groups:
  - name: searxng-gateway
    rules:
      - alert: SearxngCBStuckOpen
        expr: searxng_gateway_circuit_breaker_state == 2
        for: 5m
        annotations:
          summary: "Circuit breaker stuck open for engine {{ $labels.engine }}"
          
      - alert: SearxngRetryExhausted
        expr: rate(searxng_gateway_retry_exhausted_total[5m]) > 0.01
        for: 5m
        annotations:
          summary: "Retry exhaustion rate elevated"
          
      - alert: FallbackBillSpike
        expr: rate(searxng_gateway_engine_results_total[1h]) * 3600 > 100
        for: 10m
        annotations:
          summary: "Fallback API usage > 100 calls/hour — check billing"
```

## Endpoints

- `GET /search?q=<query>&format=json` — proxy endpoint
- `GET /healthz` — liveness
- `GET /metrics` — Prometheus exposition

## Env vars

| Var | Default | Required | Description |
|-----|---------|----------|-------------|
| `LISTEN_ADDR` | `:8080` | no | HTTP listen address |
| `SEARXNG_BACKEND_URL` | `http://searxng-primary:8080` | no | SearXNG instance URL |
| `BRAVE_API_KEY` | — | no | Brave Search API key |
| `EXA_API_KEY` | — | no | Exa Search API key |
| `EXA_MODE` | `auto` | no | Select Exa `api`, `mcp`, or `auto` (API first, MCP fallback) |
| `EXA_MCP_URL` | — | no | Exa MCP HTTP endpoint; enables MCP mode or fallback |
| `EXA_MCP_TOOL` | `exa-web-search` | no | MCP tool name used for Exa searches |
| `EXA_TIMEOUT_SECONDS` | `3` | no | HTTP timeout for the Exa backend only (other providers use `BRAVE_TIMEOUT_SECONDS`). The Exa API request uses search `type: fast` |
| `PARALLEL_API_KEY` | — | no | Parallel Search API key |
| `SERPER_API_KEY` | — | no | Serper API key for the SearXNG Serper engine |
| `TAVILY_API_KEY` | — | no | Tavily Search API key |
| `TAVILY_SEARCH_DEPTH` | `basic` | no | Tavily search depth (`basic` or `advanced`) |
| `TAVILY_INCLUDE_RAW_CONTENT` | `false` | no | Request full page content inline when `true` |
| `TAVILY_INCLUDE_ANSWER` | `false` | no | Request Tavily's direct answer when `true` |
| `SUFFICIENT_MIN_RESULTS` | `1` | no | Target distinct-URL result count; loop stops when reached (recommend 10 with premiums) |
| `FALLBACK_TIMEOUT_SECONDS` | `8` | no | Hard total request budget: the bounded SearXNG stage plus the premium stage; accumulated nonempty results are returned at the deadline |
| `SEARXNG_TIMEOUT_SECONDS` | `3` | no | Total SearXNG stage budget shared by the HTTP request and all retries/backoff; the stage is cancelled when it expires. A stage expiry while the parent request budget is still alive counts as a SearXNG failure; a parent-budget expiry or caller cancellation does not |
| `SEARXNG_FAIL_THRESHOLD` | `6` | no | Consecutive SearXNG failures before cooldown |
| `SEARXNG_FAIL_COOLDOWN_SECONDS` | `180` | no | Cooldown duration for SearXNG (seconds) |
| `BRAVE_FAIL_THRESHOLD` | `3` | no | Consecutive Brave failures before cooldown |
| `BRAVE_FAIL_COOLDOWN_SECONDS` | `300` | no | Cooldown duration for Brave (seconds) |
| `BRAVE_TIMEOUT_SECONDS` | `3` | no | Per-request timeout applied to every configured premium provider except Exa, which uses `EXA_TIMEOUT_SECONDS` (legacy variable name) |
| `OTEL_TRACES_EXPORTER` | *(empty)* | no | Tracing off. Set `console` or `otlp` to enable OpenTelemetry tracing |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(empty)* | no | OTLP/HTTP base endpoint; `/v1/traces` is appended. Required when the exporter is `otlp` |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | *(empty)* | no | Trace-specific OTLP endpoint, used verbatim; takes precedence over the generic one |
| `OTEL_TRACES_SAMPLER_ARG` | `0.1` | no | Trace-ID ratio sampler (`0`–`1`); an incoming sampled flag cannot override it |
| `OTEL_SERVICE_NAME` | `searxng-gateway` | no | Resource `service.name` for exported spans |
| `OTEL_RESOURCE_ATTRIBUTES` | *(empty)* | no | Extra standard resource attributes, e.g. `deployment.environment=homelab` |
| `CACHE_SIZE` | `1000` | no | LRU cache entries (in-memory) |
| `CACHE_TTL_SECONDS` | `3600` | no | Cache entry TTL (seconds) |
| `LOG_LEVEL` | `info` | no | Application log level |
| `METRICS_PATH` | `/metrics` | no | Prometheus metrics endpoint path |

### Adding a new provider

Implement the `SearchBackend` interface in `backends/`:

```go
type MyProvider struct { APIKey string; Timeout time.Duration }

func (m *MyProvider) Name() string { return "myprovider" }
func (m *MyProvider) IsAvailable() bool { return m.APIKey != "" }
func (m *MyProvider) Search(opts SearchOptions) ([]SearchResult, error) {
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.test/search", nil)
	if err != nil {
		return nil, err
	}
	// Execute req with an HTTP client and map the response to SearchResult values.
	return nil, nil
}
```

Provider implementations must pass `opts.Context` to outbound requests so gateway cancellation and the total response deadline stop upstream work.

Then add a case to `backends/factory.go` and set `MYPROVIDER_API_KEY` in the environment. The rest (registry, fallback chain, circuit breaker) is automatic.

## Reference deployment

- `docker-compose.example.yml` — SearXNG + gateway, one command
- `examples/searxng/` — reference SearXNG config with multi-tier engine posture
- `examples/searxng-engines/` — custom SearXNG engines (Serper, Mojeek API)
- `docs/architecture.md` — design decisions, circuit breaker, metrics
- `docs/keyless.md` — how to run without any API keys

## Build

The image is built and pushed automatically by **GitHub Actions** on every push to `main` and on `v*` tags: see [`.github/workflows/build.yml`](.github/workflows/build.yml) (`docker/build-push-action@v6`, platforms `linux/amd64,linux/arm64`, gha cache, push to GHCR). No local multi-arch build needed — the local `multiarch` buildx builder was removed from the homelab (2026-08-04).

## License

MIT — see [LICENSE](LICENSE).

## Zero-result outcomes

When both search stages return zero web results, the gateway returns HTTP 200 with `{"query":"<effective query>","results":[]}` and records `no_results` only when the request context is live, the SearXNG stage completed successfully, at least one premium call was admitted, and every registered provider has an acceptable diagnostic: an admitted call returned `empty`; an admitted `auth`, `quota`, or `not_configured` outcome carries positive structural evidence (`credentials_rejected`, `quota_exhausted`, or `disabled`); the provider was skipped for `missing_key`; or an open-breaker skip carries that epoch's frozen structural trip cause. No skip is acceptable merely because its label reads like a structural reason. `not_attempted`, unknown or contradictory evidence, a bare breaker skip, and every transient fault—including `rate_limit`/429—force `fallback_fail`. Parent deadline expiry remains `timeout`. `no_results` means no results from the capabilities available to this request, not that no results exist. Empty responses are not cached; nonempty partial results retain their existing success and cache behavior. Each zero-result request emits one structured diagnostic without query text, URLs, response bodies, or query hashes.
