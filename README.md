# searxng-gateway

Decision proxy in front of SearXNG: forwards queries to SearXNG, falls back to Brave Search API when results are too few or engine diversity is too low. Same JSON shape as SearXNG, Prometheus /metrics, in-memory LRU cache.

Forked from [byteowlz/sx](https://github.com/byteowlz/sx) for the fallback orchestration logic; this repo adds the HTTP server, per-engine circuit breaker, Prometheus metrics, cache, and Docker packaging.

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
Client ───▶ searxng-gateway (:8080) ───▶ SearXNG (Tier 1+2 engines)
                    │                        │
                    │                        ├── Serper (Google via API)
                    │                        ├── Bing, Wikipedia, GitHub...
                    │                        └── Circuit breaker tracks failures
                    │
                    └──▶ Fallback chain (Tier 3)
                         Brave → Tavily → Exa → ...
                         Triggered when SearXNG returns < SUFFICIENT_MIN_RESULTS
```

See [docs/architecture.md](docs/architecture.md) for the full design.

## Multi-tier engine posture

| Tier | Source | Cost | Key required? |
|------|--------|------|---------------|
| T1 | Serper (Google) | Free 2,500/mo | Yes (optional) |
| T2 | Bing, Wikipedia, GitHub, StackOverflow, ArXiv, PyPI, Docker Hub, Mwmbl, Marginalia | Free | No |
| T3 | Brave, Tavily, Exa, Jina, Bing API | Free tiers available | Provider-dependent |

Keyless mode (T2 only) works out of the box. See [docs/keyless.md](docs/keyless.md).

## Features

- **Circuit breaker per engine** — 4xx on an engine opens the circuit for 5 min; auto-recovers
- **Exponential backoff retry** — 3 retries with 1s/2s/4s backoff on 5xx/timeout
- **Prometheus /metrics** — 10+ gauges and counters prefixed `searxng_gateway_`
- **LRU cache** — 1000 entries, 1h TTL, in-memory
- **SearXNG config tuning** — reference `examples/searxng/` with engine selection, `suspended_times` tuning, custom User-Agent, and custom Python engines (Serper, Mojeek)
- **Brave billing alert** — `engine_results_total{engine="brave-premium"}` tracks fallback usage

## Observability

The gateway exposes Prometheus metrics at `:8080/metrics`. A reference Grafana dashboard is included:

![searxng-gateway dashboard](examples/grafana/screenshots/searxng-dashboard-full.png)

### Key panels

| Panel | What it shows |
|-------|---------------|
| **Circuit Breaker State** | Per-engine state timeline: 🟢 Closed → 🟡 Half-Open → 🔴 Open |
| ![CB State Timeline](examples/grafana/screenshots/cb-state-timeline.png) | Each engine gets its own row. When an engine returns 4xx, the circuit opens (red) for 5 minutes, then auto-recovers. |
| **Circuit Breaker Trips** | Cumulative trips per engine, colored by reason (rate_limited, access_denied, captcha) |
| ![CB Trips](examples/grafana/screenshots/cb-trips.png) | Each trip means the gateway caught a 4xx and opened the circuit before the engine could degrade further queries. |
| **Circuit Breaker Recoveries** | Auto-recovery events over time |
| ![CB Recoveries](examples/grafana/screenshots/cb-recoveries.png) | The gateway probes the engine after 5 minutes. If it responds, the circuit closes and a recovery is recorded. |

### Importing the dashboard

1. Import `examples/grafana/searxng-gateway-dashboard.json` into Grafana
2. Select your Prometheus datasource (the dashboard uses `${datasource}` variable)
3. Ensure your Prometheus instance scrapes `:8080/metrics` from the gateway container

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
          
      - alert: BraveAPIBillSpike
        expr: rate(searxng_gateway_engine_results_total{engine="brave-premium"}[1h]) * 3600 > 100
        for: 10m
        annotations:
          summary: "Brave API usage > 100 calls/hour"
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
| `FALLBACK_PROVIDERS` | `brave` | no | Comma-separated list of fallback backends (brave, tavily, exa, jina, bing) |
| `BRAVE_API_KEY` | — | no | Brave Search API key |
| `TAVILY_API_KEY` | — | no | Tavily Search API key |
| `EXA_API_KEY` | — | no | Exa Search API key |
| `JINA_API_KEY` | — | no | Jina Search API key |
| `BING_API_KEY` | — | no | Bing Search API key (currently unused — keyless HTML scraping) |
| `SUFFICIENT_MIN_RESULTS` | `1` | no | Minimum SearXNG results before triggering fallback |
| `FALLBACK_MIN_RESULTS` | `5` | no | Minimum results from any single fallback backend |
| `FALLBACK_MIN_ENGINES` | `2` | no | Minimum distinct engines in SearXNG response |
| `FALLBACK_TIMEOUT_SECONDS` | `30` | no | Maximum time for the entire fallback chain |
| `SEARXNG_TIMEOUT_SECONDS` | `25` | no | Per-request timeout for SearXNG |
| `BRAVE_TIMEOUT_SECONDS` | `15` | no | Per-request timeout for Brave API |
| `SEARXNG_FAIL_THRESHOLD` | `6` | no | Consecutive SearXNG failures before cooldown |
| `SEARXNG_FAIL_COOLDOWN_SECONDS` | `180` | no | Cooldown duration for SearXNG (seconds) |
| `CACHE_SIZE` | `1000` | no | LRU cache entries (in-memory) |
| `CACHE_TTL_SECONDS` | `3600` | no | Cache entry TTL (seconds) |
| `LOG_LEVEL` | `info` | no | Log level (debug, info, warn, error) |
| `METRICS_PATH` | `/metrics` | no | Prometheus metrics endpoint path |

## Supported fallback providers

Any `SearchBackend` registered in `backends/` can be used as a fallback. Set `FALLBACK_PROVIDERS` to a comma-separated list:

```bash
# Brave only (default)
FALLBACK_PROVIDERS=brave
BRAVE_API_KEY=xxx

# Tavily instead of Brave
FALLBACK_PROVIDERS=tavily
TAVILY_API_KEY=xxx

# Multiple fallbacks: try Brave first, then Tavily, then Exa
FALLBACK_PROVIDERS=brave,tavily,exa
BRAVE_API_KEY=xxx
TAVILY_API_KEY=xxx
EXA_API_KEY=xxx
```

| Provider | Env var | Free tier | Notes |
|----------|---------|-----------|-------|
| Brave | `BRAVE_API_KEY` | $5 credit (1,000/mo) | Good web coverage, rate-limit headers |
| Tavily | `TAVILY_API_KEY` | 1,000 credits/mo | AI-optimized, includes answers |
| Exa | `EXA_API_KEY` | 1,000 searches/mo | Semantic search, content extraction |
| Jina | `JINA_API_KEY` | 1M tokens/mo (keyless available) | Reader/search API, keyless fallback |
| Bing | — (keyless) | $0 (Azure free tier) | HTML scraping, no API key needed |

### Adding a new provider

Implement the `SearchBackend` interface in `backends/`:

```go
type MyProvider struct { APIKey string; Timeout time.Duration }

func (m *MyProvider) Name() string { return "myprovider" }
func (m *MyProvider) IsAvailable() bool { return m.APIKey != "" }
func (m *MyProvider) Search(opts SearchOptions) ([]SearchResult, error) { /* ... */ }
```

Then add a case to `backends/factory.go` and set `MYPROVIDER_API_KEY` in the environment. The rest (registry, fallback chain, circuit breaker) is automatic.

## Reference deployment

- `docker-compose.example.yml` — SearXNG + gateway, one command
- `examples/searxng/` — reference SearXNG config with multi-tier engine posture
- `examples/searxng-engines/` — custom SearXNG engines (Serper, Mojeek API)
- `docs/architecture.md` — design decisions, circuit breaker, metrics
- `docs/keyless.md` — how to run without any API keys

## Build

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/Ghilteras/searxng-gateway:latest --push .
```

## License

MIT — see [LICENSE](LICENSE). Forked from [byteowlz/sx](https://github.com/byteowlz/sx).
