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
                    └──▶ Brave Search API (Tier 3 fallback)
                         Triggered when SearXNG returns < 25 results
```

See [docs/architecture.md](docs/architecture.md) for the full design.

## Multi-tier engine posture

| Tier | Source | Cost | Key required? |
|------|--------|------|---------------|
| T1 | Serper (Google) | Free 2,500/mo | Yes (optional) |
| T2 | Bing, Wikipedia, GitHub, StackOverflow, ArXiv, PyPI, Docker Hub, Mwmbl, Marginalia | Free | No |
| T3 | Brave Search API | Free $5 credit (1,000/mo) | Yes (optional) |

Keyless mode (T2 only) works out of the box. See [docs/keyless.md](docs/keyless.md).

## Features

- **Circuit breaker per engine** — 4xx on an engine opens the circuit for 5 min; auto-recovers
- **Exponential backoff retry** — 3 retries with 1s/2s/4s backoff on 5xx/timeout
- **Prometheus /metrics** — 10+ gauges and counters prefixed `searxng_gateway_`
- **LRU cache** — 1000 entries, 1h TTL, in-memory
- **SearXNG config tuning** — reference `examples/searxng/` with engine selection, `suspended_times` tuning, custom User-Agent, and custom Python engines (Serper, Mojeek)
- **Brave billing alert** — `engine_results_total{engine="brave-premium"}` tracks fallback usage

## Endpoints

- `GET /search?q=<query>&format=json` — proxy endpoint
- `GET /healthz` — liveness
- `GET /metrics` — Prometheus exposition

## Env vars

| Var | Default | Required |
|-----|---------|----------|
| `LISTEN_ADDR` | `:8080` | no |
| `SEARXNG_BACKEND_URL` | `http://searxng-primary:8080` | no |
| `BRAVE_API_KEY` | — | no (keyless mode works without it) |
| `FALLBACK_MIN_RESULTS` | `5` | no |
| `FALLBACK_MIN_ENGINES` | `2` | no |
| `FALLBACK_TIMEOUT_SECONDS` | `30` | no |
| `SEARXNG_TIMEOUT_SECONDS` | `25` | no |
| `BRAVE_TIMEOUT_SECONDS` | `15` | no |
| `SUFFICIENT_MIN_RESULTS` | `25` | no |
| `CACHE_SIZE` | `1000` | no |
| `CACHE_TTL_SECONDS` | `3600` | no |
| `LOG_LEVEL` | `info` | no |
| `METRICS_PATH` | `/metrics` | no |

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
