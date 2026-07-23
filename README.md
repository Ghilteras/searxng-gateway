# searxng-gateway

Decision proxy in front of SearXNG: forwards queries to SearXNG, falls back to Brave Search API when results are too few or engine diversity is too low. Same JSON shape as SearXNG, Prometheus /metrics, in-memory LRU cache.

Forked from [byteowlz/sx](https://github.com/byteowlz/sx) for the fallback orchestration logic; this repo adds the HTTP server, metrics, cache, and packaging.

## Env vars

| Var | Default | Required |
|-----|---------|----------|
| `LISTEN_ADDR` | `:8080` | no |
| `SEARXNG_BACKEND_URL` | `http://searxng-primary:8080` | no |
| `BRAVE_API_KEY` | — | **yes** |
| `FALLBACK_MIN_RESULTS` | `5` | no |
| `FALLBACK_MIN_ENGINES` | `2` | no |
| `FALLBACK_TIMEOUT_SECONDS` | `30` | no |
| `SEARXNG_TIMEOUT_SECONDS` | `25` | no |
| `BRAVE_TIMEOUT_SECONDS` | `15` | no |
| `CACHE_SIZE` | `1000` | no |
| `CACHE_TTL_SECONDS` | `3600` | no |
| `LOG_LEVEL` | `info` | no |
| `METRICS_PATH` | `/metrics` | no |

## Endpoints

- `GET /search?q=<query>&format=json` — proxy endpoint
- `GET /healthz` — liveness
- `GET /metrics` — Prometheus exposition

## Build

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/Ghilteras/searxng-gateway:latest --push .
```

## Run

```bash
docker run -d --name searxng-fallback \
  -e BRAVE_API_KEY=<your-key> \
  -e SEARXNG_BACKEND_URL=http://searxng-primary:8080 \
  -p 8080:8080 \
  ghcr.io/Ghilteras/searxng-gateway:latest
```
