# SearXNG Gateway — proxy con fallback e circuit breaker

Proxy HTTP davanti a SearXNG: forward a SearXNG, fallback a provider esterni (Brave, Exa, Tavily, Jina) quando i risultati sono insufficienti. Circuit breaker per engine bloccati, metriche Prometheus, cache, quota tracking.

- **Linguaggio**: Go 1.23+
- **Build**: automatica via GitHub Actions su push main e tag `v*` — `.github/workflows/build.yml` (`docker/build-push-action@v6`, platforms `linux/amd64,linux/arm64`, cache gha, push su GHCR). NIENTE build locale multi-arch: il builder buildx `multiarch` è stato rimosso dal homelab (2026-08-04).
- **Test**: `go test ./...`
- **Lint**: `golangci-lint run`

## Architettura

```
main.go → cmd/serve.go → internal/proxy/proxy.go (forward SearXNG + premiumLoop + fallback)
                        → internal/breaker/breaker.go (circuit breaker per engine, isClientError gate)
                        → internal/quota/ (Brave quota tracking)
                        → backends/manager.go (GetAvailable, NextAvailable — round-robin atomico)
                        → backends/ (provider esterni: brave.go, exa.go, tavily.go, jina.go)
                        → history.go, search.go, cache (golang-lru/v2 in-memory, TTL da CACHE_TTL_SECONDS)
```

## Aggiungere un nuovo backend

1. Creare `backends/<nome>.go` implementando l'interfaccia `SearchBackend`:
   ```go
   type SearchBackend interface {
       Name() string
       Search(ctx context.Context, query string) (*SearchResult, error)
       IsAvailable() bool
   }
   ```
2. Registrarlo in `backends/factory.go` nel costruttore `NewManager`
3. Aggiungerlo a `FALLBACK_PROVIDERS` come env var (`FALLBACK_PROVIDERS=brave,exa,jina`)
4. Build e push dell'immagine

## Deploy
- Deploy SOLO via Portainer API (skill portainer-redeploy), MAI docker compose/run/rm.
- Stack consentito: SOLO stack 31 (id: ai).
- Qualsiasi altro stack → fermati e chiedi ad Angelo di usare homelab-config.

L'immagine si deploya da homelab-config (`stacks/31-ai.yml`): aggiornare il tag e le env var, redeploy stack 31 via Portainer.
