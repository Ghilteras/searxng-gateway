# SearXNG Gateway — proxy con fallback e circuit breaker

Proxy HTTP davanti a SearXNG: forward a SearXNG, fallback a provider esterni (Brave, Exa, Tavily, Jina) quando i risultati sono insufficienti. Circuit breaker per engine bloccati, metriche Prometheus, cache, quota tracking.

- **Linguaggio**: Go 1.23+
- **Build**: `docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/ghilteras/searxng-gateway:latest --push .`
- **Test**: `go test ./...`
- **Lint**: `golangci-lint run`

## Architettura

```
main.go → cmd/serve.go → internal/proxy/proxy.go (forward SearXNG + retry + fallback)
                        → internal/breaker/breaker.go (circuit breaker per engine)
                        → internal/quota/ (Brave quota tracking)
                        → backends/ (provider esterni: brave.go, exa.go, tavily.go)
                        → history.go, search.go, cache (Ristretto)
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
