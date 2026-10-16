# SearXNG Gateway — proxy con fallback e circuit breaker

**Current feature version: v3.2.0.** The reduced SearXNG-compatible response guarantees `query` and `results`; it does not forward the complete upstream envelope (`answers`/`corrections`/`infoboxes` are unmodelled; `suggestions`/`unresponsive_engines` are not forwarded). Empty aggregation produces `no_results` / HTTP 200 only when the request context is live, SearXNG completed successfully (engine-level degradation is diagnostic, not a veto), at least one premium call was admitted, all admitted terminal outcomes were `empty`, and no configured eligible provider was skipped except for missing configuration. Otherwise return `fallback_fail`, except parent deadline expiry (`timeout`). Empty results are never cached; fresh and cached responses always carry the effective query (cache hits copy before updating it). Every zero-result request writes one structured diagnostic; never include raw queries, URLs, response bodies, or query hashes.

Proxy HTTP davanti a SearXNG: stage SearXNG e primo provider premium (Brave, Exa, Parallel, Serper, Tavily) in parallelo, poi top-up premium seriale finché i risultati sono insufficienti. Circuit breaker per engine bloccati, metriche Prometheus, cache.

- **Linguaggio**: Go 1.25.0
- **Build**: automatica via GitHub Actions su push main e tag `v*` — `.github/workflows/build.yml` (`docker/build-push-action@v6`, platforms `linux/amd64,linux/arm64`, cache gha, push su GHCR). NIENTE build locale multi-arch: il builder buildx `multiarch` è stato rimosso dal homelab (2026-08-04).
- **Test**: `go test ./...`
- **Lint**: `golangci-lint run`
- **Semantic tooling**: Serena/gopls symbol lookup is unavailable in this repo — use direct file edits (`read`/`grep`/`edit`) and never abort a task because gopls is unavailable.

## Architettura

```
cmd/gateway/main.go → internal/proxy/proxy.go (bounded SearXNG stage and the guaranteed first admitted premium call run concurrently; serial premiumLoop top-up only while distinct URLs < target; premium-first output order)
                        → internal/breaker/breaker.go (circuit breaker per engine, isClientError gate)
                        → backends/manager.go (GetAvailable, NextAvailable — round-robin atomico)
                        → backends/ (provider esterni: brave.go, exa.go, parallel.go, serper.go, tavily.go; the Brave rate-limit header gauges live in backends/brave.go)
                        → internal/cache/cache.go (golang-lru/v2 in-memory, TTL da CACHE_TTL_SECONDS)
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
2. Registrarlo in `backends/factory.go` e nell'ordine `PremiumPoolOrder`.
3. Costruire il backend dalla sua variabile `<NAME>_API_KEY`; i backend con chiave configurata entrano automaticamente nel pool nell'ordine fisso.
4. Build e push dell'immagine

## Deploy
- Deploy SOLO via Portainer API (skill portainer-redeploy), MAI docker compose/run/rm.
- Stack 31 (id: ai) è il target di deploy ROUTINE di questo repo: un aggiornamento del gateway si deploya come qualsiasi altro stack, SENZA conferma extra.
- Qualsiasi altro stack → fermati e chiedi ad Angelo di usare homelab-config.
- Routine: improvements ship as releases without waiting for a go; stack 31 deploy is routine (owner standing rule, 2026-10-11).

L'immagine si deploya da homelab-config (`stacks/31-ai.yml`): aggiornare il tag e le env var, redeploy stack 31 via Portainer.
