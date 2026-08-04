# MEMORY — homelab-gateway

Lessons, incidenti, decisioni di processo. Datato, non sovrascritto.

---

## 2026-08-04 — Build migrata a GitHub Actions, builder buildx locale rimosso

### Modifiche strutturali

- **Le direttive di build locale sono state rimosse** da `AGENTS.md` (riga Build) e `README.md` (sezione Build): dicevano `docker buildx build --platform linux/amd64,linux/arm64 ... --push`, un comando ormai rotto.
- **Il builder buildx `multiarch` (docker-container) è stato rimosso dal homelab** (2026-08-04, da homelab-config): il container `buildx_buildkit_multiarch0` non esiste più. L'unico builder rimasto è `default` (docker driver), che NON supporta build multi-platform con `--push`.

### Decisioni di processo

- **Build = GitHub Actions, punto.** `.github/workflows/build.yml` fa tutto: build amd64+arm64, cache gha, push su GHCR, su push a `main` e tag `v*`. Non ripristinare il build locale multi-arch sulla N97 (era ~334s per arm64 e ora il builder non c'è più).
- Se in futuro servisse un build locale multi-arch di emergenza: ricreare il builder con `docker buildx create --use --name=multiarch` PRIMA di usare il comando locale — e aggiornare di nuovo queste doc.

---

## 2026-07-31 — README: Bing fuori dai fallback, chiavi provider documentate

### Decisioni di processo

- **Bing rimosso dalla tabella "Supported fallback providers"** del README (commit `efabb55`): il Bing engine keyless gira già dentro SearXNG sull'hot path; il Bing scraper gateway non è deployato e non è un provider premium (è un engine, non un fallback). **Rimozione secca, senza nota a piè di pagina** — scelta esplicita di Angelo, non aggiungere rimando.
- **Env var del gateway: documentate per tutti e 4 i provider** (commit `cd940cd`): aggiunte le righe EXA_API_KEY / JINA_API_KEY / TAVILY_API_KEY. Prima era documentata solo BRAVE_API_KEY benché tutti e 4 fossero deployati.

### Nota aperta

- **`examples/searxng/settings.example.yml`: blocco "mojeek api" resta `disabled: false`** mentre il config deployato (`home/searxng/settings.yml`) è `disabled: true` da commit `b1dc472` (homelab-config) — scraping HTML viola ToS Mojeek 3.5(e), engine 403-blocked. Probabilmente intenzionale (l'esempio mostra come abilitarlo con la chiave `mojeek_api.py`): **non toccarlo senza conferma di Angelo**.

---

## 2026-07-31 — Cache TTL attivo; Valkey non serve; limiter SearXNG

### Modifiche strutturali

- `internal/cache/cache.go`: applicato il TTL alla LRU in-memory (era configurato con `CACHE_TTL_SECONDS` ma mai usato). `New(size, ttl)`, entry con timestamp, scadenza verificata in `Get` (ttl<=0 = nessuna scadenza). `cmd/gateway/main.go` cabla `cfg.CacheTTL`.
- `examples/searxng/settings.example.yml`: `limiter: true` → `limiter: false` (il limiter di SearXNG richiede Valkey/Redis; senza backend logga `ERROR:searx.limiter: The limiter requires Valkey` a ogni riavvio).

### Decisioni di processo

- **Il gateway non ha bisogno di Valkey**: la cache è per-processo (LRU in-memory, ~1000 entry), singola istanza. Valkey aggiungerebbe un container e latenza di rete per zero guadagno. Rivalutare SOLO se il gateway va in multi-replica dietro LB (cache condivisa) o serve invalidazione cross-servizio. Valkey rimosso dal homelab (2026-07-31).
- **AGENTS.md del progetto era obsoleto**: diceva "cache (Ristretto)" ma il codice usa `hashicorp/golang-lru/v2`. Corretto. Lezione: la sezione architettura di AGENTS.md va allineata al codice quando cambia.

### Risultati

144 test passano (11 package). Commit `b161fa0`.

---

## 2026-07-29 — Refactor orchestrazione proxy: fallbackSearch → premiumLoop

### Modifiche strutturali

`backends/manager.go`: aggiunti `GetAvailable()` e `NextAvailable()` con round-robin atomico.  
`internal/proxy/proxy.go`: riscritta `Search()`. Il vecchio metodo `fallbackSearch()` (sequenziale, primo successo) rimosso, sostituito da `premiumLoop()` che itera premium via round-robin con deduplica per URL.

Vedi: `backends/manager.go`, `internal/proxy/proxy.go`

### Lezioni

- **Bug double-counting metriche**: il blocco SearXNG e il blocco outcome finale registravano entrambi `RequestsTotal`, contando lo stesso request due volte. Risolto: incremento rimosso dal blocco SearXNG, l'unica label è decisa nel blocco outcome finale.
- **Channel chiuso per "SearXNG skipped"**: quando SearXNG è in cooldown, `close(sxCh)` invece di inviare un risultato fittizio — `<-sxCh` restituisce zero value e `ok=false`. Pattern più pulito.
- **Test setup**: `SufficientMinResults` va impostato basso (2-5) nei test per non creare 10+ risultati fake.

### Decisioni di processo

- **Nessun parallelismo multi-goroutine sui premium**: il loop premium è sequenziale (un provider alla volta) per semplicità. I premium girano in parallelo solo rispetto a SearXNG. Motivo: chiamate API esterne con latenza variabile; parallelizzarle richiederebbe `errgroup` e gestione errori più complessa senza beneficio misurabile vista la deadline comune.
- **Flag `sxSkipped`**: determinato da `inCooldown()`. Se `sxSkipped && premiumHadResults` → outcome label `premium_ok`; altrimenti `searxng_plus_premium_ok` (se entrambi i path hanno risultati), o `searxng_ok` (solo SearXNG).

### Risultati

117 test passano: 7 nel package `proxy`, 110 nei `backends`.

---

## 2026-07-29 — T1 Premium Provider:  → T1_PREMIUM_COUNT (simplificato)

### Modifiche strutturali

- **`T1_PREMIUM_PROVIDERS` (lista di nomi, es. "brave,exa")** → **`T1_PREMIUM_COUNT`** (intero, es. 2).
- **`NextFromPool` rimosso** — era dead code dopo il refactor.
- **Ora il T1** usa un semplice loop (`for i := 0; i < p.cfg.T1PremiumCount; i++`) che chiama `NextAvailable(usedPremiums)` su TUTTI i `FallbackProviders`.
- **Niente pool separato**: stesso round-robin di `NextAvailable`.

### Decisioni di processo

- **`T1_PREMIUM_COUNT=0`** = nessun premium in T1 (default).
- **`T1_PREMIUM_COUNT=2`** = 2 premium diversi via round-robin in parallelo con SearXNG.
- **Più semplice**: una sola env var intera, nessun parsing di liste, nessun metodo dedicato, nessuna validazione di nomi. `NextAvailable` fa già tutto.

### Risultati

131 test passano.

---

## 2026-07-30 — CB: solo 4xx aprono il breaker; premiumLoop; deploy v0.11.0

### Modifiche strutturali

- `internal/breaker/breaker.go`: aggiunto `isClientError()` gate — solo errori 4xx aprono il CB per i premium. Prima ogni errore (timeout, 5xx) apriva il breaker.
- `internal/proxy/proxy.go`: `premiumLoop()` sostituisce `fallbackSearch()`:
  - T1: SearXNG + 1 premium (round-robin) in parallelo, merge + dedup URL
  - T2: se merged < SUFFICIENT_MIN_RESULTS (10), loop round-robin su premium rimanenti
  - Circuit breaker rispettato (`IsOpen()` → skip)
  - `FALLBACK_TIMEOUT` globale per T1 + T2
- `backends/manager.go`: `GetAvailable()` e `NextAvailable()` con round-robin atomico (`atomic.Uint64`)

### Dashboard Grafana
- Rimosso "Retry Exhaustion Rate" (inutile)
- Rimosso "Engine last-seen" (poco chiaro)
- "Brave fallback" → "Premium results per provider" (stacked bars, tutti 4 premium)

### Lezioni

- **`git add -A` senza review del diff**: abbiamo committato la rimozione di T1_PREMIUM_COUNT per sbaglio, poi revertito. Vedi anche `verify-before-completion` (AGENTS.md globale): il post-patch grep vale anche per i commit.
- **Portainer Env wipe da file obsoleto**: il PUT sovrascrive l'intero array Env. Pattern 1 della skill `portainer-redeploy` (GET/pre-PUT) è l'unico safe path.
- **git-crypt locked non recuperabile**: un backup storico era locked → irrecuperabile. Prima di fare affidamento su file criptati, verificare con `head -c 9 <file>`.

### Risultati

132 test passano. Tag `v0.11.0` su `df81245`. T1_PREMIUM_COUNT=1 in produzione.
