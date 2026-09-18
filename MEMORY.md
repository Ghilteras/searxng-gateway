# MEMORY — homelab-gateway

Lessons, incidenti, decisioni di processo. Datato, non sovrascritto. Indice: il dettaglio vive nelle entry journal (termine di ricerca indicato).

---

## 2026-08-04 — Build via GitHub Actions; builder buildx locale rimosso

- **Build = GitHub Actions, punto** (amd64+arm64, push GHCR su main e tag `v*`); non ripristinare il build locale multi-arch. Emergenza: ricreare il builder (`docker buildx create --use --name=multiarch`) prima del comando locale.
→ journal: search "buildx multiarch"

---

## 2026-07-31 — README: Bing fuori dai fallback; chiavi provider; nota mojeek

- **Il config deployato `configs/searxng/settings.yml` (c51e6dfd) ha il blocco mojeek `disabled: true`**: non toccare l'example senza conferma di Angelo.
→ journal: search "Bing removed from fallback"

---

## 2026-07-31 — Cache TTL; Valkey non serve; limiter SearXNG

- **Il gateway non ha bisogno di Valkey** (LRU in-process, ~1000 entry): rivalutare solo per multi-replica dietro LB o invalidazione cross-servizio.
- **La sezione architettura di AGENTS.md va allineata al codice quando cambia** (era "Ristretto", il codice usa golang-lru/v2).
→ journal: search "Valkey rejected"

---

## 2026-07-29 — Refactor proxy: fallbackSearch → premiumLoop

- **Nessun parallelismo multi-goroutine sui premium** (loop sequenziale; parallelo solo verso SearXNG).
- **Outcome attribuito da `sxSkipped`**: `premium_ok` se SearXNG saltato e premium con risultati, altrimenti `searxng_plus_premium_ok` / `searxng_ok`.
- **Lezioni**: un solo incremento `RequestsTotal` per request; SearXNG skipped → `close(sxCh)` (zero value, `ok=false`); `SufficientMinResults` basso (2-5) nei test.
→ journal: search "fallbackSearch"

---

## 2026-07-29 — T1 Premium Provider → T1_PREMIUM_COUNT

- **`T1_PREMIUM_COUNT` intero** (default 0 = nessun premium in T1); stesso round-robin `NextAvailable`.
→ journal: search "T1_PREMIUM_COUNT"

---

## 2026-07-30 — CB: solo 4xx aprono il breaker; premiumLoop; deploy v0.11.0

- **Solo i 4xx aprono il CB premium** (`isClientError()`); CB aperto → skip.
- **Lezioni**: `git add -A` senza review del diff mai; il PUT Portainer sovrascrive l'intero array Env (Pattern 1 di `portainer-redeploy`, GET/pre-PUT); file git-crypt locked irrecuperabili (verificare `head -c 9`).
→ journal: search "isClientError"

---

## 2026-08-28 — Premium engine alerting + dashboard diagnostic

Journal: 20260828-170000-*.md (5 entries) — deploy bypass, false sops alarm, worktree reap, dashboard panel 28, deploy script friction. Premium metric inventory now in TOOLS.md (2026-08-28 live probe: Tavily/Exa/Jina no X-RateLimit headers, Brave-only quota).

---

## 2026-09-18 — Premium pass serial by design; breaker reasons race fixed

- **Premium pass seriale per scelta** in entrambi i path (T1 hot-path + fallback `premiumLoop`), documentato in `docs/architecture.md` → "Why the premium pass is serial (deliberate)". → journal: search "serial-by-design"
- **Incidente `breaker.Manager.reasons`**: map scritta/cancellata senza lock mentre i reader usavano `RLock` → `fatal error: concurrent map writes` sotto traffico normale. Fix: `reasonMu`, lock rilasciato PRIMA di `cb.Execute` (`OnStateChange` rientra in `RLock`). Regressione `internal/breaker/breaker_test.go` (`-race`). → journal: search "breaker race"
- **Follow-up aperto (Angelo)**: `.github/workflows/build.yml` builda/pusha ma non lancia test; la copertura gira solo in locale. → journal: search "build.yml runs no tests"
