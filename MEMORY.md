# MEMORY — homelab-gateway

Lessons, incidenti, decisioni di processo. Datato, non sovrascritto.

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

## 2026-07-29 — T1 Premium Provider: pool ristretto in path caldo

### Modifiche strutturali

- **`internal/config/config.go`**: nuovo campo `T1PremiumProviders []string` nella struct `Config`, parsato da env var `T1_PREMIUM_PROVIDERS` (comma-separated, default vuoto). Valori sconosciuti ignorati silenziosamente.
- **`backends/manager.go`**: nuovo metodo `NextFromPool(pool []string, exclude map[string]bool) SearchBackend` — round-robin atomico ristretto a un subset nominale di backend. Ignora nomi non nel registry e backend non disponibili. Condivide lo stesso `rrIdx atomic.Uint64` di `NextAvailable` per distribuzione uniforme.
- **`internal/proxy/proxy.go`**: step 4 di `Search()` ora chiama UN solo premium se `T1_PREMIUM_PROVIDERS` è impostato, via `NextFromPool`. Se vuoto, nessun premium nel path caldo. Step 6 (fallback) usa `premiumLoop` su TUTTI i `FALLBACK_PROVIDERS`, saltando quelli già usati. Logica outcome label invariata.

### Decisioni di processo

- **Default vuoto**: `T1_PREMIUM_PROVIDERS=""` significa T1 = solo SearXNG. L'utente attiva esplicitamente i premium in T1.
- **Nessuna validazione**: provider sconosciuti nella lista vengono ignorati, non causano errori — flessibilità per config dinamiche.
- **Round-robin condiviso**: `NextFromPool` e `NextAvailable` condividono lo stesso contatore atomico `rrIdx`, evitando che una pool sbilanci le altre.
- **Pool T1 indipendente dai fallback**: i provider nel pool T1 vengono sorteggiati SOLO per il path caldo. I fallback usano `NextAvailable` su tutto il registry (saltando `usedPremiums`).

### Risultati

133 test passano (3 nuovi manager pool + 1 nuovo config parse + 2 nuovi proxy T1).
