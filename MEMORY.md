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
