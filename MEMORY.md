# MEMORY — homelab-gateway

Lessons, incidenti, decisioni di processo. Datato, non sovrascritto. Indice: il dettaglio vive nelle entry journal (termine di ricerca indicato).

---

## 2026-08-04 — Build via GitHub Actions; builder buildx locale rimosso

→ journal: search "buildx multiarch"

---

## 2026-07-31 — README: Bing fuori dai fallback; chiavi provider; nota mojeek

→ journal: search "Bing removed from fallback"

---

## 2026-07-31 — Cache TTL; Valkey non serve; limiter SearXNG

→ journal: search "Valkey rejected"

---

## 2026-07-29 — Refactor proxy: fallbackSearch → premiumLoop

→ journal: search "fallbackSearch"

---

## 2026-07-29 — T1 Premium Provider → T1_PREMIUM_COUNT

→ journal: search "T1_PREMIUM_COUNT"

---

## 2026-07-30 — CB: solo 4xx aprono il breaker; premiumLoop; deploy v0.11.0

→ journal: search "isClientError"

---

## 2026-08-28 — Premium engine alerting + dashboard diagnostic

→ journal: search "Dashboard panel 28"

---

## 2026-09-18 — Premium pass serial by design; breaker reasons race fixed

→ journal: search "serial-by-design" · "breaker race" · "build.yml runs no tests"

---

## 2026-09-25 — Latency bounded: parent budget + context propagation

- Uncached consumer-path queries hit 30s (one 502); the unbounded SearXNG stage, not gateway overhead (3–9 ms), dominated. Fix: propagate the request context into every backend HTTP call and keep the parent budget below the tighter caller (OpenCode fetch 10s / OpenClaw 20s, both `searxng-fallback`); retry ladder 1s/2s → 250ms/500ms.
→ journal: search "stage unbounded"

---

## 2026-09-25 — Latency metric was lying; retired and replaced

- `searxng_gateway_request_duration_seconds{source,engine}` recorded one whole-call duration once per responding engine (identical deltas on bing/brave/mwmbl) → retired. New: `..._search_request_duration_seconds`, `..._searxng_stage_duration_seconds`, `..._provider_duration_seconds{provider,phase}`. Open follow-up outside this repo: retired name still in homelab-config Grafana dashboard + alerting rules + `scripts/push-searxng-dashboard.py`.
→ journal: search "metric was misleading"

---

## 2026-09-25 — Tooling: Serena/gopls unusable; rtk prefix is by design

- Serena/gopls symbol lookup unusable here → direct file edits, never abort on gopls. An `rtk`-prefixed executor command is NOT corruption (global `rtk` plugin rewrites executor commands by design) — do NOT add a no-rtk clause to delegation prompts.
→ journal: search "Serena gopls"
