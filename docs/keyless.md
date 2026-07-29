# Keyless mode — zero API keys

The gateway works without any paid API keys. In keyless mode, all searches use Tier 2 engines only (free, keyless).

## What you get

| Engine | Source | Notes |
|--------|--------|-------|
| Bing | Microsoft | Good general coverage |
| Wikipedia | Wikimedia | Encyclopedia |
| Wikidata | Wikimedia | Structured data |
| GitHub | GitHub | Code repositories |
| StackOverflow | Stack Exchange | Q&A |
| ArXiv | Cornell | Academic papers |
| PyPI | Python Packaging | Python packages |
| Docker Hub | Docker | Container images |
| Mwmbl | Community | Non-profit search index |
| Marginalia | Independent | Small-web search engine |

## What you lose

- **No Google results** (Serper requires API key)
- **No Brave fallback** (Brave requires API key)
- **Mojeek API** (requires API key, £2 CPM)

## Setup

1. In `settings.yml`, keep `disabled: true` on `serper`, `brave`, and `mojeek api` engines
2. Start the stack without env vars:
   ```bash
   docker compose -f docker-compose.example.yml up -d
   ```
3. The gateway will still apply circuit breaker, retry, and caching — just without Tier 1 and Tier 3.

## Adding keys later

1. Get a free Serper API key: https://serper.dev (2,500 queries/month)
2. Get a free Brave Search API key: https://brave.com/search/api/ (2,000 queries/month credit)
3. Create a `.env` file:
   ```
   SERPER_API_KEY=your_key_here
   BRAVE_API_KEY=your_key_here
   ```
4. Restart: `docker compose down && docker compose up -d`
