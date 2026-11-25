---
tier: 1
description: Machine-environment facts of the gateway repo that do not change with a deploy. Injected every session.
size_limit: 180 words (wc -w)
last_curated: 2026-11-24
---

# TOOLS — homelab-gateway
Project operations and metric inventory pointers; verify deploy-varying values live.

- Metrics inventory: `docs/metrics.md`; Prometheus endpoint `/metrics`, metric prefix `searxng_gateway_`.
- Deployed environment lives only in Portainer stack 31 (`ai`); `homelab-config:stacks/31-ai.yml` is SOPS-encrypted. Never assert production values from either repo; verify from the container or Portainer.
- SearXNG is a rolling `latest` image with no tags; compare build dates.
- Request-budget constraint: `FALLBACK_TIMEOUT_SECONDS` must remain below caller timeouts; verify all deploy-varying values live.
- Request and breaker settings: `SEARXNG_TIMEOUT_SECONDS`, `BRAVE_TIMEOUT_SECONDS`, `SUFFICIENT_MIN_RESULTS`, `SEARXNG_FAIL_THRESHOLD`, `SEARXNG_FAIL_COOLDOWN_SECONDS` (names only; verify live values). `BRAVE_TIMEOUT_SECONDS` is a legacy name that sets the HTTP timeout for every premium provider (only `exa` is overridden, by `EXA_TIMEOUT_SECONDS`): treat it as a global knob, not a Brave-only tune.
- Premium providers enroll when their API key is configured.
- Tracing environment names: `OTEL_TRACES_EXPORTER`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_TRACES_SAMPLER_ARG`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`.
- Alert rules source of truth: `homelab-config:configs/grafana/provisioning/alerting/rules.yml`.
