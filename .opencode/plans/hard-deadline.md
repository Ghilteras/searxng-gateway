# Gateway: the parent budget must end the first-premium join

Authority: `@architect` ruling (2026-11-13), with `@explore` precedent. Scope: the gateway repo only.
Worktree: `/home/angelo/.local/share/opencode/worktree/181bca9bf13607eef71ceea92c764ed24f7b73dc/disco-otter`
Base: `d3daa2a` (= `origin/main` at ruling time).

## Incident that motivated this

2026-11-13T11:25Z: 2 of 22 uncached requests ended `outcome=timeout`, `reason=parent_deadline`,
logged elapsed **9.902s / 10.162s** against `FALLBACK_TIMEOUT_SECONDS=8`, with the caller's own
budget at **10s** (OpenCode `fetch`; OpenClaw is 20s). One premium call was still in flight and was
reported `cancelled`. `retry_attempts_total` shows exactly one attempt per SearXNG stage, so the
retry ladder is not involved; a code audit proved every outbound request honours the context, so no
detached request is involved either.

The one unbounded join in the request path is `firstPremium.wait()` — a bare `<-f.done`
(`internal/proxy/proxy.go:700-704`) whose comment claims cancellation bounds it. It does not.

## Rulings (architect, binding)

| Decision | Ruling | ROI / confidence |
|---|---|---|
| **D1** | Parent cancellation ENDS the wait; no grace beyond 8s. Preserve results already assembled by the request; discard an unfinished first-premium result. Keep `FALLBACK_TIMEOUT_SECONDS=8`. | 8 / 8 |
| **D2** | Leave `cpus: "0.25"` unchanged. No YAML delta, no quota-driven recreate. | 8 / 5 |
| **D3** | Attempt ONE retrospective fixed-window evaluation 2026-11-08T04:46:29Z → 2026-11-09T04:46:29Z; if historical identity/admissibility cannot be established, close the gate as permanently unevaluated. | 7 / 5 |

### Important limitation (must not be overstated)
This is **cancellation-bounded waiting**, not a hard-real-time handler guarantee. The synchronous
SearXNG call, serial premium calls, scheduling, logging and response writing remain cooperative. The
observed SearXNG-stage overrun (2 samples in (5s,8s] against a 4s bound) is **not** explained or
repaired here.

### Why not the alternatives
- Buffered result channel: the worker publishes through shared state then `close(done)`, so a late
  sender already cannot block. Buffering solves nothing here (ROI 3 / conf 9).
- Graceful lateness: contradicts parent-budget authority and spends the caller's headroom (2/8).
- Lowering the parent timeout alone: hides one sampled overrun without bounding the join (2/8).
- `errgroup` / handler-timeout wrapper / new retry machinery: wrong semantics or new complexity (1/8).

Invariant to document, not enumerate: **parent budget plus operational headroom must fit below the
tightest supported caller timeout.** The gateway cannot discover its callers' budgets, so this is not
encoded as config validation.

## Exact code (surgical hunks)

### 1. `providerDiagnostic` gains an in-flight marker

```go
type providerDiagnostic struct {
	Provider          string `json:"provider"`
	Outcome           string `json:"outcome,omitempty"`
	Results           int    `json:"results,omitempty"`
	Skip              string `json:"skip,omitempty"`
	EligibilityReason string `json:"eligibility_reason,omitempty"`
	InFlight          bool   `json:"in_flight,omitempty"`
}
```

### 2. `firstPremium` carries its start time; the worker still closes `done`

```go
// firstPremium publishes its result before closing done. The worker never
// blocks publishing, even when the request stops waiting.
type firstPremium struct {
	done    chan struct{}
	res     firstPremiumResult
	started time.Time
}
```

```go
func (p *Proxy) startFirstPremium(ctx context.Context, query string, usedPremiums map[string]bool) *firstPremium {
	f := &firstPremium{
		done:    make(chan struct{}),
		started: time.Now(),
	}
	go func() {
		defer close(f.done)
		seen := make(map[string]bool)
		defer func() { f.res.elapsed = time.Since(f.started) }()
		results, seen, _, errMsgs, admitted := p.premiumLoop(ctx, query, nil, seen, usedPremiums, nil, 1, 1)
		f.res.results, f.res.seen, f.res.errMsgs, f.res.admitted = results, seen, errMsgs, admitted
	}()
	return f
}
```

### 3. Cancellable wait

```go
// wait returns the completed first call, or stops waiting when ctx is done.
// A false return transfers no worker-owned state to the request.
func (f *firstPremium) wait(ctx context.Context) (firstPremiumResult, bool) {
	select {
	case <-f.done:
		return f.res, true
	case <-ctx.Done():
		return firstPremiumResult{}, false
	}
}
```

### 4. Call site: only a completed receive transfers worker-owned maps

```go
	premiumActive := time.Duration(0)
	if first != nil {
		if fr, completed := first.wait(timeoutCtx); completed {
			admittedTotal += fr.admitted
			premiumActive += fr.elapsed
			premiumResults = fr.results
			for u := range fr.seen {
				seenURLs[u] = true
			}
			premiumErrMsgs = append(premiumErrMsgs, fr.errMsgs...)
			if timeoutCtx.Err() == nil {
				topStart := time.Now()
				var admitted int
				premiumResults, _, _, premiumErrMsgs, admitted =
					p.premiumLoop(timeoutCtx, upstreamQuery, premiumResults, seenURLs, usedPremiums, premiumErrMsgs, 0, 0)
				admittedTotal += admitted
				premiumActive += time.Since(topStart)
			}
		} else {
			// Admission is visible independently of worker completion.
			// No serial top-up or access to worker-owned maps is allowed here.
			for _, event := range diagnostics.providers() {
				if event.InFlight || event.Outcome != "" {
					admittedTotal++
				}
			}
			premiumActive += time.Since(first.started)
			premiumErrMsgs = append(premiumErrMsgs, timeoutCtx.Err().Error())
		}
		metrics.PremiumStageDuration.Observe(premiumActive.Seconds())
	}
```

### 5. `admitPremiumCall` publishes admission BEFORE the backend call

```go
	if diagnostics, _ := ctx.Value(requestDiagnosticsKey{}).(*requestDiagnostics); diagnostics != nil {
		diagnostics.addProvider(providerDiagnostic{Provider: provider.Name(), InFlight: true})
	}
```

### 6. `addProvider` replaces only an outstanding admission

```go
func (d *requestDiagnostics) addProvider(event providerDiagnostic) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !event.InFlight && event.Outcome != "" {
		for i, previous := range d.providerEvents {
			if previous.Provider == event.Provider && previous.InFlight {
				d.providerEvents[i] = event
				return
			}
		}
	}
	d.providerEvents = append(d.providerEvents, event)
}
```

### 7. `cleanEmpty` is false while any provider is in flight

`cleanEmpty` must return `false` as soon as an event has `InFlight` true (an unfinished call is not
clean exhaustion), everything else unchanged.

### 8. Metric help states the censoring

```go
Help: "Active premium-stage duration, ending at completion or request abandonment",
```

## Observability contract at an abandoned wait

- zero results + parent deadline → existing `timeout` / `parent_deadline` / `deadline_exceeded`;
- existing SearXNG results → returned with `searxng_ok` (partial results are not errors);
- record ACTUAL elapsed, never clamp to 8s;
- `premium_calls_per_request` once, including admissions visible at finalization;
- `premium_stage_duration` once, censored at abandonment;
- diagnostic provider entry `{"provider":"brave","in_flight":true}` — never a fabricated terminal
  `cancelled`/`not_attempted` for a provider whose admission was already published; the one
  exception is the admission-publish window described under "Accepted risks" below, where a single
  line can carry both `in_flight` and `not_attempted` for the same provider (recorded, not
  restructured);
- the worker keeps its own terminal attempt/duration/breaker accounting; no second request outcome,
  no second result count, no cache insert, no second zero-result log;
- a `provider_failure` line from an abandoned worker can appear AFTER the request's `zero_result`
  line, because the worker is no longer awaited. The `log` package serialises writes internally, so
  this is an ordering change, not corruption — never correlate log order with request completion;
- no new "discarded results" metric.

## Required regression (must fail before, pass after)

A channel-held backend that observes cancellation but refuses to return until explicitly released;
SearXNG returns empty after confirming the backend started. Parent budget 50ms.

```go
select {
case got := <-searchDone:
	if got.response != nil || got.err == nil {
		t.Fatalf("response=%v error=%v; want nil response and timeout error", got.response, got.err)
	}
case <-time.After(time.Second):
	t.Fatal("Search waited for premium completion after its 50ms parent budget")
}
```

Do not release the backend until after this assertion; release in cleanup even on failure. Before
release also assert: `requests_total{outcome="timeout"}` +1, `premium_calls_per_request` count +1 and
sum +1, no second provider call, cache length 0, diagnostic contains `in_flight:true` and does not
mark that provider `not_attempted`. After release: terminal provider accounting once, request outcome
and cache unchanged. Repeat with one SearXNG result: must return before release, contain only that
result, and count `searxng_ok`.

Existing tests that must be re-bound (not deleted):
`TestSearchDeadlineCancelsPremiumAndReturnsPartialOrError` (it currently demands success from results
manufactured after cancellation) and `TestParentBudgetExpiryStopsBothStages` (it currently assumes the
worker's bookkeeping finished before `Search` returns) — wait on explicit worker completion instead of
restoring the join or adding sleeps.

## Gate, dependency order, kill criterion

1. Preserve the incident evidence; pin the narrow claim.
2. Held-worker regression demonstrated FAILING on the old code.
3. Code + accounting changes; docs/metric help updated; affected tests re-bound.
4. Raw `go test ./...`, `go test -race ./internal/proxy/`, `golangci-lint run`.
5. D1 gate: **R2** independent reviewer with raw behavioral evidence.
6. FF push → CI-derived release → homelab-config digest bump → stack 31 redeploy (architect: a
   gateway-only recreate is R1, not R3) → verify identity, all stack containers, fresh search, and
   arm the new release's acceptance target at deployment.

**KILL CRITERION:** abandon the claim that this is a complete latency fix if the held-worker test
still waits for release, any abandoned worker state reaches request-owned maps or the cache, or fresh
evidence shows another synchronous stage causes the caller-budget overrun. Investigate that path; do
not lower a timeout or raise CPU to turn the signal green.

**Accepted risks:** some otherwise successful late premium results are lost; simultaneously-ready
completion/cancellation is nondeterministic; a permanently non-returning provider still retains its
worker (and therefore emits no terminal attempt, no breaker trip and no duration sample, so a hung
provider is now invisible to those metrics while remaining invisible to nothing else); scheduler and
synchronous-stage overruns remain possible.

**Known residual in the diagnostic, not restructured on purpose (R2 review, 2026-11-13):**
`admitPremiumCall` publishes the `in_flight` record *after* `breakerMgr.Allow` returns. A request
abandoned inside that sub-microsecond window can render one zero-result diagnostic carrying both
`{"provider":"brave","skip":"not_attempted"}` and `{"provider":"brave","in_flight":true}`, and
`admittedTotal` can be 0 for a call that had just been admitted. At that instant the provider had
not been called yet, so neither value is a false claim about work done; the two entries are
inconsistent about the same provider. The ruling's instruction was not to add a lock around external
admission to remove a microscopic race, so this is recorded rather than fixed. Detection if it ever
matters: a zero-result diagnostic containing both `in_flight` and `not_attempted` for one provider.

## D2 (no change made)

`stacks/31-ai.yml:123` keeps `cpus: "0.25"`. Evidence does not establish causality (0.841s lost wall
time in 10 minutes vs ~2s per request); raising it would be insurance or a controlled experiment
(ROI 3 / conf 4), not a fix. Host precedent: 0.2-2.0 CPU across stacks; no documented rationale for
0.25 exists in memory or plans.

## D3 (separate closure, fixed window)

Window `2026-11-08T04:46:29Z → 2026-11-09T04:46:29Z`. Before arming, require historical
release/image identity for that interval, the committed applicable policy revision, retained raw
metrics satisfying the evaluator's traffic/coverage/restart/consistency rules, and explicit
`retrospective` labelling with the original missed acceptance preserved. Use native
`searxng_acceptance.py arm/evaluate`. If inadmissible, record: *historical post-deploy acceptance
permanently unevaluated; no PASS or FAIL established.* Never pick a later quiet window to obtain
acceptance.
