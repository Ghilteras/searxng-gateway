# Relevance pilot

A blind, judged measure of whether `/search` returns *usable* answers, as distinct from whether it ranks an official documentation page first.

## Method
- 24 cases: the 18 external captures of 2026-11-06, plus 6 written for this pilot (2 documentation navigation, 2 troubleshooting, 2 broader).
- Per case: gateway top-5 for all 24 cases; the upstream arm exists only for the 6 pilot cases (the 18 external captures had no second arm). Results were deduplicated by URL, shuffled with a fixed seed, arm and rank hidden.
- Grading scale: 0 irrelevant, 1 related, 2 usable evidence, 3 answers the task. Criteria were written before capture (`cases-2026-11-06.md`).
- Judged from title, URL and snippet only (no page fetches). One judge graded all 149 documents; a second graded a systematic 20% sample.

## Result
Success@5 = 1.000 (18/18 on the external cases); nDCG@5 = 0.929. The same 18 cases under a documentation-precision rubric score Hit@1 = 0.111. The gap is the finding: the gateway rarely ranks the official page first, and almost always places a usable answer in the top five.

The documentation-precision figures come from `baseline-summary-2026-11-06.json` (the capture's own expected-document ranks). `key-baseline-2026-11-06.json` records an approximation of the same matcher and flags six cases; where the two differ, the summary is authoritative.

## Limits
24 cases; model judges, not humans; kappa 0.562 (moderate); graded from snippets; two of six upstream arms unscorable. Directional, not a leaderboard.
- No baseline document was graded 0, and three cases (5, 14, 17) have a constant grade vector, so their nDCG@5 is 1.000 by construction.
- Engine and case are confounded: each baseline case's five documents come from a single engine.
- Every empty-snippet document at capture time came from Exa; the pilot measured snippet absence, not engine quality.
- Captures taken after 2026-11-23 carry the gateway's absence label (`No snippet available (result host: …)`, `content_is_fallback: true`) instead of a blank snippet. Compare like with like: a pre-change capture's "empty snippet" count and a post-change one are not the same measurement, and a judge must not read the label as page text.

## Files
- `cases-2026-11-06.md` — questions and grading criteria, written before capture.
- `blind-2026-11-06.jsonl` — the 149 graded documents, anonymised.
- `key-2026-11-06.json`, `key-baseline-2026-11-06.json` — answer keys (arm, original rank, engine). Keep out of judge prompts.
- `grades-primary-2026-11-06.txt`, `grades-second-2026-11-06.txt` — judge output, id-to-grade.
- `metrics-2026-11-06.txt` — the computed figures.
- `compute_metrics.py` — recomputes nDCG@5, Success@5 and judge agreement from the files above.

## Re-running
Capture the arm(s) for new cases, rebuild the blind file, re-grade blind, then run `python3 benchmarks/relevance/compute_metrics.py`.
The 2026-11-06 run used shuffle seed 20261106 and deduplicated by URL after lower-casing the scheme and host, stripping a leading `www.`, fragments and trailing slashes, keeping the first occurrence (gateway arm first). The blind-builder script is not committed; the rule above is the specification to reproduce it.
Record the gateway revision and cache state with any new run: a cached pass is not a fresh one.
