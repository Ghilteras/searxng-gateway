# Relevance pilot

A blind, judged measure of whether `/search` returns *usable* answers, as distinct from whether it ranks an official documentation page first.

## Method
- 24 cases: the 18 external captures of 2026-11-06, plus 6 written for this pilot (2 documentation navigation, 2 troubleshooting, 2 broader).
- Per case: the first 5 results from the gateway and the first 5 from SearXNG directly, deduplicated by URL, shuffled with a fixed seed, arm and rank hidden.
- Grading scale: 0 irrelevant, 1 related, 2 usable evidence, 3 answers the task. Criteria were written before capture (`cases-2026-11-06.md`).
- Judged from title, URL and snippet only (no page fetches). One judge graded all 149 documents; a second graded a systematic 20% sample.

## Result
Success@5 = 1.000 (18/18 on the external cases); nDCG@5 = 0.929. The same 18 cases under a documentation-precision rubric score Hit@1 = 0.111. The gap is the finding: the gateway rarely ranks the official page first, and almost always places a usable answer in the top five.

## Limits
24 cases; model judges, not humans; kappa 0.562 (moderate); graded from snippets; two of six upstream arms unscorable. Directional, not a leaderboard.

## Files
- `cases-2026-11-06.md` — questions and grading criteria, written before capture.
- `blind-2026-11-06.jsonl` — the 149 graded documents, anonymised.
- `key-2026-11-06.json`, `key-baseline-2026-11-06.json` — answer keys (arm, original rank, engine). Keep out of judge prompts.
- `grades-primary-2026-11-06.txt`, `grades-second-2026-11-06.txt` — judge output, id-to-grade.
- `metrics-2026-11-06.txt` — the computed figures.
- `compute_metrics.py` — recomputes nDCG@5, Success@5 and judge agreement from the files above.

## Re-running
Capture both arms for new cases, rebuild the blind file with the same seed, re-grade blind, then run `python3 benchmarks/relevance/compute_metrics.py`. Record the gateway revision and cache state alongside the results; a cached pass is not a fresh one.
