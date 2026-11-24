import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--pilot-dir', type=Path, default=Path(__file__).resolve().parent)
ROOT = parser.parse_args().pilot_dir

def load_grades(path):
    grades = {}
    for line_no, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        doc_id, grade = line.split()
        if doc_id in grades:
            raise AssertionError(f'{path.name}:{line_no}: duplicate id {doc_id}')
        grade = int(grade)
        if grade not in range(4):
            raise AssertionError(f'{path.name}:{line_no}: grade outside 0-3')
        grades[doc_id] = grade
    return grades

def load_json(path):
    return json.loads(path.read_text())

def dcg(grades):
    return sum((2 ** grade - 1) / math.log2(position + 2)
               for position, grade in enumerate(grades))

def score_rows(docs, grades, rank_field='rank'):
    groups = defaultdict(list)
    for doc_id, doc in docs.items():
        rank = doc.get(rank_field)
        if rank is None and rank_field == 'rank':
            rank = doc.get('original_rank')
        if rank is None:
            continue
        groups[(str(doc['case']), doc['arm'])].append((float(rank), doc_id))
    scored = {}
    used_counts = {}
    for group, entries in groups.items():
        top = sorted(entries, key=lambda row: (row[0], row[1]))[:5]
        judged = [grades[doc_id] for _, doc_id in top if doc_id in grades]
        used_counts[group] = len(judged)
        ideal = dcg(sorted(judged, reverse=True))
        ndcg = dcg(judged) / ideal if ideal else None
        values = [ndcg, int(max(judged, default=0) >= 2)]
        scored[group] = values
    return scored, used_counts

def mean(rows, index):
    vals = [row[index] for row in rows if row[index] is not None]
    return sum(vals) / len(vals) if vals else float('nan')

def fmt(value):
    return 'NA' if value is None else f'{value:.3f}'

def table(headers, rows):
    rows = [[str(x) for x in row] for row in rows]
    widths = [max(len(str(headers[i])), *(len(row[i]) for row in rows)) for i in range(len(headers))]
    print(' | '.join(str(headers[i]).ljust(widths[i]) for i in range(len(headers))).rstrip())
    print('-+-'.join('-' * widths[i] for i in range(len(headers))))
    for row in rows:
        print(' | '.join(row[i].ljust(widths[i]) for i in range(len(headers))).rstrip())

primary = load_grades(ROOT / 'grades-primary-2026-11-06.txt')
second = load_grades(ROOT / 'grades-second-2026-11-06.txt')
baseline_docs = load_json(ROOT / 'key-baseline-2026-11-06.json')
pilot_docs = load_json(ROOT / 'key-2026-11-06.json')
blind = [json.loads(line) for line in (ROOT / 'blind-2026-11-06.jsonl').read_text().splitlines() if line.strip()]
summary = load_json(ROOT / 'baseline-summary-2026-11-06.json')

baseline, baseline_used = score_rows(baseline_docs, primary)
pilot, pilot_used = score_rows(pilot_docs, primary)

print('BASELINE GATEWAY (nDCG@5, Success@5)')
base_rows = []
# Cases 4 and 15 are absent from the capture by construction: their ground-truth
# pages failed validation upstream (HTTP 404 and 429), so the baseline is 18 cases.
EXCLUDED_CASES = {4, 15}
baseline_cases = sorted({int(item['case']) for item in summary} - EXCLUDED_CASES)
for case in (str(c) for c in baseline_cases):
    key = (case, 'gateway')
    if key in baseline:
        n, s = baseline[key]
        count = baseline_used.get(key, 0)
        base_rows.append([case, fmt(n), fmt(s), count if count < 5 else ''])
    else:
        base_rows.append([case, 'NA', 'NA', baseline_used.get(key, 0)])
valid_base = [baseline[(str(case), 'gateway')] for case in baseline_cases if (str(case), 'gateway') in baseline]
base_rows.append(['MEAN', fmt(mean(valid_base, 0)), fmt(mean(valid_base, 1)), ''])
table(['case', 'nDCG@5', 'Success@5', 'used (<5 only)'], base_rows)

print('\nPILOT PER CASE AND ARM (nDCG@5, Success@5)')
pilot_rows = []
for case in [f'C{i}' for i in range(1, 7)]:
    for arm in ['gateway', 'upstream']:
        key = (case, arm)
        if key in pilot:
            n, s = pilot[key]
            count = pilot_used.get(key, 0)
            pilot_rows.append([case, arm, fmt(n), fmt(s), count if count < 5 else ''])
        else:
            pilot_rows.append([case, arm, 'NA', 'NA', pilot_used.get(key, 0)])
classes = [('doc-navigation', ['C1', 'C2']), ('troubleshooting', ['C3', 'C4']), ('broader', ['C5', 'C6']), ('overall', [f'C{i}' for i in range(1, 7)])]
for label, cases in classes:
    for arm in ['gateway', 'upstream']:
        vals = [pilot[(case, arm)] for case in cases if (case, arm) in pilot]
        pilot_rows.append([label, arm, fmt(mean(vals, 0)), fmt(mean(vals, 1)), ''])
table(['case/class', 'arm', 'nDCG@5', 'Success@5', 'used (<5 only)'], pilot_rows)

print('\nDOCUMENTATION PRECISION (summary.json; 18 cases)')
expected_count = len(summary)
if expected_count != 18:
    raise AssertionError(f'expected 18 summary cases, got {expected_count}')
ranks = [item.get('rank') for item in summary]
doc_rows = []
for k in [1, 5, 10]:
    hits = sum(rank is not None and rank <= k for rank in ranks)
    doc_rows.append([f'Hit@{k}', fmt(hits / expected_count), f'{hits}/{expected_count}'])
mrr = sum((1 / rank) if rank is not None and rank <= 10 else 0 for rank in ranks) / expected_count
doc_rows.append(['MRR@10', fmt(mrr), f'{sum(rank is not None and rank <= 10 for rank in ranks)}/{expected_count}'])
table(['metric', 'value', 'numerator/denominator'], doc_rows)

print('\nGRADE AGREEMENT (ids present in both files)')
common = sorted(primary.keys() & second.keys())
if not common:
    raise AssertionError('no shared grade ids')
exact = sum(primary[i] == second[i] for i in common)
within = sum(abs(primary[i] - second[i]) <= 1 for i in common)
observed = sum(primary[i] == second[i] for i in common) / len(common)
by_cat_a = Counter(primary[i] for i in common)
by_cat_b = Counter(second[i] for i in common)
expected = sum(by_cat_a[c] * by_cat_b[c] for c in range(4)) / (len(common) ** 2)
kappa = (observed - expected) / (1 - expected) if expected != 1 else float('nan')
table(['shared ids', 'exact %', 'within-1 %', 'Cohen kappa'], [[len(common), fmt(100*exact/len(common)), fmt(100*within/len(common)), fmt(kappa)]])

print('\nSNIPPET EFFECT (primary grades; excerpt empty if missing/blank)')
excerpt_by_id = {row['id']: row.get('excerpt', '') for row in blind}
all_docs = {}
for source in (baseline_docs, pilot_docs):
    for doc_id, doc in source.items():
        if doc_id in all_docs:
            raise AssertionError(f'duplicate id across document sets: {doc_id}')
        all_docs[doc_id] = doc
snippet_rows = []
empty_grade_counts = Counter()
for arm in ['gateway', 'upstream']:
    for empty in [True, False]:
        vals = [primary[doc_id] for doc_id, doc in all_docs.items()
                if doc['arm'] == arm and doc_id in primary
                and (not str(excerpt_by_id.get(doc_id, '')).strip()) == empty]
        snippet_rows.append([arm, 'empty' if empty else 'non-empty', fmt(mean([[x] for x in vals], 0)), len(vals)])
for doc_id, doc in all_docs.items():
    # A captured excerpt is "absent" when it is blank OR is the gateway's absence
    # label. Since 2026-11-23 the gateway replaces a missing snippet with
    # "No snippet available (result host: …)" and sets content_is_fallback, so a
    # capture taken after that change will never show a blank excerpt.
    if doc_id in primary and not str(excerpt_by_id.get(doc_id, '')).strip():
        empty_grade_counts[primary[doc_id]] += 1
table(['arm', 'excerpt', 'mean grade', 'docs'], snippet_rows)
print('empty docs by grade')
table(['grade', 'count'], [[grade, empty_grade_counts[grade]] for grade in range(4)])

print('\nASSERTIONS')
print(f'PASS: grade rows primary={len(primary)}, second={len(second)}; shared={len(common)}; blind records={len(blind)}; summary cases={expected_count}')
print(f'PASS: baseline scored groups={len(baseline)}; pilot scored groups={len(pilot)}')
