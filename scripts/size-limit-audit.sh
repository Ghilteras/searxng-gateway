#!/usr/bin/env bash
# T1 size-budget audit -- every T1 Markdown file must DECLARE a budget and honour it.
#
# Subject: every tracked *.md that declares a budget, every *.md under an agent/ directory, and any *.md declaring `tier: 1`.
#   * `tier: 1` MUST declare a `size_limit`: otherwise deleting the marker would silently disable
#     every size gate (2026-10-29 review, B1 -- the gates' subject was self-declared).
#   * measured text is the STAGED blob when the file is staged, the working copy otherwise: a
#     staged 705-word file must not look clean because the worktree was trimmed afterwards.
#   * any read/measurement failure exits 1 (fail-closed), never "looks clean".
#
# Why repo-wide: the older staged-only guard let project AGENTS.md sit at 701/700 for an unknown
# number of commits while every guard reported success.
#
# Usage: bash scripts/size-limit-audit.sh [repo-root]
# Exit: 0 = every declared budget honoured, 1 = violation or measurement failure.
set -uo pipefail

fm_keys() { # stdin: file content -> the frontmatter block only (empty when there is none)
  awk 'NR==1 && $0=="---" { fm=1; next } fm && $0=="---" { exit } fm { print }'
}

ROOT="${1:-$(git rev-parse --show-toplevel 2>/dev/null)}"
[ -n "$ROOT" ] || { echo "size-limit-audit: not inside a git repository"; exit 1; }
cd "$ROOT" || { echo "size-limit-audit: cannot cd to $ROOT"; exit 1; }

STAGED=$(git -c core.quotePath=false diff --cached --no-renames --name-only --diff-filter=ACM) \
  || { echo "size-limit-audit: git diff --cached failed (fail-closed)"; exit 1; }
FILES=$(git -c core.quotePath=false ls-files '*.md') \
  || { echo "size-limit-audit: git ls-files failed (fail-closed)"; exit 1; }

fail=0
checked=0
budgetless=0
agg_files=0; agg_words=0; agg_bytes=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  if printf '%s\n' "$STAGED" | grep -qxF "$f"; then
    blob=$(git show ":$f") || { echo "size-limit-audit: cannot read staged blob of $f (fail-closed)"; fail=1; continue; }
  else
    [ -f "$f" ] || continue
    blob=$(cat "$f") || { echo "size-limit-audit: cannot read $f (fail-closed)"; fail=1; continue; }
  fi
  fm=$(printf '%s\n' "$blob" | fm_keys)
  tier=$(printf '%s\n' "$fm" | grep -m1 -E '^tier:[[:space:]]*1[[:space:]]*$')
  lim=$(printf '%s\n' "$fm" | grep -m1 -E '^size_limit:[[:space:]]*[0-9]+[[:space:]]*(words.*)?$' | grep -oE '[0-9]+' | head -1)
  if [ -z "$fm" ] && printf '%s\n' "$blob" | head -1 | grep -qx -- '---'; then
    echo "size-limit-audit: $f opens frontmatter but it is never closed (fail-closed)"
    fail=1; continue
  fi
  fm_closed=$(printf '%s\n' "$blob" | awk 'NR==1 && $0=="---" { fm=1; next } fm && $0=="---" { closed=1; exit } END { if (closed) print "yes" }')
  if printf '%s\n' "$blob" | head -1 | grep -qx -- '---' && [ "$fm_closed" != yes ]; then
    echo "size-limit-audit: $f opens frontmatter but it is never closed (fail-closed)"
    fail=1; continue
  fi
  case "$f" in
    */agent/*.md)
      if [ -z "$lim" ]; then
        printf 'size-limit: %s is an agent prompt but declares no size_limit (a budget cannot be optional)\n' "$f"
        budgetless=$((budgetless + 1)); fail=1; continue
      fi ;;
  esac
  if [ -n "$tier" ] && [ -z "$lim" ]; then
    printf 'size-limit: %s declares tier: 1 but no size_limit (a budget cannot be optional)\n' "$f"
    budgetless=$((budgetless + 1))
    fail=1
    continue
  fi
  # Injected instruction files are a NAMED set: enrollment cannot be opt-in. A file the runtime
  # puts in every prompt must declare a budget, whether or not its author remembered the frontmatter.
  managed=0
  case "$f" in
    AGENTS.md|TOOLS.md|MEMORY.md|configs/opencode/global/AGENTS.md|configs/opencode/global/TOOLS.md) managed=1 ;;
  esac
  if [ "$managed" -eq 1 ] && [ -z "$lim" ]; then
    printf 'size-limit: %s is an injected instruction file but declares no size_limit (a budget cannot be optional)\n' "$f"
    budgetless=$((budgetless + 1)); fail=1; continue
  fi
  # MEMORY.md is injected under the plugin's hard cap. A declared budget above it is a budget the
  # injector cannot deliver: the file would be compliant and absent.
  if [ "$f" = "MEMORY.md" ] && [ "$lim" -gt 330 ]; then
    printf 'size-limit: %s declares %s words but the memory plugin injects at most 330 (a budget the injector cannot deliver is not a budget)\n' "$f" "$lim"
    fail=1; continue
  fi
  # Injected context must not hold measurements: a digest, hash, checksum or dated verification is
  # stale at constant size, so no word count can see it.
  if [ "$managed" -eq 1 ]; then
    # Value-shaped digests only: `sha256:`/`sha512:` followed by 16+ hex, including `repo@sha256:`
    # pins. Prose words ("checksum", "md5") and bare hex are NOT matched: the plan declines a truth
    # oracle (see .opencode/plans/harness-context-governance.md, "no dedicated digest guard"), and
    # reading legitimate prose would block every commit while teaching nothing.
    hits=$(printf '%s\n' "$blob" | grep -m3 -nE 'sha256:[0-9a-f]{16,}|sha512:[0-9a-f]{16,}' || true)
    if [ -n "$hits" ]; then
      printf 'size-limit: %s carries a measurement in injected context (values that go stale do not belong there):\n%s\n' "$f" "$hits"
      fail=1; continue
    fi
    agg_files=$((agg_files + 1))
    agg_words=$((agg_words + $(printf '%s\n' "$blob" | wc -w | tr -d ' ')))
    agg_bytes=$((agg_bytes + $(printf '%s' "$blob" | wc -c | tr -d ' ')))
  fi
  [ -n "$lim" ] || continue
  words=$(printf '%s\n' "$blob" | wc -w | tr -d ' ')
  [ -n "$words" ] || { echo "size-limit-audit: cannot measure $f (fail-closed)"; fail=1; continue; }
  checked=$((checked + 1))
  if [ "$words" -gt "$lim" ]; then
    printf 'size-limit: %s = %s words > %s (over by %s)\n' "$f" "$words" "$lim" "$((words - lim))"
    fail=1
  fi
done <<< "$FILES"

# User-scope instructions are injected too and belong to no repository check.
USER_DOC=${HOME}/.config/opencode/tokensave.md
if [ -f "$USER_DOC" ]; then
  agg_files=$((agg_files + 1))
  agg_words=$((agg_words + $(wc -w < "$USER_DOC" | tr -d ' ')))
  agg_bytes=$((agg_bytes + $(wc -c < "$USER_DOC" | tr -d ' ')))
fi
printf 'size-limit-audit: injected bundle %s words / %s bytes across %s files (report only: this repo + user-scope tokensave.md)\n' "$agg_words" "$agg_bytes" "$agg_files"

if [ "$fail" -ne 0 ]; then
  echo ''
  echo "A T1 file is over the budget it declares, or declares tier: 1 without a budget."
  echo "Move procedural detail into a lazy skill and leave a pointer. Do NOT delete a rule that"
  echo "carries load, and do NOT shorten a rule's own wording to buy room for new text."
  echo "If the budget is genuinely wrong, change it DELIBERATELY in the frontmatter and say why"
  echo "in the commit message."
  echo "If the offending file is not yours, fix its budget in its own commit (or tell the owner);"
  echo "do NOT reach for --no-verify: it also disables main-guard, sops-plaintext-check and"
  echo "model-ref-guard for that commit."
  exit 1
fi
if [ -z "$FILES" ] || [ "$checked" -eq 0 ]; then
  echo "size-limit-audit: no budgeted T1 file was measured (fail-closed: green requires a measured subject)"
  exit 1
fi
echo "size-limit-audit: $checked budgets checked, 0 violations, $budgetless undeclared"
exit 0
