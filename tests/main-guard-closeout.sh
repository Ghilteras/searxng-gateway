#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GUARD="$ROOT/scripts/main-guard.sh"
OID=1111111111111111111111111111111111111111
LOID=2222222222222222222222222222222222222222
run_guard() { printf '%s\n' "$2" | env OPENCODE_CLOSEOUT="${3:-}" bash "$GUARD" pre-push; }
if run_guard raw "refs/heads/topic $OID refs/heads/main $LOID"; then echo 'FAIL raw accepted'; exit 1; else rc=$?; [ "$rc" = 1 ]; echo 'PASS raw main rejected (exit 1)'; fi
run_guard sanctioned "refs/heads/topic $OID refs/heads/main $LOID" 1; echo 'PASS sanctioned landing accepted (exit 0)'
if run_guard multi "refs/heads/topic $OID refs/heads/topic $LOID
refs/heads/topic $OID refs/heads/main $LOID" 1; then echo 'FAIL unrelated ref accepted'; exit 1; else rc=$?; [ "$rc" = 1 ]; echo 'PASS unrelated refs rejected (exit 1)'; fi
if run_guard deletion "(delete) 0000000000000000000000000000000000000000 refs/heads/main $LOID" 1; then echo 'FAIL deletion accepted'; exit 1; else rc=$?; [ "$rc" = 1 ]; echo 'PASS main deletion rejected (exit 1)'; fi
python3 - "$GUARD" <<'PY'
import sys
s=open(sys.argv[1].rsplit('/scripts/',1)[0]+'/lefthook.yml').read(); block=s.split('pre-push:\n',1)[1].split('\npre-',1)[0]
assert 'use_stdin: true' in block and 'main-guard.sh {stdin}' not in block
print('PASS lefthook stdin wiring tripwire')
PY
TMP_MAIN=$(mktemp -d /tmp/opencode/main-guard.XXXXXX); trap 'rm -rf "$TMP_MAIN"' EXIT
git -C "$TMP_MAIN" init -q -b main
git -C "$TMP_MAIN" -c user.email=test@example.invalid -c user.name=Test commit -q --allow-empty -m init
if (cd "$TMP_MAIN" && env OPENCODE_CLOSEOUT=1 bash "$GUARD"); then echo 'FAIL canonical accepted'; exit 1; else rc=$?; [ "$rc" = 1 ]; echo 'PASS canonical pre-commit blocked (exit 1)'; fi
CUR_BRANCH=$(git branch --show-current)
if [ "$CUR_BRANCH" != main ] && [ "$CUR_BRANCH" != master ]; then env OPENCODE_CLOSEOUT=1 bash "$GUARD"; echo 'PASS feature worktree allowed (exit 0)'; fi
command -v lefthook >/dev/null && LH=$(command -v lefthook) || LH=/home/angelo/.local/bin/lefthook
E2E=$(mktemp -d /tmp/opencode/lefthook-guard.XXXXXX); trap 'rm -rf "$TMP_MAIN" "$E2E"' EXIT
mkdir "$E2E/repo"; git -C "$E2E/repo" init -q -b topic; git -C "$E2E/repo" config user.email test@example.invalid; git -C "$E2E/repo" config user.name Test
git init --bare -q "$E2E/remote.git"; git -C "$E2E/repo" remote add origin "$E2E/remote.git"
printf 'test\n' > "$E2E/repo/file"; git -C "$E2E/repo" add file; git -C "$E2E/repo" commit -qm fixture
cat > "$E2E/repo/lefthook.yml" <<CFG
pre-push:
  commands:
    main-guard:
      use_stdin: true
      run: env -u GIT_DIR -u GIT_WORK_TREE -u GIT_INDEX_FILE -u GIT_OBJECT_DIRECTORY -u GIT_ALTERNATE_OBJECT_DIRECTORIES -u GIT_COMMON_DIR bash "$GUARD" pre-push
CFG
(cd "$E2E/repo" && "$LH" install)
set +e
out=$(git -C "$E2E/repo" push origin HEAD:refs/heads/main 2>&1); rc=$?
set -e
printf 'E2E unmarked output:\n%s\nE2E unmarked exit=%s\n' "$out" "$rc"
[ "$rc" -ne 0 ] && [[ "$out" == *BLOCCATO* ]] || { echo 'FAIL unmarked push did not block'; exit 1; }
set +e
out=$(OPENCODE_CLOSEOUT=1 git -C "$E2E/repo" push origin HEAD:refs/heads/main 2>&1); rc=$?
set -e
printf 'E2E marked output:\n%s\nE2E marked exit=%s\n' "$out" "$rc"
[ "$rc" -eq 0 ] || { echo 'FAIL marked push failed'; exit 1; }
echo 'PASS end-to-end lefthook push contract'
