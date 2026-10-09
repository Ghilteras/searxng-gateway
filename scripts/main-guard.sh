#!/usr/bin/env bash
# main-guard.sh — lefthook guard: blocca git write su main.
# Backstop che vede anche il bash degli agent (l'unico strato che intercetta bash).
# - pre-push: legge i ref da stdin (lefthook use_stdin); blocca push a main/master,
#   salvo landing FF autorizzato tramite OPENCODE_CLOSEOUT=1: il gate git-write del primary
#   oppure homelab-pin-sync (commit pin-only generati; .opencode/plans/chamber-agent-pins.md).
# - pre-commit/pre-merge-commit: blocca solo dal MAIN checkout (git-dir==common-dir)
#   sul branch main; nei worktree (branch != main) passa sempre.
# Backstop contro incidenti, non identità/autorizzazione; --no-verify può aggirarlo.
set -u

# pre-push: refs ricevuti su stdin tramite lefthook use_stdin.
if [ "${1:-}" = "pre-push" ]; then
  BLOCK=0
  LANDING=0
  OTHER=0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if [[ $line =~ (^|[[:space:]])refs/heads/main([[:space:]]|$) ]]; then BLOCK=1; fi
    if [[ $line =~ (^|[[:space:]])refs/heads/master([[:space:]]|$) ]]; then BLOCK=1; fi
    if [[ $line =~ ^[^[:space:]]+[[:space:]]+([0-9a-f]{40})[[:space:]]+refs/heads/(main|master)[[:space:]]+[0-9a-f]{40}$ && ${BASH_REMATCH[1]} != 0000000000000000000000000000000000000000 ]]; then LANDING=1; fi
    if ! [[ $line =~ ^[^[:space:]]+[[:space:]]+[0-9a-f]{40}[[:space:]]+refs/heads/(main|master)[[:space:]]+[0-9a-f]{40}$ ]]; then OTHER=1; fi
  done
  if [ "${OPENCODE_CLOSEOUT:-}" = "1" ] && [ "$LANDING" = "1" ] && [ "$OTHER" = "0" ]; then exit 0; fi
  [ "$BLOCK" = "1" ] || exit 0
  echo "❌ main-guard: push di refs/heads/main (o master) BLOCCATO."
  echo "   Il main si aggiorna SOLO via closeout gate (review + git-write): push remote-only FF di SHA immutabile. Mai merge locale su main."
  exit 1
fi

# pre-commit / pre-merge-commit
BRANCH=$(git branch --show-current 2>/dev/null || echo "")
case "$BRANCH" in main|master) ;; *) exit 0 ;; esac

# main checkout? (in un worktree git-dir != git-common-dir)
GIT_DIR=$(git rev-parse --git-dir 2>/dev/null || echo "")
COMMON=$(git rev-parse --git-common-dir 2>/dev/null || echo "")
[ -n "$GIT_DIR" ] && [ -n "$COMMON" ] && [ "$GIT_DIR" = "$COMMON" ] || exit 0

echo "❌ main-guard: git write su main dal checkout principale BLOCCATO."
echo "   Ogni mutazione deve vivere in un worktree (OpenChamber Git UI) e arrivare a main"
echo "   solo via il closeout gate (review + git-write: push FF remote-only di <branch>:main)."
exit 1
