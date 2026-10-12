#!/usr/bin/env bash
#
# Regression for the scan-coverage guards of `make gitleaks`:
#
#   1. `--text` — a pull request can add a `.gitattributes` entry such as
#      `config.txt -diff` (or `binary`), which makes `git log -p` emit a binary
#      marker instead of the content and hides a credential from the scanner.
#   2. `-m` — `git log -p` does not emit merge-commit patches by default, so a
#      credential introduced only while resolving a conflict is invisible.
#
# Each scenario asserts that the unguarded traversal misses the credential while
# the guarded `make gitleaks` traversal finds it, so a regression in either
# direction fails this test.
#
# Usage: scripts/test-gitleaks-scan-coverage.sh
# Requires gitleaks (default: bin/gitleaks, override with GITLEAKS_BIN).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GITLEAKS_BIN="${GITLEAKS_BIN:-$REPO_ROOT/bin/gitleaks}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

[[ -x "$GITLEAKS_BIN" ]] || { echo "gitleaks not found at $GITLEAKS_BIN (run: make install-gitleaks)" >&2; exit 1; }
GITLEAKS_BIN="$(cd "$(dirname "$GITLEAKS_BIN")" && pwd)/$(basename "$GITLEAKS_BIN")"

# Credential-shaped value that never appears as such in this file.
FAKE_AWS_KEY="AKIA$(printf '0123456789ABCDEF')"

init_repo() {
  git init -q "$1"
  git -C "$1" config user.email test@example.com
  git -C "$1" config user.name test
  git -C "$1" config commit.gpgsign false
}

# The target under test reads the repository Makefile and allowlist; untracked
# copies do not affect `git log`.
stage_repo() { cp "$REPO_ROOT/Makefile" "$REPO_ROOT/.gitleaks.toml" "$1/"; }

raw_scan() { # repo, log-opts -> exit status of an unguarded gitleaks run
  local repo="$1" opts="$2" status
  set +e
  (cd "$repo" && "$GITLEAKS_BIN" git . --no-banner --redact --log-opts="$opts" --exit-code 1) \
    >"$WORK/raw.log" 2>&1
  status=$?
  set -e
  printf '%s' "$status"
}

target_scan() { # repo, log-opts -> exit status of `make gitleaks`
  local repo="$1" opts="$2" status
  set +e
  (cd "$repo" && make gitleaks GITLEAKS_BIN="$GITLEAKS_BIN" GITLEAKS_LOG_OPTS="$opts") \
    >"$WORK/target.log" 2>&1
  status=$?
  set -e
  printf '%s' "$status"
}

echo "== 1. a PR-controlled diff attribute must not hide a file"
init_repo "$WORK/attrs"
printf 'config.txt -diff\n' > "$WORK/attrs/.gitattributes"
printf 'aws_key = "%s"\n' "$FAKE_AWS_KEY" > "$WORK/attrs/config.txt"
git -C "$WORK/attrs" add -A
git -C "$WORK/attrs" commit -qm "add credential plus a hiding attribute"
stage_repo "$WORK/attrs"

status="$(raw_scan "$WORK/attrs" HEAD)"
if [[ "$status" -ne 0 ]]; then
  echo "setup error: the unguarded scan already found the credential (exit $status)" >&2
  cat "$WORK/raw.log" >&2
  exit 1
fi
echo "   ok: unguarded traversal is blind to it (exit $status)"

status="$(target_scan "$WORK/attrs" HEAD)"
if [[ "$status" -eq 0 ]]; then
  echo "FAIL: make gitleaks missed the attribute-hidden credential" >&2
  cat "$WORK/target.log" >&2
  exit 1
fi
echo "   ok: make gitleaks finds it (exit $status)"

echo "== 2. a credential introduced by a merge resolution must be scanned"
init_repo "$WORK/merge"
printf 'base\n' > "$WORK/merge/f.txt"
git -C "$WORK/merge" add -A
git -C "$WORK/merge" commit -qm base
DEFAULT_BRANCH="$(git -C "$WORK/merge" symbolic-ref --short HEAD)"
git -C "$WORK/merge" checkout -qb side
printf 'side\n' > "$WORK/merge/f.txt"
git -C "$WORK/merge" commit -qam side
git -C "$WORK/merge" checkout -q "$DEFAULT_BRANCH"
printf 'main\n' > "$WORK/merge/f.txt"
git -C "$WORK/merge" commit -qam main
set +e
git -C "$WORK/merge" merge side >/dev/null 2>&1
set -e
if [[ -z "$(git -C "$WORK/merge" ls-files -u)" ]]; then
  echo "setup error: expected a merge conflict to resolve" >&2
  exit 1
fi
printf 'aws_key = "%s"\n' "$FAKE_AWS_KEY" > "$WORK/merge/f.txt"
git -C "$WORK/merge" add -A
git -C "$WORK/merge" commit -qm "resolve conflict"
MERGE_SHA="$(git -C "$WORK/merge" rev-parse HEAD)"
if [[ "$(git -C "$WORK/merge" rev-list --parents -n1 "$MERGE_SHA" | wc -w)" -ne 3 ]]; then
  echo "setup error: $MERGE_SHA is not a merge commit" >&2
  exit 1
fi
stage_repo "$WORK/merge"

status="$(raw_scan "$WORK/merge" "$MERGE_SHA")"
if [[ "$status" -ne 0 ]]; then
  echo "setup error: the unguarded scan already found the merge-only credential (exit $status)" >&2
  cat "$WORK/raw.log" >&2
  exit 1
fi
echo "   ok: unguarded traversal is blind to it (exit $status)"

status="$(target_scan "$WORK/merge" "$MERGE_SHA")"
if [[ "$status" -eq 0 ]]; then
  echo "FAIL: make gitleaks missed the credential that only exists in the merge resolution" >&2
  cat "$WORK/target.log" >&2
  exit 1
fi
echo "   ok: make gitleaks finds it (exit $status)"

echo "== 3. content inherited from the base branch must not be reported"
init_repo "$WORK/inherited"
printf 'docs\n' > "$WORK/inherited/docs.txt"
git -C "$WORK/inherited" add -A
git -C "$WORK/inherited" commit -qm docs
INHERIT_DEFAULT="$(git -C "$WORK/inherited" symbolic-ref --short HEAD)"
git -C "$WORK/inherited" branch -q feature
printf 'aws_key = "%s"\n' "$FAKE_AWS_KEY" > "$WORK/inherited/creds.txt"
git -C "$WORK/inherited" add -A
git -C "$WORK/inherited" commit -qm "base branch gains a credential"
INHERIT_BASE="$(git -C "$WORK/inherited" rev-parse HEAD)"
git -C "$WORK/inherited" checkout -q feature
printf 'readme\n' > "$WORK/inherited/readme.txt"
git -C "$WORK/inherited" add -A
git -C "$WORK/inherited" commit -qm readme
git -C "$WORK/inherited" merge -q --no-edit "$INHERIT_DEFAULT"
INHERIT_HEAD="$(git -C "$WORK/inherited" rev-parse HEAD)"
stage_repo "$WORK/inherited"

changed="$(git -C "$WORK/inherited" diff --name-only "$INHERIT_BASE" "$INHERIT_HEAD" | tr '\n' ' ')"
if [[ "$changed" != "readme.txt " ]]; then
  echo "setup error: the PR should only change readme.txt, got: $changed" >&2
  exit 1
fi
status="$(raw_scan "$WORK/inherited" "$INHERIT_BASE")"
if [[ "$status" -eq 0 ]]; then
  echo "setup error: the credential in the base branch is not detectable" >&2
  exit 1
fi
status="$(raw_scan "$WORK/inherited" "$INHERIT_BASE..$INHERIT_HEAD -m")"
if [[ "$status" -eq 0 ]]; then
  echo "setup error: plain -m no longer reports inherited base content; adjust this guard" >&2
  exit 1
fi
echo "   ok: plain -m reports the inherited credential (the false positive being guarded)"

status="$(target_scan "$WORK/inherited" "$INHERIT_BASE..$INHERIT_HEAD")"
if [[ "$status" -ne 0 ]]; then
  echo "FAIL: make gitleaks reported content inherited from the base branch:" >&2
  cat "$WORK/target.log" >&2
  exit 1
fi
echo "   ok: make gitleaks stays clean for the inherited case (exit $status)"

echo "PASS: scan coverage guards hold for hidden diffs, merge resolutions and inherited base content"
