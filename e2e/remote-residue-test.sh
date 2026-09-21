#!/usr/bin/env bash
# Deterministic regression for e2e/remote-residue.sh verdicts (issue #936
# cleanup acceptance). Covers the three outcomes the residue contract must
# distinguish: still exists, explicitly not found (deleted), and transient or
# persistent non-404 errors that must never count as deletion. Runs in
# milliseconds with no network, no CLI binary, and no FUSE mount.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=remote-residue.sh
source "$SCRIPT_DIR/remote-residue.sh"

REQUEST_MAX_RETRIES=3
REQUEST_RETRY_SLEEP_S=0
export REQUEST_MAX_RETRIES REQUEST_RETRY_SLEEP_S

PASS=0
FAIL=0

report() {
  local desc="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  want verdict: $want"
    echo "  got verdict:  $got"
    FAIL=$((FAIL + 1))
  fi
}

# Mock drive9: each stat call consumes the next scripted outcome; once the
# script is exhausted the first outcome ("exists") repeats. The call
# counter lives in a state file because remote_root_not_found captures the
# command output in a subshell, where in-memory counters would not survive.
# Outcomes: exists, notfound (HTTP 404 text), notfound-text (plain "not
# found" wording), err500.
MOCK_OUTCOMES=(exists)
MOCK_STATE="$(mktemp)"
trap 'rm -f "$MOCK_STATE"' EXIT

mock_call_count() {
  cat "$MOCK_STATE" 2>/dev/null || echo 0
}

drive9() {
  if [ "${1:-}" != "fs" ] || [ "${2:-}" != "stat" ]; then
    echo "unexpected drive9 invocation: $*" >&2
    return 99
  fi
  local idx outcome
  idx="$(mock_call_count)"
  echo $((idx + 1)) > "$MOCK_STATE"
  outcome="${MOCK_OUTCOMES[$idx]:-exists}"
  case "$outcome" in
    exists)
      echo "size: 27"
      return 0
      ;;
    notfound)
      echo "fs stat: HTTP 404" >&2
      return 1
      ;;
    notfound-text)
      echo "fs stat: /x: not found" >&2
      return 1
      ;;
    err500)
      echo "fs stat: HTTP 500: backend unavailable" >&2
      return 1
      ;;
  esac
  echo "unknown scripted outcome: $outcome" >&2
  return 99
}

# run_verdict records the verdict and leaves the stat call count in the mock
# state file for the caller.
VERDICT=""

run_verdict() {
  echo 0 > "$MOCK_STATE"
  if remote_root_not_found "/fixture-root" >/dev/null 2>&1; then
    VERDICT=deleted
  else
    VERDICT=residue
  fi
}

MOCK_OUTCOMES=(exists)
run_verdict
report "resolvable root is residue" residue "$VERDICT"
report "resolvable root uses a single stat call" 1 "$(mock_call_count)"

MOCK_OUTCOMES=(notfound)
run_verdict
report "HTTP 404 counts as deleted" deleted "$VERDICT"

MOCK_OUTCOMES=(notfound-text)
run_verdict
report "explicit not-found text counts as deleted" deleted "$VERDICT"

MOCK_OUTCOMES=(err500 err500 err500 err500)
run_verdict
report "persistent HTTP 500 fails the residue check" residue "$VERDICT"
report "persistent HTTP 500 exhausts bounded retries" 3 "$(mock_call_count)"

MOCK_OUTCOMES=(err500 err500 notfound)
run_verdict
report "transient HTTP 500 then 404 counts as deleted" deleted "$VERDICT"
report "transient recovery consumed the scripted outcomes" 3 "$(mock_call_count)"

echo "RESULT: $PASS/$((PASS + FAIL)) passed, $FAIL failed"
exit "$FAIL"
