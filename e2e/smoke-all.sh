#!/usr/bin/env bash
# Run all dat9 smoke tests (API + CLI + FUSE).

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"

PASS=0
FAIL=0

run_case() {
  local name="$1"
  local script="$2"

  echo
  echo "=== [$name] $script ==="
  set +e
  DRIVE9_BASE="$BASE" bash "$script"
  local rc=$?
  set -e

  if [ "$rc" -eq 0 ]; then
    echo "PASS [$name]"
    PASS=$((PASS + 1))
  else
    echo "FAIL [$name] (rc=$rc)"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== dat9 smoke-all ==="
echo "BASE=$BASE"

run_case "api" "e2e/api-smoke-test.sh"
run_case "cli" "e2e/cli-smoke-test.sh"
run_case "fuse" "e2e/fuse-smoke-test.sh"

if [ -n "${TIDB_ZERO_INSTANCE_ID:-}" ]; then
  run_case "tidbcloud-native" "e2e/tidbcloud-native-smoke-test.sh"
else
  echo "\nSKIP [tidbcloud-native] TIDB_ZERO_INSTANCE_ID not set"
fi

echo
echo "=== smoke-all result ==="
echo "PASS=$PASS FAIL=$FAIL"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
