#!/usr/bin/env bash
# Run all drive9 smoke tests (API + CLI + journal + FUSE).

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
RUN_GIT_WORKSPACE_SMOKE="${RUN_GIT_WORKSPACE_SMOKE:-0}"
RUN_PORTABLE_PACK_E2E="${RUN_PORTABLE_PACK_E2E:-0}"

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

echo "=== drive9 smoke-all ==="
echo "BASE=$BASE"

run_case "api" "e2e/api-smoke-test.sh"
run_case "cli" "e2e/cli-smoke-test.sh"
run_case "journal" "e2e/journal-smoke-test.sh"
run_case "fuse" "e2e/fuse-smoke-test.sh"
run_case "posix-permission" "e2e/posix-permission-smoke-test.sh"
if [ "$RUN_GIT_WORKSPACE_SMOKE" = "1" ]; then
  run_case "git-workspace" "e2e/git-workspace-smoke-test.sh"
else
  echo
  echo "=== [git-workspace] e2e/git-workspace-smoke-test.sh ==="
  echo "SKIP [git-workspace] set RUN_GIT_WORKSPACE_SMOKE=1 to run fast-clone Git workspace coverage"
fi
if [ "$RUN_PORTABLE_PACK_E2E" = "1" ]; then
  run_case "portable-pack-unpack" "e2e/portable-pack-unpack-e2e.sh"
else
  echo
  echo "=== [portable-pack-unpack] e2e/portable-pack-unpack-e2e.sh ==="
  echo "SKIP [portable-pack-unpack] set RUN_PORTABLE_PACK_E2E=1 to run portable profile pack/unpack coverage"
fi

echo
echo "=== smoke-all result ==="
echo "PASS=$PASS FAIL=$FAIL"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
