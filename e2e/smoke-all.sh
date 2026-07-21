#!/usr/bin/env bash
# Run all drive9 smoke tests (API + CLI + journal + layer FS + FUSE).
#
# Tenant mode:
#  - Fresh (default): each suite provisions its own tenant.
#  - Existing (DRIVE9_API_KEY set in the environment): every suite that honors
#    DRIVE9_API_KEY (api, cli, journal, layer-fs, fuse, posix-permission, git
#    suites, portable pack) skips provision and reuses the tenant the key
#    belongs to. The key is explicitly re-exported below so a future `env -u`
#    in run_case cannot silently drop it.
#
# Subset knobs:
#  - RUN_API_ONLY=1 runs only api + cli (the core two). Useful for a quick
#    existing-tenant regression without pulling in journal/layer-fs/fuse.
#  - RUN_FUSE_SMOKE=0 skips the FUSE suite (and derives RUN_LAYER_FUSE_SMOKE
#    from it). macOS WebDAV fallback cannot satisfy symlink/hardlink asserts.
#  - RUN_GIT_OPS_SMOKE=1 / RUN_GIT_WORKSPACE_SMOKE=1 / RUN_PORTABLE_PACK_E2E=1
#    opt into the heavier optional suites.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
# Re-export explicitly so the existing-tenant intent is durable across run_case
# subprocesses; do not rely on implicit inheritance.
if [ -n "${DRIVE9_API_KEY:-}" ]; then
  export DRIVE9_API_KEY
fi
RUN_API_ONLY="${RUN_API_ONLY:-0}"
RUN_GIT_OPS_SMOKE="${RUN_GIT_OPS_SMOKE:-0}"
RUN_GIT_WORKSPACE_SMOKE="${RUN_GIT_WORKSPACE_SMOKE:-0}"
RUN_FUSE_SMOKE="${RUN_FUSE_SMOKE:-1}"
RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-$RUN_FUSE_SMOKE}"
export RUN_LAYER_FUSE_SMOKE
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

skip_case() {
  local name="$1"
  local script="$2"
  local reason="$3"
  echo
  echo "=== [$name] $script ==="
  echo "SKIP [$name] $reason"
}

if [ -n "${DRIVE9_API_KEY:-}" ]; then
  TENANT_MODE="existing (DRIVE9_API_KEY)"
else
  TENANT_MODE="fresh provision"
fi

echo "=== drive9 smoke-all ==="
echo "BASE=$BASE"
echo "Tenant=$TENANT_MODE"
if [ "$RUN_API_ONLY" = "1" ]; then
  echo "RUN_API_ONLY=1 (core two suites only)"
fi

run_case "api" "e2e/api-smoke-test.sh"
run_case "cli" "e2e/cli-smoke-test.sh"

if [ "$RUN_API_ONLY" = "1" ]; then
  skip_case "journal" "e2e/journal-smoke-test.sh" "set RUN_API_ONLY=0 to run journal coverage"
  skip_case "layer-fs" "e2e/layer-fs-smoke-test.sh" "set RUN_API_ONLY=0 to run layer-fs coverage"
  skip_case "fuse" "e2e/fuse-smoke-test.sh" "set RUN_API_ONLY=0 to run FUSE coverage"
  skip_case "posix-permission" "e2e/posix-permission-smoke-test.sh" "set RUN_API_ONLY=0 to run posix-permission coverage"
  skip_case "git-ops" "e2e/git-ops-smoke-test.sh" "set RUN_API_ONLY=0 to run Git ops coverage"
  skip_case "git-workspace" "e2e/git-workspace-smoke-test.sh" "set RUN_API_ONLY=0 to run Git workspace coverage"
  skip_case "portable-pack-unpack" "e2e/portable-pack-unpack-e2e.sh" "set RUN_API_ONLY=0 to run portable pack/unpack coverage"
else
  run_case "journal" "e2e/journal-smoke-test.sh"
  run_case "layer-fs" "e2e/layer-fs-smoke-test.sh"
  if [ "$RUN_FUSE_SMOKE" = "1" ]; then
    run_case "fuse" "e2e/fuse-smoke-test.sh"
  else
    skip_case "fuse" "e2e/fuse-smoke-test.sh" "set RUN_FUSE_SMOKE=1 to run FUSE symlink/hardlink coverage"
  fi
  run_case "posix-permission" "e2e/posix-permission-smoke-test.sh"
  if [ "$RUN_GIT_OPS_SMOKE" = "1" ]; then
    run_case "git-ops" "e2e/git-ops-smoke-test.sh"
  else
    skip_case "git-ops" "e2e/git-ops-smoke-test.sh" "set RUN_GIT_OPS_SMOKE=1 to run lightweight Git clone/status/restore coverage"
  fi
  if [ "$RUN_GIT_WORKSPACE_SMOKE" = "1" ]; then
    run_case "git-workspace" "e2e/git-workspace-smoke-test.sh"
  else
    skip_case "git-workspace" "e2e/git-workspace-smoke-test.sh" "set RUN_GIT_WORKSPACE_SMOKE=1 to run fast-clone Git workspace coverage"
  fi
  if [ "$RUN_PORTABLE_PACK_E2E" = "1" ]; then
    run_case "portable-pack-unpack" "e2e/portable-pack-unpack-e2e.sh"
  else
    skip_case "portable-pack-unpack" "e2e/portable-pack-unpack-e2e.sh" "set RUN_PORTABLE_PACK_E2E=1 to run portable profile pack/unpack coverage"
  fi
fi

echo
echo "=== smoke-all result ==="
echo "PASS=$PASS FAIL=$FAIL"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
