#!/usr/bin/env bash
# Run the local-e2e.yml PR-gate suites against a live drive9-server.
#
# Tenant mode:
#  - Fresh (default): each suite provisions its own tenant.
#  - Existing (DRIVE9_API_KEY set): suites that honor the key skip provision
#    and reuse that tenant. The key is re-exported so run_case cannot drop it.
#
# Default set matches `.github/workflows/local-e2e.yml` PR steps:
#  api, cli, object-store, layer-fs, fuse-release-gate, fuse-patch-storage-class,
#  git-ops, git-workspace-ondemand, pack, fuse-crash-recovery, fuse-supervision,
#  fuse-write-perf-budget.
#
# Subset knobs:
#  - RUN_API_ONLY=1 — api + cli only.
#  - RUN_FUSE_SMOKE=0 — skip FUSE-related suites (release-gate, patch,
#    crash-recovery, supervision, write-perf, git-ops, git-ondemand) and
#    layer-fs FUSE restore. macOS WebDAV cannot satisfy those asserts.
#  - RUN_JOURNAL_SMOKE=1 / RUN_POSIX_SMOKE=1 / RUN_GIT_WORKSPACE_SMOKE=1 —
#    extras used by post-merge local-e2e, not part of the PR default.
#  - RUN_TOKENS_SMOKE=1 / RUN_SSE_SMOKE=1 — HTTP tokens + SSE retention
#    extras; off by default (not even post-merge). Enable from an integrator
#    that points DRIVE9_SERVER_BIN at a server with those surfaces.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
if [ -n "${DRIVE9_API_KEY:-}" ]; then
  export DRIVE9_API_KEY
fi
RUN_API_ONLY="${RUN_API_ONLY:-0}"
RUN_FUSE_SMOKE="${RUN_FUSE_SMOKE:-1}"
RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-$RUN_FUSE_SMOKE}"
export RUN_LAYER_FUSE_SMOKE
RUN_OBJECT_STORE_SMOKE="${RUN_OBJECT_STORE_SMOKE:-1}"
RUN_JOURNAL_SMOKE="${RUN_JOURNAL_SMOKE:-0}"
RUN_POSIX_SMOKE="${RUN_POSIX_SMOKE:-0}"
RUN_GIT_WORKSPACE_SMOKE="${RUN_GIT_WORKSPACE_SMOKE:-0}"
RUN_TOKENS_SMOKE="${RUN_TOKENS_SMOKE:-0}"
RUN_SSE_SMOKE="${RUN_SSE_SMOKE:-0}"

if [ "$RUN_FUSE_SMOKE" = "1" ]; then
  export FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-1}"
  export LAYER_FUSE_STRICT_PREREQS="${LAYER_FUSE_STRICT_PREREQS:-1}"
  export RUN_FUSE_CONCURRENCY_STRESS="${RUN_FUSE_CONCURRENCY_STRESS:-0}"
  export RUN_FUSE_POSIX_FSX="${RUN_FUSE_POSIX_FSX:-0}"
fi

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

run_fuse_case() {
  local name="$1"
  local script="$2"
  if [ "$RUN_FUSE_SMOKE" = "1" ]; then
    run_case "$name" "$script"
  else
    skip_case "$name" "$script" "set RUN_FUSE_SMOKE=1 to run FUSE coverage"
  fi
}

if [ -n "${DRIVE9_API_KEY:-}" ]; then
  TENANT_MODE="existing (DRIVE9_API_KEY)"
else
  TENANT_MODE="fresh provision"
fi

echo "=== drive9 smoke-all ==="
echo "BASE=$BASE"
echo "Tenant=$TENANT_MODE"
echo "RUN_API_ONLY=$RUN_API_ONLY RUN_FUSE_SMOKE=$RUN_FUSE_SMOKE"

run_case "api" "e2e/api-smoke-test.sh"
run_case "cli" "e2e/cli-smoke-test.sh"
if [ "$RUN_OBJECT_STORE_SMOKE" = "1" ]; then
  run_case "object-store" "e2e/object-store-smoke-test.sh"
else
  skip_case "object-store" "e2e/object-store-smoke-test.sh" "set RUN_OBJECT_STORE_SMOKE=1 to run MinIO fs/mount coverage"
fi

if [ "$RUN_API_ONLY" = "1" ]; then
  skip_case "layer-fs" "e2e/layer-fs-smoke-test.sh" "set RUN_API_ONLY=0"
  skip_case "fuse-release-gate" "e2e/fuse-release-gate.sh" "set RUN_API_ONLY=0"
  skip_case "fuse-patch-storage-class" "e2e/fuse-patch-storage-class.sh" "set RUN_API_ONLY=0"
  skip_case "git-ops" "e2e/git-ops-smoke-test.sh" "set RUN_API_ONLY=0"
  skip_case "git-workspace-ondemand" "e2e/git-workspace-ondemand-smoke-test.sh" "set RUN_API_ONLY=0"
  skip_case "pack" "e2e/pack-smoke-test.sh" "set RUN_API_ONLY=0"
  skip_case "fuse-crash-recovery" "e2e/fuse-crash-recovery-test.sh" "set RUN_API_ONLY=0"
  skip_case "fuse-supervision" "e2e/fuse-supervision-test.sh" "set RUN_API_ONLY=0"
  skip_case "fuse-write-perf-budget" "e2e/fuse-write-perf-budget-test.sh" "set RUN_API_ONLY=0"
else
  run_case "layer-fs" "e2e/layer-fs-smoke-test.sh"
  run_fuse_case "fuse-release-gate" "e2e/fuse-release-gate.sh"
  run_fuse_case "fuse-patch-storage-class" "e2e/fuse-patch-storage-class.sh"
  run_fuse_case "git-ops" "e2e/git-ops-smoke-test.sh"
  run_fuse_case "git-workspace-ondemand" "e2e/git-workspace-ondemand-smoke-test.sh"
  run_case "pack" "e2e/pack-smoke-test.sh"
  run_fuse_case "fuse-crash-recovery" "e2e/fuse-crash-recovery-test.sh"
  run_fuse_case "fuse-supervision" "e2e/fuse-supervision-test.sh"
  run_fuse_case "fuse-write-perf-budget" "e2e/fuse-write-perf-budget-test.sh"
fi

if [ "$RUN_JOURNAL_SMOKE" = "1" ]; then
  run_case "journal" "e2e/journal-smoke-test.sh"
fi
if [ "$RUN_POSIX_SMOKE" = "1" ]; then
  run_case "posix-permission" "e2e/posix-permission-smoke-test.sh"
fi
if [ "$RUN_GIT_WORKSPACE_SMOKE" = "1" ]; then
  run_fuse_case "git-workspace" "e2e/git-workspace-smoke-test.sh"
fi
if [ "$RUN_TOKENS_SMOKE" = "1" ]; then
  run_case "tokens" "e2e/tokens-smoke-test.sh"
fi
if [ "$RUN_SSE_SMOKE" = "1" ]; then
  run_case "sse-retention" "e2e/sse-retention-smoke-test.sh"
fi

echo
echo "RESULT: $PASS passed, $FAIL failed"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
