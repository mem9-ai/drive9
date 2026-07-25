#!/usr/bin/env bash
# Run attachable e2e smokes against DRIVE9_BASE (intended for orb arch + local-shared).
# Skips self-contained stacks: shared-smoke, description-smoke, local-smoke, native.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

export DRIVE9_BASE="${DRIVE9_BASE:-http://host.orb.internal:9009}"
export DRIVE9_TIDBCLOUD_PUBLIC_KEY="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-local}"
export DRIVE9_TIDBCLOUD_PRIVATE_KEY="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-local}"
export RUN_SQL_CHECKS=0
export RUN_SEMANTIC_CHECKS=0
export RUN_CLI_FORK_CHECKS=0
export RUN_CLI_SEMANTIC_CHECKS=0
export FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
export CLI_SOURCE="${CLI_SOURCE:-build}"
export RUN_FUSE_GIT_CLONE="${RUN_FUSE_GIT_CLONE:-0}"
export RUN_FUSE_UMOUNT_DURABLE="${RUN_FUSE_UMOUNT_DURABLE:-0}"
export RUN_FUSE_LOG_AUDIT="${RUN_FUSE_LOG_AUDIT:-0}"
export RUN_LARGE_FILE="${RUN_LARGE_FILE:-0}"
export PATH="/usr/sbin:/usr/bin:/bin:${PATH:-}"

if [ -z "${HOME_ISOLATED:-}" ]; then
  export HOME
  HOME="$(mktemp -d /tmp/drive9-e2e-home.XXXXXX)"
  export HOME_ISOLATED=1
fi
mkdir -p "$HOME"

REPORT_DIR="$(mktemp -d /tmp/drive9-e2e-orb-batch.XXXXXX)"
SUMMARY="$REPORT_DIR/summary.txt"
: >"$SUMMARY"
echo "$REPORT_DIR" >/tmp/drive9-e2e-orb-batch-dir.txt
echo "$REPORT_DIR" >"$REPO/.e2e-orb-batch-dir" 2>/dev/null || true
: >"$REPO/.e2e-orb-summary.txt" 2>/dev/null || true

echo "=== orb/arch e2e batch (shared backend) ==="
echo "BASE=$DRIVE9_BASE"
echo "HOME=$HOME"
echo "REPORT=$REPORT_DIR"
uname -a
curl -sf "$DRIVE9_BASE/healthz"
echo

if ! file bin/drive9 2>/dev/null | grep -q "ELF.*aarch64"; then
  mkdir -p bin
  CGO_ENABLED=0 go build -o bin/drive9 ./cmd/drive9
fi
file bin/drive9 | head -1

run_one() {
  local name="$1"
  shift
  local log="$REPORT_DIR/${name}.log"
  echo ""
  echo "======== START $name $(date -u +%H:%M:%S) ========"
  local start end rc
  start=$(date +%s)
  set +e
  ( "$@" ) >"$log" 2>&1
  rc=$?
  set -e
  end=$(date +%s)
  local dur=$((end - start))
  if [ "$rc" -eq 0 ]; then
    echo "======== PASS $name (${dur}s) ========"
    echo "PASS $name ${dur}s" | tee -a "$SUMMARY" | tee -a "$REPO/.e2e-orb-summary.txt" >/dev/null
    echo "PASS $name ${dur}s"
  else
    echo "======== FAIL $name rc=$rc (${dur}s) ========"
    echo "FAIL $name rc=$rc ${dur}s" | tee -a "$SUMMARY" | tee -a "$REPO/.e2e-orb-summary.txt" >/dev/null
    echo "FAIL $name rc=$rc ${dur}s"
    grep -E 'FAIL|ERROR|invalid API|mount mode:|RESULT:|webdav|panic|connection refused' "$log" | tail -n 18 | sed 's/^/  | /' || true
    tail -n 8 "$log" | sed 's/^/  | /'
  fi
}

# Phase 1
run_one api-smoke bash e2e/api-smoke-test.sh
run_one cli-smoke bash e2e/cli-smoke-test.sh
run_one journal bash e2e/journal-smoke-test.sh
run_one posix-permission bash e2e/posix-permission-smoke-test.sh
run_one layer-fs bash e2e/layer-fs-smoke-test.sh
run_one pack bash e2e/pack-smoke-test.sh

# Phase 2 FUSE
run_one fuse-smoke bash e2e/fuse-smoke-test.sh
run_one fuse-correctness bash e2e/fuse-correctness-workload.sh
run_one fuse-posix-fsx bash e2e/fuse-posix-fsx-gate.sh
run_one fuse-sqlite bash e2e/fuse-sqlite-correctness.sh
run_one fuse-concurrency bash e2e/fuse-concurrency-stress.sh
run_one fuse-crash bash e2e/fuse-crash-recovery-test.sh
run_one fuse-write-perf bash e2e/fuse-write-perf-budget-test.sh
run_one fuse-perf-baseline bash e2e/fuse-performance-baseline.sh

# Phase 3 git
export GIT_OPS_CLONE_MODES=fast
export GIT_OPS_PROFILES=coding-agent
run_one git-ops bash e2e/git-ops-smoke-test.sh
export GIT_WORKSPACE_SCENARIOS=fast_worktree
export GIT_WORKSPACE_REPOS=hello=https://github.com/octocat/Hello-World.git
export GIT_WORKSPACE_EXISTING_FILES=5
export GIT_WORKSPACE_NEW_FILES=5
export GIT_WORKSPACE_PATCH_FILES=5
run_one git-workspace bash e2e/git-workspace-smoke-test.sh
run_one git-feature bash e2e/git-feature-smoke-test.sh

# Phase 4 release-gate (core pieces only; components already covered above)
export FUSE_STRICT_PREREQS=1
export RUN_FUSE_GIT_CLONE=0
export RUN_FUSE_SQLITE_CORRECTNESS=0
export RUN_FUSE_CONCURRENCY_STRESS=0
export RUN_FUSE_POSIX_FSX=0
export RUN_FUSE_PERFORMANCE_BASELINE=0
run_one fuse-release-gate bash e2e/fuse-release-gate.sh

echo ""
echo "=== FINAL ==="
cat "$SUMMARY"
pass=$(grep -c '^PASS ' "$SUMMARY" || true)
fail=$(grep -c '^FAIL ' "$SUMMARY" || true)
echo "TOTAL pass=$pass fail=$fail"
echo "REPORT_DIR=$REPORT_DIR"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
