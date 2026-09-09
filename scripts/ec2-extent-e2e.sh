#!/usr/bin/env bash
# EC2 runner: local drive9-server + MinIO + FUSE under coding-agent-extent.
# 1) PR-set fuse-* e2e plus extra fuse-smoke / sqlite / fsx / correctness
# 2) blackbox community.sqlite
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="/usr/local/go/bin:$HOME/go/bin:$ROOT/bin:$PATH"

LOGDIR="${LOGDIR:-$HOME/logs/extent-e2e-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$LOGDIR"
export FUSE_PROFILE="${FUSE_PROFILE:-coding-agent-extent}"
export DRIVE9_S3_BACKEND="${DRIVE9_S3_BACKEND:-minio}"
export DRIVE9_TENANT_PROVIDER="${DRIVE9_TENANT_PROVIDER:-local}"
export DRIVE9_LISTEN_ADDR="${DRIVE9_LISTEN_ADDR:-127.0.0.1:9009}"
export DRIVE9_BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
export RUN_FUSE_SMOKE=1
export FUSE_STRICT_PREREQS=1
export DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API="${DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API:-1}"

echo "LOGDIR=$LOGDIR"
echo "FUSE_PROFILE=$FUSE_PROFILE"
echo "DRIVE9_S3_BACKEND=$DRIVE9_S3_BACKEND"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing $1" >&2; exit 1; }; }
need docker
need python3
need curl
test -x "$ROOT/bin/drive9-server" || { echo "missing bin/drive9-server" >&2; exit 1; }
test -x "$ROOT/bin/drive9" || { echo "missing bin/drive9" >&2; exit 1; }
test -e /dev/fuse || { echo "missing /dev/fuse" >&2; exit 1; }

PASS=0
FAIL=0
run_case() {
  local name="$1"
  local script="$2"
  echo
  echo "=== [$name] $script ==="
  set +e
  DRIVE9_BASE="$DRIVE9_BASE" FUSE_PROFILE="$FUSE_PROFILE" bash "$script" >"$LOGDIR/${name}.log" 2>&1
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "PASS [$name]"
    PASS=$((PASS + 1))
  else
    echo "FAIL [$name] rc=$rc (log $LOGDIR/${name}.log)"
    tail -40 "$LOGDIR/${name}.log" || true
    FAIL=$((FAIL + 1))
    if [ "${ABORT_ON_FIRST_FUSE_FAIL:-0}" = 1 ]; then
      echo "ABORT: first fuse case failed ($name); skipping remaining fuse-* and community.sqlite"
      echo "$PASS $FAIL" >"$LOGDIR/fuse-e2e.summary"
      echo "skipped" >"$LOGDIR/community-sqlite.rc"
      echo "DONE fuse_fail=$FAIL sqlite_rc=skipped logs=$LOGDIR"
      exit 1
    fi
  fi
}

# Start TiDB + server + minio via e2e-local, keep them for extra suites.
echo "=== start local stack ==="
printf '#!/bin/bash\nexit 0\n' >"$LOGDIR/noop.sh"
chmod +x "$LOGDIR/noop.sh"
DRIVE9_S3_BACKEND=minio \
  FUSE_PROFILE="$FUSE_PROFILE" \
  DRIVE9_LOCAL_E2E_SMOKE_SCRIPT="$LOGDIR/noop.sh" \
  bash scripts/e2e-local.sh --keep-server --keep-db --no-build >"$LOGDIR/stack.log" 2>&1 || {
  echo "stack failed"; tail -80 "$LOGDIR/stack.log"; exit 1
}
# Capture leftover work dir / pid from stack log
grep -E 'server pid|preserving|TiDB ready|DRIVE9_S3' "$LOGDIR/stack.log" || true

if ! curl -sf -o /dev/null "http://127.0.0.1:9009/healthz"; then
  echo "server not healthy after stack start"
  tail -80 "$LOGDIR/stack.log"
  exit 1
fi
echo "server healthy"

# fuse-* e2e (skip patch-storage-class: it needs single-blob PATCH, ignores FUSE_PROFILE)
run_case fuse-smoke e2e/fuse-smoke-test.sh
run_case fuse-release-gate e2e/fuse-release-gate.sh
run_case fuse-crash-recovery e2e/fuse-crash-recovery-test.sh
run_case fuse-supervision e2e/fuse-supervision-test.sh
run_case fuse-write-perf-budget e2e/fuse-write-perf-budget-test.sh
run_case fuse-correctness-workload e2e/fuse-correctness-workload.sh
run_case fuse-concurrency-stress e2e/fuse-concurrency-stress.sh
run_case fuse-posix-fsx-gate e2e/fuse-posix-fsx-gate.sh
run_case fuse-sqlite-correctness e2e/fuse-sqlite-correctness.sh
run_case fuse-sqlite-commit-sequence e2e/fuse-sqlite-commit-sequence.sh
run_case posix-permission e2e/posix-permission-smoke-test.sh

echo
echo "FUSE_E2E_RESULT pass=$PASS fail=$FAIL"
echo "$PASS $FAIL" >"$LOGDIR/fuse-e2e.summary"
if [ "${SKIP_SQLITE:-0}" = 1 ]; then
  echo "SKIP: community.sqlite (SKIP_SQLITE=1)"
  echo "skipped" >"$LOGDIR/community-sqlite.rc"
  echo "DONE fuse_fail=$FAIL sqlite_rc=skipped logs=$LOGDIR"
  exit "$FAIL"
fi
if [ "$FAIL" -ne 0 ]; then
  echo "ABORT: fuse e2e failures; skipping community.sqlite"
  echo "skipped" >"$LOGDIR/community-sqlite.rc"
  echo "DONE fuse_fail=$FAIL sqlite_rc=skipped logs=$LOGDIR"
  exit 1
fi

echo "=== community.sqlite ==="
set +e
FUSE_PROFILE="$FUSE_PROFILE" \
  DRIVE9_S3_BACKEND=minio \
  python3 blackbox/run.py --module community.sqlite --server-mode local \
    --local-server "$ROOT/bin/drive9-server" \
    --bin "$ROOT/bin/drive9" \
    --work-dir "$LOGDIR/blackbox-sqlite" \
    >"$LOGDIR/community-sqlite.log" 2>&1
SQLITE_RC=$?
set -e
if [ "$SQLITE_RC" -eq 0 ]; then
  echo "PASS [community.sqlite]"
else
  echo "FAIL [community.sqlite] rc=$SQLITE_RC"
  tail -80 "$LOGDIR/community-sqlite.log" || true
fi
echo "$SQLITE_RC" >"$LOGDIR/community-sqlite.rc"
echo "DONE fuse_fail=$FAIL sqlite_rc=$SQLITE_RC logs=$LOGDIR"
test "$FAIL" -eq 0 && test "$SQLITE_RC" -eq 0
