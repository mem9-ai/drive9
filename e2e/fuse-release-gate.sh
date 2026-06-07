#!/usr/bin/env bash
# Strict FUSE release gate. Unlike the general smoke script, this fails when
# host FUSE prerequisites are unavailable and enables git/remount/log checks.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-1}"
export RUN_FUSE_GIT_CLONE="${RUN_FUSE_GIT_CLONE:-1}"
export RUN_FUSE_UMOUNT_DURABLE="${RUN_FUSE_UMOUNT_DURABLE:-1}"
export RUN_FUSE_LOG_AUDIT="${RUN_FUSE_LOG_AUDIT:-1}"
export RUN_FUSE_CONCURRENCY_STRESS="${RUN_FUSE_CONCURRENCY_STRESS:-0}"
export RUN_FUSE_SQLITE_CORRECTNESS="${RUN_FUSE_SQLITE_CORRECTNESS:-1}"
export RUN_FUSE_PERFORMANCE_BASELINE="${RUN_FUSE_PERFORMANCE_BASELINE:-0}"
export FUSE_GIT_CLONE_URL="${FUSE_GIT_CLONE_URL:-https://github.com/octocat/Hello-World.git}"
export FUSE_GIT_CLONE_TIMEOUT_S="${FUSE_GIT_CLONE_TIMEOUT_S:-180}"
export FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"

bash "$SCRIPT_DIR/fuse-smoke-test.sh"
bash "$SCRIPT_DIR/fuse-correctness-workload.sh"
if [ "$RUN_FUSE_SQLITE_CORRECTNESS" = "1" ]; then
  bash "$SCRIPT_DIR/fuse-sqlite-correctness.sh"
fi
if [ "$RUN_FUSE_CONCURRENCY_STRESS" = "1" ]; then
  bash "$SCRIPT_DIR/fuse-concurrency-stress.sh"
fi
if [ "$RUN_FUSE_PERFORMANCE_BASELINE" = "1" ]; then
  bash "$SCRIPT_DIR/fuse-performance-baseline.sh"
fi
