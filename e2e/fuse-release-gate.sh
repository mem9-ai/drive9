#!/usr/bin/env bash
# Strict FUSE release gate. Unlike the general smoke script, this fails when
# host FUSE prerequisites are unavailable and enables git/remount/log checks.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-1}"
export RUN_FUSE_GIT_CLONE="${RUN_FUSE_GIT_CLONE:-1}"
export RUN_FUSE_UMOUNT_DURABLE="${RUN_FUSE_UMOUNT_DURABLE:-1}"
export RUN_FUSE_LOG_AUDIT="${RUN_FUSE_LOG_AUDIT:-1}"
export FUSE_GIT_CLONE_URL="${FUSE_GIT_CLONE_URL:-https://github.com/octocat/Hello-World.git}"
export FUSE_GIT_CLONE_TIMEOUT_S="${FUSE_GIT_CLONE_TIMEOUT_S:-180}"
export FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"

bash "$SCRIPT_DIR/fuse-smoke-test.sh"
bash "$SCRIPT_DIR/fuse-correctness-workload.sh"
bash "$SCRIPT_DIR/fuse-concurrency-stress.sh"
