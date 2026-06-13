#!/usr/bin/env bash
# pjdfstest-based POSIX compatibility suite for Drive9 FUSE mounts.
#
# Supports Linux FUSE and macOS macFUSE/FUSE-T. The heavy lifting lives in
# _feature-matrix-runner.sh so the Markdown report format stays shared with
# the existing POSIX feature matrix entrypoint.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$(uname -s)" = "Darwin" ] && ! command -v mount_macfuse >/dev/null 2>&1 && ! command -v mount_fusefs >/dev/null 2>&1; then
  for macfuse_dir in "/Library/Filesystems/macfuse.fs/Contents/Resources" "/usr/local/bin" "/opt/homebrew/bin"; do
    if [ -x "$macfuse_dir/mount_macfuse" ] || [ -x "$macfuse_dir/mount_fusefs" ]; then
      PATH="$macfuse_dir:$PATH"
      export PATH
      break
    fi
  done
fi

export FEATURE_MATRIX_SUITE=posix
exec "$SCRIPT_DIR/_feature-matrix-runner.sh" "$@"
