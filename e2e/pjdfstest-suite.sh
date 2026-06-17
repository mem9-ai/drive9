#!/usr/bin/env bash
# pjdfstest-based POSIX compatibility suite for Drive9 FUSE mounts.
#
# Supports Linux FUSE and macOS macFUSE/FUSE-T. The heavy lifting lives in
# _feature-matrix-runner.sh so the Markdown report format stays shared with
# the existing POSIX feature matrix entrypoint.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FEATURE_MATRIX_SUITE=posix
exec "$SCRIPT_DIR/_feature-matrix-runner.sh" "$@"
