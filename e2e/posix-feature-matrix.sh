#!/usr/bin/env bash
# Backward-compatible POSIX-only entrypoint. Prefer pjdfstest-suite.sh when
# explicitly running the pjdfstest POSIX compatibility suite.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/pjdfstest-suite.sh" "$@"
