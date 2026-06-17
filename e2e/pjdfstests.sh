#!/usr/bin/env bash
# Compatibility alias for the pjdfstest POSIX suite.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/pjdfstest-suite.sh" "$@"
