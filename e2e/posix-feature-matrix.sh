#!/usr/bin/env bash
# POSIX-only entrypoint for the Drive9 feature matrix E2E.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FEATURE_MATRIX_SUITE=posix exec "$SCRIPT_DIR/_feature-matrix-runner.sh" "$@"
