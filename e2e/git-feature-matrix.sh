#!/usr/bin/env bash
# Git-only entrypoint for the Drive9 feature matrix E2E.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FEATURE_MATRIX_SUITE=git exec "$SCRIPT_DIR/_feature-matrix-runner.sh" "$@"
