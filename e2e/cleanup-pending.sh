#!/usr/bin/env bash
# Retry cleanup registries left by interrupted Drive9 e2e runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/cleanup-helper.sh"

DRIVE9_E2E_CLEANUP="${DRIVE9_E2E_CLEANUP:-always}"
export DRIVE9_E2E_CLEANUP

drive9_cleanup_run_pending
