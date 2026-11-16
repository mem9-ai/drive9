#!/usr/bin/env bash
# Manual diagnostic only; the paired server CI owns disposable services.
set -euo pipefail
: "${CLONE_BENCH_OUTPUT:?explicit report path required}"
cli_bin="$(mktemp /tmp/drive9-clone-cli.XXXXXX)"
trap 'rm -f "$cli_bin"' EXIT
make build-cli CLI_BIN="$cli_bin"
python3 e2e/clone-view-benchmark.py --cli "$cli_bin" --output "$CLONE_BENCH_OUTPUT"
