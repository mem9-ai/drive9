#!/usr/bin/env bash
# Manual diagnostic only; the paired server CI owns disposable services.
set -euo pipefail
: "${CLONE_BENCH_OUTPUT:?explicit report path required}"
cli_bin="$(mktemp /tmp/drive9-clone-cli.XXXXXX)"
trap 'rm -f "$cli_bin"' EXIT
make build-cli CLI_BIN="$cli_bin"
benchmark_args=(--cli "$cli_bin" --output "$CLONE_BENCH_OUTPUT")
case "${CLONE_BENCH_DIAGNOSE:-0}" in
  0) ;;
  1) benchmark_args+=(--diagnose-clone) ;;
  *) echo 'CLONE_BENCH_DIAGNOSE must be 0 or 1' >&2; exit 1 ;;
esac
python3 e2e/clone-view-benchmark.py "${benchmark_args[@]}"
