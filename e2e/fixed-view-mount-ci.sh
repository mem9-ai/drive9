#!/usr/bin/env bash
# The paired server CI owns the disposable server/database lifecycle.
# Writable acceptance requires a bounded-listing S3 provider and central quota;
# the filesystem-only mock cannot initialize retained usage.
set -euo pipefail
cli_bin="$(mktemp /tmp/drive9-fixed-cli.XXXXXX)"
trap 'rm -f "$cli_bin"' EXIT
make build-cli CLI_BIN="$cli_bin"
python3 "$(dirname "$0")/fixed-view-mount-smoke.py" --cli "$cli_bin" --write-merge
