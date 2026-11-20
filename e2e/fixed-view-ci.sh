#!/usr/bin/env bash
# Paired v10 server CI owns the loopback service/database lifecycle.
# Require supported S3 retained accounting and the complete writable mount flow.
set -euo pipefail
python3 "$(dirname "$0")/fixed-view-smoke.py" --provision-local
cli_bin="$(mktemp /tmp/drive9-fixed-cli.XXXXXX)"
trap 'rm -f "$cli_bin"' EXIT
make build-cli CLI_BIN="$cli_bin"
python3 "$(dirname "$0")/fixed-view-mount-smoke.py" --cli "$cli_bin" --write-merge
