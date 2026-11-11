#!/usr/bin/env bash
# Paired v10 server CI owns the loopback service/database lifecycle.
set -euo pipefail
python3 "$(dirname "$0")/fixed-view-smoke.py" --provision-local
