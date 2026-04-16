#!/usr/bin/env bash
# Adapt e2e/fuse-smoke-test.sh for drive9-server-local single-tenant mode.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-local-dev-key}"
SOURCE_SCRIPT="$SCRIPT_DIR/fuse-smoke-test.sh"
PATCHED_SCRIPT="$(mktemp)"

cleanup() {
  rm -f "$PATCHED_SCRIPT"
}
trap cleanup EXIT

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

health_body="$(mktemp)"
health_code="$(curl -sS -o "$health_body" -w "%{http_code}" "$BASE/healthz" || true)"
if [ "$health_code" != "200" ]; then
  echo "drive9-server-local health check failed: base=$BASE code=$health_code" >&2
  if [ -s "$health_body" ]; then
    echo "healthz body:" >&2
    cat "$health_body" >&2
  fi
  rm -f "$health_body"
  exit 1
fi
rm -f "$health_body"

if [ ! -f "$SOURCE_SCRIPT" ]; then
  echo "source smoke script not found: $SOURCE_SCRIPT" >&2
  exit 1
fi

python3 - "$SOURCE_SCRIPT" "$PATCHED_SCRIPT" <<'PY'
from pathlib import Path
import sys

source_path = Path(sys.argv[1])
patched_path = Path(sys.argv[2])
source = source_path.read_text()

old = """echo "[1] provision tenant"
if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  state=$(printf '%s' "$sbody" | jq -r '.status // empty')
  echo "status=${scode}:${state}"
  if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$state" "active"
"""

new = """echo "[1] local single-tenant mode"
API_KEY="$DRIVE9_API_KEY"
check_cmd "DRIVE9_API_KEY is non-empty" test -n "$API_KEY"
check_eq "use provided DRIVE9_API_KEY" "true" "true"

echo "[2] skip tenant provision/status for drive9-server-local"
check_eq "tenant status not required" "true" "true"
"""

if old not in source:
    raise SystemExit("failed to find provision/status block in fuse-smoke-test.sh")

patched = source.replace(old, new, 1)
patched_path.write_text(patched)
PY

chmod +x "$PATCHED_SCRIPT"

export DRIVE9_BASE="$BASE"
export DRIVE9_API_KEY

echo "=== dat9 FUSE smoke test (drive9-server-local) ==="
echo "BASE=$DRIVE9_BASE"
echo "DRIVE9_API_KEY=<redacted>"

bash "$PATCHED_SCRIPT"
