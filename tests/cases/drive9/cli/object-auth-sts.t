#!/bin/sh
# drive9/cli/object-auth-sts: `[prod-ext]` Hosted `--auth=server` STS mint & namespace isolation (`--no-mount`).
set -eu

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEME="${DRIVE9_E2E_STS_SCHEME:-${DRIVE9_E2E_OBJECT_SCHEME:-s3}}"
export SCHEME

require --claim 'sts-hosted-credentials-configured' -- sh -c '
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_STS_BUCKET:-${DRIVE9_E2E_OBJECT_BUCKET:-}}" ] &&
    [ -n "${DRIVE9_E2E_STS_ACCESS_KEY_ID:-${DRIVE9_E2E_OBJECT_ACCESS_KEY_ID:-}}" ] &&
    [ -n "${DRIVE9_E2E_STS_SECRET_ACCESS_KEY:-${DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY:-}}" ]
'

check shell --req 'drive9-obj-auth-sts' --claim 'server-minted-sts-isolation-and-roundtrip' -- \
    sh "$CASE_DIR/tools/object_auth_runner.sh" "$SCHEME" "us-east-1" "0" "STS"
