#!/bin/sh
# drive9/cli/object-auth-cos: `[prod-ext]` Hosted Tencent COS `--auth=server` STS mint & isolation (`--no-mount`).
set -eu

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
require --claim 'cos-hosted-credentials-configured' -- sh -c '
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_COS_BUCKET:-${DRIVE9_E2E_OBJECT_BUCKET:-}}" ] &&
    [ -n "${DRIVE9_E2E_COS_ACCESS_KEY_ID:-${DRIVE9_E2E_OBJECT_ACCESS_KEY_ID:-}}" ] &&
    [ -n "${DRIVE9_E2E_COS_SECRET_ACCESS_KEY:-${DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY:-}}" ]
'

check shell --req 'drive9-obj-auth-cos' --claim 'cos-hosted-sts-mint-copy-and-namespace-isolation' -- \
    sh "$CASE_DIR/tools/object_auth_runner.sh" "cos" "ap-beijing" "0" "COS"
