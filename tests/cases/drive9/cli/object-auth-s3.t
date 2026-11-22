#!/bin/sh
# drive9/cli/object-auth-s3: `[prod-ext]` Hosted AWS S3 `--auth=server` STS mint & isolation (`--no-mount`).
set -eu

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
require --claim 's3-hosted-credentials-configured' -- sh -c '
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_S3_BUCKET:-${DRIVE9_E2E_OBJECT_BUCKET:-}}" ] &&
    [ -n "${DRIVE9_E2E_S3_ACCESS_KEY_ID:-${DRIVE9_E2E_OBJECT_ACCESS_KEY_ID:-}}" ] &&
    [ -n "${DRIVE9_E2E_S3_SECRET_ACCESS_KEY:-${DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY:-}}" ]
'

check shell --req 'drive9-obj-auth-s3' --claim 's3-hosted-sts-mint-copy-and-namespace-isolation' -- \
    sh "$CASE_DIR/tools/object_auth_runner.sh" "s3" "ap-southeast-1" "0" "S3"
