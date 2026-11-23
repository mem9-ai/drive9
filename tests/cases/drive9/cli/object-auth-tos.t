#!/bin/sh
# drive9/cli/object-auth-tos: `[prod-ext]` Hosted Volcengine TOS `--auth=server` STS mint & isolation (`--no-mount`).
set -eu

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
require --claim 'tos-hosted-credentials-configured' -- sh -c '
    [ -n "${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}" ] &&
    [ -n "${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}" ] &&
    [ -n "${DRIVE9_E2E_TOS_BUCKET:-${DRIVE9_E2E_OBJECT_BUCKET:-}}" ] &&
    [ -n "${DRIVE9_E2E_TOS_ACCESS_KEY_ID:-${DRIVE9_E2E_OBJECT_ACCESS_KEY_ID:-}}" ] &&
    [ -n "${DRIVE9_E2E_TOS_SECRET_ACCESS_KEY:-${DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY:-}}" ] &&
    [ -n "${DRIVE9_E2E_TOS_ROLE_ARN:-${DRIVE9_E2E_OBJECT_ROLE_ARN:-}}" ]
'

check shell --req 'drive9-obj-auth-tos' --claim 'tos-hosted-sts-mint-copy-and-namespace-isolation' -- \
    sh "$CASE_DIR/tools/object_auth_runner.sh" "tos" "cn-beijing" "1" "TOS"
