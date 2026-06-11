#!/usr/bin/env bash
# drive9 layer filesystem smoke test against the shared dev deployment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export DRIVE9_BASE="${DRIVE9_BASE:-http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com}"
export RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-1}"
export LAYER_FUSE_STRICT_PREREQS="${LAYER_FUSE_STRICT_PREREQS:-1}"
export CLI_SOURCE="${CLI_SOURCE:-build}"

echo "=== drive9 layer filesystem realdev smoke test ==="
echo "BASE=${DRIVE9_BASE}"
echo "RUN_LAYER_FUSE_SMOKE=${RUN_LAYER_FUSE_SMOKE}"
echo "LAYER_FUSE_STRICT_PREREQS=${LAYER_FUSE_STRICT_PREREQS}"

exec bash "${SCRIPT_DIR}/layer-fs-smoke-test.sh"
