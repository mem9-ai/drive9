#!/bin/bash
# Run server_op_probe.py against the staging endpoint with per-statement timing enabled.
# Usage: run-stg-op-probe.sh [iterations] [label]
set -euo pipefail

ITERATIONS=${1:-100}
LABEL=${2:-op}
CRED=${DRIVE9_BENCH_CRED:-/home/ubuntu/.drive9-create.json}

cd "$(dirname "$0")"
export DRIVE9_BENCH_CRED=$CRED
export DRIVE9_BENCH_HOST=$(python3 -c "import json,urllib.parse;print(urllib.parse.urlsplit(json.load(open('$CRED'))['server']).hostname)")
export DRIVE9_BENCH_SCHEME=http
export DRIVE9_BENCH_PORT=80
export DRIVE9_BENCH_REMOTE_ROOT=/benchmark
export DRIVE9_BENCH_OP_ITERATIONS=$ITERATIONS

echo "label=$LABEL iterations=$ITERATIONS host=$DRIVE9_BENCH_HOST start=$(date -u +%FT%TZ)"
python3 server_op_probe.py
echo "label=$LABEL end=$(date -u +%FT%TZ)"
