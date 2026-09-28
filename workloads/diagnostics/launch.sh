#!/bin/bash
set -euo pipefail

# Reference diagnostics mount: writeback + perf recorder + pprof on 127.0.0.1:16111.
# Override any path via env; defaults match the reference EC2 host (see workloads/AGENTS.md).
BIN="${DRIVE9_BENCH_BIN:-/home/ubuntu/drive9-main-fe9cdcf/drive9}"
SERVER="${DRIVE9_BENCH_SERVER:-https://drive9.example.invalid}"
CRED_DIR="${DRIVE9_BENCH_CRED_DIR:-/home/ubuntu/drive9-sixway-20260911/credentials}"
GROUP="${DRIVE9_BENCH_GROUP:-none-a}"
DIAG_DIR="${DRIVE9_DIAG_DIR:-/home/ubuntu/d9diag}"
MOUNT="${DRIVE9_DIAG_MOUNT:-/mnt/d9-diag}"
REMOTE_ROOT="${DRIVE9_BENCH_REMOTE_ROOT:-/benchmark}"
PPROF_ADDR="${DRIVE9_DIAG_PPROF_ADDR:-127.0.0.1:16111}"

KEY=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['api_key'])" "$CRED_DIR/$GROUP.json")
export DRIVE9_API_KEY="$KEY"
exec "$BIN" mount --foreground --mode=fuse \
  --server "$SERVER" --profile none --durability fsync \
  --cache-dir "$DIAG_DIR/cache" --dir-ttl 30s --attr-ttl 30s --entry-ttl 30s \
  --allow-other --gvisor-compat=false \
  --perf-dir "$DIAG_DIR/perf" --perf-addr "$PPROF_ADDR" --perf-interval 2s \
  ":$REMOTE_ROOT/diag-mount" "$MOUNT"
