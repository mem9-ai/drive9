#!/bin/bash
set -uo pipefail

BIN="${DRIVE9_BENCH_BIN:-/home/ubuntu/drive9-main-fe9cdcf/drive9}"
CRED_DIR="${DRIVE9_BENCH_CRED_DIR:-/home/ubuntu/drive9-sixway-20260911/credentials}"
GROUP="${DRIVE9_BENCH_GROUP:-none-a}"
DIAG_DIR="${DRIVE9_DIAG_DIR:-/home/ubuntu/d9diag}"
MOUNT_NAME="${DRIVE9_DIAG_MOUNT:-/mnt/d9-diag}"
cd "$DIAG_DIR"
PID=$(pgrep -f "drive9 mount --foreground.*$(basename "$MOUNT_NAME")" | head -1)
export DRIVE9_API_KEY=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['api_key'])" "$CRED_DIR/$GROUP.json")
"$BIN" mount drain --timeout 300s --json "$MOUNT_NAME" > /dev/null
sleep 3
sudo strace -f -T -tt -e trace=fsync,fdatasync,pwrite64,write,openat,rename,renameat,ftruncate,futex,nanosleep -o strace-clean.txt -p "$PID" &
STR=$!
sleep 2
PYTHONPATH="$DIAG_DIR" python3 -c "import diag_probe as d; print(d.flush_write(100,'flush-clean'))" > flush-clean.out 2>&1
sleep 2
kill -INT $STR 2>/dev/null; wait $STR 2>/dev/null
echo STRACE_DONE
