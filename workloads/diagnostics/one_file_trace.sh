#!/bin/bash
set -uo pipefail

BIN="${DRIVE9_BENCH_BIN:-/home/ubuntu/drive9-main-fe9cdcf/drive9}"
CRED_DIR="${DRIVE9_BENCH_CRED_DIR:-/home/ubuntu/drive9-sixway-20260911/credentials}"
GROUP="${DRIVE9_BENCH_GROUP:-none-a}"
DIAG_DIR="${DRIVE9_DIAG_DIR:-/home/ubuntu/d9diag}"
MOUNT_NAME="${DRIVE9_DIAG_MOUNT:-/mnt/d9-diag}"
export MOUNT_NAME
cd "$DIAG_DIR"
PID=$(pgrep -f "drive9 mount --foreground.*$(basename "$MOUNT_NAME")" | head -1)
export DRIVE9_API_KEY=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['api_key'])" "$CRED_DIR/$GROUP.json")
"$BIN" mount drain --timeout 300s --json "$MOUNT_NAME" > /dev/null
sleep 2
mkdir -p "$MOUNT_NAME/probe/trace3"
rm -f "$MOUNT_NAME"/probe/trace3/*
sudo strace -f -y -T -e trace=fsync,fdatasync,sync_file_range,openat,rename,renameat,ftruncate -o trace3.txt -p "$PID" &
STR=$!
sleep 2
python3 - <<"PY"
import os, time
root=os.environ["MOUNT_NAME"] + "/probe/trace3"
for i in range(3):
    p=f"{root}/t{i}.bin"
    fd=os.open(p, os.O_CREAT|os.O_WRONLY, 0o644)
    os.write(fd, b"x"*212)
    d=os.dup(fd); os.close(d)
    os.close(fd)
    time.sleep(0.4)
PY
sleep 2
kill -INT $STR 2>/dev/null; wait $STR 2>/dev/null
echo TRACE3_DONE
