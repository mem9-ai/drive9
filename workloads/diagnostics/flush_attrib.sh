#!/bin/bash
set -uo pipefail

BIN="${DRIVE9_BENCH_BIN:-/home/ubuntu/drive9-main-fe9cdcf/drive9}"
DIAG_DIR="${DRIVE9_DIAG_DIR:-/home/ubuntu/d9diag}"
MOUNT_NAME="${DRIVE9_DIAG_MOUNT:-/mnt/d9-diag}"
PPROF_ADDR="${DRIVE9_DIAG_PPROF_ADDR:-127.0.0.1:16111}"
cd "$DIAG_DIR"
PID=$(pgrep -f "drive9 mount --foreground.*$(basename "$MOUNT_NAME")" | head -1)
echo "mount pid=$PID"
curl -sS -o cpu-flush.pprof "http://$PPROF_ADDR/debug/pprof/profile?seconds=30" &
CURL=$!
sleep 2
PYTHONPATH="$DIAG_DIR" python3 -c "import diag_probe as d; print(d.flush_write(800,'flush-cpu'))" > flush-cpu.out 2>&1
wait $CURL
echo "=== cpu top (cum) ==="
go tool pprof -top -nodecount=22 "$BIN" cpu-flush.pprof 2>/dev/null | head -32
sudo timeout -s INT 30 strace -c -f -p "$PID" -o strace-flush.txt &
STR=$!
sleep 3
PYTHONPATH="$DIAG_DIR" python3 -c "import diag_probe as d; print(d.flush_write(400,'flush-strace'))" > flush-strace.out 2>&1
wait $STR
echo "=== strace summary ==="
head -22 strace-flush.txt
echo DONE
