#!/bin/bash
# Run the three measurement-gap probes in order (G2 fsync, G1 rename, G3
# concurrent-files) against a mount that was started with --perf-dir, writing
# window markers so perf.jsonl samples can be sliced per probe.
set -euo pipefail

BIN=${DRIVE9_BENCH_BIN:-/home/ubuntu/drive9-main-fe9cdcf/drive9}
MOUNT=${DRIVE9_BENCH_MOUNT:-/mnt/d9-stg}
OUT=${DRIVE9_BENCH_PERF_OUT:-/home/ubuntu/perf-probes}
WORKLOADS=${DRIVE9_BENCH_WORKLOADS:-/home/ubuntu/workloads}

mkdir -p "$OUT"

mark() { date -u +"%Y-%m-%dT%H:%M:%SZ $1" >> "$OUT/windows.txt"; }
drain() {
  "$BIN" mount drain --timeout 1800s --json "$MOUNT" > "$OUT/drain-$1.json" 2>&1 || true
}

mark "g2-fsync-start"
cd "$WORKLOADS/extended"
python3 cases.py op-fsync "$MOUNT/g2-fsync" "$OUT/g2-fsync.json" --scale 1
mark "g2-fsync-end"
drain g2

mark "g1-rename-start"
cd "$WORKLOADS/main"
python3 cases.py rename "$MOUNT/g1-rename" "$OUT/g1-rename.json" --scale 1
mark "g1-rename-end"
drain g1

mark "g3-concurrent-start"
cd "$WORKLOADS/main"
python3 cases.py concurrent-files "$MOUNT/g3-conc" "$OUT/g3-conc.json" --scale 1
mark "g3-concurrent-end"
drain g3

cp /home/ubuntu/d9perf/perf.jsonl "$OUT/perf.jsonl" 2>/dev/null || true
echo DONE
