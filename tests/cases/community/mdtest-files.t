#!/bin/sh
# community/mdtest-files: metadata throughput on a files-only tree
# (no directory operations) — file create/read/stat/remove rates across
# THREADS ranks on a shared tree; per-op totalRate lands in the JSON
# envelope. Report runs upsize N to 500 / THREADS 4. Knobs: THREADS, N, Z
# (depth), B (breadth).
set -eu

THREADS=${THREADS:-2}
N=${N:-100}
Z=${Z:-1}
B=${B:-4}

check fstest --suite mdtest --args "--threads $THREADS -F -n $N -z $Z -b $B" --req 'community-mdtest-files'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-files-sync'
