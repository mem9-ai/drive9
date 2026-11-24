#!/bin/sh
# community/mdtest-full: metadata throughput over the FULL matrix —
# directory create/stat/rename/remove interleaved with file ops, the
# closest metadata shape to real workspaces. Heaviest mdtest variant
# (upsizing N multiplies runtime fast: the full 500-file matrix runs
# minutes on a network mount). Knobs: THREADS, N, Z, B.
set -eu

THREADS=${THREADS:-2}
N=${N:-100}
Z=${Z:-1}
B=${B:-4}

check fstest --suite mdtest --args "--threads $THREADS -n $N -z $Z -b $B" --req 'community-mdtest-full'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-full-sync'
