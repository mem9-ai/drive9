#!/bin/sh
# community/mdworkbench-mixed: steady-state metadata on a FIXED
# working set — repeated stat/read/delete-oldest/create-newest (FIFO) with
# readers/writers rank-offset; per-phase op latency percentiles + obj/s.
# The long-running working-set shape (as opposed to mdtest's one-shot
# tree). Knobs: THREADS, P (objects/rank), I (iterations), D (dirs), R
# (runs).
set -eu

THREADS=${THREADS:-2}
P=${P:-300}
I=${I:-100}
D=${D:-2}
R=${R:-2}

check fstest --suite mdworkbench --args "--threads $THREADS -P $P -I $I -D $D -R $R" --req 'community-mdworkbench-mixed'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdworkbench-mixed-sync'
