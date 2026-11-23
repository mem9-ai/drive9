#!/bin/sh
# community/mdworkbench-readonly: steady-state READ-ONLY metadata on
# a fixed working set (precreate then stat/read loops) — sustained cache
# hit-rate behavior with zero write interference; the read-side companion
# of mdworkbench-mixed. Knobs: THREADS, P, I, D, R.
set -eu

THREADS=${THREADS:-2}
P=${P:-300}
I=${I:-100}
D=${D:-2}
R=${R:-2}

check fstest --suite mdworkbench --args "--threads $THREADS -P $P -I $I -D $D -R $R --read-only" --req 'community-mdworkbench-readonly'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdworkbench-readonly-sync'
