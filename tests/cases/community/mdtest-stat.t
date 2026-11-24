#!/bin/sh
# community/mdtest-stat: stat-only metadata rate. The prepare phase
# (-C: create the tree) is a separate measurement and is fully drained
# BEFORE the timed -T pass, so the measured stats hit committed objects
# with an empty commit queue — the pure positive-cache lookup rate.
# Knobs: THREADS, N, Z, B.
set -eu

THREADS=${THREADS:-2}
N=${N:-100}
Z=${Z:-1}
B=${B:-4}

check fstest --suite mdtest --args "--threads $THREADS -C -n $N -z $Z -b $B" --req 'community-mdtest-stat-prepare'
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-stat-prepare-sync'
check fstest --suite mdtest --args "--threads $THREADS -T -n $N -z $Z -b $B" --req 'community-mdtest-stat'
check fstest --suite mdtest --args "--threads $THREADS -r -n $N -z $Z -b $B" --req 'community-mdtest-stat-cleanup'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-mdtest-stat-sync'
