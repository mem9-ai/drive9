#!/bin/sh
# community/fsmark-s1: file-creation rate WITH explicit fsync —
# every file pays the durability toll on the spot, so the number is the
# honest floor for sync-writing clients. Same envelope as S0 (files/s
# percentiles + per-syscall µs). Knobs: N, SIZE, D, L.
set -eu

N=${N:-200}
SIZE=${SIZE:-4096}
D=${D:-4}
L=${L:-3}

check fstest --suite fsmark --args "-n $N -s $SIZE -S 1 -D $D -L $L" --req 'community-fsmark-s1'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fsmark-s1-sync'
