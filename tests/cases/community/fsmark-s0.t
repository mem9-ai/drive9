#!/bin/sh
# community/fsmark-s0: file-creation rate WITHOUT explicit fsync —
# the writeback-friendly shape: creat/write/close hit the FUSE write path
# and the commit queue drains afterwards (drain time is part of the
# story). Report envelope carries files/s mean + p50/p90/p99 and per-syscall
# µs breakdown. Knobs: N (files per round), SIZE, D (dirs), L (levels).
set -eu

N=${N:-200}
SIZE=${SIZE:-4096}
D=${D:-4}
L=${L:-3}

check fstest --suite fsmark --args "-n $N -s $SIZE -S 0 -D $D -L $L" --req 'community-fsmark-s0'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fsmark-s0-sync'
