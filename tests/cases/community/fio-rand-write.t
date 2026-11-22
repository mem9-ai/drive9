#!/bin/sh
# community/fio-rand-write: pure 4k random writes on a fresh namespace —
# isolates the write path (FUSE write buffering, extent coalescing, commit
# drain). Uses `--unlink=0` and `--end_fsync=1` so fio cannot trigger
# unlink write-cancellation and includes flush latency, then asserts exact
# mount-vs-server SHA-256 parity across every written file.
set -eu

SIZE_MIB=${SIZE_MIB:-64}
RUNTIME=${RUNTIME:-20}
JOBS=${JOBS:-2}
IODEPTH=${IODEPTH:-4}
DIRECT=${DIRECT:-0}

check fstest --suite fio --args "--rw=randwrite --bs=4k --size=${SIZE_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --filename_format=randw.\$jobnum --end_fsync=1" --req 'community-fio-rand-write'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-rand-write-sync'

sample written --as mount
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"
sample remounted --as mount
sample remote_truth --as remote
check manifest-equals --this @remounted --against @written --ignore '.sim/**' --req 'community-fio-rand-write-remount-parity'
check manifest-equals --this @remote_truth --against @written --ignore '.sim/**' --req 'community-fio-rand-write-remote-parity'

rm -f randw.*
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-rand-write-cleanup-sync'
