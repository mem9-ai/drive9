#!/bin/sh
# community/fio-randrw-read-heavy: read-intensive variant of the 4k random
# mix (rwmixread 90) — dominates in read amplification while keeping a write
# stream alive, the shape closest to coding-agent working sets.
set -eu

SEED_MIB=${SEED_MIB:-64}
RUNTIME=${RUNTIME:-20}
JOBS=${JOBS:-2}
IODEPTH=${IODEPTH:-4}
DIRECT=${DIRECT:-0}

i=0
while [ "$i" -lt "$JOBS" ]; do
    dd if=/dev/urandom of="seed.$i" bs=1048576 count="$SEED_MIB"
    i=$((i+1))
done
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-read-heavy-seed-sync'

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check fstest --suite fio --args "--rw=randrw --bs=4k --size=${SEED_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --rwmixread=90 --group_reporting=1 --unlink=0 --filename_format=seed.\$jobnum --allow_file_create=0 --end_fsync=1" --req 'community-fio-read-heavy'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-read-heavy-sync'

sample post_rw --as mount
sample remote_truth --as remote
check manifest-equals --this @remote_truth --against @post_rw --ignore '.sim/**' --req 'community-fio-read-heavy-parity'

rm -f seed.*
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-read-heavy-cleanup-sync'
