#!/bin/sh
# community/fio-rand-read: pure 4k random reads over committed real data —
# isolates the random read path (client cache misses/hits, range GET latency,
# and read-ahead behavior without commit-queue noise). Remounts with
# `--cache fresh` after seeding so reads do not merely hit warm writer RAM.
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
check sync-ok --req 'community-fio-rand-read-seed-sync'

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check fstest --suite fio --args "--rw=randread --bs=4k --size=${SEED_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --filename_format=seed.\$jobnum --allow_file_create=0 --readonly" --req 'community-fio-rand-read'

rm -f seed.*
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-rand-read-sync'
