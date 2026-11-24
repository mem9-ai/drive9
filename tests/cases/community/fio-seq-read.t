#!/bin/sh
# community/fio-seq-read: 1 MiB sequential reads over committed real data
# after a `--cache fresh` remount so sequential reads exercise cold client/server
# streaming paths rather than warm writer RAM.
set -eu

SEED_MIB=${SEED_MIB:-256}
RUNTIME=${RUNTIME:-20}
JOBS=${JOBS:-2}
IODEPTH=${IODEPTH:-4}
DIRECT=${DIRECT:-0}

i=0
while [ "$i" -lt "$JOBS" ]; do
    dd if=/dev/urandom of="seed.$i" bs=1048576 count="$SEED_MIB"
    i=$((i + 1))
done
drain --mode require-ok --timeout 300s

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check fstest --suite fio --args "--rw=read --bs=1m --size=${SEED_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --filename_format=seed.\$jobnum --allow_file_create=0 --readonly" --req 'community-fio-seq-read'

rm -f seed.*
drain --mode require-ok --timeout 300s
