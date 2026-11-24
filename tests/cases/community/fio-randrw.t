#!/bin/sh
# community/fio-randrw: 4k random read/write mix (75/25) on pre-seeded real
# data. Remounts `--cache fresh` after seeding, runs with `--rwmixread=75`,
# `--unlink=0`, and `--end_fsync=1`, and verifies fresh-remount & remote SHA-256 parity.
set -eu

SEED_MIB=${SEED_MIB:-64}
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

check fstest --suite fio --args "--rw=randrw --rwmixread=75 --bs=4k --size=${SEED_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --filename_format=seed.\$jobnum --allow_file_create=0 --end_fsync=1" --req 'community-fio-randrw'

drain --mode require-ok --timeout 300s
sample post_rw --as mount

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"
sample remounted --as mount
sample remote_truth --as remote
check manifest-equals --this @remounted --against @post_rw --ignore '.sim/**' --req 'community-fio-randrw-remount-parity'
check manifest-equals --this @remote_truth --against @post_rw --ignore '.sim/**' --req 'community-fio-randrw-parity'

rm -f seed.*
drain --mode require-ok --timeout 300s
