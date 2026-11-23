#!/bin/sh
# community/fio-smallfiles: multi-small-file I/O benchmark & integrity check
# across 512 independent 16 KiB files (`4 jobs × 128 files`, exercising the
# inline small-object path + open/read/write/close metadata churn):
#   1. seq-write:  whole-file 16 KiB sequential create+write+fsync
#   2. seq-read:   cold whole-file 16 KiB sequential read after `--cache fresh` remount
#   3. rand-write: 4 KiB sub-file random overwrites with inline `crc32c` verification
#   4. rand-read:  cold 4 KiB random reads across all 512 small files + server SHA-256 parity
set -eu

JOBS=${JOBS:-4}
NRFILES=${NRFILES:-128}
FILESIZE=${FILESIZE:-16k}
OPENFILES=${OPENFILES:-16}
IODEPTH=${IODEPTH:-4}
RUNTIME=${RUNTIME:-10}
DIRECT=${DIRECT:-0}

# 1. Small-file sequential create + write
check fstest --suite fio --args "--rw=write --bs=$FILESIZE --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --group_reporting=1 --unlink=0 --nrfiles=$NRFILES --filesize=$FILESIZE --openfiles=$OPENFILES --filename_format=sf.\$jobnum.\$filenum --end_fsync=1" --req 'community-fio-smallfiles-seq-write'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-smallfiles-seq-write-sync'

# 2. Cold remount + small-file sequential read
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check fstest --suite fio --args "--rw=read --bs=$FILESIZE --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --group_reporting=1 --unlink=0 --nrfiles=$NRFILES --filesize=$FILESIZE --openfiles=$OPENFILES --filename_format=sf.\$jobnum.\$filenum --readonly --allow_file_create=0" --req 'community-fio-smallfiles-seq-read'

# 3. Small-file 4 KiB random overwrite + CRC32C verification
check fstest --suite fio --args "--rw=randwrite --bs=4k --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --verify=crc32c --group_reporting=1 --unlink=0 --nrfiles=$NRFILES --filesize=$FILESIZE --openfiles=$OPENFILES --filename_format=sf.\$jobnum.\$filenum --serialize_overlap=1 --end_fsync=1" --req 'community-fio-smallfiles-rand-write'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-smallfiles-rand-write-sync'

sample post_randw --as mount

# 4. Cold remount + small-file 4 KiB random read + server truth parity
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check fstest --suite fio --args "--rw=randread --bs=4k --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --nrfiles=$NRFILES --filesize=$FILESIZE --openfiles=$OPENFILES --filename_format=sf.\$jobnum.\$filenum --readonly --allow_file_create=0" --req 'community-fio-smallfiles-rand-read'

sample remote_truth --as remote
check manifest-equals --this @remote_truth --against @post_randw --ignore '.sim/**' --req 'community-fio-smallfiles-parity'

rm -f sf.*
drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fio-smallfiles-cleanup-sync'
