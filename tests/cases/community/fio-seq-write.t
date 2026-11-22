#!/bin/sh
# community/fio-seq-write: 1 MiB sequential writes with `--end_fsync=1`,
# followed by fresh-remount and stateless-remote SHA-256 parity checks.
set -eu

SIZE_MIB=${SIZE_MIB:-256}
RUNTIME=${RUNTIME:-20}
JOBS=${JOBS:-2}
IODEPTH=${IODEPTH:-4}
DIRECT=${DIRECT:-0}

check fstest --suite fio --args "--rw=write --bs=1m --size=${SIZE_MIB}m --numjobs=$JOBS --iodepth=$IODEPTH --direct=$DIRECT --runtime=$RUNTIME --time_based --group_reporting=1 --unlink=0 --filename_format=seqw.\$jobnum --end_fsync=1" --req 'community-fio-seq-write'

drain --mode require-ok --timeout 300s
sample written --as mount

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"
sample remounted --as mount
sample remote_truth --as remote
check manifest-equals --this @remounted --against @written --ignore '.sim/**' --req 'community-fio-seq-write-remount-parity'
check manifest-equals --this @remote_truth --against @written --ignore '.sim/**' --req 'community-fio-seq-write-parity'

rm -f seqw.*
drain --mode require-ok --timeout 300s
