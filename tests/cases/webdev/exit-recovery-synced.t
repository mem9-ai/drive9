#!/bin/sh
# Exit recovery D3: sudden client SIGKILL after `drain --mode require-ok`
# confirms zero pending items. Fresh-cache remount must match the pre-exit
# manifest byte-for-byte with no resurrection of deleted files.
set -eu

mkdir -p data
printf 'prior confirmed control\n' > data/prior-control.txt
drain --mode require-ok --timeout 120s

printf 'confirmed-1\n' > data/control-1.txt.part
fsync data/control-1.txt.part
mv data/control-1.txt.part data/control-1.txt
head -c 65536 /dev/zero | tr '\0' 'D' > data/control-2.bin.part
fsync data/control-2.bin.part
mv data/control-2.bin.part data/control-2.bin
printf 'deleted before exit\n' > data/doomed.txt
fsync data/doomed.txt
mv data/control-2.bin data/renamed.bin
rm data/doomed.txt
sample pre-exit

drain --mode require-ok --timeout 120s
halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'prior confirmed control\n' | cmp - data/prior-control.txt
printf 'confirmed-1\n'             | cmp - data/control-1.txt
head -c 65536 /dev/zero | tr '\0' 'D' | cmp - data/renamed.bin
[ ! -e data/doomed.txt ]

printf 'post-d3\n' > data/next.txt
printf 'post-d3\n' | cmp - data/next.txt
drain --mode require-ok --timeout 120s

check no-pending --drain 1 --req 'D-step4'
check manifest-equals --this @recovered --against @pre-exit \
    --ignore '.sim/**' --req 'D-check1'
check manifest-absent --this @recovered \
    --paths 'data/doomed.txt' --req 'D-check2'
