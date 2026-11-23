#!/bin/sh
# Exit recovery D1: sudden client SIGKILL with no drain after writes/closes.
# Prior confirmed controls and fsynced publishes must survive on fresh remount,
# deleted files must not resurrect, and non-fsynced volatile loss is recorded.
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
head -c 65536 /dev/zero | tr '\0' 'V' > data/volatile.bin
printf 'deleted before exit\n' > data/doomed.txt
mv data/control-2.bin data/renamed.bin
rm data/doomed.txt
sample pre-exit

halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'prior confirmed control\n' | cmp - data/prior-control.txt
printf 'confirmed-1\n'             | cmp - data/control-1.txt
head -c 65536 /dev/zero | tr '\0' 'D' | cmp - data/renamed.bin
[ ! -e data/doomed.txt ]
if [ -e data/volatile.bin ]; then
  echo "volatile-outcome: survived"
else
  echo "volatile-outcome: lost"
fi

printf 'new round\n' > data/next.txt
printf 'new round\n' | cmp - data/next.txt
drain --mode require-ok --timeout 120s

check manifest-contains --this @recovered --against @pre-exit \
    --paths 'data/prior-control.txt,data/control-1.txt,data/renamed.bin' --req 'D-check1'
check manifest-absent --this @recovered \
    --paths 'data/doomed.txt' --req 'D-check2'
check artifact-contains --artifact script.out \
    --pattern 'volatile-outcome:( |\t)*(survived|lost)' --req 'D-check2'
