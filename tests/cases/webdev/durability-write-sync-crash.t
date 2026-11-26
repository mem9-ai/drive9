#!/bin/sh
# Durability matrix (write-sync): every write that returned is remote-durable,
# so both the control and the written file must survive a client SIGKILL.
set -eu

mkdir -p data
printf 'write-sync-confirmed\n' > data/confirmed.txt.part
fsync data/confirmed.txt.part
mv data/confirmed.txt.part data/confirmed.txt
printf 'write-sync-written\n' > data/written.txt
sample pre-crash

halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'write-sync-confirmed\n' | cmp - data/confirmed.txt
printf 'write-sync-written\n' | cmp - data/written.txt

check manifest-contains --this @recovered --against @pre-crash \
    --paths 'data/confirmed.txt,data/written.txt' --req 'M3-write-sync-survives'
