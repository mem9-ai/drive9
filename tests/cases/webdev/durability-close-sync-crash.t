#!/bin/sh
# Durability matrix (close-sync): close(2) is the commit point, so a closed file
# must survive a client SIGKILL without an explicit fsync.
set -eu

mkdir -p data
printf 'close-sync-confirmed\n' > data/confirmed.txt.part
fsync data/confirmed.txt.part
mv data/confirmed.txt.part data/confirmed.txt
printf 'close-sync-closed\n' > data/closed.txt
sample pre-crash

halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'close-sync-confirmed\n' | cmp - data/confirmed.txt
printf 'close-sync-closed\n' | cmp - data/closed.txt

check manifest-contains --this @recovered --against @pre-crash \
    --paths 'data/confirmed.txt,data/closed.txt' --req 'M2-close-sync-survives'
