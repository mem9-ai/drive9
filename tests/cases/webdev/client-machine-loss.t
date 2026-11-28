#!/bin/sh
# Machine loss: the sandbox and its local cache disappear with unflushed writes.
# Only data confirmed on the server may come back; nothing half-written may
# resurface after a fresh-cache remount.
set -eu

mkdir -p data
printf 'machine-confirmed\n' > data/confirmed.txt.part
fsync data/confirmed.txt.part
mv data/confirmed.txt.part data/confirmed.txt
drain --mode require-ok --timeout 120s
printf 'machine-unsynced\n' > data/unsynced.txt
sample pre-crash

halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'machine-confirmed\n' | cmp - data/confirmed.txt
if [ -e data/unsynced.txt ]; then
  echo "unsynced-outcome: survived"
else
  echo "unsynced-outcome: lost"
fi

check manifest-contains --this @recovered --against @pre-crash \
    --paths 'data/confirmed.txt' --req 'M4-machine-loss-confirmed-survives'
check artifact-contains --artifact script.out \
    --pattern 'unsynced-outcome:( |\t)*(survived|lost)' --req 'M4-machine-loss-unconfirmed-recorded'
