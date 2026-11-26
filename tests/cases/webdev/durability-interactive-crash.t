#!/bin/sh
# Durability matrix (interactive): fsync is the only durability boundary.
# A confirmed control and an fsynced file must survive a client SIGKILL; a
# closed-but-unfsynced file may be lost and its outcome is recorded.
set -eu

mkdir -p data
printf 'interactive-confirmed\n' > data/confirmed.txt.part
fsync data/confirmed.txt.part
mv data/confirmed.txt.part data/confirmed.txt
printf 'interactive-unsynced\n' > data/unsynced.txt
sample pre-crash

halt client

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'interactive-confirmed\n' | cmp - data/confirmed.txt
if [ -e data/unsynced.txt ]; then
  echo "unsynced-outcome: survived"
else
  echo "unsynced-outcome: lost"
fi

check manifest-contains --this @recovered --against @pre-crash \
    --paths 'data/confirmed.txt' --req 'M1-interactive-fsynced-survives'
check artifact-contains --artifact script.out \
    --pattern 'unsynced-outcome:( |\t)*(survived|lost)' --req 'M1-interactive-loss-recorded'
