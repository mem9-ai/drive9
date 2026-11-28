#!/bin/sh
# Power cut: the writer is killed mid-stream so volatile bytes never reach the
# server. Server-confirmed data must survive; the interrupted file may be
# absent or complete, but it must never come back truncated.
set -eu

mkdir -p data
printf 'power-confirmed\n' > data/confirmed.txt.part
fsync data/confirmed.txt.part
mv data/confirmed.txt.part data/confirmed.txt
drain --mode require-ok --timeout 120s
sample pre-crash

async --timeout 120s -- sh -c '
  i=0
  while [ "$i" -lt 200 ]; do
    head -c 65536 /dev/zero | tr "\0" P > data/volatile.bin.part || exit 0
    mv data/volatile.bin.part data/volatile.bin || exit 0
    i=$((i + 1))
  done
'
hold --for 1s
halt client --method kill
await --expect interrupted --timeout 60s

remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'power-confirmed\n' | cmp - data/confirmed.txt
if [ -e data/volatile.bin ]; then
  size=$(d9-stat size data/volatile.bin)
  if [ "$size" -eq 65536 ]; then
    echo "volatile-outcome: complete"
  else
    echo "volatile-outcome: truncated size=$size"
  fi
else
  echo "volatile-outcome: absent"
fi

check manifest-contains --this @recovered --against @pre-crash \
    --paths 'data/confirmed.txt' --req 'M5-power-cut-confirmed-survives'
check artifact-contains --artifact script.out \
    --pattern 'volatile-outcome:( |\t)*(complete|absent)' --req 'M5-power-cut-no-truncation'
