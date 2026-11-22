#!/bin/sh
# Exit recovery D2: best-effort drain with a 1s budget interrupted by sudden
# SIGKILL of the mount daemon, verifying fsynced control survival and honest
# recording of unconfirmed volatile/streamed payloads on fresh remount.
set -eu

mkdir -p data
printf 'confirmed-1\n' > data/control-1.txt.part
fsync data/control-1.txt.part
mv data/control-1.txt.part data/control-1.txt
head -c 8388608 /dev/zero | tr '\0' 'B' > data/bulk-payload.bin
sample pre-exit

drain --mode best-effort --timeout 1s
halt client
remount --cache fresh
cd "$MOUNT"
sample recovered

printf 'confirmed-1\n' | cmp - data/control-1.txt
if [ -e data/bulk-payload.bin ]; then
  echo "bulk-outcome: survived"
else
  echo "bulk-outcome: lost"
fi

# Variant: drain initiated while concurrent writes continue arriving.
mkdir -p data2
printf 'v2 control\n' > data2/control.txt.part
fsync data2/control.txt.part
mv data2/control.txt.part data2/control.txt

async --timeout 10m -- sh -c '
i=0
while [ "$i" -lt 200 ]; do
  k=$((i + 1))
  printf "streaming %s\n" "$k" > "data2/stream-$k.txt" 2>/dev/null || true
  i=$((i + 1))
done
'
drain --mode best-effort --timeout 1s
halt client
await --expect interrupted --timeout 60s
remount --cache fresh
cd "$MOUNT"

printf 'v2 control\n' | cmp - data2/control.txt
n=$(ls data2/stream-*.txt 2>/dev/null | wc -l)
echo "variant bulk-outcome: survived-stream-count=$n"

check manifest-contains --this @recovered --against @pre-exit \
    --paths 'data/control-1.txt' --req 'D-check3'
check artifact-contains --artifact sync-0.json --pattern '.' --req 'D-step4'
check artifact-contains --artifact script.out \
    --pattern 'bulk-outcome:' --req 'D-check3'
