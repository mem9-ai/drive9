#!/bin/sh
# Immediate cross-process read-after-save consistency:
#   1. `write -> fsync -> rename` immediately readable in another process without torn bytes
#   2. In-place partial overwrite (`conv=notrunc` at offset 1024) + truncating rewrite
#   3. 30 rounds of alternating 128KiB / 64KiB atomic replacements while a concurrent
#      reader reopens and verifies complete A or complete B with zero path gaps
#   4. 4-worker concurrent batch append + publish
set -eu
BATCH=${BATCH:-40}
export BATCH

S="$RUN_DIR/save-read"
rm -rf "$S" && mkdir -p "$S/batch" live

# 1. Cross-process visibility immediately after atomic publish.
printf 'marker-v1\n' > live/marker.txt.part
fsync live/marker.txt.part
mv live/marker.txt.part live/marker.txt
printf 'marker-v1\n' > "$S/expect-marker"
sh -c '
  S=$1; i=0
  while [ "$i" -lt 400 ]; do
    if [ -f live/marker.txt ]; then
      cmp live/marker.txt "$S/expect-marker" && exit 0
      echo "TORN-READ"; exit 1
    fi
    i=$((i + 1)); sleep 0.05
  done
  echo "TIMEOUT-VISIBILITY"; exit 1
' -- "$S"

# 2. In-place partial overwrite (64B at offset 1024) + truncating atomic rewrite.
head -c 131072 /dev/zero | tr '\0' 'A' > live/source.txt
dd if=/dev/zero bs=64 count=1 2>/dev/null | tr '\0' 'X' > "$S/patch"
{ head -c 1024 live/source.txt; cat "$S/patch"; tail -c +1089 live/source.txt; } > "$S/expect-source"
dd if="$S/patch" of=live/source.txt bs=1 seek=1024 conv=notrunc 2>/dev/null
cmp live/source.txt "$S/expect-source"

printf 'short rewrite\n' > live/source.txt.part
fsync live/source.txt.part
mv live/source.txt.part live/source.txt
printf 'short rewrite\n' | cmp - live/source.txt

# 3. Concurrent reader during 30 alternating A(128KiB) / B(64KiB) atomic replacements.
head -c 131072 /dev/zero | tr '\0' 'A' > "$S/A.bin"
head -c 65536  /dev/zero | tr '\0' 'B' > "$S/B.bin"
sh -c '
  S=$1; w=0
  while [ ! -e live/combined.bin ] && [ "$w" -lt 2000 ]; do w=$((w + 1)); sleep 0.01; done
  i=0
  while [ "$i" -lt 300 ]; do
    if [ ! -e live/combined.bin ]; then echo "VIOLATION-gap"; exit 1; fi
    ok=0
    cmp -s live/combined.bin "$S/A.bin" && ok=1 || true
    if [ "$ok" -eq 0 ]; then cmp -s live/combined.bin "$S/B.bin" && ok=1 || true; fi
    if [ "$ok" -eq 0 ]; then cmp -s live/combined.bin "$S/A.bin" && ok=1 || true; fi
    if [ "$ok" -eq 0 ]; then
      echo "VIOLATION-mixed size=$(wc -c < live/combined.bin)"
      exit 1
    fi
    i=$((i + 1))
  done
' -- "$S" &
reader=$!

k=0
while [ "$k" -lt 30 ]; do
  cp "$S/A.bin" live/combined.bin.part && mv live/combined.bin.part live/combined.bin
  cp "$S/B.bin" live/combined.bin.part && mv live/combined.bin.part live/combined.bin
  k=$((k + 1))
done
wait "$reader"
cmp live/combined.bin "$S/B.bin"

# 4. 4-worker concurrent copy + append + atomic publish.
i=0
while [ "$i" -lt "$BATCH" ]; do
  k=$((i + 1))
  printf 'batch line %s\n' "$k" > "$S/batch/orig-$k.txt"
  { cat "$S/batch/orig-$k.txt"; printf 'v2\n'; } > "$S/batch/mod-$k.txt"
  i=$((i + 1))
done
worker() {
  lo=$1; hi=$2; j=$lo
  while [ "$j" -le "$hi" ]; do
    cp "$S/batch/orig-$j.txt" "live/batch-$j.txt.part" \
      && printf 'v2\n' >> "live/batch-$j.txt.part" \
      && mv "live/batch-$j.txt.part" "live/batch-$j.txt"
    j=$((j + 1))
  done
}
q=$((BATCH / 4))
worker 1               "$q" &
worker $((q + 1))      $((2 * q)) &
worker $((2 * q + 1))  $((3 * q)) &
worker $((3 * q + 1))  "$BATCH" &
wait

i=0
while [ "$i" -lt "$BATCH" ]; do
  k=$((i + 1))
  cmp "live/batch-$k.txt" "$S/batch/mod-$k.txt"
  i=$((i + 1))
done

sample final
drain --mode require-ok --timeout 120s
sample remote-final --as remote

check manifest-equals --this control --against @final \
    --ignore '.sim/**' --req 'scenario2-check4'
check manifest-equals --this @remote-final --against @final \
    --ignore '.sim/**' --req 'scenario2-check4'
