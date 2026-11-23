#!/bin/sh
# Mid-upload body truncation (`upload-mid-cut` + TCP RST) and upstream `http-5xx`
# (`503 Service Unavailable`) fault injection during synced publishes.
# Verifies zero partial-body corruption on surviving files, clean replay after
# disarm, and byte-exact persistence across a fresh-cache remount.
set -eu

S="$RUN_DIR/cut5xx"
rm -rf "$S" && mkdir -p "$S" settled mid srv
head -c 524288 /dev/zero | tr '\0' 'M' > "$S/blob-512k.bin"
i=1
while [ "$i" -le 10 ]; do
  { cat "$S/blob-512k.bin"; printf 'mid-payload-%s\n' "$i"; } > "$S/blob-$i.bin"
  i=$((i + 1))
done

printf 'settled baseline\n' > settled/base.txt.part
fsync settled/base.txt.part
mv settled/base.txt.part settled/base.txt
drain --mode require-ok --timeout 120s
sample settled

fault midcut upload-mid-cut --pattern 'PUT' --count 2 \
    --hold 4s --expect may-fail-inflight
fault srv5xx http-5xx --pattern 'PUT /v1/fs' --count 2 \
    --hold 4s --expect may-fail-inflight

# 1. `upload-mid-cut`: truncate PUT bodies halfway and send TCP RST.
arm midcut
window --expect may-fail-inflight --timeout 5m -- sh -c '
S=$1
i=1
while [ "$i" -le 10 ]; do
  if cp "$S/blob-$i.bin" "mid/f-$i.bin.part" 2>/dev/null && fsync "mid/f-$i.bin.part" 2>/dev/null; then
    mv "mid/f-$i.bin.part" "mid/f-$i.bin" 2>/dev/null || true
  fi
  i=$((i + 1))
done
' -- "$S"
disarm midcut
hold --for 2s

# Any finalized file under `mid/` must never contain a half-uploaded body.
i=1
while [ "$i" -le 10 ]; do
  if [ -e "mid/f-$i.bin" ]; then
    cmp "$S/blob-$i.bin" "mid/f-$i.bin"
  fi
  i=$((i + 1))
done
rm -f mid/*.part

# Replay mid-cut batch cleanly.
i=1
while [ "$i" -le 10 ]; do
  cp "$S/blob-$i.bin" "mid/f-$i.bin.part"
  fsync "mid/f-$i.bin.part"
  mv "mid/f-$i.bin.part" "mid/f-$i.bin"
  i=$((i + 1))
done

# 2. `http-5xx`: synthetic 503 Service Unavailable on PUT /v1/fs/*.
arm srv5xx
window --expect may-fail-inflight --timeout 5m -- sh -c '
i=1
while [ "$i" -le 12 ]; do
  if printf "srv5xx payload %s\n" "$i" > "srv/item-$i.txt.part" 2>/dev/null && fsync "srv/item-$i.txt.part" 2>/dev/null; then
    mv "srv/item-$i.txt.part" "srv/item-$i.txt" 2>/dev/null || true
  fi
  i=$((i + 1))
done
'
disarm srv5xx
hold --for 2s
rm -f srv/*.part

# Replay 5xx batch cleanly.
i=1
while [ "$i" -le 12 ]; do
  printf 'srv5xx payload %s\n' "$i" > "srv/item-$i.txt.part"
  fsync "srv/item-$i.txt.part"
  mv "srv/item-$i.txt.part" "srv/item-$i.txt"
  i=$((i + 1))
done

sample final
drain --mode require-ok --timeout 180s
halt client
remount --cache fresh
cd "$MOUNT"
sample fresh

check fault-fired --fault midcut --req 'fault-upload-mid-cut'
check fault-fired --fault srv5xx --req 'fault-http-5xx'
check artifact-contains --artifact injector/proxy.jsonl \
    --pattern 'upload-mid-cut' --req 'fault-upload-mid-cut'
check artifact-contains --artifact injector/proxy.jsonl \
    --pattern 'http-5xx' --req 'fault-http-5xx'
check manifest-contains --this @fresh --against @settled \
    --paths 'settled/' --req 'fault-settled-intact'
check manifest-equals --this @fresh --against @final \
    --ignore '.sim/**' --req 'fault-fresh-remount-converged'
