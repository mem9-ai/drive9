#!/bin/sh
# Anomaly matrix X2: slow-link (+2s delay, slower-not-broken) followed by
# connection blackhole (`cut-variant`, may-fail-inflight) and post-window replay.
set -eu
FILES=${FILES:-80}
export FILES

fault slow-link delay --pattern 'PUT' --count 20 \
    --hold 20s --expect slower-not-broken --delay 2s
fault cut-variant blackhole --pattern 'PUT' --count 20 \
    --hold 8s --expect may-fail-inflight

arm slow-link
mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'net payload %s\n' "$k" > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part"
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  i=$((i + 1))
done
disarm slow-link

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'net payload %s\n' "$k" | cmp - "src/file-$k.txt"
  i=$((i + 1))
done

arm cut-variant
window --expect may-fail-inflight --timeout 10m -- sh -c '
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if printf "cut payload %s\n" "$k" > "src/file-$k.txt.part" 2>/dev/null; then
    fsync "src/file-$k.txt.part" 2>/dev/null || true
    r=0
    while ! mv "src/file-$k.txt.part" "src/file-$k.txt" 2>/dev/null; do
      r=$((r + 1))
      [ "$r" -ge 10 ] && break
      sleep 1
    done
  fi
  i=$((i + 1))
done
'
disarm cut-variant
find src -name '*.part' -delete 2>/dev/null || true

mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'net payload %s\n' "$k" > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part"
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  i=$((i + 1))
done

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'net payload %s\n' "$k" | cmp - "src/file-$k.txt"
  i=$((i + 1))
done

printf 'life after net events\n' > after.txt
printf 'life after net events\n' | cmp - after.txt

sample final
drain --mode require-ok --timeout 180s

check fault-fired --fault slow-link --req 'X2'
check fault-fired --fault cut-variant --req 'X2'
check manifest-equals --this control --against @final \
    --ignore '.sim/**' --req 'X2'
check metrics --metric op.write-fsync.p95 --budget write-fsync \
    --accept pass,unconfirmed --req 'section5-latency'
