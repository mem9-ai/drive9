#!/bin/sh
# Exception matrix X1: normal child-process exit (`exit 0`) mid-batch during a
# concurrent multi-worker install. Completed items must be byte-exact with zero
# spurious failures, and replaying the batch converges cleanly.
set -eu
ASSETS=${ASSETS:-24}
WORKERS=${WORKERS:-8}

T="$RUN_DIR/child-exit"
rm -rf "$T" && mkdir -p "$T/assets" incoming installed

manifest_dir() {
  dir=$1
  (
    cd "$dir"
    for f in a-*.bin; do
      [ -e "$f" ] || continue
      printf '%s  %s\n' "$(d9-stat sha256 "$f")" "$f"
    done
  ) | sort -k2
}

i=0
while [ "$i" -lt "$ASSETS" ]; do
  k=$((i + 1))
  head -c 4096 /dev/zero | tr '\0' 'd' > "$T/assets/a-$k.bin"
  i=$((i + 1))
done
manifest_dir "$T/assets" > "$T/exp-x1"

N=$ASSETS
W=$WORKERS
q=$(( (N + W - 1) / W ))
worker() {
  id=$1
  lo=$(( (id - 1) * q + 1 ))
  hi=$(( id * q ))
  [ "$hi" -gt "$N" ] && hi=$N
  [ "$lo" -gt "$N" ] && exit 0
  mid=$(( (lo + hi) / 2 ))
  j=$lo
  while [ "$j" -le "$hi" ]; do
    if [ $((id % 2)) -eq 0 ] && [ "$j" -gt "$mid" ]; then
      echo "worker-normal-exit: worker=$id at-item=$j"
      exit 0
    fi
    cp "$T/assets/a-$j.bin" "incoming/a-$j.tmp" && mv "incoming/a-$j.tmp" "installed/a-$j.bin"
    j=$((j + 1))
  done
}

i=1
while [ "$i" -le "$W" ]; do
  worker "$i" &
  i=$((i + 1))
done
wait

present=0
missing=0
j=1
while [ "$j" -le "$N" ]; do
  if [ -e "installed/a-$j.bin" ]; then
    cmp "$T/assets/a-$j.bin" "installed/a-$j.bin"
    present=$((present + 1))
  else
    missing=$((missing + 1))
  fi
  j=$((j + 1))
done
echo "audit PRESENT=$present MISSING=$missing"
[ "$present" -gt 0 ] && [ "$missing" -gt 0 ]

j=1
while [ "$j" -le "$N" ]; do
  cp "$T/assets/a-$j.bin" "incoming/a-$j.tmp" && mv "incoming/a-$j.tmp" "installed/a-$j.bin"
  j=$((j + 1))
done
manifest_dir installed > "$T/got-x1"
cmp "$T/got-x1" "$T/exp-x1"
[ "$(ls incoming 2>/dev/null | wc -l)" -eq 0 ]

sample final
drain --mode require-ok --timeout 120s

check artifact-contains --artifact script.out \
    --pattern 'worker-normal-exit:' --req 'X1'
check manifest-equals --this control --against @final \
    --ignore '.sim/**,incoming/**' --req 'scenario4-check4'
