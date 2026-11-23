#!/bin/sh
# Anomaly matrix X4: SIGTERM cancellation of a batch writer mid-publish.
# Every published target must be byte-exact (`BAD=0`), explicit `.part` cleanup
# removes leftovers, and replaying the batch converges cleanly.
set -eu
FILES=${FILES:-200}
export FILES

STATUS_FILE="$RUN_DIR/x4-cancel-status"
export STATUS_FILE
rm -f "$STATUS_FILE"

async --timeout 20m -- sh -c '
mkdir -p bulk
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf "cancel payload %s\n" "$k" > "bulk/file-$k.txt.part"
  fsync "bulk/file-$k.txt.part" 2>/dev/null || true
  mv "bulk/file-$k.txt.part" "bulk/file-$k.txt"
  echo "$k" > "$STATUS_FILE"
  i=$((i + 1))
done
'
hold --for 2s

halt workload --method term
await --expect interrupted --timeout 60s

mkdir -p bulk
present=0
missing=0
bad=0
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if [ -f "bulk/file-$k.txt" ]; then
    if printf 'cancel payload %s\n' "$k" | cmp - "bulk/file-$k.txt"; then
      present=$((present + 1))
    else
      bad=$((bad + 1))
    fi
  else
    missing=$((missing + 1))
  fi
  i=$((i + 1))
done
echo "audit PRESENT=$present MISSING=$missing BAD=$bad"
[ "$bad" -eq 0 ]

find bulk -name '*.part' -delete 2>/dev/null || true
rm -f bulk/*.tmp

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'cancel payload %s\n' "$k" > "bulk/file-$k.txt.part"
  fsync "bulk/file-$k.txt.part"
  mv "bulk/file-$k.txt.part" "bulk/file-$k.txt"
  i=$((i + 1))
done

sample final
drain --mode require-ok --timeout 180s

check artifact-contains --artifact script.out \
    --pattern 'audit PRESENT=[0-9]+ MISSING=[0-9]+ BAD=0' --req 'X4'
check manifest-equals --this control --against @final \
    --ignore '.sim/**,bulk/**/*.tmp,bulk/**/*.part' --req 'X4'
