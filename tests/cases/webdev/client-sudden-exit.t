#!/bin/sh
# Fault matrix X5: sudden client SIGKILL mid-import. Recounts on a fresh-cache
# remount: any present file logged before the kill must be byte-exact (BAD=0),
# and replaying the batch converges cleanly.
set -eu
FILES=${FILES:-200}
export FILES

PROGRESS_LOG="$RUN_DIR/x5-progress.log"
export PROGRESS_LOG
rm -f "$PROGRESS_LOG"

async --timeout 10m -- sh -c '
mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf "sudden-exit payload %s\n" "$k" > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part" 2>/dev/null || true
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  echo "$k" >> "$PROGRESS_LOG"
  i=$((i + 1))
done
'
hold --for 3s

halt client
await --expect interrupted --timeout 60s

remount --cache fresh
cd "$MOUNT"

ok=0
lost=0
bad=0
if [ -f "$PROGRESS_LOG" ]; then
  for k in $(cat "$PROGRESS_LOG"); do
    if [ -f "src/file-$k.txt" ]; then
      printf 'sudden-exit payload %s\n' "$k" | cmp - "src/file-$k.txt" && ok=$((ok + 1)) || bad=$((bad + 1))
    else
      lost=$((lost + 1))
    fi
  done
fi
echo "rescan OK=$ok LOST=$lost BAD=$bad"
[ "$bad" -eq 0 ]

mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'sudden-exit payload %s\n' "$k" > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part"
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  i=$((i + 1))
done

printf 'life goes on\n' > after-recovery.txt
printf 'life goes on\n' | cmp - after-recovery.txt

sample final
drain --mode require-ok --timeout 180s

check artifact-contains --artifact script.out \
    --pattern 'rescan OK=[0-9]+ LOST=[0-9]+ BAD=0' --req 'X5'
check manifest-equals --this control --against @final \
    --ignore '.sim/**' --req 'X5'
