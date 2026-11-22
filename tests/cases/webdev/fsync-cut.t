#!/bin/sh
# Connection blackhole during per-file synced saves (`write -> fsync -> rename`).
# Failed `fsync` must surface explicitly and skip `rename`; settled files stay
# untouched; replay after disarm converges with zero resurrected temp files.
set -eu
FILES=${FILES:-100}
export FILES

mkdir -p cal
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'cal %s\n' "$k" > "cal/$k.txt.part"
  chmod 644 "cal/$k.txt.part"
  fsync "cal/$k.txt.part"
  mv "cal/$k.txt.part" "cal/$k.txt"
  i=$((i + 1))
done

mkdir -p settled
i=0
while [ "$i" -lt 10 ]; do
  k=$((i + 1))
  printf 'settled payload %s\n' "$k" > "settled/formal-$k.txt.part"
  fsync "settled/formal-$k.txt.part"
  mv "settled/formal-$k.txt.part" "settled/formal-$k.txt"
  i=$((i + 1))
done
sample settled

fault cut blackhole --pattern 'PUT' --count 40 \
    --hold 8s --expect may-fail-inflight
arm cut

window --expect may-fail-inflight --timeout 10m -- sh -c '
mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if ! printf "import payload %s\n" "$k" > "src/file-$k.txt.part" 2>/dev/null; then
    echo "write-failed keep-temp file=$k"
    i=$((i + 1))
    continue
  fi
  chmod 644 "src/file-$k.txt.part" 2>/dev/null || true
  if ! fsync "src/file-$k.txt.part" 2>/dev/null; then
    echo "fsync-failed keep-temp file=$k"
    i=$((i + 1))
    continue
  fi
  if ! mv "src/file-$k.txt.part" "src/file-$k.txt" 2>/dev/null; then
    echo "rename-failed keep-temp file=$k"
  fi
  i=$((i + 1))
done
'
disarm cut
hold --for 3s

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if [ -e "src/file-$k.txt" ]; then
    printf 'import payload %s\n' "$k" | cmp - "src/file-$k.txt"
  fi
  i=$((i + 1))
done
find src -name '*.part' -delete 2>/dev/null || true
rm -f src/*.tmp

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  printf 'import payload %s\n' "$k" > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part"
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  i=$((i + 1))
done

sample final
drain --mode require-ok --timeout 120s
hold --for 5s
sample post-drain

check fault-fired --fault cut --req 'A-step2'
check manifest-contains --this @post-drain --against @settled \
    --paths 'settled/' --req 'A-check4'
check manifest-absent --this @post-drain --paths 'src/*.tmp,src/*.part' --req 'A-check4'
check manifest-equals --this control --against @final \
    --ignore '.sim/**,settled/**' --req 'A-check5'
