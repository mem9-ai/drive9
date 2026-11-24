#!/bin/sh
# Concurrent bulk asset installation via copy, hardlink, and same-dir rename.
# Verifies zero cross-asset corruption, temp-file cleanup isolation, nlink
# drop-back to 1 after unlinking hardlink temps, and remote convergence.
set -eu
ASSETS=${ASSETS:-20}
WORKERS=${WORKERS:-1}
export ASSETS WORKERS

T="$RUN_DIR/asset-install"
rm -rf "$T" && mkdir -p "$T/assets" incoming installed

manifest_dir() {
  dir=$1; prefix=$2; strip_re=$3
  (
    cd "$dir"
    for f in ${prefix}*.bin; do
      [ -e "$f" ] || continue
      name=$f
      if [ -n "$strip_re" ]; then
        name=$(printf '%s\n' "$f" | sed "$strip_re")
      fi
      printf '%s  %s\n' "$(d9-stat sha256 "$f")" "$name"
    done
  ) | sort -k2
}

i=0
while [ "$i" -lt "$ASSETS" ]; do
  k=$((i + 1))
  m=$((i % 3))
  case $m in
    0) head -c 4096     /dev/zero | tr '\0' 'a' > "$T/assets/a-$k.bin" ;;
    1) head -c 1048576  /dev/zero | tr '\0' 'b' > "$T/assets/a-$k.bin" ;;
    2) head -c 20971520 /dev/zero | tr '\0' 'c' > "$T/assets/a-$k.bin" ;;
  esac
  i=$((i + 1))
done
manifest_dir "$T/assets" "a-" "" > "$T/exp-copy"
[ "$(wc -l < "$T/exp-copy")" -eq "$ASSETS" ]

# 1. Copy install: temp -> copy -> verify in separate process -> delete temp -> re-verify.
seq 1 "$ASSETS" | xargs -P "$WORKERS" -I{} sh -c '
  k={}
  cp "'"$T"'/assets/a-$k.bin" "incoming/a-$k.tmp" && cp "incoming/a-$k.tmp" "installed/copy-a-$k.bin"
'
sh -c '
  set -e
  T=$1; N=$2; k=1
  while [ "$k" -le "$N" ]; do
    cmp "$T/assets/a-$k.bin" "installed/copy-a-$k.bin"
    rm -f "incoming/a-$k.tmp"
    cmp "$T/assets/a-$k.bin" "installed/copy-a-$k.bin"
    k=$((k + 1))
  done
' -- "$T" "$ASSETS"
manifest_dir installed "copy-a-" 's/^copy-a-/a-/' > "$T/got-copy"
cmp "$T/got-copy" "$T/exp-copy"
echo "copy install verified n=$ASSETS w=$WORKERS"

# 2. Hardlink install: temp (inside mount) -> ln -> delete temp -> verify nlink==1.
hl_skip=0
cp "$T/assets/a-1.bin" incoming/link-src-probe
probe=$(ln incoming/link-src-probe incoming/link-probe 2>&1) || hl_skip=1
rm -f incoming/link-src-probe incoming/link-probe
if [ "$hl_skip" -eq 0 ]; then
  seq 1 "$ASSETS" | xargs -P "$WORKERS" -I{} sh -c '
    k={}
    cp "'"$T"'/assets/a-$k.bin" "incoming/a-$k.tmp" && ln "incoming/a-$k.tmp" "installed/link-a-$k.bin"
  '
  sh -c '
    set -e
    T=$1; N=$2; k=1
    while [ "$k" -le "$N" ]; do
      cmp "$T/assets/a-$k.bin" "installed/link-a-$k.bin"
      rm -f "incoming/a-$k.tmp"
      cmp "$T/assets/a-$k.bin" "installed/link-a-$k.bin"
      nlink=$(d9-stat nlink "installed/link-a-$k.bin")
      [ "$nlink" -eq 1 ] || { echo "NLINK-NOT-ONE link-$k nlink=$nlink"; exit 1; }
      k=$((k + 1))
    done
  ' -- "$T" "$ASSETS"
  manifest_dir installed "link-a-" 's/^link-a-/a-/' > "$T/got-link"
  cmp "$T/got-link" "$T/exp-copy"
  echo "hardlink install verified nlink-resolved n=$ASSETS w=$WORKERS"
else
  echo "hardlink unsupported: $probe"
fi

# 3. Same-directory atomic rename install: temp -> mv -> verify temp gone + bytes match.
seq 1 "$ASSETS" | xargs -P "$WORKERS" -I{} sh -c '
  k={}
  cp "'"$T"'/assets/a-$k.bin" "incoming/a-$k.tmp" && mv "incoming/a-$k.tmp" "installed/ren-a-$k.bin"
'
sh -c '
  set -e
  T=$1; N=$2; k=1
  while [ "$k" -le "$N" ]; do
    [ ! -e "incoming/a-$k.tmp" ] || { echo "TMP-SURVIVES ren-$k"; exit 1; }
    cmp "$T/assets/a-$k.bin" "installed/ren-a-$k.bin"
    k=$((k + 1))
  done
' -- "$T" "$ASSETS"
manifest_dir installed "ren-a-" 's/^ren-a-/a-/' > "$T/got-ren"
cmp "$T/got-ren" "$T/exp-copy"
echo "rename install verified n=$ASSETS w=$WORKERS"

sample final
drain --mode require-ok --timeout 300s
sample remote-final --as remote

check manifest-equals --this control --against @final \
    --ignore '.sim/**,incoming/**' --req 'scenario4-check1'
check manifest-equals --this @remote-final --against @final \
    --ignore '.sim/**,incoming/**' --req 'scenario4-step4'
