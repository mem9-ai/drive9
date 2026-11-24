#!/bin/sh
# Fault matrix X3: HTTP-layer throttling (`429 Too Many Requests` + `Retry-After`)
# during batch publishes, followed by clean post-window replay and convergence.
set -eu
FILES=${FILES:-80}
export FILES

fault throttle http-limit --pattern 'PUT' --count 30 \
    --hold 6s --expect may-fail-inflight

arm throttle
window --expect may-fail-inflight --timeout 10m -- sh -c '
mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if { head -c 65536 /dev/zero | tr "\0" x; printf "throttled payload %s\n" "$k"; } > "src/file-$k.txt.part" 2>"$RUN_DIR/tl-err"; then
    chmod 644 "src/file-$k.txt.part" 2>/dev/null || true
    fsync "src/file-$k.txt.part" 2>/dev/null || true
    mv "src/file-$k.txt.part" "src/file-$k.txt" 2>/dev/null || echo "throttle mv-failed file=$k"
  else
    echo "throttle write-failed file=$k err=$(tail -1 "$RUN_DIR/tl-err")"
  fi
  i=$((i + 1))
done
'
disarm throttle
find src -name '*.part' -delete 2>/dev/null || true

mkdir -p src
i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  { head -c 65536 /dev/zero | tr '\0' x; printf 'throttled payload %s\n' "$k"; } > "src/file-$k.txt.part"
  fsync "src/file-$k.txt.part"
  mv "src/file-$k.txt.part" "src/file-$k.txt"
  i=$((i + 1))
done

i=0
while [ "$i" -lt "$FILES" ]; do
  k=$((i + 1))
  if [ -e "src/file-$k.txt" ]; then
    { head -c 65536 /dev/zero | tr '\0' x; printf 'throttled payload %s\n' "$k"; } > "$RUN_DIR/x3-exp"
    cmp "$RUN_DIR/x3-exp" "src/file-$k.txt"
  else
    echo "MISSING-AFTER-REPLAY file=$k"; exit 1
  fi
  i=$((i + 1))
done

sample final
hold --for 10s
drain --mode best-effort --timeout 180s

check fault-fired --fault throttle --req 'X3'
check artifact-contains --artifact injector/proxy.jsonl \
    --pattern 'http-limit|429|503|Retry-After' --req 'X3'
check manifest-equals --this control --against @final \
    --ignore '.sim/**' --req 'X3'
