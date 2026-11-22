#!/bin/sh
# Ambiguous lost-response faults (`drop-response`: upstream executes, proxy drops
# the 2xx response with TCP RST) across write, hardlink, rename, and unlink.
# Verifies idempotent convergence on explicit retry and absence of late revival.
set -eu

fault drop-write drop-response --pattern 'PUT' --count 3 \
    --expect one-shot-ambiguous
fault drop-hardlink drop-response --pattern 'POST /v1/fs' --count 1 \
    --expect one-shot-ambiguous
fault drop-rename drop-response --pattern 'POST /v1/fs' --count 1 \
    --expect one-shot-ambiguous
fault drop-unlink drop-response --pattern 'DELETE' --count 1 \
    --expect one-shot-ambiguous

mkdir -p out
head -c 4096 /dev/zero | tr '\0' 'w' > out/ctl.bin

# 1. Write under one-shot response drop -> explicit idempotent retry.
arm drop-write
window --expect one-shot-ambiguous -- sh -c '
i=0
while [ "$i" -lt 8 ]; do
  k=$((i + 1))
  if head -c 16384 /dev/zero | tr "\0" "c" > "out/file-$k.bin"; then
    echo "orig-write ok file=$k"
  else
    echo "orig-write FAILED file=$k rc=$?"
  fi
  i=$((i + 1))
done
'
disarm drop-write
hold --for 5s

i=0
while [ "$i" -lt 8 ]; do
  k=$((i + 1))
  head -c 16384 /dev/zero | tr '\0' 'c' > "out/file-$k.bin.part" && mv "out/file-$k.bin.part" "out/file-$k.bin"
  head -c 16384 /dev/zero | tr '\0' 'c' | cmp - "out/file-$k.bin"
  i=$((i + 1))
done

# 2. Hardlink under one-shot response drop -> explicit retry (EEXIST valid if applied).
arm drop-hardlink
window --expect one-shot-ambiguous -- sh -c '
if ln out/ctl.bin out/ctl.link 2>"$RUN_DIR/lnerr"; then
  echo "orig-hardlink ok"
else
  echo "orig-hardlink FAILED: $(cat "$RUN_DIR/lnerr")"
fi
'
disarm drop-hardlink
hold --for 5s

if ln out/ctl.bin out/ctl.link 2>/dev/null; then
  echo "link-retry-created"
elif [ -e out/ctl.link ]; then
  echo "link-retry-eexists-already-there"
else
  exit 1
fi
cmp out/ctl.link out/ctl.bin

# 3. Rename under one-shot response drop -> explicit retry & source disappearance.
printf 'rename-payload\n' > out/renamesrc.bin
arm drop-rename
window --expect one-shot-ambiguous -- sh -c '
if mv out/renamesrc.bin out/renamed.bin 2>"$RUN_DIR/mverr"; then
  echo "orig-rename ok"
else
  echo "orig-rename FAILED: $(cat "$RUN_DIR/mverr")"
fi
'
disarm drop-rename
hold --for 5s

if mv out/renamesrc.bin out/renamed.bin 2>/dev/null; then
  echo "rename-retry-moved-now"
elif [ -f out/renamed.bin ]; then
  echo "rename-retry-already-moved"
else
  exit 1
fi
n=0
while [ -e out/renamesrc.bin ] && [ "$n" -lt 18 ]; do n=$((n + 1)); sleep 5; done
if [ -e out/renamesrc.bin ]; then
  echo "NOT-CONVERGED: out/renamesrc.bin still present after $((n * 5))s"
  check shell --accept pass,unconfirmed --req 'C-step4' -- sh -c 'exit 3'
else
  check shell --req 'C-step4' -- test ! -e out/renamesrc.bin
fi
printf 'rename-payload\n' | cmp - out/renamed.bin

# 4. Unlink under one-shot response drop -> explicit retry & fresh remount check.
printf 'unlink-me\n' > out/doomed.bin
drain --mode best-effort --timeout 60s

arm drop-unlink
window --expect one-shot-ambiguous -- sh -c '
if rm out/doomed.bin 2>"$RUN_DIR/rmerr"; then
  echo "orig-unlink ok"
else
  echo "orig-unlink FAILED: $(cat "$RUN_DIR/rmerr")"
fi
'
drain --mode best-effort --timeout 60s
disarm drop-unlink
hold --for 5s

if rm out/doomed.bin 2>/dev/null; then
  echo "unlink-retry-deleted-now"
elif [ ! -e out/doomed.bin ]; then
  echo "unlink-retry-already-gone"
else
  exit 1
fi
[ ! -e out/doomed.bin ]

hold --for 10s
drain --mode best-effort --timeout 120s
halt client --method kill
remount --cache fresh
cd "$MOUNT"
n=0
while ! ls out >/dev/null 2>&1 && [ "$n" -lt 36 ]; do n=$((n + 1)); sleep 5; done
sample post-drain --optional

check fault-fired --fault drop-write --req 'C-step2'
check fault-fired --fault drop-hardlink --req 'C-step2'
check fault-fired --fault drop-rename --req 'C-step2'
check fault-fired --fault drop-unlink --accept pass,blocked --req 'C-step2'
check manifest-absent --this @post-drain --paths 'out/doomed.bin' --req 'C-check2'
check manifest-equals --this control --against @post-drain \
    --ignore '.sim/**,out/doomed.bin' --req 'C-check1'
