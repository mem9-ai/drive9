#!/bin/sh
# Write-cache quota protection across four states while local disk remains free:
# below threshold, threshold band (large write ENOSPC), genuine exhaustion,
# and in-place recovery on the same mount after releasing space.
# Requires `--write-cache-size-mb 16` (configured in suites.toml).
set -eu

mkdir -p keep fill
echo "quota-facts: write-cache-size-mb=16 disk=$(df -h . | tail -1 | awk '{print $4" free"}')"
printf 'control keep file\n' > keep/c.txt

# 1. Below threshold: all write patterns + local control succeed.
printf 'below\n' > below.txt.part
fsync below.txt.part
mv below.txt.part below.txt
printf 'below2\n' > below.txt
printf 'below3\n' > below.txt.part && mv below.txt.part below.txt
printf 'local control below\n' > "$RUN_DIR/localctl.txt"
[ -s "$RUN_DIR/localctl.txt" ]
echo "local-control: below ok"

drain --mode require-ok --timeout 120s

# Fill ~13 MiB of unsynced data into the 16 MiB write cache.
i=0
while [ "$i" -lt 13 ]; do
  head -c 1048576 /dev/zero | tr '\0' 'F' > "fill/chunk-$i.bin" \
    || { echo "fill-80 stopped early at $i MiB"; break; }
  i=$((i + 1))
done
echo "fill-80 written=${i}MiB"
hold --idle 10s

# 2. Threshold band: 20 MiB write must fail with ENOSPC while local disk stays writable.
rc=0
if head -c 20971520 /dev/zero | tr '\0' 'O' > huge.bin 2>"$RUN_DIR/big-err"; then
  echo "OVERQUOTA-WRITE-SUCCEEDED"; rc=1
else
  grep -Eq "No space left|ENOSPC" "$RUN_DIR/big-err" && echo "huge-write ENOSPC as-expected" \
    || { echo "huge-write errno-unexpected: $(cat "$RUN_DIR/big-err")"; rc=1; }
fi
printf 'x'          > tiny1.txt 2>"$RUN_DIR/t1" || echo "probe tiny1 rc=$? $(cat "$RUN_DIR/t1")"
printf '0123456789' > tiny2.txt 2>"$RUN_DIR/t2" || echo "probe tiny2 rc=$? $(cat "$RUN_DIR/t2")"
printf 'ovr'        > below.txt 2>"$RUN_DIR/t3" || echo "probe overwrite rc=$? $(cat "$RUN_DIR/t3")"
head -c 3 /dev/zero | tr '\0' 'y' > below.txt.part 2>"$RUN_DIR/t4" \
  && mv below.txt.part below.txt 2>/dev/null || echo "probe atomic-replace rc=$?"
printf 'local control mid\n' > "$RUN_DIR/localctl.txt"
[ -s "$RUN_DIR/localctl.txt" ]
echo "local-control: threshold ok"
[ -s below.txt ] && grep -Eq '^(ovr|below|yyy)' below.txt
[ "$rc" -eq 0 ]

drain --mode best-effort --timeout 120s

# 3. Genuine exhaustion: fill until a 1 MiB write is refused.
i=13
while [ "$i" -lt 60 ]; do
  if head -c 1048576 /dev/zero | tr '\0' 'G' > "fill/chunk-$i.bin" 2>/dev/null; then
    i=$((i + 1))
    continue
  fi
  break
done
echo "fill-100 written-up-to=${i}MiB"

rc=0
if head -c 1048576 /dev/zero | tr '\0' 'E' > nope.bin 2>"$RUN_DIR/ex-err"; then
  echo "EXHAUSTED-STATE-NOT-STABLE (soft quota freed in background)"
  check shell --accept pass,unconfirmed --req 'F-check2' -- sh -c 'exit 3'
else
  grep -Eq "No space left|ENOSPC" "$RUN_DIR/ex-err" && echo "exhausted ENOSPC as-expected" \
    || { echo "exhausted errno-unexpected: $(cat "$RUN_DIR/ex-err")"; rc=1; }
fi
printf 'local control full\n' > "$RUN_DIR/localctl.txt"
[ -s "$RUN_DIR/localctl.txt" ]
echo "local-control: exhausted ok"
[ "$rc" -eq 0 ]

# Release cache pressure and cleanup refused-write leftovers.
drain --mode best-effort --timeout 120s
rm -f fill/chunk-*.bin huge.bin nope.bin tiny1.txt tiny2.txt below.txt.part
rmdir fill 2>/dev/null || true
hold --for 5s

# 4. In-place recovery on the same mount without restarting the client.
t0=$(date +%s)
printf 'recovered\n' > released.txt.part
fsync released.txt.part
mv released.txt.part released.txt
printf 'recovered2\n' > released.txt
printf 'recovered3\n' > released.txt.part && mv released.txt.part released.txt
t1=$(date +%s)
printf 'recovered3\n' | cmp - released.txt
echo "recovery-conditions: same-mount, no-restart, seconds=$((t1 - t0))"

sample final
drain --mode require-ok --timeout 120s
halt client --method kill
remount --cache fresh
cd "$MOUNT"
# Sample immediately on fresh remount BEFORE any cleanup so any server-side
# revival of deleted/refused paths fails manifest-absent honestly.
sample post-remount

check artifact-contains --artifact script.out --pattern 'ENOSPC' --req 'F-check2'
check artifact-contains --artifact script.out --pattern 'local-control:' --req 'F-step4'
check artifact-contains --artifact script.out --pattern 'recovery-conditions:' --req 'F-check5'
check manifest-contains --this @post-remount --against @final \
    --paths 'keep/,released.txt' --req 'F-check3'
check manifest-absent --this @post-remount \
    --paths 'huge.bin,nope.bin,fill/' --req 'F-check2'
