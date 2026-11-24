#!/bin/sh
# Exclusive path contention across three non-clobbering interfaces:
#   1. O_EXCL (`set -C`) concurrent create race + pre-existing target preservation
#   2. No-clobber hardlink (`ln` without `-f`) race
#   3. Linux `renameat2(..., RENAME_NOREPLACE)` race (records unsupported when absent)
set -eu

S="$RUN_DIR/excl"
rm -rf "$S" && mkdir -p "$S" installed
rm -f installed/cover.bin contended.bin

printf 'pre-existing content\n' > existing.bin
printf 'src-a %s\n' a      > "$S/src-a.bin"
printf 'src-b %s\n' bbbbbb > "$S/src-b.bin"

# 1. O_EXCL race: exactly one writer wins and its complete bytes land.
( set -C; printf 'branch-a-won\n' > contended.bin ) 2>/dev/null && touch "$S/winner-a" &
( set -C; printf 'branch-b-won\n' > contended.bin ) 2>/dev/null && touch "$S/winner-b" &
wait
a=0; b=0
[ -f "$S/winner-a" ] && a=1
[ -f "$S/winner-b" ] && b=1
[ $((a + b)) -eq 1 ] || { echo "WINNERS=$a$b"; exit 1; }
printf 'branch-a-won\n' > "$S/exp-a"
printf 'branch-b-won\n' > "$S/exp-b"
if [ "$a" -eq 1 ]; then cmp contended.bin "$S/exp-a"; else cmp contended.bin "$S/exp-b"; fi

# 2. O_EXCL against an existing file must fail without modifying existing bytes.
if ( set -C; printf 'must-not-appear\n' > existing.bin ) 2>/dev/null; then
  echo "NOCLOBBER-DID-NOT-FAIL"; exit 1
fi
printf 'pre-existing content\n' | cmp - existing.bin

# 3. No-clobber hardlink race inside the mount.
cp "$S/src-a.bin" src-a.bin
cp "$S/src-b.bin" src-b.bin
ln src-a.bin installed/cover.bin 2>/dev/null && touch "$S/ln-winner-a" &
ln src-b.bin installed/cover.bin 2>/dev/null && touch "$S/ln-winner-b" &
wait
a=0; b=0
[ -f "$S/ln-winner-a" ] && a=1
[ -f "$S/ln-winner-b" ] && b=1
[ $((a + b)) -eq 1 ] || { echo "LN-WINNERS=$a$b"; exit 1; }
if [ "$a" -eq 1 ]; then cmp installed/cover.bin src-a.bin; else cmp installed/cover.bin src-b.bin; fi
cmp src-a.bin "$S/src-a.bin" && cmp src-b.bin "$S/src-b.bin"

# 4. Linux `renameat2(RENAME_NOREPLACE)` race (guarded on Linux kernel ABI).
if command -v python3 >/dev/null 2>&1; then
  (
    cat > "$S/rennr.py" <<'EOF'
import ctypes, platform, sys
if not sys.platform.startswith("linux"):
    print("RENNR-UNSUPPORTED-OS")
    sys.exit(2)
NR = {"x86_64": 316, "aarch64": 276}.get(platform.machine())
if NR is None:
    print("RENNR-ERRNO-38")
    sys.exit(2)
libc = ctypes.CDLL(None, use_errno=True)
RENAME_NOREPLACE = 1
AT_FDCWD = -100
src, dst = sys.argv[1], sys.argv[2]
r = libc.syscall(NR, AT_FDCWD, src.encode(), AT_FDCWD, dst.encode(), RENAME_NOREPLACE)
if r != 0:
    e = ctypes.get_errno()
    print(f"RENNR-ERRNO-{e}")
    sys.exit(2 if e in (22, 38, 95) else 1)
print("RENNR-OK")
EOF
    mkdir -p noreplace
    printf 'nr-a %s\n' a   > noreplace/src-a.bin
    printf 'nr-b %s\n' bbb > noreplace/src-b.bin
    rm -f noreplace/target.bin noreplace/probe noreplace/probe-src
    printf 'probe\n' > noreplace/probe-src
    rc=0
    python3 "$S/rennr.py" noreplace/probe-src noreplace/probe >"$S/rennr-probe" 2>&1 || rc=$?
    if [ "$rc" -eq 2 ]; then
      echo "RENAME_NOREPLACE unsupported: $(cat "$S/rennr-probe")"
      rm -f noreplace/probe noreplace/probe-src
      exit 0
    fi
    rm -f noreplace/probe noreplace/probe-src
    python3 "$S/rennr.py" noreplace/src-a.bin noreplace/target.bin >"$S/ra" 2>&1 && touch "$S/rn-a" &
    python3 "$S/rennr.py" noreplace/src-b.bin noreplace/target.bin >"$S/rb" 2>&1 && touch "$S/rn-b" &
    wait
    a=0; b=0
    [ -f "$S/rn-a" ] && a=1
    [ -f "$S/rn-b" ] && b=1
    [ $((a + b)) -eq 1 ] || { echo "RN-WINNERS=$a$b"; exit 1; }
    if [ "$a" -eq 1 ]; then
      cmp noreplace/target.bin noreplace/src-b.bin && exit 1 || true
      [ -f noreplace/src-b.bin ]
    else
      [ -f noreplace/src-a.bin ]
    fi
  )
fi

sample final
drain --mode require-ok --timeout 120s

check manifest-equals --this control --against @final \
    --ignore '.sim/**,noreplace/**,contended.bin,installed/**' --req 'scenario5-step5'
