#!/bin/sh
# drive9/fuse/cas-conflict: Multi-client concurrent CAS write conflicts & atomic rename
# convergence across two live FUSE mounts sharing the same tenant, verified after fresh remount.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

MP_B="$RUN_DIR/client-b"
CACHE_B="$RUN_DIR/cache-b"
LOCAL_B="$RUN_DIR/local-b"
LOG_B="$RUN_DIR/mount-b.log"
mkdir -p "$MP_B" "$CACHE_B" "$LOCAL_B"

cleanup() {
    "$CLI" umount --timeout 30s "$MP_B" >/dev/null 2>&1 || true
    if [ "$(uname -s)" = "Linux" ]; then
        fusermount3 -uz "$MP_B" >/dev/null 2>&1 || fusermount -uz "$MP_B" >/dev/null 2>&1 || true
    else
        umount -f "$MP_B" >/dev/null 2>&1 || diskutil unmount force "$MP_B" >/dev/null 2>&1 || true
    fi
    if [ -n "${PID_B:-}" ] && kill -0 "$PID_B" 2>/dev/null; then
        kill -9 "$PID_B" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

mkdir -p "$MOUNT/cas"
printf 'seed-v0\n' > "$MOUNT/cas/shared.txt"
drain --mode require-ok --timeout 60s

"$CLI" mount --mode fuse --durability fsync --profile coding-agent \
    -server "$SERVER" -api-key "$API_KEY" \
    -cache-dir "$CACHE_B" --local-root "$LOCAL_B" \
    --foreground :/ "$MP_B" >"$LOG_B" 2>&1 &
PID_B=$!

i=0
while [ "$i" -lt 60 ]; do
    if [ -f "$MP_B/cas/shared.txt" ]; then
        break
    fi
    sleep 0.5
    i=$((i + 1))
done
test -f "$MP_B/cas/shared.txt"

export MP_B
check shell --req 'drive9-fuse-cas-concurrent' --claim 'two-mounts-concurrent-cas-writes-never-splice-bytes' -- python3 -c '
import concurrent.futures, errno, os, time

mnt_a = os.environ["MOUNT"]
mnt_b = os.environ["MP_B"]

# Payloads straddle both inline (< 64 KiB) and extent (>= 64 KiB) sizes; each writer
# publishes uniform blocks so any torn/spliced merge across clients is caught byte-for-byte.
INLINE_A = (b"A" * 8191 + b"\n") * 2   # 16 KiB (inline)
INLINE_B = (b"B" * 8191 + b"\n") * 2   # 16 KiB (inline)
LARGE_A = (b"X" * 4095 + b"\n") * 20   # 80 KiB (extent)
LARGE_B = (b"Y" * 4095 + b"\n") * 20   # 80 KiB (extent)

def retry_fs(fn):
    for attempt in range(12):
        try:
            return fn()
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EBUSY, errno.ESTALE) and attempt < 11:
                time.sleep(0.15)
                continue
            raise

def write_sync(root, tag, rel, payload, rounds):
    target = os.path.join(root, "cas", rel)
    for r in range(rounds):
        tmp = f"{target}.tmp.{tag}.{r}"
        def publish_once():
            with open(tmp, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
        retry_fs(publish_once)

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    futs = [
        pool.submit(write_sync, mnt_a, "a", "inline.bin", INLINE_A, 8),
        pool.submit(write_sync, mnt_b, "b", "inline.bin", INLINE_B, 8),
        pool.submit(write_sync, mnt_a, "a", "large.bin", LARGE_A, 4),
        pool.submit(write_sync, mnt_b, "b", "large.bin", LARGE_B, 4),
    ]
    for fut in futs:
        fut.result()
'

"$CLI" mount drain --timeout 120s "$MP_B" >/dev/null
drain --mode require-ok --timeout 120s
"$CLI" umount --timeout 30s "$MP_B" >/dev/null 2>&1 || true

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-fuse-cas-remount-whole-winner' --claim 'cas-winner-is-byte-pure-after-fresh-remount' -- python3 -c '
import os

mnt = os.environ["MOUNT"]
INLINE_A = (b"A" * 8191 + b"\n") * 2
INLINE_B = (b"B" * 8191 + b"\n") * 2
LARGE_A = (b"X" * 4095 + b"\n") * 20
LARGE_B = (b"Y" * 4095 + b"\n") * 20

got_inline = open(os.path.join(mnt, "cas", "inline.bin"), "rb").read()
assert got_inline in (INLINE_A, INLINE_B), f"corrupted inline CAS winner len={len(got_inline)}"

got_large = open(os.path.join(mnt, "cas", "large.bin"), "rb").read()
assert got_large in (LARGE_A, LARGE_B), f"corrupted large CAS winner len={len(got_large)}"
'
