#!/bin/sh
# drive9/fuse/patch-storage-class: Verifies that `X-Dat9-Storage-Type` prevents
# `patch_unsupported_target` loops when a file seeded inline (`db9`) under a high
# `inline_threshold` (256 KiB) is partially overwritten after the server restarts
# with a lower `inline_threshold` (50 KiB), healing the object to `s3`.
# Run with `--no-mount` (manages its own server restarts via `$SIMULATOR_BIN server`).
set -eu

for d in /Library/Filesystems/macfuse.fs/Contents/Resources /usr/local/bin /opt/homebrew/bin; do
    if [ -x "$d/mount_macfuse" ] || [ -x "$d/mount_fusefs" ]; then
        PATH="$d:$PATH"; export PATH; break
    fi
done

require --req 'drive9-patch-storage-prereq' --claim 'fuse-host-available' -- sh -c '
    if [ "$(uname -s)" = "Linux" ]; then
        [ -e /dev/fuse ] && { command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1; }
    elif [ "$(uname -s)" = "Darwin" ]; then
        command -v mount_macfuse >/dev/null 2>&1 || command -v mount_fusefs >/dev/null 2>&1
    else
        exit 2
    fi
'

SRV_DIR="$RUN_DIR/patch-srv"
SRV_LOG="$RUN_DIR/patch-srv.log"
MP="$RUN_DIR/patch-mnt"
SEED_FILE="$RUN_DIR/seed.bin"
EXP_FILE="$RUN_DIR/expected.bin"
SRV_PID=""

cleanup() {
    if [ -n "${PUB_URL:-}" ] && [ -n "${LOCAL_KEY:-}" ]; then
        DRIVE9_SERVER="$PUB_URL" DRIVE9_API_KEY="$LOCAL_KEY" "$CLI" umount --timeout 20s "$MP" >/dev/null 2>&1 || true
    fi
    if [ "$(uname -s)" = "Linux" ]; then
        fusermount3 -uz "$MP" >/dev/null 2>&1 || fusermount -uz "$MP" >/dev/null 2>&1 || true
    else
        umount -f "$MP" >/dev/null 2>&1 || diskutil unmount force "$MP" >/dev/null 2>&1 || true
    fi
    if [ -n "$SRV_PID" ] && kill -0 "$SRV_PID" 2>/dev/null; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
PUB_URL="http://127.0.0.1:$PORT"
export PORT PUB_URL SRV_DIR SRV_LOG MP SEED_FILE EXP_FILE

# Phase 1: Start server with 256 KiB inline threshold, seed 200 KiB inline (`db9`).
"$SIMULATOR_BIN" server --port "$PORT" --dir "$SRV_DIR" --inline-threshold 262144 >"$SRV_LOG" 2>&1 &
SRV_PID=$!

python3 - "$PUB_URL" "$SEED_FILE" "$RUN_DIR/key.txt" <<'PY'
import json, sys, time, urllib.request
base, seed_path, key_out = sys.argv[1], sys.argv[2], sys.argv[3]
deadline = time.time() + 30
while time.time() < deadline:
    try:
        if urllib.request.urlopen(base + "/healthz", timeout=2).status == 200:
            break
    except Exception:
        time.sleep(0.2)
req = urllib.request.Request(base + "/v1/provision", method="POST")
key = json.load(urllib.request.urlopen(req, timeout=10))["api_key"]
open(key_out, "w").write(key)
data = bytes((i % 256) for i in range(204800))
open(seed_path, "wb").write(data)
put = urllib.request.Request(base + "/v1/fs/patch-e2e/seed.bin", data=data, method="PUT", headers={"Authorization": f"Bearer {key}"})
assert urllib.request.urlopen(put, timeout=15).status == 200
head = urllib.request.Request(base + "/v1/fs/patch-e2e/seed.bin", method="HEAD", headers={"Authorization": f"Bearer {key}"})
with urllib.request.urlopen(head, timeout=10) as r:
    assert r.headers.get("X-Dat9-Storage-Type") == "db9", dict(r.headers)
PY

LOCAL_KEY=$(cat "$RUN_DIR/key.txt")
export LOCAL_KEY

# Phase 2: Restart server on same state dir with 50 KiB inline threshold.
kill "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
"$SIMULATOR_BIN" server --port "$PORT" --dir "$SRV_DIR" --inline-threshold 51200 >>"$SRV_LOG" 2>&1 &
SRV_PID=$!
python3 - "$PUB_URL" <<'PY'
import sys, time, urllib.request
deadline = time.time() + 30
while time.time() < deadline:
    try:
        if urllib.request.urlopen(sys.argv[1] + "/healthz", timeout=2).status == 200:
            sys.exit(0)
    except Exception:
        time.sleep(0.2)
sys.exit(1)
PY

LOG_MARK=$(wc -l < "$SRV_LOG" | tr -d ' ')
export LOG_MARK

check shell --req 'drive9-patch-storage-overwrite-and-heal' --claim 'partial-overwrite-avoids-db9-patch-and-heals-to-s3' -- sh -c '
    set -eu
    mkdir -p "$MP"
    DRIVE9_SERVER="$PUB_URL" DRIVE9_API_KEY="$LOCAL_KEY" \
        "$CLI" mount --mode=fuse --profile none --durability write-sync ":/" "$MP" >"$RUN_DIR/mount.log" 2>&1

    python3 - "$MP/patch-e2e/seed.bin" "$SEED_FILE" "$EXP_FILE" <<'\''PY'\''
import os, sys
target, seed_path, exp_path = sys.argv[1], sys.argv[2], sys.argv[3]
patch = bytes((i * 7) % 256 for i in range(4096))
fd = os.open(target, os.O_WRONLY)
try:
    os.pwrite(fd, patch, 65536)
    os.fsync(fd)
finally:
    os.close(fd)
data = bytearray(open(seed_path, "rb").read())
data[65536:65536 + 4096] = patch
open(exp_path, "wb").write(data)
PY

    patch_errs=$(tail -n +"$((LOG_MARK + 1))" "$SRV_LOG" | grep -c "patch_unsupported_target" || true)
    test "$patch_errs" = "0"

    python3 - "$PUB_URL" "$LOCAL_KEY" "$EXP_FILE" <<'\''PY'\''
import hashlib, sys, urllib.request
base, key, exp_path = sys.argv[1], sys.argv[2], sys.argv[3]
want = hashlib.sha256(open(exp_path, "rb").read()).hexdigest()
req = urllib.request.Request(base + "/v1/fs/patch-e2e/seed.bin", headers={"Authorization": f"Bearer {key}"})
got = hashlib.sha256(urllib.request.urlopen(req, timeout=15).read()).hexdigest()
assert got == want, (got, want)
head = urllib.request.Request(base + "/v1/fs/patch-e2e/seed.bin", method="HEAD", headers={"Authorization": f"Bearer {key}"})
with urllib.request.urlopen(head, timeout=10) as r:
    assert r.headers.get("X-Dat9-Storage-Type") == "s3", dict(r.headers)
PY
    DRIVE9_SERVER="$PUB_URL" DRIVE9_API_KEY="$LOCAL_KEY" "$CLI" umount --timeout 30s "$MP"
'
