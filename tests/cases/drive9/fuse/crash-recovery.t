#!/bin/sh
# drive9/fuse/crash-recovery: Interactive-durability daemon SIGKILL recovery gate.
# Run with `--durability interactive`.
# - fsync'd small files + fsync'd-then-unlinked `doomed.txt` + large ShadowSpill file
# - immediate `halt client --method kill` (SIGKILL, no graceful flush)
# - `remount --cache same` replays local journal.wal & ShadowSpill to the server
# - `remount --cache fresh` verifies every surviving file byte-for-byte from remote
#   and proves `doomed.txt` did NOT resurrect
set -eu

SMALL_FILES="${CRASH_SMALL_FILES:-10}"
SMALL_KB="${CRASH_SMALL_KB:-16}"
LARGE_MB="${CRASH_LARGE_MB:-32}"
EXPECTED_MANIFEST="$RUN_DIR/crash-expected.json"
export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"
export SMALL_FILES SMALL_KB LARGE_MB EXPECTED_MANIFEST

check shell --req 'drive9-crash-pre-workload' --claim 'crash-write-fsync-and-unlink-doomed' -- python3 -c '
import hashlib, json, os

root = os.path.join(os.environ["MOUNT"], "crash-work")
os.makedirs(root, exist_ok=True)
small_files = int(os.environ["SMALL_FILES"])
small_kb = int(os.environ["SMALL_KB"])
large_mb = int(os.environ["LARGE_MB"])
manifest = {}

def payload(name, size):
    seed = hashlib.sha256(name.encode()).digest()
    out = bytearray()
    c = 0
    while len(out) < size:
        out.extend(hashlib.sha256(seed + c.to_bytes(8, "big")).digest())
        c += 1
    return bytes(out[:size])

def write_fsync(rel, data):
    p = os.path.join(root, rel)
    fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        off = 0
        while off < len(data):
            off += os.write(fd, data[off:off + (1 << 20)])
        os.fsync(fd)
    finally:
        os.close(fd)
    manifest[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

for i in range(small_files):
    rel = f"small-{i:02d}.bin"
    write_fsync(rel, payload(rel, small_kb * 1024))

write_fsync("doomed.txt", b"doomed-by-unlink\n")
os.unlink(os.path.join(root, "doomed.txt"))
del manifest["doomed.txt"]

write_fsync("large-spill.bin", payload("large-spill.bin", large_mb << 20))

with open(os.environ["EXPECTED_MANIFEST"], "w") as f:
    json.dump(manifest, f, sort_keys=True)
'

# SIGKILL the mount daemon immediately, then remount reusing the SAME cache dir
# so journal.wal and ShadowSpill replay to the server.
halt client --method kill
remount --cache same
cd "$MOUNT"

drain --mode require-ok --timeout 240s
check sync-ok --req 'drive9-crash-wal-replay-drain'

# Remount with a FRESH cache dir so reads come strictly from remote truth.
remount --cache fresh
cd "$MOUNT"
sample recovered_fresh

check manifest-absent --this @recovered_fresh --paths 'crash-work/doomed.txt' \
    --req 'drive9-crash-no-resurrection'

check shell --req 'drive9-crash-remote-manifest' --claim 'crash-recovered-files-match-sha256' -- python3 -c '
import hashlib, json, os

root = os.path.join(os.environ["MOUNT"], "crash-work")
with open(os.environ["EXPECTED_MANIFEST"]) as f:
    manifest = json.load(f)

assert not os.path.exists(os.path.join(root, "doomed.txt")), "doomed.txt resurrected!"
for rel, want in sorted(manifest.items()):
    data = open(os.path.join(root, rel), "rb").read()
    got = hashlib.sha256(data).hexdigest()
    assert len(data) == want["size"] and got == want["sha256"], (rel, len(data), got, want)
'
