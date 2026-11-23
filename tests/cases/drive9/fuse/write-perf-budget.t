#!/bin/sh
# drive9/fuse/write-perf-budget: FUSE write-path remote-op & fsync budget gate.
# Run with `--durability interactive --perf`.
# Asserts deterministic op-count budgets (write, stat, list, mutation, commit
# retries/errors, and local fsync latency) from the shutdown perf summary.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

WRITE_PERF_FILES="${WRITE_PERF_FILES:-64}"
WRITE_PERF_FILE_KB="${WRITE_PERF_FILE_KB:-64}"
WRITE_PERF_OVERWRITES="${WRITE_PERF_OVERWRITES:-32}"
WRITE_PERF_RENAMES="${WRITE_PERF_RENAMES:-16}"
WRITE_PERF_MEDIUM_MB="${WRITE_PERF_MEDIUM_MB:-8}"
MANIFEST="$RUN_DIR/expected-manifest.json"
WORK_REL="wperf-$$"
export WRITE_PERF_FILES WRITE_PERF_FILE_KB WRITE_PERF_OVERWRITES WRITE_PERF_RENAMES WRITE_PERF_MEDIUM_MB MANIFEST WORK_REL

check shell --req 'drive9-wperf-workload' --claim 'fsync-heavy-creates-overwrites-renames-and-medium-file' -- sh -c '
    python3 - "$MOUNT/$WORK_REL" "$MANIFEST" \
        "$WRITE_PERF_FILES" "$WRITE_PERF_FILE_KB" \
        "$WRITE_PERF_OVERWRITES" "$WRITE_PERF_RENAMES" "$WRITE_PERF_MEDIUM_MB" <<'\''PY'\''
import hashlib, json, os, sys

root, manifest_path = sys.argv[1], sys.argv[2]
n_files, file_kb, n_over, n_ren, med_mb = map(int, sys.argv[3:])
os.makedirs(root, exist_ok=True)
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

sz = file_kb * 1024
for i in range(n_files):
    rel = f"small-{i:03d}.bin"
    write_fsync(rel, payload(rel, sz))
for i in range(n_over):
    rel = f"small-{i:03d}.bin"
    write_fsync(rel, payload(rel + "-v2", sz))
for i in range(n_over, n_over + n_ren):
    old, new = f"small-{i:03d}.bin", f"renamed-{i:03d}.bin"
    os.rename(os.path.join(root, old), os.path.join(root, new))
    manifest[new] = manifest.pop(old)
write_fsync("medium.bin", payload("medium.bin", med_mb << 20))
with open(manifest_path, "w") as f:
    json.dump(manifest, f, sort_keys=True)
PY
'

drain --mode require-ok --timeout 240s
check sync-ok --req 'drive9-wperf-drain'

# Unmount mount-1 cleanly via remount so the shutdown perf summary flushes to mount-1.log.
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-wperf-remote-parity' --claim 'fresh-remount-manifest-sha256-parity' -- sh -c '
    python3 - "$MOUNT/$WORK_REL" "$MANIFEST" <<'\''PY'\''
import hashlib, json, os, sys
root, manifest_path = sys.argv[1], sys.argv[2]
manifest = json.load(open(manifest_path))
for rel, want in sorted(manifest.items()):
    data = open(os.path.join(root, rel), "rb").read()
    got = hashlib.sha256(data).hexdigest()
    assert len(data) == want["size"] and got == want["sha256"], (rel, len(data), got, want)
PY
'

check shell --req 'drive9-wperf-budgets' --claim 'remote-op-and-fsync-latency-budgets' -- sh -c '
    python3 - "$RUN_DIR/mount-1.log" \
        "$WRITE_PERF_FILES" "$WRITE_PERF_OVERWRITES" "$WRITE_PERF_RENAMES" <<'\''PY'\''
import re, sys

log_path = sys.argv[1]
n_files, n_over, n_ren = map(int, sys.argv[2:])
UNITS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}

def parse_dur(txt):
    return sum(float(m.group(1)) * UNITS[m.group(2)] for m in re.finditer(r"([0-9.]+)(h|ms|m|s|µs|us|ns)", txt))

uptime = None
remote, fuse = {}, {}
commit = None
with open(log_path, "r", errors="replace") as f:
    for line in f:
        m = re.search(r"drive9: FUSE perf summary uptime=(\S+)", line)
        if m:
            uptime = parse_dur(m.group(1))
            remote.clear(); fuse.clear(); commit = None
            continue
        m = re.search(r"drive9: perf (remote|fuse) (\S+) count=(\d+) errors=(\d+) bytes=(\d+) avg=(\S+)", line)
        if m:
            (remote if m.group(1) == "remote" else fuse)[m.group(2)] = {
                "count": int(m.group(3)), "errors": int(m.group(4)), "avg_s": parse_dur(m.group(6))
            }
            continue
        m = re.search(r"drive9: perf commit enqueue=(\d+) enqueue_errors=(\d+) retries=(\d+) success=(\d+) failure=(\d+)", line)
        if m:
            commit = {k: int(m.group(i)) for i, k in enumerate(["enqueue", "enqueue_errors", "retries", "success", "failure"], 1)}

assert uptime is not None and commit is not None, f"perf summary missing in {log_path} (run with --perf)"
secs = int(uptime) + 1
commits = n_files + n_over + 1
cnt = lambda t, k: t.get(k, {"count": 0})["count"]

checks = [
    ("remote_write", cnt(remote, "write"), 20 + 4 * commits),
    ("remote_stat", cnt(remote, "stat"), 50 + 8 * (n_files + 1) + 5 * secs),
    ("remote_list", cnt(remote, "list"), 30 + 3 * secs),
    ("remote_mutation", cnt(remote, "mutation"), 20 + 4 * n_ren),
    ("commit_retries", commit["retries"], 5),
    ("commit_enqueue_errors", commit["enqueue_errors"], 0),
    ("commit_failures", commit["failure"], 0),
    ("fsync_avg_ms", int(fuse.get("fsync", {"avg_s": 0.0})["avg_s"] * 1000.0), 150),
]
bad = [f"{k}={got}>{lim}" for k, got, lim in checks if got > lim]
print(" ".join(f"{k}={got}/{lim}" for k, got, lim in checks))
assert not bad, f"perf budget exceeded: {bad}"
PY
'
