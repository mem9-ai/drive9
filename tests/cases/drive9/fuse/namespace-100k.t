#!/bin/sh
# drive9/fuse/namespace-100k: 100000-file workspace on FUSE.
#
# The GA bar is the workspace size customers actually mount (10^5 files), not a
# 1k fixture. This case builds that tree through the mount, proves it survives a
# fresh-cache remount, then measures the three operations a large workspace is
# judged by: readdir, per-file stat, and a full find traversal. Metrics land in
# $RUN_DIR/namespace-100k.json for the performance baseline; the assertions here
# are completeness (every file present and stat-readable) plus a generous hang
# guard, so a slow runner fails loudly instead of hanging.
#
# Tunables:
#   NAMESPACE_FILES        files to create (default: 100000)
#   NAMESPACE_DIRS         fan-out directories (default: 100)
#   NAMESPACE_WORKERS      parallel creators (default: 8)
#   NAMESPACE_CREATE_CAP_S hang guard for creation (default: 1800)
#   NAMESPACE_LIST_CAP_S   hang guard for readdir pass (default: 300)
#   NAMESPACE_STAT_CAP_S   hang guard for stat pass (default: 900)
#   NAMESPACE_FIND_CAP_S   hang guard for find traversal (default: 900)
set -eu

NAMESPACE_FILES="${NAMESPACE_FILES:-100000}"
NAMESPACE_DIRS="${NAMESPACE_DIRS:-100}"
NAMESPACE_WORKERS="${NAMESPACE_WORKERS:-8}"
NAMESPACE_CREATE_CAP_S="${NAMESPACE_CREATE_CAP_S:-1800}"
NAMESPACE_LIST_CAP_S="${NAMESPACE_LIST_CAP_S:-300}"
NAMESPACE_STAT_CAP_S="${NAMESPACE_STAT_CAP_S:-900}"
NAMESPACE_FIND_CAP_S="${NAMESPACE_FIND_CAP_S:-900}"
export NAMESPACE_FILES NAMESPACE_DIRS NAMESPACE_WORKERS
export NAMESPACE_CREATE_CAP_S NAMESPACE_LIST_CAP_S NAMESPACE_STAT_CAP_S NAMESPACE_FIND_CAP_S

check shell --req 'drive9-namespace-100k-build' --claim 'create-100k-files-through-fuse-then-readdir-stat-and-find-them' -- sh -c '
    python3 - "$MOUNT" "$RUN_DIR" \
        "$NAMESPACE_FILES" "$NAMESPACE_DIRS" "$NAMESPACE_WORKERS" \
        "$NAMESPACE_CREATE_CAP_S" "$NAMESPACE_LIST_CAP_S" \
        "$NAMESPACE_STAT_CAP_S" "$NAMESPACE_FIND_CAP_S" <<'\''PY'\''
import concurrent.futures
import json
import os
import subprocess
import sys
import time

mount, run_dir, files, dirs, workers, create_cap, list_cap, stat_cap, find_cap = (
    sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]),
    float(sys.argv[6]), float(sys.argv[7]), float(sys.argv[8]), float(sys.argv[9]),
)
root = os.path.join(mount, "ns100k")
per_dir = max(1, files // dirs)
expected = per_dir * dirs


def timed(fn, cap, label):
    start = time.perf_counter()
    value = fn()
    elapsed = time.perf_counter() - start
    if elapsed > cap:
        raise SystemExit("%s exceeded hang guard: %.1fs > %.0fs" % (label, elapsed, cap))
    return value, elapsed


def create():
    for d in range(dirs):
        os.makedirs(os.path.join(root, "d%04d" % d), exist_ok=True)

    def one(args):
        d, i = args
        path = os.path.join(root, "d%04d" % d, "f%06d" % i)
        with open(path, "wb") as fh:
            fh.write(b"x")
        return path

    work = [(d, i) for d in range(dirs) for i in range(per_dir)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        created = sum(1 for _ in pool.map(one, work, chunksize=256))
    if created != expected:
        raise SystemExit("created %d files, expected %d" % (created, expected))
    return created


def listdir():
    seen = 0
    for d in range(dirs):
        with os.scandir(os.path.join(root, "d%04d" % d)) as it:
            for entry in it:
                if entry.is_file():
                    seen += 1
    if seen != expected:
        raise SystemExit("readdir saw %d files, expected %d" % (seen, expected))
    return seen


def stat_all():
    seen = 0
    for d in range(dirs):
        for i in range(per_dir):
            path = os.path.join(root, "d%04d" % d, "f%06d" % i)
            st = os.stat(path)
            if st.st_size != 1:
                raise SystemExit("unexpected size for %s: %d" % (path, st.st_size))
            seen += 1
    return seen


def find_all():
    out = subprocess.run(
        ["find", root, "-type", "f"],
        check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    if len(out) != expected:
        raise SystemExit("find saw %d files, expected %d" % (len(out), expected))
    return len(out)


created, create_s = timed(create, create_cap, "create")
listed, list_s = timed(listdir, list_cap, "readdir")
stat_n, stat_s = timed(stat_all, stat_cap, "stat")
find_n, find_s = timed(find_all, find_cap, "find")

create_rate = round(created / create_s, 1) if create_s else 0.0
stat_rate = round(stat_n / stat_s, 1) if stat_s else 0.0

report = {
    "schema_version": "drive9-bench/v1",
    "case": "drive9/fuse/namespace-100k",
    "files": created,
    "dirs": dirs,
    "workers": workers,
    "metrics": {
        "create.total_s": round(create_s, 3),
        "create.files_per_s": create_rate,
        "readdir.total_s": round(list_s, 3),
        "stat.total_s": round(stat_s, 3),
        "stat.ops_per_s": stat_rate,
        "find.total_s": round(find_s, 3),
    },
    "caps_s": {
        "create": create_cap,
        "readdir": list_cap,
        "stat": stat_cap,
        "find": find_cap,
    },
    "verified": {"readdir": listed, "stat": stat_n, "find": find_n},
}
with open(os.path.join(run_dir, "namespace-100k.json"), "w", encoding="utf-8") as fh:
    json.dump(report, fh, indent=2)

md = [
    "# Drive9 FUSE 100k workspace",
    "",
    "- Files: %d across %d directories (%d creators)" % (created, dirs, workers),
    "",
    "| Phase | Total (s) | Rate |",
    "|---|---:|---:|",
    "| create | %.1f | %s files/s |" % (create_s, create_rate),
    "| readdir | %.1f | %d entries |" % (list_s, listed),
    "| stat | %.1f | %s ops/s |" % (stat_s, stat_rate),
    "| find | %.1f | %d files |" % (find_s, find_n),
]
with open(os.path.join(run_dir, "namespace-100k.md"), "w", encoding="utf-8") as fh:
    fh.write("\n".join(md) + "\n")
print("\n".join(md))
PY
'

drain --mode require-ok --timeout 900s
check sync-ok --req 'drive9-namespace-100k-drain'

# Fresh cache: the tree has to come back from server state, not from the
# write-back staging it was just created in.
remount --cache fresh
check shell --req 'drive9-namespace-100k-remount' --claim 'fresh-cache-remount-sees-the-whole-tree' -- sh -c '
    count=$(find "$MOUNT/ns100k" -type f | wc -l | tr -d " ")
    expected=$(( NAMESPACE_FILES / NAMESPACE_DIRS * NAMESPACE_DIRS ))
    if [ "$count" != "$expected" ]; then
        echo "file count after remount: got $count, want $expected" >&2
        exit 1
    fi
'
