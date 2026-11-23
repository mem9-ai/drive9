#!/bin/sh
# drive9/fuse/namespace-100k: 100000-file workspace on FUSE.
#
# The GA bar is the workspace size customers actually mount (10^5 files), not a
# 1k fixture. This case builds that tree through the mount, drains and remounts
# with a fresh cache so the tree must come from server state, and only then
# measures the read operations a large workspace is judged by: readdir,
# per-file stat and a full find traversal. Metrics land in
# $RUN_DIR/namespace-100k.json for the performance baseline.
#
# Each phase enforces its own hard deadline in-process: the work runs on a
# daemon thread and the process exits 124 when the cap passes, so a blocked FUSE
# operation fails the check instead of running into the suite timeout.
# Completeness assertions (file count, stat size, post-remount readdir) are what
# the case enforces; the numbers are what the baseline consumes.
#
# The tree lives under a unique hidden path per run and is removed at the end,
# so the case can run against an existing tenant without touching its data.
#
# Tunables:
#   NAMESPACE_FILES        files to create (default: 100000)
#   NAMESPACE_DIRS         fan-out directories (default: 100)
#   NAMESPACE_WORKERS      parallel creators (default: 8)
#   NAMESPACE_FILE_BYTES   payload per file, metadata-only by default (default: 1)
#   NAMESPACE_KEEP=1       keep the tree on disk for debugging (default: remove)
#   NAMESPACE_{CREATE,READDIR,STAT,FIND,CLEANUP}_CAP_S  per-phase deadlines
#     (worst case 1800+300+600+600+600 = 3900s, plus the 900s drain, stays under
#      the 90-minute case timeout in tests/cases/suites.toml)
set -eu

NAMESPACE_FILES="${NAMESPACE_FILES:-100000}"
NAMESPACE_DIRS="${NAMESPACE_DIRS:-100}"
NAMESPACE_WORKERS="${NAMESPACE_WORKERS:-8}"
NAMESPACE_FILE_BYTES="${NAMESPACE_FILE_BYTES:-1}"
NAMESPACE_KEEP="${NAMESPACE_KEEP:-0}"
NAMESPACE_CREATE_CAP_S="${NAMESPACE_CREATE_CAP_S:-1800}"
NAMESPACE_READDIR_CAP_S="${NAMESPACE_READDIR_CAP_S:-300}"
NAMESPACE_STAT_CAP_S="${NAMESPACE_STAT_CAP_S:-600}"
NAMESPACE_FIND_CAP_S="${NAMESPACE_FIND_CAP_S:-600}"
NAMESPACE_CLEANUP_CAP_S="${NAMESPACE_CLEANUP_CAP_S:-600}"

# Unique, hidden tree: never collide with (or truncate) data an existing tenant
# already has at a well-known path.
NS_ROOT="$MOUNT/.sim-namespace-100k-$$-$(date +%s)"
export NS_ROOT
export NAMESPACE_FILES NAMESPACE_DIRS NAMESPACE_WORKERS NAMESPACE_FILE_BYTES
export NAMESPACE_CREATE_CAP_S NAMESPACE_READDIR_CAP_S NAMESPACE_STAT_CAP_S
export NAMESPACE_FIND_CAP_S NAMESPACE_CLEANUP_CAP_S

LIB="$RUN_DIR/namespace_lib.py"
cat > "$LIB" <<'PY'
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time

PHASES = os.path.join(os.environ["RUN_DIR"], "namespace-phases.jsonl")


def env_int(name, default):
    raw = os.environ.get(name, "")
    return int(raw) if raw.strip() else default


def cfg():
    files = env_int("NAMESPACE_FILES", 100000)
    dirs = max(1, env_int("NAMESPACE_DIRS", 100))
    return {
        "root": os.environ["NS_ROOT"],
        "files": files,
        "dirs": dirs,
        "workers": max(1, env_int("NAMESPACE_WORKERS", 8)),
        "file_bytes": max(0, env_int("NAMESPACE_FILE_BYTES", 1)),
        "expected": max(1, files // dirs) * dirs,
    }


def path_for(root, d, i):
    return os.path.join(root, "d%04d" % d, "f%06d" % i)


def record(phase, seconds, extra=None):
    entry = {"phase": phase, "seconds": round(seconds, 3)}
    if extra:
        entry.update(extra)
    with open(PHASES, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def cap_for(phase):
    return float(os.environ.get("NAMESPACE_%s_CAP_S" % phase.upper(), "0") or 0)


def run_phased(phase, fn):
    """Run one phase with a hard deadline enforced in-process.

    A blocked FUSE syscall never returns, so an elapsed-time check after the
    call cannot bound it. The work runs on a daemon thread; when the deadline
    passes the process exits with 124 immediately, which the case surfaces as a
    failed check. Threads cannot be cancelled, but os._exit ends the whole
    process including a stuck syscall.
    """
    cap = cap_for(phase)
    start = time.perf_counter()
    outcome = {}

    def target():
        try:
            outcome["extra"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised in the main thread
            outcome["error"] = exc

    worker = threading.Thread(target=target, name="phase-" + phase, daemon=True)
    worker.start()
    worker.join(cap if cap > 0 else None)
    if worker.is_alive():
        print("phase %s exceeded its %.0fs cap and was killed" % (phase, cap), file=sys.stderr)
        sys.stderr.flush()
        os._exit(124)
    elapsed = time.perf_counter() - start
    if "error" in outcome:
        raise outcome["error"]
    record(phase, elapsed, outcome.get("extra"))
    return 0


def create(c):
    root, dirs, per_dir = c["root"], c["dirs"], max(1, c["files"] // c["dirs"])
    payload = b"x" * c["file_bytes"]
    for d in range(dirs):
        os.makedirs(os.path.join(root, "d%04d" % d), exist_ok=True)

    def one(args):
        d, i = args
        with open(path_for(root, d, i), "wb") as fh:
            fh.write(payload)
        return 1

    work = [(d, i) for d in range(dirs) for i in range(per_dir)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=c["workers"]) as pool:
        made = sum(pool.map(one, work, chunksize=256))
    if made != c["expected"]:
        raise SystemExit("created %d files, expected %d" % (made, c["expected"]))
    return {"files": made, "bytes_each": c["file_bytes"]}


def readdir(c):
    root, dirs, per_dir = c["root"], c["dirs"], max(1, c["files"] // c["dirs"])
    seen = 0
    for d in range(dirs):
        with os.scandir(os.path.join(root, "d%04d" % d)) as it:
            seen += sum(1 for e in it if e.is_file())
    if seen != c["expected"]:
        raise SystemExit("readdir saw %d files, expected %d" % (seen, c["expected"]))
    return {"entries": seen}


def stat_all(c):
    root, dirs, per_dir = c["root"], c["dirs"], max(1, c["files"] // c["dirs"])
    seen = 0
    for d in range(dirs):
        for i in range(per_dir):
            st = os.stat(path_for(root, d, i))
            if st.st_size != c["file_bytes"]:
                raise SystemExit("unexpected size for %s: %d" % (path_for(root, d, i), st.st_size))
            seen += 1
    if seen != c["expected"]:
        raise SystemExit("stat saw %d files, expected %d" % (seen, c["expected"]))
    return {"ops": seen}


def find_all(c):
    out = subprocess.run(
        ["find", c["root"], "-type", "f"], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    if len(out) != c["expected"]:
        raise SystemExit("find saw %d files, expected %d" % (len(out), c["expected"]))
    return {"files": len(out)}


def cleanup(c):
    if not os.path.isdir(c["root"]):
        return {"removed": False}
    subprocess.run(
        ["find", c["root"], "-mindepth", "1", "-delete"],
        check=True, capture_output=True, text=True,
    )
    os.rmdir(c["root"])
    return {"removed": True}


def summary(c):
    phases = []
    if os.path.exists(PHASES):
        with open(PHASES, encoding="utf-8") as fh:
            phases = [json.loads(line) for line in fh if line.strip()]
    by_name = {p["phase"]: p for p in phases}
    create_s = by_name.get("create", {}).get("seconds", 0.0)
    stat_s = by_name.get("stat", {}).get("seconds", 0.0)
    report = {
        "schema_version": "drive9-bench/v1",
        "case": "drive9/fuse/namespace-100k",
        "root": c["root"],
        "files": c["expected"],
        "file_bytes": c["file_bytes"],
        "dirs": c["dirs"],
        "workers": c["workers"],
        "phases": phases,
        "metrics": {
            "create.total_s": create_s,
            "create.files_per_s": round(c["expected"] / create_s, 1) if create_s else 0.0,
            "readdir.total_s": by_name.get("readdir", {}).get("seconds", 0.0),
            "stat.total_s": stat_s,
            "stat.ops_per_s": round(c["expected"] / stat_s, 1) if stat_s else 0.0,
            "find.total_s": by_name.get("find", {}).get("seconds", 0.0),
        },
    }
    out = os.path.join(os.environ["RUN_DIR"], "namespace-100k.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    md = [
        "# Drive9 FUSE 100k workspace",
        "",
        "- Files: %d x %d bytes across %d directories" % (c["expected"], c["file_bytes"], c["dirs"]),
        "",
        "| Phase | Seconds |",
        "|---|---:|",
    ]
    md += ["| %s | %.1f |" % (p["phase"], p["seconds"]) for p in phases]
    with open(os.path.join(os.environ["RUN_DIR"], "namespace-100k.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(md) + "\n")
    print("\n".join(md))
    return {}


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: namespace_lib.py create|readdir|stat|find|cleanup|summary")
    command = sys.argv[1]
    table = {
        "create": create,
        "readdir": readdir,
        "stat": stat_all,
        "find": find_all,
        "cleanup": cleanup,
        "summary": summary,
    }
    if command not in table:
        raise SystemExit("unknown phase %r" % command)
    c = cfg()
    if command == "summary":
        summary(c)
        return 0
    return run_phased(command, lambda: table[command](c))


if __name__ == "__main__":
    raise SystemExit(main())
PY

check shell --req 'drive9-namespace-100k-create' --claim 'create-100k-files-through-fuse' -- \
    python3 "$LIB" create

drain --mode require-ok --timeout 900s
check sync-ok --req 'drive9-namespace-100k-drain'

# Measure the server-backed tree, not the warm cache the create phase just
# populated: readdir/stat/find all run after the fresh remount.
remount --cache fresh

check shell --req 'drive9-namespace-100k-readdir' --claim 'readdir-100k-files-after-fresh-remount' -- \
    python3 "$LIB" readdir
check shell --req 'drive9-namespace-100k-stat' --claim 'stat-every-file-testing-size' -- \
    python3 "$LIB" stat
check shell --req 'drive9-namespace-100k-find' --claim 'find-traversal-over-100k-files' -- \
    python3 "$LIB" find
check shell --req 'drive9-namespace-100k-summary' --claim 'emit-bench-artifact' -- \
    python3 "$LIB" summary

if [ "$NAMESPACE_KEEP" != "1" ]; then
  check shell --req 'drive9-namespace-100k-cleanup' --claim 'remove-the-100k-tree' -- \
      python3 "$LIB" cleanup
fi
