#!/bin/sh
# drive9/fuse/namespace-100k: 100000-file workspace on FUSE.
#
# The GA bar is the workspace size customers actually mount (10^5 files), not a
# 1k fixture. This case builds that tree through the mount, then measures each
# read operation from a cold cache — readdir, stat and find each start after
# their own `remount --cache fresh` — and records how long each phase took for
# the performance baseline.
#
# Each phase enforces its own deadline in-process: the work runs on a daemon
# thread and the process exits 124 when the cap passes, so a blocked FUSE
# operation fails its check instead of running into the suite timeout. A phase
# also refuses to start when the phase it depends on has no log line, so a
# capped create cannot make stat/find burn their own caps on an empty tree.
#
# The tree lives under a unique hidden path per run and is removed at the end,
# so the case can run against an existing tenant without touching its data.
#
# Tunables:
#   NAMESPACE_FILES        files to create (default: 100000)
#   NAMESPACE_DIRS         fan-out directories (default: 100)
#   NAMESPACE_WORKERS      parallel creators (default: 8)
#   NAMESPACE_FILE_BYTES   payload per file, metadata-only by default (default: 1)
#   NAMESPACE_CLEANUP=1    delete the tree at the end (default: leave it; see below)
#   NAMESPACE_{CREATE,READDIR,STAT,FIND,CLEANUP}_CAP_S  per-phase deadlines
#     (required, must be > 0; deadlines are sized from a measured 100k run on a
#      local SSD — create 5.4s, drain 200s, readdir 274s, stat 282s, find 286s —
#      so they bound hangs rather than assert performance. Worst case with
#      cleanup enabled: 600+900+900+900+1200 = 4500s of phases, a 2x480s drain,
#      three ~210s remounts and a ~190s initial mount = about 6280s. The
#      suites.toml `timeout` is 120m, which only bounds a standalone run: in the
#      scheduled job the job's own 120-minute limit fires first and these phase
#      deadlines are the effective bound. The default (no cleanup) is ~5080s.)
set -eu

NAMESPACE_FILES="${NAMESPACE_FILES:-100000}"
NAMESPACE_DIRS="${NAMESPACE_DIRS:-100}"
NAMESPACE_WORKERS="${NAMESPACE_WORKERS:-8}"
NAMESPACE_FILE_BYTES="${NAMESPACE_FILE_BYTES:-1}"
NAMESPACE_CLEANUP="${NAMESPACE_CLEANUP:-0}"
NAMESPACE_CREATE_CAP_S="${NAMESPACE_CREATE_CAP_S:-600}"
NAMESPACE_READDIR_CAP_S="${NAMESPACE_READDIR_CAP_S:-900}"
NAMESPACE_STAT_CAP_S="${NAMESPACE_STAT_CAP_S:-900}"
NAMESPACE_FIND_CAP_S="${NAMESPACE_FIND_CAP_S:-900}"
NAMESPACE_CLEANUP_CAP_S="${NAMESPACE_CLEANUP_CAP_S:-1200}"

# Unique, hidden tree: never collide with (or truncate) data an existing tenant
# already has at a well-known path.
NS_ROOT="$MOUNT/.sim-namespace-100k-$$-$(date +%s)"
export NS_ROOT
export NAMESPACE_FILES NAMESPACE_DIRS NAMESPACE_WORKERS NAMESPACE_FILE_BYTES
export NAMESPACE_CLEANUP
export NAMESPACE_CREATE_CAP_S NAMESPACE_READDIR_CAP_S NAMESPACE_STAT_CAP_S
export NAMESPACE_FIND_CAP_S NAMESPACE_CLEANUP_CAP_S

LIB="$RUN_DIR/namespace_lib.py"

# Deleting 100000 files through FUSE was measured at >20 minutes, which dwarfs
# every measurement this case exists to take. Cleanup is therefore opt-in
# (NAMESPACE_CLEANUP=1) for runs against a shared/existing tenant; the default
# leaves the tree at a unique hidden path and prints it, which is what an
# ephemeral bench runner wants. A `check` never aborts the case, but `drain` and
# `remount` do when they return non-zero, so the trap covers those aborts too;
# it runs at most once and the engine's SIGKILL at the case timeout bypasses it.
cleanup_tree() {
  if [ "${NAMESPACE_CLEANUP:-0}" != "1" ] || [ ! -f "$LIB" ]; then
    return 0
  fi
  if [ -f "$RUN_DIR/namespace-cleanup-started" ]; then
    return 0
  fi
  : > "$RUN_DIR/namespace-cleanup-started"
  python3 "$LIB" cleanup || echo "cleanup after abort failed; tree left at $NS_ROOT" >&2
}
trap cleanup_tree EXIT

cat > "$LIB" <<'PY'
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time

PHASES = os.path.join(os.environ["RUN_DIR"], "namespace-phases.jsonl")
REQUIRES = {"readdir": ["create"], "stat": ["readdir"], "find": ["stat"]}
ACTIVE = []


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


def completed():
    if not os.path.exists(PHASES):
        return set()
    out = set()
    with open(PHASES, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.add(json.loads(line)["phase"])
    return out


def ensure_requires(phase):
    have = completed()
    for req in REQUIRES.get(phase, []):
        if req not in have:
            raise SystemExit("phase %s cannot run: %s did not complete" % (phase, req))


def record(phase, seconds, extra=None):
    entry = {"phase": phase, "seconds": round(seconds, 3)}
    if extra:
        entry.update(extra)
    with open(PHASES, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def run_cmd(argv):
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ACTIVE.append(proc)
    try:
        out, err = proc.communicate()
    finally:
        if proc in ACTIVE:
            ACTIVE.remove(proc)
    if proc.returncode != 0:
        raise SystemExit("%s failed (rc=%d): %s" % (argv[0], proc.returncode, err.decode("utf-8", "replace").strip()))
    return out.decode("utf-8", "replace")


def cap_for(phase):
    key = "NAMESPACE_%s_CAP_S" % phase.upper()
    raw = os.environ.get(key, "")
    if not raw.strip():
        raise SystemExit("%s is required" % key)
    cap = float(raw)
    if cap <= 0:
        raise SystemExit("%s must be > 0 (got %r)" % (key, raw))
    return cap


def run_phased(phase, fn):
    """Run one phase with a hard deadline enforced in-process.

    A blocked FUSE syscall never returns, so an elapsed-time check after the
    call cannot bound it. The work runs on a daemon thread; when the deadline
    passes any child process is killed and the process exits 124, which the
    case surfaces as a failed check naming the phase and its cap.
    """
    ensure_requires(phase)
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
    worker.join(cap)
    if worker.is_alive():
        for proc in list(ACTIVE):
            proc.kill()
        print("phase %s exceeded its %.0fs cap and was killed" % (phase, cap), file=sys.stderr)
        sys.stderr.flush()
        os._exit(124)
    elapsed = time.perf_counter() - start
    if "error" in outcome:
        raise outcome["error"]
    record(phase, elapsed, outcome.get("extra"))
    return 0


def create(c):
    root, dirs = c["root"], c["dirs"]
    per_dir = max(1, c["files"] // c["dirs"])
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
    root, dirs = c["root"], c["dirs"]
    seen = 0
    for d in range(dirs):
        # Count entries; this phase asserts completeness, it does not assert a
        # per-entry lookup. The listing paths emit typed modes, so is_file()
        # would not have cost a stat either way.
        with os.scandir(os.path.join(root, "d%04d" % d)) as it:
            seen += sum(1 for _ in it)
    if seen != c["expected"]:
        raise SystemExit("readdir saw %d files, expected %d" % (seen, c["expected"]))
    return {"entries": seen}


def stat_all(c):
    root, dirs = c["root"], c["dirs"]
    per_dir = max(1, c["files"] // c["dirs"])
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
    out = run_cmd(["find", c["root"], "-type", "f"]).splitlines()
    if len(out) != c["expected"]:
        raise SystemExit("find saw %d files, expected %d" % (len(out), c["expected"]))
    return {"files": len(out)}


def cleanup(c):
    if not os.path.isdir(c["root"]):
        return {"removed": False}
    run_cmd(["find", c["root"], "-mindepth", "1", "-delete"])
    os.rmdir(c["root"])
    return {"removed": True}


def drain_seconds():
    path = os.path.join(os.environ["RUN_DIR"], "sync-0.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            return round(json.load(fh)["duration_ms"] / 1000.0, 3)
    except (KeyError, ValueError, OSError):
        return None


def summary(c):
    logged = []
    if os.path.exists(PHASES):
        with open(PHASES, encoding="utf-8") as fh:
            logged = [json.loads(line) for line in fh if line.strip()]
    by_name = {p["phase"]: p for p in logged}
    # Fixed list: a missing phase must be visible in the table, not absent.
    phases = []
    for name in ("create", "readdir", "stat", "find", "cleanup"):
        entry = by_name.get(name)
        phases.append({"phase": name, "seconds": entry["seconds"] if entry else None})
    phases.insert(1, {"phase": "drain", "seconds": drain_seconds()})
    cleanup_state = "completed"
    if os.environ.get("NAMESPACE_CLEANUP", "0") != "1":
        cleanup_state = "skipped"
    elif "cleanup" not in by_name:
        cleanup_state = "incomplete"

    def seconds(name):
        # A phase that never completed is null, not 0.0: 0.0 reads as "instant".
        entry = by_name.get(name)
        return entry["seconds"] if entry else None

    create_s = seconds("create")
    stat_s = seconds("stat")
    drain_s = drain_seconds()
    report = {
        "schema_version": "drive9-bench/namespace-v1",
        "case": "drive9/fuse/namespace-100k",
        "root": c["root"],
        "files": c["expected"],
        "file_bytes": c["file_bytes"],
        "dirs": c["dirs"],
        "workers": c["workers"],
        "phases": phases,
        "cleanup": cleanup_state,
        "metrics": {
            "create.total_s": create_s,
            "create.files_per_s": round(c["expected"] / create_s, 1) if create_s else None,
            "readdir.total_s": seconds("readdir"),
            "stat.total_s": stat_s,
            "stat.ops_per_s": round(c["expected"] / stat_s, 1) if stat_s else None,
            "find.total_s": seconds("find"),
            "drain.total_s": drain_s,
            "cleanup.total_s": seconds("cleanup"),
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
    md += [
        "| %s | %s |" % (p["phase"], "n/a" if p["seconds"] is None else "%.1f" % p["seconds"])
        for p in phases
    ]
    md += ["", "Cleanup: `%s`" % cleanup_state]
    with open(os.path.join(os.environ["RUN_DIR"], "namespace-100k.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(md) + "\n")
    print("\n".join(md))


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

drain --mode require-ok --timeout 480s
check sync-ok --req 'drive9-namespace-100k-drain'

# One fresh remount per read phase: shares kernel dirent/attr caches otherwise
# (60s/30s), so a warm stat pass would measure the kernel, not the mount.
remount --cache fresh
cd "$MOUNT"
check shell --req 'drive9-namespace-100k-readdir' --claim 'cold-readdir-over-100k-files' -- \
    python3 "$LIB" readdir

remount --cache fresh
cd "$MOUNT"
check shell --req 'drive9-namespace-100k-stat' --claim 'cold-stat-over-100k-files' -- \
    python3 "$LIB" stat

remount --cache fresh
cd "$MOUNT"
check shell --req 'drive9-namespace-100k-find' --claim 'cold-find-over-100k-files' -- \
    python3 "$LIB" find

if [ "$NAMESPACE_CLEANUP" = "1" ]; then
  : > "$RUN_DIR/namespace-cleanup-started"
  check shell --req 'drive9-namespace-100k-cleanup' --claim 'remove-the-100k-tree' -- \
      python3 "$LIB" cleanup
  # The explicit check owns cleanup once it has run: leaving the trap armed
  # would append a duplicate phase line, or retry an already-capped delete under
  # a second cap and push the worst case past the documented bound.
  trap - EXIT
else
  echo "tree left at $NS_ROOT (set NAMESPACE_CLEANUP=1 to remove it)"
fi

# After cleanup so its duration is in the report.
check shell --req 'drive9-namespace-100k-summary' --claim 'emit-bench-artifact' -- \
    python3 "$LIB" summary
