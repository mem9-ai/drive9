#!/bin/sh
# drive9/fuse/toolchain-bench: real-world developer toolchain benchmark & verification on FUSE.
#
# Exercises and times macro developer workflows directly on $MOUNT:
#   1. macro_git_clone        — `git clone --no-local` of a multi-hundred-file Go repo onto $MOUNT
#   2. macro_git_status_dirty — `git status --porcelain` on a dirty working tree
#   3. macro_git_diff_dirty   — `git diff --name-only` on a dirty working tree
#   4. macro_find_files       — `find . -type f` full tree traversal & file-count check
#   5. macro_go_build         — `go build` compiling the cloned Go module on $MOUNT + executing binary
#
# Emits `$RUN_DIR/toolchain-bench.json` (`schema_version: "drive9-bench/v1"`) and
# `$RUN_DIR/toolchain-bench.md`.
#
# Tunables:
#   BENCH_MACRO_FILES      Number of data files in synthetic repo (default: 200; daily/full: 1000)
#   BENCH_MACRO_TOTAL_MIB  Total payload size across data files in MiB (default: 10; daily/full: 50)
#   BENCH_RUNS             Timed iterations per workload (default: 3)
set -eu

HOST_HOME="$(python3 -c 'import os, pwd; print(pwd.getpwuid(os.getuid()).pw_dir)')"
export GOCACHE="${GOCACHE:-$(HOME="$HOST_HOME" go env GOCACHE 2>/dev/null || echo "$HOST_HOME/.cache/go-build")}"
export GOMODCACHE="${GOMODCACHE:-$(HOME="$HOST_HOME" go env GOMODCACHE 2>/dev/null || echo "$HOST_HOME/go/pkg/mod")}"

BENCH_MACRO_FILES="${BENCH_MACRO_FILES:-200}"
BENCH_MACRO_TOTAL_MIB="${BENCH_MACRO_TOTAL_MIB:-10}"
BENCH_RUNS="${BENCH_RUNS:-3}"
BENCH_OUT_JSON="$RUN_DIR/toolchain-bench.json"
BENCH_OUT_MD="$RUN_DIR/toolchain-bench.md"
export BENCH_MACRO_FILES BENCH_MACRO_TOTAL_MIB BENCH_RUNS BENCH_OUT_JSON BENCH_OUT_MD

check shell --req 'drive9-toolchain-bench-run' --claim 'macro-git-find-and-go-build-workloads-pass-and-emit-metrics' -- sh -c '
    python3 - "$MOUNT" "$RUN_DIR" "$BENCH_OUT_JSON" "$BENCH_OUT_MD" \
        "$BENCH_MACRO_FILES" "$BENCH_MACRO_TOTAL_MIB" "$BENCH_RUNS" <<'\''PY'\''
import json
import math
import os
import shutil
import statistics
import subprocess
import sys
import time

mount_dir, run_dir, out_json, out_md = sys.argv[1:5]
n_files = max(1, int(sys.argv[5]))
total_mib = max(1, int(sys.argv[6]))
runs = max(1, int(sys.argv[7]))

if shutil.which("git") is None:
    raise SystemExit("git is required on PATH for toolchain-bench")

src_repo = os.path.join(run_dir, "bench-source-repo")
os.makedirs(os.path.join(src_repo, "data"), exist_ok=True)

with open(os.path.join(src_repo, "go.mod"), "w", encoding="utf-8") as f:
    f.write("module example.com/drive9bench\n\ngo 1.21\n")

with open(os.path.join(src_repo, "main.go"), "w", encoding="utf-8") as f:
    f.write(
        """package main

import (
\t"fmt"
\t"os"
\t"path/filepath"
)

func main() {
\tb, err := os.ReadFile(filepath.Join("data", "file00000.txt"))
\tif err != nil || len(b) == 0 {
\t\tos.Exit(2)
\t}
\tfmt.Println("ok")
}
"""
    )

per_file = max(64, (total_mib * 1024 * 1024) // n_files)
line = ("drive9-toolchain-bench-payload-" * 4 + "\n").encode("ascii")
chunk = (line * ((per_file // len(line)) + 2))[:per_file]
for i in range(n_files):
    p = os.path.join(src_repo, "data", f"file{i:05d}.txt")
    with open(p, "wb") as f:
        f.write(chunk)

git_env = dict(os.environ)
git_env.update(
    {
        "GIT_AUTHOR_NAME": "drive9-bench",
        "GIT_AUTHOR_EMAIL": "bench@drive9.local",
        "GIT_COMMITTER_NAME": "drive9-bench",
        "GIT_COMMITTER_EMAIL": "bench@drive9.local",
    }
)


def run_cmd(argv, cwd=None, check=True):
    return subprocess.run(
        argv,
        cwd=cwd,
        env=git_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


run_cmd(["git", "init", "-q"], cwd=src_repo)
run_cmd(["git", "add", "."], cwd=src_repo)
run_cmd(["git", "commit", "-q", "-m", "initial benchmark seed"], cwd=src_repo)

expected_src_files = int(
    run_cmd(["sh", "-c", "find . -type f | wc -l"], cwd=src_repo).stdout.strip()
)

bench_root = os.path.join(mount_dir, f"toolchain-bench-{os.getpid()}")
os.makedirs(bench_root, exist_ok=True)


def summarize(name, samples):
    ordered = sorted(samples)
    p95_idx = min(len(ordered) - 1, max(0, math.ceil(0.95 * len(ordered)) - 1))
    return {
        "name": name,
        "runs": len(samples),
        "median_s": round(statistics.median(ordered), 6),
        "p95_s": round(ordered[p95_idx], 6),
        "min_s": round(ordered[0], 6),
        "max_s": round(ordered[-1], 6),
        "samples_s": [round(x, 6) for x in samples],
    }


results = []

# 1. macro_git_clone
clone_samples = []
active_clone = None
for i in range(runs):
    dst = os.path.join(bench_root, f"clone-{i}")
    if os.path.exists(dst):
        shutil.rmtree(dst)
    t0 = time.perf_counter()
    run_cmd(["git", "clone", "--quiet", "--no-local", src_repo, dst])
    clone_samples.append(time.perf_counter() - t0)
    if not os.path.isfile(os.path.join(dst, "main.go")):
        raise SystemExit(f"clone missing main.go in {dst}")
    if i < runs - 1:
        shutil.rmtree(dst)
    else:
        active_clone = dst
results.append(summarize("macro_git_clone", clone_samples))

# Dirty a tracked file once on the mounted clone for status/diff workloads.
dirty_target = os.path.join(active_clone, "data", "file00000.txt")
with open(dirty_target, "ab") as f:
    f.write(b"dirty-line\n")
    f.flush()
    os.fsync(f.fileno())

# 2. macro_git_status_dirty
status_samples = []
for _ in range(runs):
    t0 = time.perf_counter()
    proc = run_cmd(["git", "status", "--porcelain"], cwd=active_clone)
    status_samples.append(time.perf_counter() - t0)
    if "data/file00000.txt" not in proc.stdout:
        raise SystemExit(f"git status missed dirty file: {proc.stdout!r}")
results.append(summarize("macro_git_status_dirty", status_samples))

# 3. macro_git_diff_dirty
diff_samples = []
for _ in range(runs):
    t0 = time.perf_counter()
    proc = run_cmd(["git", "diff", "--name-only"], cwd=active_clone)
    diff_samples.append(time.perf_counter() - t0)
    if proc.stdout.strip() != "data/file00000.txt":
        raise SystemExit(f"git diff unexpected output: {proc.stdout!r}")
results.append(summarize("macro_git_diff_dirty", diff_samples))

# 4. macro_find_files
find_samples = []
for _ in range(runs):
    t0 = time.perf_counter()
    proc = run_cmd(["sh", "-c", "find . -type f | wc -l"], cwd=active_clone)
    find_samples.append(time.perf_counter() - t0)
    got_files = int(proc.stdout.strip())
    if got_files < 2 + n_files:
        raise SystemExit(f"find count too small: got {got_files}, expected >= {2 + n_files} (src={expected_src_files})")
results.append(summarize("macro_find_files", find_samples))

# 5. macro_go_build (when Go toolchain is available)
if shutil.which("go") is not None:
    bin_out = os.path.join(run_dir, "bench-bin", "drive9bench")
    os.makedirs(os.path.dirname(bin_out), exist_ok=True)
    build_samples = []
    for _ in range(runs):
        if os.path.exists(bin_out):
            os.remove(bin_out)
        t0 = time.perf_counter()
        run_cmd(["go", "build", "-o", bin_out, "."], cwd=active_clone)
        build_samples.append(time.perf_counter() - t0)
    exec_proc = run_cmd([bin_out], cwd=active_clone)
    if exec_proc.stdout.strip() != "ok":
        raise SystemExit(f"compiled binary output mismatch: {exec_proc.stdout!r}")
    results.append(summarize("macro_go_build", build_samples))

report = {
    "schema_version": "drive9-bench/v1",
    "config": {
        "macro_files": n_files,
        "macro_total_mib": total_mib,
        "runs": runs,
    },
    "workloads": results,
}
with open(out_json, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

md_lines = [
    "# Drive9 FUSE Toolchain Benchmark",
    "",
    f"- Files: `{n_files}` (`~{total_mib} MiB`), iterations per workload: `{runs}`",
    "",
    "| Workload | Runs | Median (s) | P95 (s) | Min (s) | Max (s) |",
    "|---|---:|---:|---:|---:|---:|",
]
for w in results:
    w_name = w["name"]
    w_runs = w["runs"]
    w_med = w["median_s"]
    w_p95 = w["p95_s"]
    w_min = w["min_s"]
    w_max = w["max_s"]
    md_lines.append(
        f"| `{w_name}` | {w_runs} | {w_med:.4f} | {w_p95:.4f} | {w_min:.4f} | {w_max:.4f} |"
    )
with open(out_md, "w", encoding="utf-8") as f:
    f.write("\n".join(md_lines) + "\n")
print("\n".join(md_lines))
PY
'

drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-toolchain-bench-drain'
