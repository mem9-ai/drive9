"""Split the concurrent-files gap into "extra RPCs" and "per-RPC amplification".

Runs the same primitive operations sequentially and with N worker processes so
the two effects can be separated:

  * extra RPCs: mutations/statistics per operation (e.g. the post-upload Chmod
    that fires when a file's mode differs from the server default)
  * amplification: per-RPC latency growth between the sequential and the
    concurrent phase, measured from the perf samples

Phases: create+write+fsync (sequential), create+write+fsync (concurrent),
unlink (sequential), unlink (concurrent). Every phase drains first so the
phases do not overlap.

Usage: concurrency_probe.py <mount-dir> [files] [workers]
"""

import json
import multiprocessing as mp
import os
import statistics
import subprocess
import sys
import time

BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")


def op_stat(latencies):
    s = sorted(latencies)
    return {
        "count": len(s),
        "avg_ms": round(statistics.mean(s), 2),
        "p50_ms": round(s[len(s) // 2], 2),
        "p95_ms": round(s[max(0, int(len(s) * 0.95) - 1)], 2),
    }


def mark(tag):
    print(json.dumps({"mark": tag, "epoch": time.time()}), flush=True)


def drain(mount):
    subprocess.run([BIN, "mount", "drain", "--timeout", "1800s", "--json", mount],
                   capture_output=True, text=True)


def create_worker(args):
    root, count, worker = args
    # Each parallel worker writes into its own directory, matching the
    # concurrent-files case. Sharing one directory would serialize unlink(2)
    # on the kernel's parent-directory i_rwsem and pollute the measurement.
    if worker > 0:
        root = os.path.join(root, f"w{worker:02d}")
        os.makedirs(root, exist_ok=True)
    payload = b"x" * 4096
    latencies = []
    for i in range(count):
        path = os.path.join(root, f"w{worker:02d}-f{i:04d}.dat")
        start = time.perf_counter()
        with open(path, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def unlink_worker(args):
    paths = args
    latencies = []
    for path in paths:
        start = time.perf_counter()
        os.unlink(path)
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def run_create_phase(root, files, workers):
    if workers == 1:
        return create_worker((root, files, 0))
    per = files // workers
    with mp.get_context("fork").Pool(workers) as pool:
        results = pool.map(create_worker, [(root, per, w) for w in range(workers)])
    return [ms for part in results for ms in part]


def run_unlink_phase(paths, workers):
    if workers == 1:
        return unlink_worker(paths)
    chunks = [paths[i::workers] for i in range(workers)]
    with mp.get_context("fork").Pool(workers) as pool:
        results = pool.map(unlink_worker, chunks)
    return [ms for part in results for ms in part]


def main():
    if len(sys.argv) < 2:
        print("usage: concurrency_probe.py <mount-dir> [files] [workers]", file=sys.stderr)
        return 2
    mount = sys.argv[1]
    files = int(sys.argv[2]) if len(sys.argv) > 2 else 120
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    # Ubuntu defaults to umask 0002, which makes Python-created files 0664 and
    # triggers the post-upload Chmod. Use the server default so this probe can
    # isolate the amplification from the extra RPC.
    os.umask(0o022)

    base = os.path.join(mount, "concprobe-" + str(int(time.time())))
    seq_root = os.path.join(base, "seq")
    par_root = os.path.join(base, "par")
    os.makedirs(seq_root)
    os.makedirs(par_root)

    drain(mount)
    mark("seq-create-start")
    seq_create = run_create_phase(seq_root, files, 1)
    mark("seq-create-end")
    drain(mount)

    mark("par-create-start")
    par_create = run_create_phase(par_root, files, workers)
    mark("par-create-end")
    drain(mount)

    seq_paths = sorted(os.path.join(seq_root, n) for n in os.listdir(seq_root))
    par_paths = []
    for name in sorted(os.listdir(par_root)):
        entry = os.path.join(par_root, name)
        if os.path.isdir(entry):
            par_paths.extend(sorted(os.path.join(entry, f) for f in os.listdir(entry)))
        else:
            par_paths.append(entry)

    mark("seq-unlink-start")
    seq_unlink = run_unlink_phase(seq_paths, 1)
    mark("seq-unlink-end")
    drain(mount)

    mark("par-unlink-start")
    par_unlink = run_unlink_phase(par_paths, workers)
    mark("par-unlink-end")
    drain(mount)

    print(json.dumps({
        "files": files,
        "workers": workers,
        "seq_create": op_stat(seq_create),
        "par_create": op_stat(par_create),
        "seq_unlink": op_stat(seq_unlink),
        "par_unlink": op_stat(par_unlink),
        "root": base,
    }, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
