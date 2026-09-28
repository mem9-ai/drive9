"""Mount-level evidence probes against the profiling diagnostic mount.

Runs on the EC2 host; reads the mount's continuous perf JSONL before/after each
probe so every number can be attributed to fuse_ops / remote_ops counters.
"""

import json
import os
import pathlib
import statistics
import subprocess
import time

MOUNT = pathlib.Path(os.environ.get("DRIVE9_DIAG_MOUNT", "/mnt/d9-diag"))
ROOT = MOUNT / "probe"
PERF = pathlib.Path(os.environ.get("DRIVE9_DIAG_PERF", "/home/ubuntu/d9diag/perf/perf.jsonl"))
BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")
PAYLOAD = b"x" * 212


def last_sample():
    if not PERF.exists():
        return None
    last = ""
    with PERF.open() as handle:
        for line in handle:
            if line.strip():
                last = line
    return json.loads(last) if last else None


def diff(before, after, section):
    prev_all = (before or {}).get(section) or {}
    curr_all = (after or {}).get(section) or {}
    out = {}
    for key, value in curr_all.items():
        prev = prev_all.get(key) or {}
        count = (value.get("count") or 0) - (prev.get("count") or 0)
        if count <= 0:
            continue
        total = (value.get("total_ns") or 0) - (prev.get("total_ns") or 0)
        out[key] = {"count": count, "avg_ms": round(total / count / 1e6, 3)}
    return out


def stats(values):
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "avg_ms": round(statistics.mean(ordered) * 1000, 3),
        "p50_ms": round(ordered[len(ordered) // 2] * 1000, 3),
        "p95_ms": round(ordered[max(0, int(len(ordered) * 0.95) - 1)] * 1000, 3),
        "max_ms": round(ordered[-1] * 1000, 3),
    }


def drain():
    start = time.perf_counter()
    proc = subprocess.run([BIN, "mount", "drain", "--timeout", "1800s", "--json", str(MOUNT)],
                          capture_output=True, text=True, timeout=1815)
    return {"ok": proc.returncode == 0, "wall_s": round(time.perf_counter() - start, 3),
            "stdout": proc.stdout.strip()[:160]}


def probe(name, fn):
    time.sleep(2.3)
    before = last_sample()
    start = time.time()
    metrics = fn()
    end = time.time()
    time.sleep(2.3)
    after = last_sample()
    print(json.dumps({"probe": name, "start": round(start, 3), "end": round(end, 3),
                      "metrics": metrics, "fuse_ops": diff(before, after, "fuse_ops"),
                      "remote_ops": diff(before, after, "remote_ops"),
                      "queues": (after or {}).get("queues")}, ensure_ascii=False), flush=True)


def flush_write(count, subdir="flush"):
    target = ROOT / subdir
    target.mkdir(parents=True, exist_ok=True)
    phases = {"open_ms": [], "write_ms": [], "flush_ms": [], "release_ms": []}
    for index in range(count):
        path = target / f"f{index:04d}.bin"
        start = time.perf_counter()
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        phases["open_ms"].append(time.perf_counter() - start)
        start = time.perf_counter()
        os.write(fd, PAYLOAD)
        phases["write_ms"].append(time.perf_counter() - start)
        start = time.perf_counter()
        dup = os.dup(fd)
        os.close(dup)
        phases["flush_ms"].append(time.perf_counter() - start)
        start = time.perf_counter()
        os.close(fd)
        phases["release_ms"].append(time.perf_counter() - start)
    return {key: stats(value) for key, value in phases.items()}


def create_files(count, subdir):
    target = ROOT / subdir
    target.mkdir(parents=True, exist_ok=True)
    values = []
    for index in range(count):
        path = target / f"f{index:04d}.bin"
        start = time.perf_counter()
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, PAYLOAD)
        os.close(fd)
        values.append(time.perf_counter() - start)
    return stats(values)


def scan(count, subdir):
    target = ROOT / subdir
    values = []
    for index in range(count):
        path = target / f"f{index:04d}.bin"
        start = time.perf_counter()
        os.stat(path)
        values.append(time.perf_counter() - start)
    return stats(values)


def utime_after_delay(count, delay_ms, subdir):
    target = ROOT / subdir
    target.mkdir(parents=True, exist_ok=True)
    values = []
    for index in range(count):
        path = target / f"u{index:03d}.bin"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, PAYLOAD)
        os.close(fd)
        if delay_ms:
            time.sleep(delay_ms / 1000)
        start = time.perf_counter()
        os.utime(path, (time.time(), time.time()))
        values.append(time.perf_counter() - start)
    return stats(values)


def chmod_after_delay(count, delay_ms, subdir):
    target = ROOT / subdir
    target.mkdir(parents=True, exist_ok=True)
    values = []
    for index in range(count):
        path = target / f"c{index:03d}.bin"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, PAYLOAD)
        os.close(fd)
        if delay_ms:
            time.sleep(delay_ms / 1000)
        start = time.perf_counter()
        os.chmod(path, 0o600)
        values.append(time.perf_counter() - start)
    return stats(values)


def unlink_probe(count, subdir, settle):
    target = ROOT / subdir
    target.mkdir(parents=True, exist_ok=True)
    for index in range(count):
        path = target / f"u{index:04d}.bin"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, PAYLOAD)
        os.close(fd)
    if settle:
        drain()
    values = []
    for index in range(count):
        path = target / f"u{index:04d}.bin"
        start = time.perf_counter()
        os.unlink(path)
        values.append(time.perf_counter() - start)
    return stats(values)


def main():
    probe("flush_100_files", lambda: flush_write(100))
    probe("drain_after_flush", drain)

    probe("create_300_files", lambda: create_files(300, "scan"))
    probe("drain_after_create", drain)
    probe("stat_scan_round1_immediate", lambda: scan(300, "scan"))
    probe("stat_scan_round2_immediate", lambda: scan(300, "scan"))
    time.sleep(35)
    probe("stat_scan_round3_after_attr_ttl", lambda: scan(300, "scan"))

    for delay in (0, 50, 100, 200):
        probe(f"utime_after_{delay}ms", lambda d=delay: utime_after_delay(20, d, f"utime-{d}"))
    probe("chmod_pending", lambda: chmod_after_delay(30, 0, "chmod"))

    probe("unlink_pending", lambda: unlink_probe(30, "unlink-pending", False))
    probe("drain_before_settled", drain)
    probe("unlink_settled", lambda: unlink_probe(30, "unlink-settled", True))


if __name__ == "__main__":
    main()
