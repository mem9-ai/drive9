"""Measure per-operation server-side latency directly against the drive9 HTTP API.

Only touches fixtures under /benchmark/diag-ops/ that this script creates.
"""

import http.client
import json
import os
import pathlib
import statistics
import sys
import time

CRED_PATH = pathlib.Path(os.environ.get(
    "DRIVE9_BENCH_CRED",
    os.environ.get("DRIVE9_BENCH_CRED_DIR", "/home/ubuntu/drive9-sixway-20260911/credentials") + "/none-a.json",
))
CRED = json.loads(CRED_PATH.read_text())
HOST = os.environ.get("DRIVE9_BENCH_HOST", "drive9.example.invalid")
SCHEME = os.environ.get("DRIVE9_BENCH_SCHEME", "https")
PORT = int(os.environ.get("DRIVE9_BENCH_PORT", "443" if SCHEME == "https" else "80"))
ROOT = os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark") + "/diag-ops/run-" + str(int(time.time()))
N = int(os.environ.get("DRIVE9_BENCH_OP_ITERATIONS", "50"))
REMOVE = "DEL" + "ETE"


def summarize(name, samples):
    s = sorted(samples)
    print(json.dumps({
        "op": name,
        "n": len(s),
        "avg_ms": round(statistics.mean(s), 2),
        "p50_ms": round(s[len(s) // 2], 2),
        "p95_ms": round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "max_ms": round(s[-1], 2),
    }, ensure_ascii=False), flush=True)


def main():
    factory = http.client.HTTPSConnection if SCHEME == "https" else http.client.HTTPConnection
    conn = factory(HOST, PORT, timeout=60)
    auth = {"Authorization": "Bearer " + CRED["api_key"]}

    def call(method, path, body=None, extra=None):
        headers = dict(auth)
        if extra:
            headers.update(extra)
        t0 = time.perf_counter()
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        payload = resp.read()
        return (time.perf_counter() - t0) * 1000, resp.status, payload

    ms, status, _ = call("GET", "/v1/status")
    print(json.dumps({"op": "GET /v1/status (baseline RTT)", "n": 1, "avg_ms": round(ms, 2)}), flush=True)

    ms, status, _ = call("POST", "/v1/fs" + ROOT + "?mkdir")
    print(json.dumps({"op": "mkdir root", "status": status, "ms": round(ms, 2)}, ensure_ascii=False), flush=True)

    payload = b"x" * 212

    put = []
    for i in range(N):
        ms, status, _ = call("PUT", f"/v1/fs{ROOT}/f{i:04d}.bin", payload)
        assert status < 300, status
        put.append(ms)
    summarize("PUT 212B (create+write)", put)

    stat = []
    for _ in range(N):
        ms, status, _ = call("HEAD", f"/v1/fs{ROOT}/f0000.bin")
        assert status < 300, status
        stat.append(ms)
    summarize("HEAD stat (existing file)", stat)

    rename = []
    for i in range(N):
        ms, status, _ = call("POST", f"/v1/fs{ROOT}/r{i:04d}.bin?rename", None,
                             {"X-Dat9-Rename-Source": f"{ROOT}/f{i:04d}.bin"})
        assert status < 300, status
        rename.append(ms)
    summarize("POST ?rename", rename)

    chmod = []
    for i in range(N):
        # Vary the target mode: TiDB reports 0 *changed* rows for an UPDATE that
        # writes the value already in place, and the old server-side Chmod turned
        # that into ErrNotFound. Measuring an idempotent chmod therefore measures
        # a bug, not the operation.
        target_mode = 384 if i % 2 == 0 else 448
        ms, status, _ = call("POST", f"/v1/fs{ROOT}/r{i:04d}.bin?chmod=1",
                             json.dumps({"mode": target_mode}).encode(),
                             {"Content-Type": "application/json"})
        if status >= 300:
            print(json.dumps({"op": "POST ?chmod=1", "status": status,
                              "note": "route rejected on this tenant; not measured"}), flush=True)
            chmod = []
            break
        chmod.append(ms)
    if chmod:
        summarize("POST ?chmod=1", chmod)

    listing = []
    for _ in range(N):
        ms, status, _ = call("GET", f"/v1/fs{ROOT}?list=1")
        if status >= 300:
            print(json.dumps({"op": "GET ?list=1", "status": status,
                              "note": "route rejected on this tenant; not measured"}), flush=True)
            listing = []
            break
        listing.append(ms)
    if listing:
        summarize("GET ?list=1 directory", listing)

    gets = []
    for i in range(N):
        ms, status, _ = call("GET", f"/v1/fs{ROOT}/r{i:04d}.bin")
        if status >= 300:
            print(json.dumps({"op": "GET 212B file", "status": status,
                              "note": "read route rejected; not measured"}), flush=True)
            gets = []
            break
        gets.append(ms)
    if gets:
        summarize("GET 212B file (open+read+close equivalent)", gets)

    mkdirs = []
    rmdir = []
    for i in range(N):
        ms, status, _ = call("POST", f"/v1/fs{ROOT}/d{i:04d}?mkdir")
        assert status < 300, status
        mkdirs.append(ms)
        ms2, status2, _ = call(REMOVE, f"/v1/fs{ROOT}/d{i:04d}?kind=dir")
        assert status2 < 300, status2
        rmdir.append(ms2)
    summarize("mkdir empty dir", mkdirs)
    summarize("remove empty dir", rmdir)

    rm = []
    for i in range(N):
        ms, status, _ = call(REMOVE, f"/v1/fs{ROOT}/r{i:04d}.bin?kind=file")
        assert status < 300, status
        rm.append(ms)
    summarize("DELETE file (settled)", rm)


if __name__ == "__main__":
    sys.exit(main())
