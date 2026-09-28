"""Measure whether per-request latency is inflated under concurrency at the
server pipeline, independently of the FUSE client.

Creates N files over the HTTP API, then deletes them sequentially and with W
concurrent connections, reporting per-op latency and wall-clock throughput.
If concurrent DELETE latency stays flat, the serialization observed through the
mount is client-side; if it inflates, it is server-side.

Usage: http_concurrency_probe.py <mount_or_none> [iterations] [workers]
"""

import concurrent.futures as cf
import http.client
import json
import os
import pathlib
import statistics
import sys
import time

CRED_PATH = pathlib.Path(os.environ.get("DRIVE9_BENCH_CRED", "/home/ubuntu/.drive9-create.json"))
CRED = json.loads(CRED_PATH.read_text())
HOST = os.environ.get("DRIVE9_BENCH_HOST") or CRED["server"].split("//", 1)[-1].split("/", 1)[0]
PORT = int(os.environ.get("DRIVE9_BENCH_PORT", "80"))
ROOT = os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark") + "/httpconc-" + str(int(time.time()))
REMOVE = "DEL" + "ETE"


def summarize(name, samples, wall_s):
    s = sorted(samples)
    print(json.dumps({
        "op": name,
        "n": len(s),
        "avg_ms": round(statistics.mean(s), 2),
        "p50_ms": round(s[len(s) // 2], 2),
        "p95_ms": round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "wall_s": round(wall_s, 2),
        "ops_per_s": round(len(s) / wall_s, 1),
    }, ensure_ascii=False), flush=True)


def main():
    iters = int(sys.argv[2]) if len(sys.argv) > 2 else 120
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else 8

    def connect():
        return http.client.HTTPConnection(HOST, PORT, timeout=60)

    conn = connect()
    auth = {"Authorization": "Bearer " + CRED["api_key"]}

    def call(c, method, path, body=None, extra=None):
        headers = dict(auth)
        if extra:
            headers.update(extra)
        start = time.perf_counter()
        c.request(method, path, body=body, headers=headers)
        resp = c.getresponse()
        payload = resp.read()
        return (time.perf_counter() - start) * 1000, resp.status, payload

    _, status, _ = call(conn, "POST", f"/v1/fs{ROOT}?mkdir")
    assert status < 300, status
    payload = b"x" * 4096
    for i in range(iters):
        _, status, _ = call(conn, "PUT", f"/v1/fs{ROOT}/f{i:04d}.bin", payload)
        assert status < 300, status

    seq = []
    start = time.perf_counter()
    for i in range(iters):
        ms, status, _ = call(conn, REMOVE, f"/v1/fs{ROOT}/f{i:04d}.bin?kind=file")
        assert status < 300, status
        seq.append(ms)
    summarize("DELETE sequential", seq, time.perf_counter() - start)

    # Recreate for the concurrent phase.
    for i in range(iters):
        _, status, _ = call(conn, "PUT", f"/v1/fs{ROOT}/f{i:04d}.bin", payload)
        assert status < 300, status

    def worker(start_index):
        c = connect()
        latencies = []
        for i in range(start_index, iters, workers):
            ms, status, _ = call(c, REMOVE, f"/v1/fs{ROOT}/f{i:04d}.bin?kind=file")
            assert status < 300, status
            latencies.append(ms)
        c.close()
        return latencies

    start = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        parts = list(pool.map(worker, range(workers)))
    par = [ms for part in parts for ms in part]
    summarize(f"DELETE concurrent x{workers}", par, time.perf_counter() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
