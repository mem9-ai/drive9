"""Focused PUT probe: measures the server-side small-file write path only.

Used for the P3 (write path / quota cache) A/B on staging: run it once as the
baseline, change a server-side knob, run it again, and compare the
server_write_timing / backend_write_create_timing logs for both windows.
"""

import http.client
import json
import os
import pathlib
import statistics
import time

CRED_PATH = pathlib.Path(os.environ.get(
    "DRIVE9_BENCH_CRED",
    os.environ.get("DRIVE9_BENCH_CRED_DIR", "/home/ubuntu/drive9-sixway-20260911/credentials") + "/none-a.json",
))
CRED = json.loads(CRED_PATH.read_text())
HOST = os.environ.get("DRIVE9_BENCH_HOST", "drive9.example.invalid")
SCHEME = os.environ.get("DRIVE9_BENCH_SCHEME", "https")
PORT = int(os.environ.get("DRIVE9_BENCH_PORT", "443" if SCHEME == "https" else "80"))
REMOTE_ROOT = os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark")
COUNT = int(os.environ.get("PUT_PROBE_COUNT", "60"))
LABEL = os.environ.get("PUT_PROBE_LABEL", "run")


def main():
    factory = http.client.HTTPSConnection if SCHEME == "https" else http.client.HTTPConnection
    conn = factory(HOST, PORT, timeout=120)
    auth = {"Authorization": "Bearer " + CRED["api_key"]}

    def call(method, path, body=None, extra=None):
        headers = dict(auth)
        if extra:
            headers.update(extra)
        start = time.perf_counter()
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        payload = resp.read()
        return (time.perf_counter() - start) * 1000, resp.status, payload

    root = f"{REMOTE_ROOT}/putprobe-{LABEL}-{int(time.time())}"
    status = call("POST", f"/v1/fs{root}?mkdir")[1]
    if status >= 300:
        raise SystemExit(f"mkdir failed: {status}")

    values = []
    payload = b"x" * 212
    for index in range(COUNT):
        ms, status, body = call("PUT", f"/v1/fs{root}/f{index:04d}.bin", payload)
        if status >= 300:
            raise SystemExit(f"put failed: {status} {body[:120]!r}")
        values.append(ms)

    ordered = sorted(values)
    print(json.dumps({
        "label": LABEL,
        "root": root,
        "count": COUNT,
        "avg_ms": round(statistics.mean(values), 2),
        "p50_ms": round(ordered[len(ordered) // 2], 2),
        "p95_ms": round(ordered[max(0, int(len(ordered) * 0.95) - 1)], 2),
        "min_ms": round(ordered[0], 2),
        "max_ms": round(ordered[-1], 2),
        "epoch": int(time.time()),
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
