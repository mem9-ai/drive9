"""Compare per-path HEAD stat against /v1/fs:batch-stat on the same file set."""

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
# The server rejects batches above 256 paths (maxBatchStatPaths); stay under it.
COUNT = int(os.environ.get("BATCH_PROBE_COUNT", "250"))


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

    # Build a flat directory with COUNT small files via the API itself.
    root = f"{REMOTE_ROOT}/srvopt-{int(time.time())}"
    call("POST", f"/v1/fs{root}?mkdir")
    for index in range(COUNT):
        status = call("PUT", f"/v1/fs{root}/f{index:04d}.bin", b"x" * 212)[1]
        if status >= 300:
            raise SystemExit(f"put failed: {status}")

    paths = [f"{root}/f{index:04d}.bin" for index in range(COUNT)]

    individual = []
    for path in paths:
        ms, status, _ = call("HEAD", f"/v1/fs{path}")
        assert status < 300, status
        individual.append(ms)

    batch = []
    for _ in range(5):
        body = json.dumps({"paths": paths}).encode()
        ms, status, payload = call("POST", "/v1/fs:batch-stat", body, {"Content-Type": "application/json"})
        assert status < 300, (status, payload[:200])
        batch.append(ms)

    # Set-based proxy: one directory listing returns per-entry metadata for all
    # COUNT children in a single query, which is what a set-based StatPaths
    # would look like at the datastore layer.
    listing = []
    for _ in range(5):
        ms, status, payload = call("GET", f"/v1/fs{root}?list=1")
        assert status < 300, (status, payload[:200])
        decoded = json.loads(payload)
        entries = decoded["entries"] if isinstance(decoded, dict) else decoded
        assert len(entries) >= COUNT - 2, payload[:200]
        listing.append(ms)

    print(json.dumps({
        "count": COUNT,
        "individual_head": {"total_s": round(sum(individual) / 1000, 3),
                            "avg_ms": round(statistics.mean(individual), 2),
                            "p50_ms": round(sorted(individual)[len(individual) // 2], 2)},
        "batch_stat": {"runs": len(batch), "avg_ms": round(statistics.mean(batch), 2),
                       "per_path_ms": round(statistics.mean(batch) / COUNT, 3)},
        "directory_list_proxy": {"runs": len(listing), "avg_ms": round(statistics.mean(listing), 2),
                                 "per_path_ms": round(statistics.mean(listing) / COUNT, 3)},
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
