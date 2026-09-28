#!/usr/bin/env python3
"""Restart the none-a tune mount with different flags and measure 300-file unzip."""

import json
import os
import pathlib
import subprocess
import sys
import time

BASE = pathlib.Path(os.environ.get("DRIVE9_BENCH_RUN_DIR", "/home/ubuntu/drive9-replays/replay-20260921-ext"))
MOUNT = os.environ.get("DRIVE9_BENCH_MOUNT", "/mnt/d9-six-none-a")
BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")
CRED_DIR = os.environ.get("DRIVE9_BENCH_CRED_DIR", "/home/ubuntu/drive9-sixway-20260911/credentials")
CRED = json.load(open(os.path.join(CRED_DIR, "none-a.json")))
CACHE = os.environ.get("DRIVE9_BENCH_CACHE", "/home/ubuntu/drive9-replays/replay-20260921-main/cache/none-a")

VARIANTS = [
    ("v0-baseline-fsync", ["--durability", "fsync"]),
    ("v1-auto", ["--durability", "auto"]),
    ("v2-auto-batch100ms", ["--durability", "auto", "--writeback-batch-window", "100ms"]),
    ("v3-auto-batch10ms", ["--durability", "auto", "--writeback-batch-window", "10ms"]),
    ("v4-auto-debounce0", ["--durability", "auto", "--flush-debounce", "0"]),
    ("v5-close-sync", ["--durability", "close-sync"]),
]


def sh(args, timeout=900):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def drain():
    sh([BIN, "mount", "drain", "--timeout", "120s", "--json", MOUNT], timeout=150)


def restart_mount(extra):
    drain()
    for _ in range(30):
        sh(["sudo", "-n", "umount", MOUNT], timeout=60)
        if sh(["mountpoint", "-q", MOUNT], timeout=30).returncode != 0:
            break
        time.sleep(1)
    else:
        raise SystemExit("mount busy: " + MOUNT)
    args = [BIN, "mount", "--foreground", "--mode=fuse", "--server", CRED["server"],
            "--profile", "none", "--cache-dir", CACHE,
            "--dir-ttl", "30s", "--attr-ttl", "30s", "--entry-ttl", "30s",
            "--allow-other", "--gvisor-compat=false"]
    args += extra
    args += [":/benchmark", MOUNT]
    env = {k: v for k, v in os.environ.items() if not k.startswith("DRIVE9_")}
    env["DRIVE9_API_KEY"] = CRED["api_key"]
    with open(BASE / "tune-mount.log", "a") as log:
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=log,
                                stdin=subprocess.DEVNULL, start_new_session=True)
    for _ in range(60):
        if proc.poll() is not None:
            raise SystemExit("mount exited; see tune-mount.log")
        if sh(["mountpoint", "-q", MOUNT], timeout=30).returncode == 0:
            return
        time.sleep(1)
    raise SystemExit("mount timeout")


def measure(tag):
    root = pathlib.Path(MOUNT) / ("tune-" + tag)
    out = BASE / ("tune-" + tag + ".json")
    proc = sh([sys.executable, str(BASE / "cases.py"), "unzip-15k", str(root), str(out),
               "--scale", "0.02"], timeout=900)
    row = json.loads(out.read_text()) if out.exists() else {"verified": False, "error": "no result"}
    return {"tag": tag, "duration_s": row.get("duration_s"), "verified": row.get("verified"),
            "exit": proc.returncode}


def main():
    results = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            row = measure(tag)
            row["flags"] = extra
            results.append(row)
            print(f"{tag:22s} unzip300={row['duration_s']} verified={row['verified']} flags={extra}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "tune-results.json").write_text(json.dumps(results, indent=2))
        print("restored baseline mount; results in tune-results.json", flush=True)


if __name__ == "__main__":
    main()
