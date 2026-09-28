#!/usr/bin/env python3
"""Stage a small image and a larger direct child through one Linux file handle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path


def write_all_at(fd: int, data: bytes, offset: int) -> None:
    written = 0
    while written < len(data):
        count = os.pwrite(fd, data[written:], offset + written)
        if count <= 0:
            raise OSError("short pwrite")
        written += count


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--parent-kib", type=int, default=512)
    parser.add_argument("--size-kib", type=int, default=768)
    args = parser.parse_args()
    if args.parent_kib < 4 or args.size_kib <= args.parent_kib:
        parser.error("size-kib must be greater than parent-kib >= 4")

    args.path.parent.mkdir(parents=True, exist_ok=True)
    args.expected.parent.mkdir(parents=True, exist_ok=True)
    parent_total = args.parent_kib << 10
    total = args.size_kib << 10
    header = b"SQLite format 3\x00" + bytes(4096 - 16)
    pattern = bytes(range(256)) * 256
    tail_size = total - len(header)
    payload = header + (
        pattern * ((tail_size + len(pattern) - 1) // len(pattern))
    )[:tail_size]
    timings: dict[str, float] = {}

    args.expected.write_bytes(payload)
    digest = hashlib.sha256(payload)

    fd = os.open(args.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        started = time.monotonic()
        write_all_at(fd, payload[:parent_total], 0)
        os.ftruncate(fd, parent_total)
        timings["parent_write_ms"] = (time.monotonic() - started) * 1000

        started = time.monotonic()
        os.fsync(fd)
        timings["parent_fsync_ms"] = (time.monotonic() - started) * 1000

        started = time.monotonic()
        write_all_at(fd, payload, 0)
        os.ftruncate(fd, total)
        timings["growth_write_ms"] = (time.monotonic() - started) * 1000

        started = time.monotonic()
        os.fsync(fd)
        timings["growth_fsync_ms"] = (time.monotonic() - started) * 1000
        staged_size = os.fstat(fd).st_size
    finally:
        started = time.monotonic()
        os.close(fd)
        timings["close_ms"] = (time.monotonic() - started) * 1000

    result = {
        "expected_sha256": digest.hexdigest(),
        "mounted_sha256": sha256_file(args.path),
        "size": args.path.stat().st_size,
        "staged_size": staged_size,
        "timings_ms": timings,
    }
    args.result.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if (
        result["size"] == total
        and result["staged_size"] == total
        and result["mounted_sha256"] == result["expected_sha256"]
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
