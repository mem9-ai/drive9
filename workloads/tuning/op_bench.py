#!/usr/bin/env python3
"""Small-file operation micro-benchmark for locating per-op costs."""

import os
import pathlib
import sys
import time


def bench(label, count, fn):
    t0 = time.perf_counter()
    fn()
    dt = time.perf_counter() - t0
    print(f"{label:26s} {dt:9.3f}s total  {dt/count*1000:9.3f} ms/op", flush=True)


def make_paths(target, prefix, n):
    return [target / f"{prefix}{i:05d}.dat" for i in range(n)]


def main():
    mode = sys.argv[1]
    target = pathlib.Path(sys.argv[2])
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    payload = b"x" * 212
    if mode == "standard":
        target.mkdir(parents=True, exist_ok=False)
        paths = make_paths(target, "w", n)

        def create_write_close():
            for p in paths:
                with p.open("wb") as f:
                    f.write(payload)

        bench("create+write+close", n, create_write_close)

        def rewrite():
            t_open = t_write = t_close = 0.0
            for p in paths:
                t0 = time.perf_counter()
                fd = os.open(p, os.O_WRONLY | os.O_TRUNC)
                t1 = time.perf_counter()
                os.write(fd, payload)
                t2 = time.perf_counter()
                os.close(fd)
                t3 = time.perf_counter()
                t_open += t1 - t0
                t_write += t2 - t1
                t_close += t3 - t2
            print(f"{'open(existing,trunc)':26s} {t_open:9.3f}s total  {t_open/n*1000:9.3f} ms/op", flush=True)
            print(f"{'write 212B':26s} {t_write:9.3f}s total  {t_write/n*1000:9.3f} ms/op", flush=True)
            print(f"{'close':26s} {t_close:9.3f}s total  {t_close/n*1000:9.3f} ms/op", flush=True)

        bench("rewrite (same files)", n, rewrite)

        def stat_all():
            for p in paths:
                p.stat()

        bench("stat (warm)", n, stat_all)

        def read_all():
            for p in paths:
                with p.open("rb") as f:
                    f.read()

        bench("read 212B", n, read_all)

        def unlink_all():
            for p in paths:
                p.unlink()

        bench("unlink", n, unlink_all)
        empty = make_paths(target, "e", n)

        def create_empty():
            for p in empty:
                os.close(os.open(p, os.O_CREAT | os.O_WRONLY, 0o644))

        bench("create-empty+close", n, create_empty)

        def unlink_empty():
            for p in empty:
                p.unlink()

        bench("unlink-empty", n, unlink_empty)
    elif mode == "dirsize":
        fresh = target / "fresh"
        fresh.mkdir(parents=True, exist_ok=False)
        big = pathlib.Path(sys.argv[4])
        for label, d in (("fresh-dir", fresh), ("big-dir", big)):
            ps = [d / f"ds{label}-{i:05d}.dat" for i in range(n)]

            def run():
                for p in ps:
                    with p.open("wb") as f:
                        f.write(payload)

            bench(label + " create+w+c", n, run)
            for p in ps:
                p.unlink()
    else:
        raise SystemExit("unknown mode")


if __name__ == "__main__":
    main()
