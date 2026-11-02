#!/usr/bin/env python3
"""juicefs/random-stress helper: 4 workers x 64 files of concurrent
create / read-back / rename, all inside the current directory (the mount).
Exit 0 = every read-back matched."""
import os
import sys
import threading

WORKERS = 4
FILES = 64


def worker(tid: int) -> list[str]:
    errors = []
    for n in range(FILES):
        name = f"stress-{tid}-{n}.part"
        data = f"w{tid}f{n}".encode() * 64
        with open(name, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        with open(name, "rb") as f:
            if f.read() != data:
                errors.append(f"readback mismatch {name}")
        final = f"stress-{tid}-{n}"
        os.replace(name, final)
        with open(final, "rb") as f:
            if f.read() != data:
                errors.append(f"post-rename mismatch {final}")
    return errors


threads: dict[int, list[str]] = {}
ts = []
for tid in range(WORKERS):
    t = threading.Thread(target=lambda tid=tid: threads.__setitem__(tid, worker(tid)))
    t.start()
    ts.append(t)
for t in ts:
    t.join()

all_errors = [e for errs in threads.values() for e in errs]
for e in all_errors:
    print(e, file=sys.stderr)
print(f"random-stress: {WORKERS * FILES} files, {len(all_errors)} errors")
sys.exit(1 if all_errors else 0)
