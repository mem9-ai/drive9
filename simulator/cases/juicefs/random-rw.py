#!/usr/bin/env python3
"""juicefs/random-rw helper: 1024 random 1-4096B pwrite+fsync ops against a
4MiB file in the current directory (the mount), verified against an
in-memory model. Deterministic seed. Exit 0 = model verified."""
import os
import random
import sys

SIZE = 4 << 20
OPS = 1024
rng = random.Random(20260930)
model = bytearray(SIZE)
with open("rw.bin", "wb") as f:
    f.write(model)
    f.flush()
    os.fsync(f.fileno())
    for _ in range(OPS):
        off = rng.randrange(0, SIZE - 4096)
        n = rng.randrange(1, 4097)
        data = rng.randbytes(n)
        f.seek(off)
        f.write(data)
        os.fsync(f.fileno())
        model[off : off + n] = data
with open("rw.bin", "rb") as f:
    got = f.read()
if got != bytes(model):
    print(f"model mismatch: {len(got)} bytes read vs {len(model)} expected", file=sys.stderr)
    sys.exit(1)
print("random-rw model verified")
