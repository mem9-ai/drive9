#!/usr/bin/env python3
"""juicefs/fsrand helper: deterministic random filesystem operations with a
local-directory oracle mirror. Every 25 ops (and at the end) the mount tree
is compared to the oracle: identical path sets, kinds and sha256 digests.
Exit 0 = final comparison clean."""
import hashlib
import os
import random
import shutil
import sys
import tempfile

OPS = 1000
SEED = 9
CHECK_EVERY = 25


def digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot(root: str) -> dict[str, tuple[str, str]]:
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        if rel != ".":
            out[rel + "/"] = ("dir", "")
        for name in filenames:
            p = os.path.join(dirpath, name)
            rp = os.path.relpath(p, root)
            out[rp] = ("file", digest(p))
    return out


rng = random.Random(SEED)
names = [f"f{i:03d}" for i in range(40)]
dirs = ["."]


def rand_path() -> str:
    return os.path.join(rng.choice(dirs), rng.choice(names))


oracle = tempfile.mkdtemp(prefix="fsrand-oracle-")
mount = os.getcwd()
failures = []
for step in range(OPS):
    op = rng.choice(["mkdir", "write", "append", "rename", "unlink", "truncate"])
    src = os.path.join(oracle, rand_path())
    msrc = os.path.join(mount, os.path.relpath(src, oracle))
    try:
        if op == "mkdir":
            d = f"d{rng.randrange(100):03d}"
            os.mkdir(os.path.join(oracle, d))
            os.mkdir(os.path.join(mount, d))
            dirs.append(d)
        elif op == "write":
            data = f"op{step}".encode() * rng.randrange(1, 20)
            with open(src, "wb") as f:
                f.write(data)
            with open(msrc, "wb") as f:
                f.write(data)
        elif op == "append" and os.path.exists(src) and os.path.isfile(src):
            data = f"+{step}".encode()
            with open(src, "ab") as f:
                f.write(data)
            with open(msrc, "ab") as f:
                f.write(data)
        elif op == "rename" and os.path.exists(src):
            dst_name = f"r{rng.randrange(1000):04d}"
            os.replace(src, os.path.join(os.path.dirname(src), dst_name))
            os.replace(msrc, os.path.join(os.path.dirname(msrc), dst_name))
        elif op == "unlink" and os.path.exists(src) and os.path.isfile(src):
            os.unlink(src)
            os.unlink(msrc)
        elif op == "truncate" and os.path.exists(src) and os.path.isfile(src):
            size = rng.randrange(0, 50)
            os.truncate(src, size)
            os.truncate(msrc, size)
    except FileNotFoundError:
        continue
    if (step + 1) % CHECK_EVERY == 0:
        a, b = snapshot(oracle), snapshot(mount)
        if a != b:
            failures.append(f"divergence at op {step + 1}")
            break

if not failures:
    a, b = snapshot(oracle), snapshot(mount)
    if a != b:
        failures.append("final divergence")
shutil.rmtree(oracle, ignore_errors=True)
for f in failures:
    print(f, file=sys.stderr)
print(f"fsrand: {OPS} ops, {len(failures)} divergences")
sys.exit(1 if failures else 0)
