"""Extended filesystem workloads: unzip 15k small files, git clone --depth=1, git status."""

import argparse
import hashlib
import json
import os
import pathlib
import random
import subprocess
import time
import traceback

CASES = ("unzip-15k", "git-clone-depth1", "git-status", "overlay-install", "bare-writes",
         "op-write", "op-open-create", "op-flush", "op-close", "op-fsync",
         "op-unlink-pending", "op-unlink-settled", "op-rename-pending", "op-utimes")

DRIVE9_BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")

FIXTURES = pathlib.Path(os.environ.get("DRIVE9_BENCH_FIXTURES", "/home/ubuntu/d9work/ext-fixtures"))
MANIFEST = FIXTURES / "manifest.json"


def load_manifest():
    data = json.loads(MANIFEST.read_text())
    return data


def file_content(index):
    return f"file-{index:06d}-content-".encode() + b"x" * 192


def file_name(index):
    return f"f{index:05d}.txt"


def install_file_bytes(pkg, idx):
    return f"pkg-{pkg:03d}-file-{idx:02d}-".encode() + b"x" * 200


def payload(size, generation=0):
    return random.Random(731 + generation).randbytes(size)


def write_file(path, data):
    with path.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def fixture_counts(scale):
    manifest = load_manifest()
    if scale >= 1:
        return manifest["zip_full"], manifest["repo_full"], FIXTURES / manifest["repo_full_name"]
    return manifest["zip_smoke"], manifest["repo_smoke"], FIXTURES / manifest["repo_smoke_name"]


class Clock:
    def __init__(self):
        self.phases = {}

    def call(self, phase, fn):
        start = time.perf_counter_ns()
        value = fn()
        elapsed = (time.perf_counter_ns() - start) / 1e9
        self.phases[phase] = self.phases.get(phase, 0) + elapsed
        return value


def run_tool(args, timeout):
    subprocess.run(args, check=True, timeout=timeout,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_for_count(directory, expected, timeout=120.0):
    """Poll a directory listing until it shows `expected` entries (writeback listings can lag)."""
    deadline = time.monotonic() + timeout
    while True:
        count = len(list(directory.iterdir()))
        if count == expected:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"listing {directory} shows {count} of {expected} entries")
        time.sleep(1.0)


def op_stat(latencies):
    values = sorted(latencies)
    if not values:
        return None

    def pct(p):
        return values[min(len(values) - 1, max(0, round(len(values) * p) - 1))]

    return {"count": len(values), "avg_ms": round(sum(values) / len(values) * 1000, 3),
            "p50_ms": round(pct(0.50) * 1000, 3), "p95_ms": round(pct(0.95) * 1000, 3),
            "max_ms": round(values[-1] * 1000, 3)}


def settle(root):
    """Flush pending writes before timing settled-op variants (drive9 drain / sync -f)."""
    target = subprocess.check_output(["findmnt", "-T", str(root), "-o", "TARGET", "-n"],
                                     text=True).strip()
    fstype = subprocess.check_output(["findmnt", "-T", str(root), "-o", "FSTYPE", "-n"],
                                     text=True).strip()
    if fstype == "fuse.drive9":
        proc = subprocess.run([DRIVE9_BIN, "mount", "drain", "--timeout", "300s", "--json", target],
                              check=True, capture_output=True, text=True, timeout=330)
        assert json.loads(proc.stdout)["ok"]
    else:
        subprocess.run(["sync", "-f", target], check=True, capture_output=True, timeout=330)


def op_count(scale):
    return max(1, int(100 * scale))


def execute_op_write(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    target = root / "write.dat"
    lat = []
    fd = os.open(target, os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        for _ in range(n):
            t0 = time.perf_counter()
            os.write(fd, data)
            lat.append(time.perf_counter() - t0)
    finally:
        os.close(fd)
    clock.phases["write"] = sum(lat)
    return {"checks": n, "sample": target, "sample_data": data * n, "op_stats": op_stat(lat)}


def execute_op_open_create(root, clock, scale):
    n = op_count(scale)
    lat = []
    fds = []
    for i in range(n):
        p = root / f"open-{i:04d}.dat"
        t0 = time.perf_counter()
        fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
        lat.append(time.perf_counter() - t0)
        fds.append(fd)
    for fd in fds:
        os.close(fd)
    clock.phases["open_create"] = sum(lat)
    return {"checks": n, "sample": root / "open-0000.dat", "sample_data": b"",
            "op_stats": op_stat(lat)}


def execute_op_flush(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        p = root / f"flush-{i:04d}.dat"
        fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, data)
        dup = os.dup(fd)
        t0 = time.perf_counter()
        os.close(dup)
        lat.append(time.perf_counter() - t0)
        os.close(fd)
    clock.phases["flush_only"] = sum(lat)
    return {"checks": n, "sample": root / "flush-0000.dat", "sample_data": data,
            "op_stats": op_stat(lat)}


def execute_op_close(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        p = root / f"close-{i:04d}.dat"
        fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, data)
        t0 = time.perf_counter()
        os.close(fd)
        lat.append(time.perf_counter() - t0)
    clock.phases["close_flush_release"] = sum(lat)
    return {"checks": n, "sample": root / "close-0000.dat", "sample_data": data,
            "op_stats": op_stat(lat)}


def execute_op_fsync(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        p = root / f"fsync-{i:04d}.dat"
        with p.open("wb") as stream:
            stream.write(data)
            stream.flush()
            t0 = time.perf_counter()
            os.fsync(stream.fileno())
            lat.append(time.perf_counter() - t0)
    clock.phases["fsync"] = sum(lat)
    return {"checks": n, "sample": root / "fsync-0000.dat", "sample_data": data,
            "op_stats": op_stat(lat)}


def execute_op_unlink_pending(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        p = root / f"ul-p-{i:04d}.dat"
        with p.open("wb") as stream:
            stream.write(data)
            stream.flush()
        t0 = time.perf_counter()
        p.unlink()
        lat.append(time.perf_counter() - t0)
    clock.phases["unlink_pending"] = sum(lat)
    return {"checks": n, "sample": None, "absent": str((root / "ul-p-0000.dat").relative_to(root)),
            "op_stats": op_stat(lat)}


def execute_op_unlink_settled(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    paths = []
    for i in range(n):
        p = root / f"ul-s-{i:04d}.dat"
        with p.open("wb") as stream:
            stream.write(data)
        paths.append(p)
    settle(root)
    lat = []
    for p in paths:
        t0 = time.perf_counter()
        p.unlink()
        lat.append(time.perf_counter() - t0)
    clock.phases["unlink_settled"] = sum(lat)
    return {"checks": n, "sample": None, "absent": str(paths[0].relative_to(root)),
            "op_stats": op_stat(lat)}


def execute_op_rename_pending(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        src = root / f"rn-src-{i:04d}.dat"
        dst = root / f"rn-dst-{i:04d}.dat"
        with src.open("wb") as stream:
            stream.write(data)
            stream.flush()
        t0 = time.perf_counter()
        os.rename(src, dst)
        lat.append(time.perf_counter() - t0)
    clock.phases["rename_pending"] = sum(lat)
    return {"checks": n, "sample": root / "rn-dst-0000.dat", "sample_data": data,
            "absent": str((root / "rn-src-0000.dat").relative_to(root)), "op_stats": op_stat(lat)}


def execute_op_utimes(root, clock, scale):
    n = op_count(scale)
    data = payload(4096)
    lat = []
    for i in range(n):
        p = root / f"ut-{i:04d}.dat"
        with p.open("wb") as stream:
            stream.write(data)
            stream.flush()
        t0 = time.perf_counter()
        os.utime(p, None)
        lat.append(time.perf_counter() - t0)
    clock.phases["utimes"] = sum(lat)
    return {"checks": n, "sample": root / "ut-0000.dat", "sample_data": data,
            "op_stats": op_stat(lat)}


def execute_unzip(root, clock, scale):
    zip_count, _, _ = fixture_counts(scale)
    archive = FIXTURES / f"unzip-{zip_count}.zip"
    target = root / "extracted"
    target.mkdir()
    clock.call("unzip", lambda: run_tool(
        ["unzip", "-q", "-o", str(archive), "-d", str(target)], 3600))
    files = sorted(p.name for p in target.iterdir())
    assert len(files) == zip_count, (len(files), zip_count)
    for idx in (0, zip_count // 2, zip_count - 1):
        assert (target / file_name(idx)).read_bytes() == file_content(idx)
    sample = target / file_name(0)
    return {"checks": zip_count, "sample": sample, "sample_data": file_content(0)}


def execute_clone(root, clock, scale):
    _, repo_count, source = fixture_counts(scale)
    target = root / "clone"
    clock.call("clone", lambda: run_tool(
        ["git", "clone", "--depth=1", "--no-local", f"file://{source}", str(target)], 3600))
    tracked = [t.decode() for t in
               subprocess.check_output(["git", "-C", str(target), "ls-files", "-z"]).split(b"\0") if t]
    assert len(tracked) == repo_count, (len(tracked), repo_count)
    sample_rel = tracked[0]
    sample = target / sample_rel
    sample_data = sample.read_bytes()
    source_data = subprocess.check_output(
        ["git", "--git-dir", str(source), "show", f"HEAD:{sample_rel}"])
    assert sample_data == source_data, sample_rel
    return {"checks": repo_count, "sample": sample, "sample_data": sample_data}


def execute_status(root, clock, scale):
    _, repo_count, source = fixture_counts(scale)
    target = root / "clone"
    run_tool(["git", "clone", "--depth=1", "--no-local", f"file://{source}", str(target)], 3600)
    clock.call("status", lambda: run_tool(["git", "-C", str(target), "status"], 3600))
    porcelain = subprocess.check_output(
        ["git", "-C", str(target), "status", "--porcelain"], text=True)
    assert porcelain.strip() == "", porcelain
    tracked = [t.decode() for t in
               subprocess.check_output(["git", "-C", str(target), "ls-files", "-z"]).split(b"\0") if t]
    assert len(tracked) == repo_count, (len(tracked), repo_count)
    sample_rel = tracked[0]
    sample = target / sample_rel
    sample_data = sample.read_bytes()
    source_data = subprocess.check_output(
        ["git", "--git-dir", str(source), "show", f"HEAD:{sample_rel}"])
    assert sample_data == source_data, sample_rel
    return {"checks": repo_count, "sample": sample, "sample_data": sample_data}


def execute_overlay_install(root, clock, scale):
    packages = max(1, int(100 * scale))
    per_package = 20
    proj = root / "proj"
    mods = proj / "node_modules"
    mods.mkdir(parents=True)

    def install():
        for p in range(packages):
            d = mods / f"pkg-{p:03d}"
            d.mkdir()
            for f in range(per_package):
                with (d / f"f{f:02d}.js").open("wb") as stream:
                    stream.write(install_file_bytes(p, f))
        with (proj / "package.json").open("wb") as stream:
            stream.write(b'{"name":"bench-install","version":"1.0.0"}')

    clock.call("install", install)
    for p, f in ((0, 0), (packages - 1, per_package - 1)):
        assert (mods / f"pkg-{p:03d}" / f"f{f:02d}.js").read_bytes() == install_file_bytes(p, f)
    wait_for_count(mods, packages)
    manifest = proj / "package.json"
    return {"checks": packages * per_package + 1, "sample": manifest,
            "sample_data": manifest.read_bytes()}


def execute_bare_writes(root, clock, scale):
    count = max(1, int(2000 * scale))
    target = root / "files"
    target.mkdir()
    data = payload(212)

    def write_all():
        for i in range(count):
            with (target / f"w{i:05d}.dat").open("wb") as stream:
                stream.write(data)

    clock.call("write", write_all)
    wait_for_count(target, count)
    sample = target / "w00000.dat"
    sample_data = sample.read_bytes()
    assert sample_data == data
    return {"checks": count, "sample": sample, "sample_data": sample_data}


def execute(case, root, scale=1):
    root = pathlib.Path(root)
    root.mkdir(mode=0o755, parents=True, exist_ok=False)
    clock = Clock()
    if case == "unzip-15k":
        result = execute_unzip(root, clock, scale)
    elif case == "git-clone-depth1":
        result = execute_clone(root, clock, scale)
    elif case == "git-status":
        result = execute_status(root, clock, scale)
    elif case == "overlay-install":
        result = execute_overlay_install(root, clock, scale)
    elif case == "bare-writes":
        result = execute_bare_writes(root, clock, scale)
    elif case == "op-write":
        result = execute_op_write(root, clock, scale)
    elif case == "op-open-create":
        result = execute_op_open_create(root, clock, scale)
    elif case == "op-flush":
        result = execute_op_flush(root, clock, scale)
    elif case == "op-close":
        result = execute_op_close(root, clock, scale)
    elif case == "op-fsync":
        result = execute_op_fsync(root, clock, scale)
    elif case == "op-unlink-pending":
        result = execute_op_unlink_pending(root, clock, scale)
    elif case == "op-unlink-settled":
        result = execute_op_unlink_settled(root, clock, scale)
    elif case == "op-rename-pending":
        result = execute_op_rename_pending(root, clock, scale)
    elif case == "op-utimes":
        result = execute_op_utimes(root, clock, scale)
    else:
        raise ValueError(case)
    witness = None
    if result.get("sample") is not None:
        sample_data = result["sample_data"]
        witness = {"path": str(result["sample"].relative_to(root)),
                   "size": len(sample_data),
                   "sha256": hashlib.sha256(sample_data).hexdigest()}
    row = {"case": case, "duration_s": sum(clock.phases.values()),
            "phases_s": clock.phases, "verified": True, "checks": result["checks"],
            "sample": witness, "absent": None, "scale": scale, "root": str(root)}
    if result.get("op_stats"):
        row["op_stats"] = result["op_stats"]
    if result.get("absent"):
        row["absent"] = result["absent"]
    return row


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=CASES)
    parser.add_argument("root")
    parser.add_argument("output")
    parser.add_argument("--scale", type=float, default=1)
    args = parser.parse_args()
    started = time.time()
    try:
        row = execute(args.case, args.root, args.scale)
    except Exception as error:
        row = {"case": args.case, "verified": False, "error_type": type(error).__name__,
               "error": str(error), "traceback": traceback.format_exc(), "root": args.root}
    row.update(start_epoch=started, end_epoch=time.time())
    pathlib.Path(args.output).write_text(json.dumps(row, indent=2))
    print(json.dumps(row), flush=True)
    raise SystemExit(0 if row["verified"] else 1)
