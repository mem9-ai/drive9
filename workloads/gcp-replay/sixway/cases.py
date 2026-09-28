"""Ten fixed filesystem workloads. No mount management or cross-test parallelism."""

import argparse
import errno
import hashlib
import json
import multiprocessing as mp
import os
import pathlib
import random
import stat
import subprocess
import time
import traceback

CASES = (
    "rename",
    "atomic-replace",
    "ls-stat-consistency",
    "copy-file",
    "copy-tree",
    "overwrite-delete-recreate",
    "many-small-files",
    "medium-file",
    "concurrent-files",
    "permissions",
)


def payload(size, generation=0):
    return random.Random(731 + generation).randbytes(size)


def write_file(path, data):
    with path.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def copy_file(source, target):
    with source.open("rb") as src, target.open("wb") as dst:
        while block := src.read(65536):
            dst.write(block)
        dst.flush()
        os.fsync(dst.fileno())


class Clock:
    def __init__(self):
        self.phases = {}

    def call(self, phase, fn):
        start = time.perf_counter_ns()
        value = fn()
        elapsed = (time.perf_counter_ns() - start) / 1e9
        self.phases[phase] = self.phases.get(phase, 0) + elapsed
        return value


def execute(case, root, scale=1):
    root = pathlib.Path(root)
    root.mkdir(mode=0o755, parents=True, exist_ok=False)
    clock = Clock()
    data = payload(4096)
    checks = 0
    sample = None
    sample_data = None
    absent = None
    count = max(1, int(100 * scale))
    if case == "rename":
        src, dst = root / "source", root / "destination"
        src.mkdir()
        dst.mkdir()
        for i in range(count):
            write_file(src / f"file-{i:04d}.dat", data)

        def same():
            for i in range(count):
                os.rename(src / f"file-{i:04d}.dat", src / f"renamed-{i:04d}.dat")

        def cross():
            for i in range(count):
                os.rename(src / f"renamed-{i:04d}.dat", dst / f"renamed-{i:04d}.dat")

        clock.call("same_directory", same)
        clock.call("cross_directory", cross)
        assert not list(src.iterdir()) and len(list(dst.iterdir())) == count
        for path in dst.iterdir():
            assert path.read_bytes() == data
        checks = count
        sample, sample_data = dst / "renamed-0000.dat", data
        absent = src / "file-0000.dat"
    elif case == "atomic-replace":
        target = root / "target.dat"
        generations = [payload(4096, i) for i in range(count + 1)]
        write_file(target, generations[0])
        for i in range(count):
            staged = root / f"staged-{i:04d}.new"
            clock.call(
                "write_fsync_close", lambda: write_file(staged, generations[i + 1])
            )
            clock.call("replace", lambda: os.replace(staged, target))
        assert target.read_bytes() == generations[-1] and len(list(root.iterdir())) == 1
        sample, sample_data, checks = target, generations[-1], 1
    elif case == "ls-stat-consistency":
        count = max(1, int(200 * scale))
        paths = [root / f"file-{i:04d}.dat" for i in range(count)]
        clock.call("create_fsync", lambda: [write_file(path, data) for path in paths])
        names = clock.call("list", lambda: os.listdir(root))
        attrs = clock.call("stat", lambda: [os.stat(path) for path in paths])
        assert set(names) == {p.name for p in paths}, {
            "names": names,
            "expected": [p.name for p in paths],
        }
        assert all(s.st_size == 4096 and stat.S_ISREG(s.st_mode) for s in attrs), [
            {"size": s.st_size, "mode": s.st_mode} for s in attrs
        ]
        checks = count
        sample, sample_data = paths[0], data
    elif case == "copy-file":
        for size in (1024, 65536, 1048576):
            content = payload(size)
            source, target = root / f"source-{size}.dat", root / f"copy-{size}.dat"
            write_file(source, content)
            assert (
                source.read_bytes() == content
            )  # Explicit warm source, outside timing.
            clock.call(f"copy_{size}_bytes", lambda: copy_file(source, target))
            assert source.read_bytes() == target.read_bytes() == content
            checks += 1
            sample, sample_data = target, content
    elif case == "copy-tree":
        source, target = root / "source", root / "destination"
        source.mkdir()
        expected = {}
        for directory in range(3):
            parent = source / f"dir-{directory}"
            parent.mkdir()
            for size in (1024, 4096, 65536, 1048576):
                rel = pathlib.Path(f"dir-{directory}/file-{size}.dat")
                expected[str(rel)] = payload(size, directory)
                write_file(source / rel, expected[str(rel)])
                assert (source / rel).read_bytes() == expected[str(rel)]

        def tree_copy():
            target.mkdir()
            for directory, dirs, names in os.walk(source):
                relative = pathlib.Path(directory).relative_to(source)
                for name in sorted(dirs):
                    (target / relative / name).mkdir()
                for name in sorted(names):
                    copy_file(pathlib.Path(directory) / name, target / relative / name)

        clock.call("copy_tree", tree_copy)
        got = {
            str(p.relative_to(target)): p.read_bytes()
            for p in target.rglob("*")
            if p.is_file()
        }
        assert got == expected
        sample = target / "dir-0/file-1024.dat"
        sample_data, checks = expected["dir-0/file-1024.dat"], 12
    elif case == "overwrite-delete-recreate":
        target = root / "same-path.dat"
        generations = [payload(4096, i) for i in range(2 * count + 1)]
        write_file(target, generations[0])
        for i in range(count):
            clock.call(
                "overwrite_fsync", lambda: write_file(target, generations[2 * i + 1])
            )
            assert target.read_bytes() == generations[2 * i + 1]
            clock.call("delete", target.unlink)
            assert not target.exists()
            clock.call(
                "recreate_fsync", lambda: write_file(target, generations[2 * i + 2])
            )
            assert target.read_bytes() == generations[2 * i + 2]
        sample, sample_data, checks = target, generations[-1], 2 * count
    elif case == "many-small-files":
        count = max(1, int(500 * scale))
        paths = [root / f"file-{i:04d}.dat" for i in range(count)]
        clock.call("create_fsync", lambda: [write_file(path, data) for path in paths])
        contents = clock.call("read", lambda: [path.read_bytes() for path in paths])
        names = clock.call("list", lambda: os.listdir(root))
        clock.call("delete", lambda: [path.unlink() for path in paths])
        assert len(names) == count and all(content == data for content in contents)
        assert not list(root.iterdir())
        absent, checks = paths[0], count
    elif case == "medium-file":
        target = root / "medium.dat"
        content = payload(16 * 1048576)

        def writing():
            stream = target.open("wb")
            try:
                clock.call(
                    "write_16MiB",
                    lambda: [
                        stream.write(content[i : i + 1048576])
                        for i in range(0, len(content), 1048576)
                    ],
                )
                clock.call(
                    "flush_fsync_close",
                    lambda: (stream.flush(), os.fsync(stream.fileno()), stream.close()),
                )
            finally:
                stream.close()

        writing()

        def reading():
            with target.open("rb") as stream:
                return b"".join(iter(lambda: stream.read(1048576), b""))

        got = clock.call("read_16MiB", reading)
        assert got == content
        sample, sample_data, checks = target, content, 1
    elif case == "concurrent-files":
        count = max(1, int(50 * scale))
        ctx = mp.get_context("fork")
        ready, result = ctx.Queue(), ctx.Queue()
        gate = ctx.Event()
        workers = []
        for index in range(8):
            parent = root / f"worker-{index}"
            parent.mkdir()
            worker = ctx.Process(
                target=concurrent_worker,
                args=(parent, count, data, ready, gate, result),
            )
            worker.start()
            workers.append(worker)
        try:
            for _ in workers:
                ready.get(timeout=60)
            start = time.perf_counter_ns()
            gate.set()
            outputs = [result.get(timeout=600) for _ in workers]
            clock.phases["eight_workers_wall"] = (
                max(row["end_ns"] for row in outputs) - start
            ) / 1e9
            assert all(row["ok"] for row in outputs), outputs
            for worker in workers:
                worker.join(timeout=10)
                assert worker.exitcode == 0
            checks = count * 8
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
        assert not list(root.rglob("*.dat"))
        absent = root / "worker-0/file-0000.dat"
    elif case == "permissions":
        target, directory = root / "private.dat", root / "private-dir"
        write_file(target, data)
        directory.mkdir(mode=0o755)
        control = root / "public-control.dat"
        write_file(control, data)
        os.chmod(control, 0o644)
        os.chmod(target, 0o644)
        clock.call("chmod_file", lambda: os.chmod(target, 0o600))
        clock.call("chmod_directory", lambda: os.chmod(directory, 0o700))
        attrs = clock.call("stat", lambda: (target.stat(), directory.stat()))
        assert (
            stat.S_IMODE(attrs[0].st_mode) == 0o600
            and stat.S_IMODE(attrs[1].st_mode) == 0o700
        )
        assert target.read_bytes() == data and list(directory.iterdir()) == []
        script = """import pathlib,sys
r=pathlib.Path(sys.argv[1])
assert len((r/'public-control.dat').read_bytes())==4096
for p,op in ((r/'private.dat',lambda p:p.read_bytes()),(r/'private-dir',lambda p:list(p.iterdir()))):
 try: op(p)
 except PermissionError: pass
 else: raise AssertionError('unauthorized access allowed')
"""
        subprocess.run(
            ["sudo", "-n", "-u", "nobody", "python3", "-c", script, str(root)],
            check=True,
            capture_output=True,
            timeout=30,
        )
        sample, sample_data, checks = target, data, 4
    else:
        raise ValueError(case)
    witness = None
    if sample is not None:
        witness = {
            "path": str(sample.relative_to(root)),
            "size": len(sample_data),
            "sha256": hashlib.sha256(sample_data).hexdigest(),
        }
    return {
        "case": case,
        "duration_s": sum(clock.phases.values()),
        "phases_s": clock.phases,
        "verified": True,
        "checks": checks,
        "sample": witness,
        "absent": str(absent.relative_to(root)) if absent is not None else None,
        "scale": scale,
        "root": str(root),
    }


def concurrent_worker(root, count, data, ready, gate, result):
    ready.put(True)
    gate.wait()
    start = time.perf_counter_ns()
    contents = []
    try:
        for i in range(count):
            path = root / f"file-{i:04d}.dat"
            write_file(path, data)
            contents.append(path.read_bytes())
            path.unlink()
        end = time.perf_counter_ns()
        assert all(c == data for c in contents)
        result.put({"ok": True, "end_ns": end, "duration_s": (end - start) / 1e9})
    except Exception as error:
        result.put({"ok": False, "end_ns": time.perf_counter_ns(), "error": str(error)})


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
        row = {
            "case": args.case,
            "verified": False,
            "error_type": type(error).__name__,
            "error": str(error),
            "traceback": traceback.format_exc(),
            "root": args.root,
        }
    row.update(start_epoch=started, end_epoch=time.time())
    pathlib.Path(args.output).write_text(json.dumps(row, indent=2))
    print(json.dumps(row), flush=True)
    raise SystemExit(0 if row["verified"] else 1)
