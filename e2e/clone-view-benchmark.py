#!/usr/bin/env python3
"""Manual diagnostic: current main versus existing Layer COW, not a main-COW prototype."""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
import re
import signal
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

_spec = importlib.util.spec_from_file_location("mount_probe", Path(__file__).with_name("fixed-view-mount-smoke.py"))
mount = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mount)
http = mount.http
require = http.require
MOUNT_OPTIONS = ["--mode=fuse", "--foreground", "--no-persist-credentials", "--profile=none",
                 "--extensions=core,sse,cache,writeback", "--durability=close-sync", "--flush-debounce=0"]


def failure_categories(raw):
    text = raw[:65536].decode("utf-8", errors="replace").lower()
    patterns = {"operation not supported": "unsupported", "permission denied": "permission",
                "invalid argument": "invalid", "input/output error": "io", "no such file or directory": "not_found",
                "file exists": "exists", "read-only file system": "read_only", "could not write config file": "git_config",
                "unable to write": "git_write", "could not create work tree dir": "git_worktree",
                "unable to access": "git_access", "failed to write": "git_write_failure",
                "died of signal 7": "git_sigbus", "bus error": "bus_error",
                "unable to read": "git_read", "read error": "read_error",
                "mmap failed": "mmap_failure", "could not mmap": "mmap_failure",
                "invalid index-pack output": "invalid_pack", "early eof": "early_eof",
                "bad object": "bad_object", "corrupt": "corruption"}
    return ",".join(value for key, value in patterns.items() if key in text) or "unclassified"


def command(args, *, env=None, timeout=300):
    try:
        result = subprocess.run(args, env=env, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise http.ProbeFailure("benchmark subprocess timed out; sample rejected") from None
    require(result.returncode == 0, "benchmark subprocess failed; exit=" + str(result.returncode)
            + " categories=" + failure_categories(result.stderr) + "; sample rejected")
    return result.stdout


def syscall_failure_counts(raw):
    # Only enumerated operation/error/path classes leave this local trace.
    calls = {"open", "openat", "creat", "mkdir", "mkdirat", "rename", "renameat", "renameat2",
             "unlink", "unlinkat", "rmdir", "link", "linkat", "symlink", "symlinkat", "readlink",
             "readlinkat", "chmod", "fchmod", "fchmodat", "stat", "lstat", "newfstatat", "statx",
             "access", "faccessat", "faccessat2", "fsync", "fdatasync", "close", "utimensat",
             "read", "pread64", "write", "pwrite64", "ftruncate"}
    errors = {"EIO", "EACCES", "EPERM", "EINVAL", "EROFS", "ENOSYS", "EOPNOTSUPP", "ENOSPC", "ESTALE"}
    paths = (("/.git/hooks/", "git_hooks"), ("/.git/objects/", "git_objects"),
             ("/.git/refs/", "git_refs"), ("/.git/config", "git_config"), ("/.git/HEAD", "git_head"))
    counts = {}
    for line in raw[-(4 << 20):].decode("utf-8", errors="replace").splitlines():
        op = re.match(r"^(?:[0-9]+ +)?(?:([a-z0-9_]+)\(|<\.\.\. ([a-z0-9_]+) resumed>)", line)
        error = re.search(r"= -1 (E[A-Z0-9]+) \(", line)
        name = (op.group(1) or op.group(2)) if op else None
        if name not in calls or not error or error.group(1) not in errors:
            continue
        category = next((value for pattern, value in paths if pattern in line), "other")
        key = (name, error.group(1), category)
        counts[key] = counts.get(key, 0) + 1
    return [{"syscall": op, "errno": error, "path_class": category, "count": count}
            for (op, error, category), count in sorted(counts.items())]


def signal_mapping_counts(raw):
    # Correlate only addresses observed in this bounded trace tail. Missing,
    # resumed or inherited mappings remain unknown; no path/address is output.
    mappings = {}
    counts = {}
    classes = (("/.git/objects/", "git_objects"), ("/.git/index", "git_index"),
               ("/.git/config", "git_config"))
    for line in raw[-(4 << 20):].decode("utf-8", errors="replace").splitlines():
        prefix = re.match(r"^(?:(\d+) +)?(.*)$", line)
        pid, event = prefix.groups()
        if re.match(r"(?:execve(?:at)?\(.*|<\.\.\. execve(?:at)? resumed>.*) = 0$", event):
            mappings.pop(pid, None)
            continue
        mapped = re.match(r"mmap\([^,]+, (\d+),.*\) = (0x[0-9a-f]+)$", event)
        unmapped = re.match(r"munmap\((0x[0-9a-f]+), (\d+)\) = 0$", event)
        if mapped or unmapped:
            start = int(mapped.group(2) if mapped else unmapped.group(1), 16)
            length = int(mapped.group(1) if mapped else unmapped.group(2))
            end = start + length
            # A partial unmap conservatively discards the whole old range.
            current = [(a, b, c) for a, b, c in mappings.get(pid, []) if b <= start or a >= end]
            if mapped:
                category = next((c for path, c in classes if path in event), "other")
                current.append((start, end, category))
            mappings[pid] = current
        fault = re.match(r"--- (SIGBUS|SIGSEGV) \{.*si_code=([A-Z_]+).*si_addr=(0x[0-9a-f]+).*", event)
        if fault:
            address = int(fault.group(3), 16)
            category = next((c for a, b, c in reversed(mappings.get(pid, [])) if a <= address < b), "unknown")
            code = fault.group(2) if fault.group(2) in {"BUS_ADRALN", "BUS_ADRERR", "BUS_OBJERR", "SEGV_MAPERR", "SEGV_ACCERR"} else "unknown"
            key = (fault.group(1), code, category)
            counts[key] = counts.get(key, 0) + 1
    return [{"signal": sig, "code": code, "mapping_class": category, "count": count}
            for (sig, code, category), count in sorted(counts.items())]


def diagnose_clone_failure(args, trace):
    # A separate, unmeasured destination preserves the failed sample. Raw trace
    # and Git output stay in the temporary fixture and are never uploaded.
    if shutil.which("strace") is None:
        print("CHECK clone diagnostic unavailable", flush=True)
        return
    with trace.with_suffix(".stderr").open("wb") as stderr:
        process = subprocess.Popen(["strace", "-f", "-qq", "-yy", "-e",
                                    "trace=%file,mmap,munmap,fsync,fdatasync,close,fchmod,ftruncate,read,pread64,write,pwrite64",
                                    "-e", "raw=read,pread64,write,pwrite64", "-o", str(trace), *args],
                                   stdout=subprocess.DEVNULL, stderr=stderr, start_new_session=True)
        try:
            result = process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            print("CHECK clone diagnostic timed out; summarizing partial trace", flush=True)
            result = -signal.SIGKILL
    with trace.open("rb") as source:
        source.seek(0, os.SEEK_END)
        source.seek(max(0, source.tell() - (4 << 20)))
        raw = source.read(4 << 20)
        failures = syscall_failure_counts(raw)
        signals = signal_mapping_counts(raw)
    with trace.with_suffix(".stderr").open("rb") as source:
        categories = failure_categories(source.read(65536))
    print("CHECK clone diagnostic exit=" + str(result) + " failures=" + json.dumps(failures, sort_keys=True)
          + " signals=" + json.dumps(signals, sort_keys=True) + " categories=" + categories, flush=True)


def clone_fixture(source, target, trace):
    args = ["git", "-c", "protocol.file.allow=always", "clone", "--no-local", source.as_uri()]
    try:
        command([*args, str(target)], timeout=600)
    except http.ProbeFailure:
        try:
            diagnose_clone_failure([*args, str(target.with_name("diagnostic-repo"))], trace)
        except Exception:
            print("CHECK clone diagnostic unavailable", flush=True)
        raise


def fixture(root, count):
    require(1 <= count <= 512, "fixture file count must be 1..512")
    root.mkdir()
    manifest = {}
    for i in range(count):
        name = f"dir-{i % 8:02d}/file-{i:04d}.txt"
        path = root / name
        path.parent.mkdir(exist_ok=True)
        data = (hashlib.sha256(f"clone-benchmark-{i}".encode()).hexdigest().encode() * 16)
        path.write_bytes(data)
        manifest[name] = hashlib.sha256(data).hexdigest()
    env = dict(os.environ, GIT_AUTHOR_DATE="2020-01-01T00:00:00Z", GIT_COMMITTER_DATE="2020-01-01T00:00:00Z")
    command(["git", "init", "--initial-branch=main", str(root)], env=env)
    command(["git", "-C", str(root), "add", "."], env=env)
    command(["git", "-C", str(root), "-c", "user.name=Benchmark", "-c", "user.email=benchmark@example.invalid",
             "-c", "commit.gpgsign=false", "commit", "-m", "deterministic small-file fixture"], env=env)
    return manifest, command(["git", "-C", str(root), "rev-parse", "HEAD"]).decode().strip()


@contextmanager
def mounted(cli, probe, directory, work):
    directory.mkdir()
    point = directory / "mount"
    point.mkdir()
    wrapper = mount.MountProbe(cli, probe, str(point))
    process = None
    try:
        with (directory / "mount.log").open("wb") as log:
            process = subprocess.Popen([cli, "mount", *MOUNT_OPTIONS, "--cache-dir", str(directory / "cache"), str(point)],
                                       env=wrapper.env, stdout=log, stderr=log)
            until = time.monotonic() + 60
            while not os.path.ismount(point):
                require(process.poll() is None, "mount exited before readiness")
                require(time.monotonic() < until, "mount readiness timed out")
                time.sleep(.1)
            initial = wrapper.status()
            require(initial["binding"] == mount.binding(), "mount did not start on main")
            if work:
                wrapper.switch(initial, work)
            yield point
    finally:
        if process is not None:
            if sys.exc_info()[0] is not None:
                print("CHECK mount categories=" + mount.mount_failure_categories(directory / "mount.log"), flush=True)
            try:
                mount.unmount_owned_process(cli, wrapper.env, str(point), process)
                require(process.wait(timeout=10) == 0, "mount exited with an error")
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
        require(not os.path.ismount(point), "mount remains active; fixture retained")


def sync_tree(root):
    # File and directory fsync are part of the measured persistence interval.
    def fail(error):
        raise error

    for parent, dirs, files in os.walk(root, topdown=False, onerror=fail):
        for name in files:
            with open(os.path.join(parent, name), "rb") as file:
                os.fsync(file.fileno())
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def verify(root, manifest, commit):
    require(command(["git", "-C", str(root), "rev-parse", "HEAD"]).decode().strip() == commit,
            "clone commit differs from source")
    command(["git", "-C", str(root), "fsck", "--full"])
    names = command(["git", "-C", str(root), "ls-files", "-z"]).decode().split("\0")[:-1]
    require(set(names) == set(manifest), "tracked file set differs from source")
    for name, digest in manifest.items():
        require(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, "file content differs from source")


def worker(args):
    # Child process bounds blocked kernel I/O without hiding failed samples.
    if args.operation == "sync":
        sync_tree(Path(args.tree))
    else:
        value = json.loads(Path(args.manifest).read_text())
        verify(Path(args.tree), value["files"], value["commit"])


def sample(cli, base, source, manifest_file, case, index, root):
    probe = http.Probe(base, "")
    probe.provision_local()
    work = None
    try:
        # Identical seed namespace; only COW samples create a Layer/work session.
        probe.request("PUT", probe.fs(probe.root + "/seed.txt"), data=b"seed", statuses=(200, 201))
        if case == "layer_cow":
            work = probe.create("clone")
            mount.wait_retained_accounting(probe, work)
        print(f"START case={case} trial={index}", flush=True)
        with mounted(cli, probe, root / "write", work) as point:
            target = point / probe.root.lstrip("/") / "repo"
            started = time.monotonic()
            print("CHECK phase=clone", flush=True)
            clone_fixture(source, target, root / "clone.strace")
            cloned = time.monotonic()
            print("CHECK phase=sync", flush=True)
            command([sys.executable, __file__, "--operation=sync", "--tree", str(target)], timeout=300)
            synced = time.monotonic()
        unmounted = time.monotonic()
        # New kernel mount and empty read/write cache prove server persistence.
        with mounted(cli, probe, root / "read", work) as point:
            target = point / probe.root.lstrip("/") / "repo"
            print("CHECK phase=remount_verify", flush=True)
            verify_start = time.monotonic()
            command([sys.executable, __file__, "--operation=verify", "--tree", str(target), "--manifest", str(manifest_file)], timeout=600)
            verified = time.monotonic()
        result = {"case": case, "trial": index, "clone_seconds": cloned-started,
                  "sync_seconds": synced-cloned, "unmount_seconds": unmounted-synced,
                  "durable_seconds": unmounted-started, "remount_verify_seconds": verified-verify_start,
                  "verified": True}
        print("SAMPLE " + json.dumps(result, sort_keys=True), flush=True)
        return result
    finally:
        probe.cleanup()


def summary(samples, trials):
    result = {}
    for case in ("main_no_layer", "layer_cow"):
        rows = [s for s in samples if s["case"] == case]
        require(len(rows) == trials and {s["trial"] for s in rows} == set(range(1, trials+1))
                and all(s["verified"] for s in rows), "incomplete benchmark; no ratio reported")
        result[case] = {key: statistics.median(s[key] for s in rows)
                        for key in ("clone_seconds", "durable_seconds", "remount_verify_seconds")}
    result["layer_over_main_durable_ratio"] = result["layer_cow"]["durable_seconds"] / result["main_no_layer"]["durable_seconds"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli")
    parser.add_argument("--output")
    parser.add_argument("--files", type=int, default=64)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--operation", choices=("sync", "verify"))
    parser.add_argument("--tree")
    parser.add_argument("--manifest")
    args = parser.parse_args()
    if args.operation:
        worker(args)
        return
    require(sys.platform == "linux" and os.path.exists("/dev/fuse"), "Linux FUSE required; cannot skip")
    require(args.cli and os.path.isfile(args.cli) and os.access(args.cli, os.X_OK) and args.output, "CLI and output required")
    require(1 <= args.trials <= 5, "trials must be 1..5")
    base = os.environ.get("DRIVE9_BASE", "")
    parsed = http.urllib.parse.urlsplit(base)
    require(parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and not parsed.username
            and not parsed.query and not parsed.fragment and parsed.path in ("", "/")
            and not os.environ.get("DRIVE9_API_KEY"), "requires fresh tenants on explicit loopback test server")
    root = Path(tempfile.mkdtemp(prefix="d9-clone-bench-", dir="/tmp"))
    output = Path(args.output)
    report = {"complete": False, "comparison": "current PR main vs existing Layer COW; not main-COW implementation",
              "mount_options": MOUNT_OPTIONS, "fixture_files": args.files, "bytes_per_file": 1024,
              "trials": args.trials, "git_version": command(["git", "--version"]).decode().strip(), "samples": []}
    try:
        manifest, commit = fixture(root / "source", args.files)
        report["source_commit"] = commit
        mf = root / "manifest.json"
        mf.write_text(json.dumps({"files": manifest, "commit": commit}))
        for i in range(1, args.trials+1):
            cases = ("main_no_layer", "layer_cow") if i % 2 else ("layer_cow", "main_no_layer")
            for case in cases:
                trial = root / f"{i}-{case}"
                trial.mkdir()
                report["samples"].append(sample(os.path.abspath(args.cli), base, root / "source", mf, case, i, trial))
                output.write_text(json.dumps(report, indent=2) + "\n")
        report["summary"] = summary(report["samples"], args.trials)
        report["complete"] = True
        print("BENCHMARK COMPLETE " + json.dumps(report["summary"], sort_keys=True), flush=True)
    finally:
        output.write_text(json.dumps(report, indent=2) + "\n")
        # Inspect only the known mount slots; never walk inside a mount.
        slots = list(root.glob("*-*/write/mount")) + list(root.glob("*-*/read/mount"))
        active = any(os.path.ismount(p) for p in slots)
        if not active:
            shutil.rmtree(root)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("FAIL: " + (str(error) if isinstance(error, http.ProbeFailure) else type(error).__name__), file=sys.stderr)
        sys.exit(1)
