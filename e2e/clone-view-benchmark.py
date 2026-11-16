#!/usr/bin/env python3
"""Manual diagnostic: current main versus existing Layer COW, not a main-COW prototype."""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
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


def command(args, *, env=None, timeout=300):
    try:
        result = subprocess.run(args, env=env, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise http.ProbeFailure("benchmark subprocess timed out; sample rejected") from None
    require(result.returncode == 0, "benchmark subprocess failed; sample rejected")
    return result.stdout


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
            command(["git", "-c", "protocol.file.allow=always", "clone", "--no-local", source.as_uri(), str(target)], timeout=600)
            cloned = time.monotonic()
            command([sys.executable, __file__, "--operation=sync", "--tree", str(target)], timeout=300)
            synced = time.monotonic()
        unmounted = time.monotonic()
        # New kernel mount and empty read/write cache prove server persistence.
        with mounted(cli, probe, root / "read", work) as point:
            target = point / probe.root.lstrip("/") / "repo"
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
