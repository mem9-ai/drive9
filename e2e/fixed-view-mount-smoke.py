#!/usr/bin/env python3
"""Linux fixed-view CLI/FUSE switching probe against an already running server."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

_spec = importlib.util.spec_from_file_location("fixed_view_http", Path(__file__).with_name("fixed-view-smoke.py"))
http = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(http)
require = http.require


def binding(work=None):
    return {"LayerID": work["layer_id"] if work else "", "WorkID": work["work_id"] if work else "",
            "Fence": work["fence"] if work else 0, "Validation": False}


class MountProbe:
    def __init__(self, cli, probe, point):
        self.cli, self.probe, self.point = cli, probe, point
        self.env = dict(os.environ, DRIVE9_SERVER=probe.base, DRIVE9_API_KEY=probe.key)

    def call(self, args, timeout=30):
        # Credentials stay in the environment, never argv, stdout or failure text.
        try:
            result = subprocess.run([self.cli, *args], env=self.env, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise http.ProbeFailure("CLI request timed out; no automatic mutation retry") from None
        require(result.returncode == 0, "CLI " + " ".join(args[:2]) + " failed; no automatic mutation retry")
        try:
            return json.loads(result.stdout)
        except (ValueError, UnicodeError):
            raise http.ProbeFailure("CLI returned an invalid JSON receipt") from None

    def status(self):
        status = self.call(["mount", "status", "--json", self.point])
        view = status.get("view", {})
        require(not status.get("view_error") and view.get("mount_id") and view.get("generation", 0) > 0
                and not view.get("pending"), "mount did not report a settled view identity")
        return view

    def switch(self, previous, work=None):
        target, operation = binding(work), self.probe.op()
        args = ["--mount-id", previous["mount_id"], "--operation", operation,
                "--generation", str(previous["generation"]), "--layer", work["layer_id"] if work else "main"]
        if work:
            args += ["--work", work["work_id"], "--fence", str(work["fence"])]
        args.append(self.point)
        result = self.call(["mount", "switch", *args])
        response = result.get("response", {})
        receipt = response.get("receipt", {})
        require(result.get("unknown") is False and not result.get("error")
                and response.get("mount_id") == previous["mount_id"] and response.get("operation_id") == operation
                and receipt.get("state") == "DONE" and receipt.get("operation_id") == operation
                and receipt.get("from_generation") == previous["generation"]
                and receipt.get("generation") == previous["generation"] + 1
                and receipt.get("from") == previous["binding"] and receipt.get("target") == target,
                "switch did not confirm the original mount, target and next generation")
        # Query and replay exactly the same request; neither may switch a second time.
        for action in ("switch-operation", "switch"):
            again = self.call(["mount", action, *args])
            require(again == result, "switch query/replay changed the original receipt")
        current = self.status()
        require(current["mount_id"] == previous["mount_id"] and current["generation"] == receipt["generation"]
                and current["binding"] == target, "switch status drifted or mount identity changed")
        return current

    def read(self, path, expected):
        result = subprocess.run(["cat", self.point + path], capture_output=True, timeout=30)
        require(result.returncode == 0 and result.stdout == expected, "mounted view returned wrong file bytes")


def kernel_mount_id(point):
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if len(fields) > 5 and fields[4] == point:
            return fields[0]
    raise http.ProbeFailure("test directory is not a kernel mount")


def run(cli, probe):
    require(sys.platform == "linux" and os.path.exists("/dev/fuse"), "Linux /dev/fuse is required; this test cannot skip")
    path = probe.root + "/mount.txt"
    probe.request("PUT", probe.fs(path), data=b"before", statuses=(200, 201))
    directory = tempfile.mkdtemp(prefix="d9-fv-", dir="/tmp")
    point = directory + "/mount"
    os.mkdir(point)
    mounted, process = MountProbe(cli, probe, point), None
    try:
        layer = probe.prefix + "-mount"
        probe.layers.append(layer)
        created = mounted.call(["fs", "layer", "create", "--operation", probe.op(), layer])
        require(created.get("layer_id") == layer, "CLI create returned a different Layer")
        claim = mounted.call(["fs", "layer", "work", "--operation", probe.op(), "--ttl", "10m", layer])
        work = claim.get("work", {})
        require(claim.get("launch") is True and work.get("layer_id") == layer and work.get("work_id")
                and work.get("fence", 0) > 0 and not work.get("validation"), "CLI did not claim the requested work")
        probe.request("PUT", probe.fs(path), data=b"after", statuses=(200, 201))
        with open(directory + "/mount.log", "wb") as log:
            process = subprocess.Popen([cli, "mount", "--mode=fuse", "--foreground", "--no-persist-credentials",
                                        "--read-only", "--cache-dir", directory + "/cache", point],
                                       env=mounted.env, stdout=log, stderr=log)
            deadline = time.monotonic() + 30
            while not os.path.ismount(point):
                require(process.poll() is None, "mount process exited before becoming ready")
                require(time.monotonic() < deadline, "mount readiness timed out")
                time.sleep(0.1)
            initial = mounted.status()
            require(initial["binding"] == binding(), "unspecified mount view was not main")
            mount_id = kernel_mount_id(point)
            mounted.read(path, b"after")
            current = mounted.switch(initial, work)
            mounted.read(path, b"before")
            probe.request("PUT", probe.fs(path), data=b"latest", statuses=(200, 201))
            mounted.read(path, b"before")
            mounted.switch(current)
            mounted.read(path, b"latest")
            require(process.poll() is None and kernel_mount_id(point) == mount_id,
                    "switch replaced the mount process or kernel mount")
            print("PASS same-process main -> fixed Layer -> main, stable kernel mount and original switch replay")
    finally:
        # Never recursively remove a directory while it might still expose remote data.
        if process is not None:
            try:
                subprocess.run([cli, "umount", "--timeout", "10s", point], env=mounted.env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
        require(not os.path.ismount(point), "mount cleanup incomplete; local fixture retained at " + directory)
        shutil.rmtree(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", required=True)
    args = parser.parse_args()
    require(os.path.isfile(args.cli) and os.access(args.cli, os.X_OK), "provide an executable paired CLI")
    require(sys.platform == "linux" and os.path.exists("/dev/fuse"), "Linux /dev/fuse is required; this test cannot skip")
    base = os.environ.get("DRIVE9_BASE", "")
    parsed = http.urllib.parse.urlsplit(base)
    require(parsed.scheme in ("http", "https") and parsed.netloc and not parsed.username
            and not parsed.query and not parsed.fragment, "invalid DRIVE9_BASE")
    probe = http.Probe(base, os.environ.get("DRIVE9_API_KEY", ""))
    # This CI entry creates one isolated tenant; no production/existing-tenant mode.
    probe.provision_local()
    try:
        run(os.path.abspath(args.cli), probe)
    finally:
        probe.cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error) if isinstance(error, http.ProbeFailure) else type(error).__name__
        print("FAIL:", message, file=sys.stderr)
        sys.exit(1)
