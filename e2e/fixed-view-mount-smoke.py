#!/usr/bin/env python3
"""Linux fixed-view CLI/FUSE switching probe against an already running server."""

import argparse
import errno
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


def switch_failure_details(raw):
    # Only fixed protocol enums, local refusal classes and a boolean may leave the captured response.
    # Paths, operation identities, server errors and stderr remain private.
    try:
        result = json.loads(raw[:1024 * 1024])
    except (ValueError, UnicodeError, RecursionError):
        return "receipt=unavailable"
    if not isinstance(result, dict):
        return "receipt=unavailable"
    response = result.get("response")
    response = response if isinstance(response, dict) else {}
    receipt = response.get("receipt")
    receipt = receipt if isinstance(receipt, dict) else {}
    kind = response.get("error_kind")
    state = receipt.get("state")
    kind = kind if isinstance(kind, str) and kind in ("BUSY", "INVALID", "CONFLICT", "FORBIDDEN", "UNSUPPORTED", "RECOVERING") else "unclassified"
    state = state if isinstance(state, str) and state in ("RECOVERING", "DONE") else "absent"
    unknown = result.get("unknown")
    unknown = str(unknown).lower() if type(unknown) is bool else "unavailable"
    detail = f"kind={kind} state={state} unknown={unknown}"
    if kind == "BUSY":
        error = response.get("error")
        error = error if isinstance(error, str) else ""
        reasons = {
            "mount view has open handles or locks:": "handles_or_locks",
            "mount view retains kernel lookup references:": "kernel_lookups",
            "mount view retains references or private extension state (": "final_preflight",
            "mount view extension_state:": "extension_state",
        }
        reason = next((value for prefix, value in reasons.items() if error.startswith(prefix)), "unclassified")
        detail += f" busy={reason}"
    return detail


MOUNTED_WRITE_SCRIPT = """import json, os, sys
stage = "open"
try:
    f = open(sys.argv[1], "wb")
    stage = "write"
    f.write(sys.stdin.buffer.read())
    stage = "flush"
    f.flush()
    stage = "fsync"
    os.fsync(f.fileno())
    stage = "close"
    f.close()
except Exception as error:
    print(json.dumps({"stage": stage, "errno": error.errno if isinstance(error, OSError) else None}))
    sys.exit(1)
"""


def write_failure_details(raw):
    try:
        result = json.loads(raw[:4096])
    except (ValueError, UnicodeError, RecursionError):
        return "stage=unknown errno=unknown"
    if not isinstance(result, dict):
        return "stage=unknown errno=unknown"
    stage = result.get("stage")
    stage = stage if isinstance(stage, str) and stage in ("open", "write", "flush", "fsync", "close") else "unknown"
    number = result.get("errno")
    code = errno.errorcode.get(number, "unknown") if type(number) is int else "unknown"
    return f"stage={stage} errno={code}"


def mount_failure_categories(path):
    # Client errors can contain paths and response bodies. Only fixed labels
    # leave the captured log; cap the read and never echo an unknown message.
    patterns = {
        "fixed-view content accounting is not available": "accounting_unavailable",
        "complete retained storage usage is unavailable": "retained_unavailable",
        "revision conflict": "revision_conflict",
        "fixed view operation conflicts with current state": "view_conflict",
        "context deadline exceeded": "deadline",
        "permission denied": "permission",
        "operation not supported": "unsupported",
    }
    try:
        with open(path, "rb") as log:
            log.seek(0, os.SEEK_END)
            log.seek(max(0, log.tell() - (4 << 20)))
            text = log.read(4 << 20).decode("utf-8", errors="replace").lower()
    except OSError:
        return "unavailable"
    return ",".join(label for pattern, label in patterns.items() if pattern in text) or "unclassified"


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
        if result.returncode != 0:
            details = "; " + switch_failure_details(result.stdout) if args[:2] in (["mount", "switch"], ["mount", "switch-operation"]) else ""
            raise http.ProbeFailure("CLI " + " ".join(args[:2]) + " failed" + details + "; no automatic mutation retry")
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
        try:
            result = subprocess.run(["cat", self.point + path], capture_output=True, timeout=30)
        except subprocess.TimeoutExpired:
            raise http.ProbeFailure("mounted file read timed out") from None
        require(result.returncode == 0 and result.stdout == expected, "mounted view returned wrong file bytes")


    def write(self, path, data):
        # A child bounds a blocked FUSE write/fsync; credentials never enter argv.
        try:
            result = subprocess.run([sys.executable, "-c", MOUNTED_WRITE_SCRIPT, self.point + path],
                                    input=data, capture_output=True, timeout=60)
        except subprocess.TimeoutExpired:
            raise http.ProbeFailure("mounted file write timed out; no automatic retry") from None
        require(result.returncode == 0, "mounted file write or fsync failed; " + write_failure_details(result.stdout))

    def read_validation(self, path, expected, work):
        args = [self.cli, "fs", "cat", "--layer", work["layer_id"], "--work", work["work_id"],
                "--fence", str(work["fence"]), "--validation", path]
        try:
            result = subprocess.run(args, env=self.env, capture_output=True, timeout=60)
        except subprocess.TimeoutExpired:
            raise http.ProbeFailure("validation read timed out") from None
        require(result.returncode == 0 and result.stdout == expected, "validation returned wrong file bytes")



def wait_retained_accounting(probe, work, timeout=240):
    """Wait via real admission requests, never by setting SQL quota counters.

    Initiation checks accounting before reserving/registering a session. Only
    its explicit accounting-unavailable response can retry the original ID.
    Success is cancelled immediately; no object or file is published here.
    """
    require(0 < timeout <= 240, "invalid accounting readiness timeout")
    deadline = time.monotonic() + timeout
    operation = probe.op()
    route = "/v1/views/" + work["layer_id"] + "/v2/uploads"
    request = {"path": probe.root + "/accounting-readiness", "total_size": 1,
               "expected_revision": 0, "layout": "extent"}

    def remaining():
        left = deadline - time.monotonic()
        require(left > 0, "retained accounting readiness timed out")
        return min(10, left)

    while True:
        raw, headers, status = probe.request("POST", route + "/initiate", value=request, work=work,
            headers={"Idempotency-Key": operation}, statuses=(202, 503), limit=8192,
            timeout=remaining(), with_status=True)
        remaining()  # A late response cannot turn an expired wait into success.
        require(headers.get("Idempotency-Key") == operation, "accounting readiness response lost operation identity")
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise http.ProbeFailure("invalid accounting readiness response") from None
        if status == 503:
            require(result == {"error": "fixed-view content accounting is not available"},
                    "unexpected accounting readiness failure; no automatic retry")
            time.sleep(min(1, remaining()))
            continue
        require(isinstance(result, dict), "invalid accounting readiness plan")
        upload = result.get("upload_id")
        require(isinstance(upload, str) and 0 < len(upload) <= 128
                and all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in upload)
                and result.get("content_layout") == "extent" and result.get("total_parts") == 1
                and type(result.get("part_size")) is int and result["part_size"] >= 1,
                "invalid accounting readiness plan")
        cancel = probe.op()
        outcome = probe.api("POST", route + "/" + upload + "/abort", work=work,
                            headers={"Idempotency-Key": cancel}, timeout=remaining())
        remaining()
        require(outcome == {"operation_id": cancel, "upload_id": upload, "status": "ABORTED"},
                "accounting readiness reservation was not cancelled")
        print("CHECK retained accounting admitted and temporary reservation cancelled", flush=True)
        return



def finish_write_merge(mounted, current, work, path):
    data = b"m" * ((4 << 20) + 19)
    mounted.write(path, data)
    mounted.read(path, data)
    mounted.probe.read(path, b"before")
    mounted.switch(current)
    mounted.read(path, b"before")
    validation = mounted.probe.seal_and_validate(work)
    mounted.read_validation(path, data, validation)
    operation = mounted.probe.op()
    args = ["fs", "layer", "merge", "--operation", operation,
            "--work", validation["work_id"], "--fence", str(validation["fence"]), work["layer_id"]]
    first = mounted.call(args)
    require(first.get("state") == "DONE" and not first.get("code")
            and first.get("layer", {}).get("layer_id") == work["layer_id"]
            and first.get("layer", {}).get("phase") == "MERGED", "merge did not confirm the candidate")
    require(mounted.call(args) == first, "merge replay changed the original receipt")
    mounted.read(path, data)



def kernel_mount_id(point):
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if len(fields) > 5 and fields[4] == point:
            return fields[0]
    raise http.ProbeFailure("test directory is not a kernel mount")



def unmount_owned_process(cli, env, point, process):
    # The foreground mount is our child. Reap it while the CLI waits for its
    # PID to disappear; blocking in subprocess.run leaves an exited child as
    # a zombie and makes the CLI's kill(pid, 0) check report it as still alive.
    cleanup = subprocess.Popen([cli, "umount", "--timeout", "10s", "--no-auto-pack", point],
                               env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # CLI --timeout applies to each wait phase, not the complete command.
    deadline = time.monotonic() + 60
    try:
        while True:
            process.poll()
            try:
                code = cleanup.wait(timeout=0.1)
                require(code == 0, "unmount cleanup failed")
                return
            except subprocess.TimeoutExpired:
                if time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired("unmount cleanup", 60) from None
    finally:
        if cleanup.poll() is None:
            cleanup.kill()
            cleanup.wait(timeout=5)


def run(cli, probe, write_merge=False):
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
        if write_merge:
            wait_retained_accounting(probe, work)
        if not write_merge:
            probe.request("PUT", probe.fs(path), data=b"after", statuses=(200, 201))
        with open(directory + "/mount.log", "wb") as log:
            process = subprocess.Popen([cli, "mount", "--mode=fuse", "--foreground", "--no-persist-credentials",
                                        *(["--read-only"] if not write_merge else []), "--cache-dir", directory + "/cache", point],
                                       env=mounted.env, stdout=log, stderr=log)
            deadline = time.monotonic() + 30
            while not os.path.ismount(point):
                require(process.poll() is None, "mount process exited before becoming ready")
                require(time.monotonic() < deadline, "mount readiness timed out")
                time.sleep(0.1)
            print("CHECK mounted process ready; querying main binding", flush=True)
            initial = mounted.status()
            require(initial["binding"] == binding(), "unspecified mount view was not main")
            mount_id = kernel_mount_id(point)
            print("CHECK main file read", flush=True)
            mounted.read(path, b"before" if write_merge else b"after")
            print("CHECK switching main to Layer", flush=True)
            current = mounted.switch(initial, work)
            print("CHECK Layer file read", flush=True)
            mounted.read(path, b"before")
            if write_merge:
                print("CHECK mounted extent write, switchback, validation and merge", flush=True)
                finish_write_merge(mounted, current, work, path)
            else:
                probe.request("PUT", probe.fs(path), data=b"latest", statuses=(200, 201))
                mounted.read(path, b"before")
                print("CHECK switching Layer to main", flush=True)
                mounted.switch(current)
                print("CHECK latest main file read", flush=True)
                mounted.read(path, b"latest")
            require(process.poll() is None and kernel_mount_id(point) == mount_id,
                    "switch replaced the mount process or kernel mount")
            print("PASS same-process mounted write -> switch -> validate -> merge" if write_merge else
                  "PASS same-process main -> fixed Layer -> main, stable kernel mount and original switch replay")
    finally:
        # Never recursively remove a directory while it might still expose remote data.
        if process is not None:
            primary_error = sys.exc_info()[1]
            if primary_error is not None:
                print("CHECK mount error categories=" + mount_failure_categories(directory + "/mount.log"), flush=True)
            print("CHECK unmount cleanup", flush=True)
            detail = str(primary_error) if isinstance(primary_error, http.ProbeFailure) else type(primary_error).__name__
            try:
                unmount_owned_process(cli, mounted.env, point, process)
            except subprocess.TimeoutExpired:
                raise http.ProbeFailure("unmount cleanup timed out; prior failure: " + detail) from None
            except http.ProbeFailure as error:
                raise http.ProbeFailure(str(error) + "; prior failure: " + detail) from None
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
    parser.add_argument("--write-merge", action="store_true", help="verify writable same-mount Layer publication")
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
        run(os.path.abspath(args.cli), probe, args.write_merge)
    finally:
        probe.cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error) if isinstance(error, http.ProbeFailure) else type(error).__name__
        print("FAIL:", message, file=sys.stderr)
        sys.exit(1)
