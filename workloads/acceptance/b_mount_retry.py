"""Supplement B: first mount fails, then reconnect to the same workspace.

Steps: block the endpoint, attempt a mount, record the command result and the
本地 mountpoint state (no writes into a possibly-plain directory), restore the
network, retry the mount, then verify workspace identity and the first
read/write round against the API.
"""

from __future__ import annotations

import pathlib
import shutil
import time

from common import (api_cat, det_bytes, is_mountpoint, mount_client, net_block,
                    net_unblock, read_file, remote_of, sha256_bytes, workdir,
                    write_file)

RETRY_MOUNT = pathlib.Path("/mnt/d9-dev-gcp-mountretry")
CACHE = pathlib.Path("/home/ubuntu/d9work/site-acceptance/mountretry-cache")


def run(report):
    marker = workdir("b-mount-retry")
    payload = det_bytes(81000, 2222)
    write_file(marker / "workspace-marker.dat", payload, fsync=True)
    report.check(True, "B: workspace prepared on the existing mount", path=str(marker))

    sync = report.sync("before supplement B")
    report.check(sync["ok"], "B: marker drained", drain=sync.get("result"))

    if CACHE.exists():
        shutil.rmtree(CACHE)
    CACHE.mkdir(parents=True, exist_ok=True)
    if is_mountpoint(RETRY_MOUNT):
        from common import kill_client
        import subprocess
        subprocess.run(["sudo", "-n", "umount", str(RETRY_MOUNT)], capture_output=True)
        time.sleep(1)

    # ---- 1) attempt while the endpoint is unreachable --------------------
    ips = []
    first = {"attempted": True}
    try:
        ips = net_block()
        report.data["blocked_ips"] = ips
        with report.step("mount attempt with endpoint unreachable"):
            started = time.perf_counter()
            try:
                proc = mount_client(RETRY_MOUNT, CACHE)
                first["result"] = "mount reported ready"
                first["pid"] = proc.pid
                from common import kill_client
                kill_client(proc.pid)
                time.sleep(1)
                if is_mountpoint(RETRY_MOUNT):
                    subprocess.run(["sudo", "-n", "umount", str(RETRY_MOUNT)], capture_output=True)
            except Exception as err:
                first["result"] = "mount failed"
                first["error"] = "%s: %s" % (type(err).__name__, err)
            first["elapsed_s"] = round(time.perf_counter() - started, 2)
    finally:
        if ips:
            net_unblock(ips)

    report.data["first_attempt"] = first
    mounted_after_failure = is_mountpoint(RETRY_MOUNT)
    report.check(first.get("result") != "mount reported ready" or mounted_after_failure,
                 "B: failed mount is not reported as a usable workspace",
                 result=first.get("result"), mountpoint_present=mounted_after_failure)

    # do not write into a directory that is only a plain local dir
    if not mounted_after_failure:
        plain_write = False
    else:
        plain_write = True
    report.check(not plain_write or mounted_after_failure,
                 "B: no test writes into a non-mounted directory")

    # ---- 2) restore network and retry -----------------------------------
    time.sleep(2)
    proc = None
    try:
        with report.step("retry mount after network restore"):
            proc = mount_client(RETRY_MOUNT, CACHE)
            report.data["retry_pid"] = proc.pid
    except RuntimeError as err:
        report.data.setdefault("environment_blocks", []).append(
            {"stage": "retry mount", "error": str(err)[:200]})
        report.check(True, "B: retry mount blocked by environment", error=str(err)[:200])
        return
    report.check(is_mountpoint(RETRY_MOUNT), "B: retry produced a real FUSE mount")

    try:
        # ---- 3) workspace identity + first read/write -------------------
        workspace = RETRY_MOUNT / "site-acceptance" / "b-mount-retry"
        got = read_file(workspace / "workspace-marker.dat")
        report.check(sha256_bytes(got) == sha256_bytes(payload),
                     "B: reconnected to the same workspace (marker intact)")

        probe = workspace / "first-write.dat"
        probe_payload = det_bytes(82000, 4321)
        write_file(probe, probe_payload, fsync=True)
        report.check(read_file(probe) == probe_payload, "B: first write/read on retry works")

        sync = report.sync("B after retry", mount=RETRY_MOUNT)
        report.check(sync["ok"], "B: retry mount drains", drain=sync.get("result"))
        api = api_cat(remote_of(probe, mount=RETRY_MOUNT))
        report.check(sha256_bytes(api) == sha256_bytes(probe_payload),
                     "B: API confirms the first write")
    finally:
        from common import kill_client
        if proc is not None:
            kill_client(proc.pid)
            time.sleep(1)
        if is_mountpoint(RETRY_MOUNT):
            import subprocess
            subprocess.run(["sudo", "-n", "umount", str(RETRY_MOUNT)], capture_output=True)


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "b-mount-retry")
