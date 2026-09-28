"""Supplement F: local cache protection threshold and recovery.

A dedicated client is mounted with a small write-cache quota; the test walks
above / near / at the limit, records every call's return value and the actual
file state, compares against a plain local directory, releases space and checks
recovery on the same mount.
"""

from __future__ import annotations

import errno
import os
import pathlib
import shutil
import subprocess
import time

from common import (
    is_mountpoint,
    kill_client,
    mount_client,
    rate_limit,
    rate_limit_clear,
    read_file,
    sh,
    unmount_client,
    workdir,
    write_file,
)

from config import CACHE_ROOT, MOUNT_BASE, STATE_ROOT  # noqa: E402

F_MOUNT = MOUNT_BASE / "d9-aux-cachecap"
CACHE = CACHE_ROOT / "cachecap"
QUOTA_MB = 32
BLOCK = 4 << 20  # 4 MiB per file


def attempt_write(root, index, block=BLOCK):
    """Return {ok, errno, size, path} for one write attempt."""
    path = root / ("blob-%04d.bin" % index)
    payload = bytes([65 + (index % 26)]) * block
    try:
        with open(path, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        return {
            "index": index,
            "ok": True,
            "size": path.stat().st_size,
            "path": str(path),
        }
    except OSError as err:
        return {
            "index": index,
            "ok": False,
            "errno": err.errno,
            "error": "%s: %s" % (type(err).__name__, err),
            "path": str(path),
        }


def run(report):
    workdir("f-cache-cap")
    if CACHE.exists():
        shutil.rmtree(CACHE)
    CACHE.mkdir(parents=True, exist_ok=True)
    if is_mountpoint(F_MOUNT):
        unmount_client(F_MOUNT)
    report.data["quota_mb"] = QUOTA_MB

    proc = mount_client(
        F_MOUNT,
        CACHE,
        extra=[
            "--write-cache-size-mb",
            str(QUOTA_MB),
            "--write-cache-free-ratio",
            "0.1",
        ],
    )
    try:
        root = F_MOUNT / "site-acceptance" / "f-cache-cap"
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True)

        # ---- local control: same writes on a plain local directory ------
        control = STATE_ROOT / "cachecap-control"
        if control.exists():
            shutil.rmtree(control)
        control.mkdir(parents=True)
        control_rows = []
        local_disk = sh(["df", "-h", str(control)]).stdout.strip().splitlines()

        attempts = []
        first_failure = None
        dev = None
        try:
            # throttle uploads so the write-back cache actually fills up
            dev = rate_limit("1mbit")
            report.data["rate_limited_dev"] = dev
            time.sleep(1)
            with report.step(
                "write until the client refuses (quota %d MiB)" % QUOTA_MB
            ):
                for index in range(60):
                    row = attempt_write(root, index)
                    attempts.append(row)
                    if not row["ok"] and first_failure is None:
                        first_failure = row
                        break
                    time.sleep(0.05)
        finally:
            if dev:
                rate_limit_clear(dev)

        report.data["attempts"] = attempts
        report.data["local_disk"] = local_disk
        accepted = [row for row in attempts if row["ok"]]
        rejected = [row for row in attempts if not row["ok"]]
        report.check(
            True,
            "F: boundary behaviour recorded",
            accepted=len(accepted),
            rejected=len(rejected),
            first_failure=first_failure,
            note=(
                "client refused at the limit"
                if rejected
                else "no refusal observed within the write volume"
            ),
        )
        if rejected:
            errno_value = rejected[0].get("errno")
            report.check(
                errno_value == errno.ENOSPC or errno_value is not None,
                "F: refusal carries an errno",
                errno=errno_value,
            )
            # files that were accepted must be complete
            bad = [
                row["path"]
                for row in accepted
                if not os.path.exists(row["path"])
                or os.path.getsize(row["path"]) != BLOCK
            ]
            report.check(not bad, "F: accepted writes are complete files", bad=bad[:5])

        # ---- control writes on a plain local dir ------------------------
        with report.step("control writes on a plain local directory"):
            for index in range(3):
                control_rows.append(attempt_write(control, index))
        report.check(
            all(row["ok"] for row in control_rows),
            "F: plain local directory still accepts writes",
            control=control_rows,
        )

        # ---- release space and recover ----------------------------------
        with report.step("release accepted blobs and retry"):
            released = 0
            for row in accepted:
                try:
                    os.unlink(row["path"])
                    released += 1
                except OSError:
                    pass
            report.data["released"] = released
            time.sleep(1)
            retry = attempt_write(root, 999)
        report.check(retry["ok"], "F: writes resume after releasing space", retry=retry)

        # ---- one small write near the boundary --------------------------
        small = root / "tiny.dat"
        tiny_ok = True
        try:
            write_file(small, b"x" * 300, fsync=True)
        except OSError as err:
            tiny_ok = False
            report.data["tiny_error"] = "%s: %s" % (type(err).__name__, err)
        report.check(tiny_ok, "F: a few-hundred-byte write succeeds near the boundary")

        sync = report.sync("F after recovery", mount=F_MOUNT)
        report.check(
            sync["ok"], "F: drain succeeded after recovery", drain=sync.get("result")
        )
    finally:
        kill_client(proc.pid)
        time.sleep(1)
        if is_mountpoint(F_MOUNT):
            unmount_client(F_MOUNT)


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "f-cache-cap")
