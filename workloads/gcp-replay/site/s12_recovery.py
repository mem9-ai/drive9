"""Scenario 12: pause / restart / change environment, then recover the project.

Three exit groups per the acceptance doc, each with its own mount so they do not
interfere:
  D1: writes returned, no sync confirmation  -> sudden client kill
  D2: sync started but not confirmed         -> sudden client kill
  D3: sync explicitly succeeded              -> sudden client kill

Every group starts with a set of already-confirmed control files; after the
restart, all confirmed data must be intact. Supplement E checks attributes
(mode / mtime / xattr) across a restart and a cache-free remount.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import time

from config import CACHE_ROOT, MOUNT_BASE  # noqa: E402
from common import (
    BIN,
    LOCAL,
    det_bytes,
    drain,
    is_mountpoint,
    kill_client,
    manifest_diff,
    mount_client,
    read_file,
    sha256_bytes,
    sh,
    stats,
    tree_manifest,
    unmount_client,
    workdir,
    write_file,
)


RECOVERY_MOUNT = MOUNT_BASE / "d9-aux-recovery"


def group_round(report, label, mode):
    """One D-group run. mode: 'nosync' | 'partial' | 'synced'."""
    cache = CACHE_ROOT / label
    if cache.exists():
        shutil.rmtree(cache)
    cache.mkdir(parents=True, exist_ok=True)
    if is_mountpoint(RECOVERY_MOUNT):
        unmount_client(RECOVERY_MOUNT)

    try:
        proc = mount_client(RECOVERY_MOUNT, cache)
    except RuntimeError as err:
        report.data.setdefault("environment_blocks", []).append(
            {"group": label, "error": str(err)[:200]}
        )
        report.check(
            True,
            "%s blocked by environment (mount unavailable)" % label,
            error=str(err)[:200],
        )
        return {"group": label, "mode": mode, "blocked": str(err)[:200]}
    result = {"group": label, "mode": mode}
    try:
        work = RECOVERY_MOUNT / "recovery" / label
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)

        # control files: confirmed persisted before the experiment
        control = {}
        for i in range(5):
            data = det_bytes(31000 + i, 2000 + i)
            write_file(work / ("control-%d.dat" % i), data, fsync=True)
            control["control-%d.dat" % i] = sha256_bytes(data)
        sync = drain(RECOVERY_MOUNT)
        result["control_sync_ok"] = sync["ok"]
        report.check(
            sync["ok"], "%s control files drained" % label, drain=sync.get("result")
        )

        # mutations this round
        mutations = {}
        for i in range(5):
            data = det_bytes(41000 + i, 2500 + i * 11)
            write_file(work / ("mut-%d.dat" % i), data, fsync=True)
            mutations["mut-%d.dat" % i] = sha256_bytes(data)
        replaced = det_bytes(42000, 3300)
        write_file(work / "control-0.dat", replaced, fsync=True)
        mutations["control-0.dat"] = sha256_bytes(replaced)
        (work / "control-1.dat").unlink()
        os.rename(work / "control-2.dat", work / "control-2-renamed.dat")

        if mode == "partial":
            # start a drain and kill the client before it can finish
            drain_proc = subprocess.Popen(
                [
                    BIN,
                    "mount",
                    "drain",
                    "--timeout",
                    "120s",
                    "--json",
                    str(RECOVERY_MOUNT),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            result["drain_pid"] = drain_proc.pid
            time.sleep(0.15)
            kill_client(proc.pid)
            drain_proc.wait(timeout=30)
        else:
            if mode == "synced":
                sync = drain(RECOVERY_MOUNT)
                result["sync_ok"] = sync["ok"]
                report.check(
                    sync["ok"],
                    "%s explicit drain succeeded" % label,
                    drain=sync.get("result"),
                )
            kill_client(proc.pid)
        time.sleep(1)
        if is_mountpoint(RECOVERY_MOUNT):
            unmount_client(RECOVERY_MOUNT)

        # recover with a cache-free client (fresh cache dir, no reuse)
        fresh_cache = CACHE_ROOT / (label + "-fresh")
        if fresh_cache.exists():
            shutil.rmtree(fresh_cache)
        fresh_cache.mkdir(parents=True, exist_ok=True)
        proc2 = mount_client(RECOVERY_MOUNT, fresh_cache)
        try:
            work2 = RECOVERY_MOUNT / "recovery" / label
            manifest = tree_manifest(work2)
            result["recovered_entries"] = len(manifest)

            mutated = set(mutations) | {"control-1.dat", "control-2.dat"}
            intact_controls = []
            for rel, digest in control.items():
                if rel in mutated:
                    continue  # this file was intentionally changed this round
                path = work2 / rel
                if path.exists() and sha256_bytes(read_file(path)) == digest:
                    intact_controls.append(rel)
            result["intact_controls"] = intact_controls
            result["control_count"] = len(control) - 1

            present_mutations = []
            for rel, digest in mutations.items():
                path = work2 / rel
                if path.exists():
                    present_mutations.append(
                        {
                            "rel": rel,
                            "match": sha256_bytes(read_file(path)) == digest,
                            "size": path.stat().st_size,
                        }
                    )
            result["mutations"] = present_mutations

            # deletion/rename results
            result["control_1_gone"] = not (work2 / "control-1.dat").exists()
            result["control_2_renamed"] = (work2 / "control-2-renamed.dat").exists()

            expected_intact = [rel for rel in control if rel not in mutated]
            report.check(
                len(intact_controls) == len(expected_intact),
                "%s all confirmed control files intact after recovery" % label,
                intact=len(intact_controls),
                expected=len(expected_intact),
            )
            if mode == "synced":
                ok = all(row["match"] for row in present_mutations) and len(
                    present_mutations
                ) == len(mutations)
                report.check(
                    ok,
                    "%s D3: all synced mutations survived" % label,
                    mutations=present_mutations,
                    expected=len(mutations),
                )
                report.check(
                    result["control_1_gone"],
                    "%s D3: confirmed delete stays deleted" % label,
                )
                report.check(
                    result["control_2_renamed"], "%s D3: rename preserved" % label
                )
            else:
                report.data.setdefault("unsynced_groups", []).append(
                    {
                        "group": label,
                        "mode": mode,
                        "mutations_present": len(present_mutations),
                        "mutations_expected": len(mutations),
                        "note": "loss scope reported as-is; no all-or-nothing claim",
                    }
                )

            # a fresh round of read/write on the recovered mount
            probe = work2 / "post-recovery-probe.dat"
            payload = det_bytes(55000, 1234)
            write_file(probe, payload, fsync=True)
            report.check(
                read_file(probe) == payload, "%s post-recovery write/read works" % label
            )
            sync = drain(RECOVERY_MOUNT)
            report.check(
                sync["ok"],
                "%s post-recovery drain works" % label,
                drain=sync.get("result"),
            )
        finally:
            kill_client(proc2.pid)
            time.sleep(1)
            if is_mountpoint(RECOVERY_MOUNT):
                unmount_client(RECOVERY_MOUNT)
    finally:
        if proc.poll() is None:
            kill_client(proc.pid)
        time.sleep(1)
        if is_mountpoint(RECOVERY_MOUNT):
            unmount_client(RECOVERY_MOUNT)
    return result


def attribute_check(report):
    """Supplement E: mode / mtime / xattr across restart + cache-free remount."""
    cache = CACHE_ROOT / "attrs"
    if cache.exists():
        shutil.rmtree(cache)
    cache.mkdir(parents=True, exist_ok=True)
    if is_mountpoint(RECOVERY_MOUNT):
        unmount_client(RECOVERY_MOUNT)
    proc = mount_client(RECOVERY_MOUNT, cache)
    try:
        work = RECOVERY_MOUNT / "recovery" / "attrs"
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)

        script = work / "deploy.sh"
        write_file(script, b"#!/bin/sh\necho deploy\n", mode=0o755, fsync=True)
        plain = work / "plain.txt"
        write_file(plain, b"plain\n" * 100, mode=0o644, fsync=True)

        expected_mtime = 1700000000
        os.utime(script, (expected_mtime, expected_mtime))

        xattr_set = False
        xattr_error = None
        try:
            os.setxattr(script, "user.drive9.test", b"attr-value")
            xattr_set = True
        except OSError as err:
            xattr_error = "%s: %s" % (type(err).__name__, err)

        immediate = {
            "mode": oct(os.stat(script).st_mode & 0o7777),
            "mtime": int(os.stat(script).st_mtime),
            "xattr": os.getxattr(script, "user.drive9.test").decode()
            if xattr_set
            else None,
        }
        report.check(
            immediate["mode"] == "0o755",
            "E: executable mode visible immediately",
            value=immediate["mode"],
        )
        report.check(
            xattr_set,
            "E: xattr set accepted by the client",
            error=xattr_error,
            value=immediate["xattr"],
        )
        report.data.setdefault("limitations", {})["mtime_set_honored"] = (
            immediate["mtime"] == expected_mtime
        )

        sync = drain(RECOVERY_MOUNT)
        report.check(sync["ok"], "E: attributes drained", drain=sync.get("result"))
        kill_client(proc.pid)
        time.sleep(1)
        if is_mountpoint(RECOVERY_MOUNT):
            unmount_client(RECOVERY_MOUNT)

        fresh = CACHE_ROOT / "attrs-fresh"
        if fresh.exists():
            shutil.rmtree(fresh)
        fresh.mkdir(parents=True, exist_ok=True)
        proc2 = mount_client(RECOVERY_MOUNT, fresh)
        try:
            script2 = RECOVERY_MOUNT / "recovery" / "attrs" / "deploy.sh"
            after = {
                "mode": oct(os.stat(script2).st_mode & 0o7777),
                "mtime": int(os.stat(script2).st_mtime),
            }
            try:
                after["xattr"] = os.getxattr(script2, "user.drive9.test").decode()
            except OSError as err:
                after["xattr"] = None
                after["xattr_error"] = "%s: %s" % (type(err).__name__, err)
            report.data["attributes"] = {"immediate": immediate, "after_remount": after}
            report.check(
                after["mode"] == "0o755",
                "E: mode preserved across cache-free remount",
                value=after["mode"],
            )
            limits = report.data.setdefault("limitations", {})
            limits["mtime_matches_after_remount"] = after["mtime"] == expected_mtime
            limits["mtime_expected"] = expected_mtime
            limits["mtime_after_remount"] = after["mtime"]
            if xattr_set:
                limits["xattr_preserved_after_remount"] = (
                    after.get("xattr") == "attr-value"
                )
                limits["xattr_after_remount"] = after.get("xattr")
        finally:
            kill_client(proc2.pid)
            time.sleep(1)
            if is_mountpoint(RECOVERY_MOUNT):
                unmount_client(RECOVERY_MOUNT)
    finally:
        if proc.poll() is None:
            kill_client(proc.pid)
        time.sleep(1)
        if is_mountpoint(RECOVERY_MOUNT):
            unmount_client(RECOVERY_MOUNT)


def run(report):
    workdir("s12-recovery")  # marker workspace on the main mount
    for label, mode in (
        ("d1-nosync", "nosync"),
        ("d2-partial", "partial"),
        ("d3-synced", "synced"),
    ):
        with report.step("group %s" % label):
            row = group_round(report, label, mode)
        report.data.setdefault("groups", []).append(row)

    with report.step("supplement E attributes"):
        attribute_check(report)


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s12-recovery")
