"""Supplement A: per-file fsync while the connection drops (doc S1).

Sequence per file: create temp -> write -> chmod -> fsync -> close -> rename.
The connection is cut while a batch is in flight; the test records each step's
return, keeps temp and formal files for inspection, deletes temp explicitly,
restores the network, retries, and re-verifies through the API.
"""

from __future__ import annotations

import json
import os
import pathlib
import time

from common import (api_cat, det_bytes, net_block, net_unblock, read_file,
                    remote_of, sha256_bytes, wait_mount_ready, workdir, write_file)


def pipeline(target, data, *, chmod_mode=0o644):
    """create tmp -> write -> chmod -> fsync -> close -> rename (returns steps)."""
    steps = []
    tmp = target.parent / (target.name + ".new")
    started = time.perf_counter()
    steps.append(("start", started))
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.chmod(tmp, chmod_mode)
        os.fsync(fh.fileno())
        steps.append(("fsync", time.perf_counter()))
    steps.append(("close", time.perf_counter()))
    os.replace(tmp, target)
    steps.append(("rename", time.perf_counter()))
    return {"tmp": str(tmp), "target": str(target),
            "steps": [(name, round(ts - started, 4)) for name, ts in steps]}


def run(report):
    # a previous network-injection test can leave the mount degraded briefly
    ready = wait_mount_ready()
    report.check(ready, "A: mount ready before the run")
    root = workdir("a-fsync-interrupt")
    files_dir = root / "files"
    files_dir.mkdir()

    # ---- 1) normal control run ------------------------------------------
    control = []
    with report.step("normal control run (10 files)"):
        for i in range(10):
            data = det_bytes(71000 + i, 3000 + i * 101)
            row = pipeline(files_dir / ("ctl-%02d.dat" % i), data)
            row["sha256"] = sha256_bytes(data)
            control.append(row)
    bad = []
    for row in control:
        target = pathlib.Path(row["target"])
        if not target.exists() or sha256_bytes(read_file(target)) != row["sha256"]:
            bad.append(row["target"])
    report.check(not bad, "A: control run writes and renames correctly", bad=bad)

    # ---- 2) inject a connection drop mid-batch --------------------------
    inject_dir = root / "interrupted"
    inject_dir.mkdir()
    ips = []
    results = []
    try:
        ips = net_block()
        report.data["blocked_ips"] = ips
        with report.step("write batch while endpoint is unreachable"):
            for i in range(5):
                data = det_bytes(72000 + i, 2500 + i * 77)
                target = inject_dir / ("int-%02d.dat" % i)
                tmp = target.parent / (target.name + ".new")
                entry = {"file": target.name, "sha256": sha256_bytes(data), "steps": []}
                started = time.perf_counter()
                try:
                    with open(tmp, "wb") as fh:
                        fh.write(data)
                        fh.flush()
                        os.chmod(tmp, 0o644)
                        os.fsync(fh.fileno())
                        entry["steps"].append(("fsync", round(time.perf_counter() - started, 4)))
                    entry["steps"].append(("close", round(time.perf_counter() - started, 4)))
                    os.replace(tmp, target)
                    entry["steps"].append(("rename", round(time.perf_counter() - started, 4)))
                    entry["result"] = "completed"
                except OSError as err:
                    entry["result"] = "error"
                    entry["errno"] = err.errno
                    entry["error"] = str(err)
                entry["elapsed_s"] = round(time.perf_counter() - started, 3)
                results.append(entry)
                time.sleep(0.3)
    finally:
        if ips:
            net_unblock(ips)

    report.data["interrupted_batch"] = results
    report.check(any(row.get("result") == "error" for row in results),
                 "A: interrupted batch reported errors (or completed without error)",
                 note="recorded as-is; no all-or-nothing assumption",
                 completed=sum(1 for row in results if row.get("result") == "completed"))

    # temp files must be visible for inspection, then deletable
    with report.step("inspect and clean temp files"):
        try:
            leftovers = sorted(p.name for p in inject_dir.iterdir() if p.name.endswith(".new"))
        except OSError as err:
            leftovers = []
            report.data["inspect_error"] = "%s: %s" % (type(err).__name__, err)
        cleaned = []
        for name in leftovers:
            path = inject_dir / name
            try:
                path.unlink()
                cleaned.append(name)
            except OSError as err:
                cleaned.append("%s: %s" % (name, err))
    report.check(True, "A: temp file state recorded and cleanup attempted",
                 leftovers=leftovers, cleaned=cleaned[:5])

    # ---- 3) restore network, retry, verify via API ----------------------
    time.sleep(2)
    retried = []
    with report.step("retry after network restore"):
        for i in range(5):
            data = det_bytes(73000 + i, 3500 + i * 55)
            target = inject_dir / ("retry-%02d.dat" % i)
            row = pipeline(target, data)
            row["sha256"] = sha256_bytes(data)
            retried.append(row)
    problems = [row["target"] for row in retried
                if not pathlib.Path(row["target"]).exists()
                or sha256_bytes(read_file(row["target"])) != row["sha256"]]
    report.check(not problems, "A: retried writes succeed after restore", problems=problems)

    sync = report.sync("after supplement A")
    report.check(sync["ok"], "A: drain succeeded", drain=sync.get("result"))
    api_ok = 0
    for row in retried:
        remote = remote_of(row["target"])
        if sha256_bytes(api_cat(remote)) == row["sha256"]:
            api_ok += 1
    report.check(api_ok == len(retried), "A: API confirms retried files",
                 api_ok=api_ok, expected=len(retried))


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "a-fsync-interrupt")
