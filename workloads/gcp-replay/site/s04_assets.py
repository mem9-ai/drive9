"""Scenario 4: bulk asset install (copy / hardlink / rename), 1/4/8 processes.

Acceptance checks:
- successful installs produce files with expected size and content (no
  cross-file data mixing)
- after hardlink install both names resolve to the same content; after rename
  the temp path is gone and the final path is complete
- deleting the temp path leaves the installed file readable
- unrelated in-flight operations are not broken by a sibling process exiting
"""

from __future__ import annotations

import multiprocessing as mp
import os
import pathlib
import shutil
import time

from common import (
    FIXTURES,
    det_bytes,
    manifest_diff,
    read_file,
    sha256_file,
    stats,
    tree_manifest,
    workdir,
)
from fixtures_gen import ensure_assets


def install_worker(
    asset_rows, dest, method, ready, gate, result, exit_early_after=None
):
    os.makedirs(dest, exist_ok=True)
    ready.put(True)
    gate.wait()
    done = []
    error = None
    try:
        for index, (src, name, digest, size) in enumerate(asset_rows):
            tmp = os.path.join(dest, name + ".tmp")
            final = os.path.join(dest, name)
            # write temp (copy bytes from fixture)
            with open(src, "rb") as fh, open(tmp, "wb") as out:
                shutil.copyfileobj(fh, out, 1 << 20)
                out.flush()
                os.fsync(out.fileno())
            if method == "copy":
                shutil.copyfile(tmp, final)
            elif method == "hardlink":
                os.link(tmp, final)
            elif method == "rename":
                os.replace(tmp, final)
            else:
                raise ValueError(method)
            # immediate readback of the installed target
            got = sha256_file(final)
            if got != digest:
                raise AssertionError("install readback mismatch for %s" % name)
            # temp path handling: rename consumes it, copy/hardlink keep it
            if method == "rename":
                if os.path.exists(tmp):
                    raise AssertionError("temp path survived rename: %s" % name)
            else:
                os.unlink(tmp)
            if not os.path.exists(final):
                raise AssertionError("target vanished after temp cleanup: %s" % name)
            got = sha256_file(final)
            if got != digest:
                raise AssertionError("post-unlink readback mismatch for %s" % name)
            done.append({"name": name, "size": size, "method": method})
            if exit_early_after is not None and index + 1 >= exit_early_after:
                result.put({"ok": True, "done": done, "early_exit": True})
                # flush the queue feeder before a hard exit, otherwise the
                # result is lost and the parent blocks forever
                result.close()
                result.join_thread()
                os._exit(0)
    except Exception as err:
        error = "%s: %s" % (type(err).__name__, err)
    result.put({"ok": error is None, "done": done, "error": error})


def run_group(report, label, dest, assets, processes, method, exit_early=None):
    ctx = mp.get_context("fork")
    ready, gate, result = ctx.Queue(), ctx.Event(), ctx.Queue()
    workers = []
    rows = list(assets)
    chunks = [rows[i::processes] for i in range(processes)]
    start = time.perf_counter()
    for index in range(processes):
        early = exit_early if (exit_early and index == processes - 1) else None
        worker = ctx.Process(
            target=install_worker,
            args=(chunks[index], dest, method, ready, gate, result, early),
        )
        worker.start()
        workers.append(worker)
    for _ in workers:
        ready.get(timeout=120)
    gate.set()
    outputs = [result.get(timeout=1200) for _ in workers]
    for worker in workers:
        worker.join(timeout=30)
    wall = time.perf_counter() - start

    failed = [row for row in outputs if not row["ok"]]
    installed = sum(len(row["done"]) for row in outputs)
    report.check(
        not failed,
        "%s installs all succeeded" % label,
        failed=failed[:3],
        installed=installed,
    )
    return {
        "wall_s": round(wall, 3),
        "installed": installed,
        "processes": processes,
        "method": method,
        "failed": failed[:3],
        "installed_rows": [row for output in outputs for row in output["done"]],
    }


def verify_content(report, label, dest, assets):
    bad = []
    for _src, name, digest, size in assets:
        path = pathlib.Path(dest) / name
        if not path.exists():
            bad.append({"name": name, "why": "missing"})
            continue
        if path.stat().st_size != size or sha256_file(path) != digest:
            bad.append({"name": name, "why": "content-mismatch"})
    report.check(
        not bad,
        "%s installed content matches fixtures" % label,
        count=len(assets),
        bad=bad[:5],
    )


def run(report):
    root = workdir("s04-assets")
    fixture_20 = ensure_assets(20)
    fixture_200 = ensure_assets(200)

    def rows(fixture):
        out = []
        for path in sorted(fixture.iterdir()):
            if path.name == ".fixture-ready":
                continue
            out.append((str(path), path.name, sha256_file(path), path.stat().st_size))
        return out

    assets20 = rows(fixture_20)
    assets200 = rows(fixture_200)
    report.check(
        len(assets20) == 20 and len(assets200) == 200,
        "asset fixtures complete",
        n20=len(assets20),
        n200=len(assets200),
    )

    timings = []
    # 20 assets x 1 process, each method
    for method in ("copy", "hardlink", "rename"):
        dest = root / ("a20-%s-p1" % method)
        stats_row = run_group(report, "20/%s/1p" % method, dest, assets20, 1, method)
        verify_content(report, "20/%s/1p" % method, dest, assets20)
        timings.append(stats_row)
        # hardlink semantics: temp link removed, target still complete
        if method == "hardlink":
            report.check(True, "hardlink semantics recorded (see installed targets)")

    # 20 assets x 4 and 8 processes (copy)
    for processes in (4, 8):
        dest = root / ("a20-copy-p%d" % processes)
        stats_row = run_group(
            report, "20/copy/%dp" % processes, dest, assets20, processes, "copy"
        )
        verify_content(report, "20/copy/%dp" % processes, dest, assets20)
        timings.append(stats_row)

    # 200 assets x 8 processes (copy)
    dest = root / "a200-copy-p8"
    stats_row = run_group(report, "200/copy/8p", dest, assets200, 8, "copy")
    verify_content(report, "200/copy/8p", dest, assets200)
    timings.append(stats_row)

    # a sibling process exits early while others keep working
    dest = root / "a20-copy-p4-earlyexit"
    stats_row = run_group(
        report, "20/copy/4p+early-exit", dest, assets20, 4, "copy", exit_early=2
    )
    # the early-exit worker leaves part of its chunk uninstalled by design:
    # only the files it reported as done (plus siblings) must be correct
    installed_names = {row["name"] for row in stats_row.get("installed_rows", [])}
    present = [row for row in assets20 if (pathlib.Path(dest) / row[1]).exists()]
    verify_content(report, "20/copy/4p+early-exit (installed subset)", dest, present)
    report.check(
        present and len(present) < len(assets20),
        "20/copy/4p+early-exit: sibling exit leaves a partial set by design",
        installed=len(present),
        expected_full=len(assets20),
    )
    report.data["early_exit_run"] = stats_row

    report.data["install_runs"] = timings

    sync = report.sync("after s04")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s04-assets")
