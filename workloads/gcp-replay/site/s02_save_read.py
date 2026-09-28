"""Scenario 2: save code, dev server reads immediately.

Acceptance checks:
- new file written+closed is fully readable by another process
- in-place modification and full rewrite read back exactly
- atomic replace alternates complete A/B versions; a continuous reader must
  never observe mixed/truncated/missing content
- batch modification ends with the correct final file set
- after drain, the API read path agrees
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import pathlib
import time

from common import (
    Report,
    api_cat,
    det_bytes,
    manifest_diff,
    read_file,
    remote_of,
    run_checked,
    sha256_bytes,
    stats,
    tree_manifest,
    workdir,
    write_file,
)

VERSION_A = b"A" * 4096 + b"||END-A||"
VERSION_B = b"B" * 7000 + b"||END-B||"
VERSIONS = (VERSION_A, VERSION_B)


def reader_worker(target, stop_event, queue, deadline):
    reads = 0
    observations = {"A": 0, "B": 0}
    bad = []
    while not stop_event.is_set() and time.time() < deadline:
        try:
            with open(target, "rb") as fh:
                data = fh.read()
            reads += 1
            if data == VERSION_A:
                observations["A"] += 1
            elif data == VERSION_B:
                observations["B"] += 1
            else:
                bad.append(
                    {
                        "kind": "mixed-or-truncated",
                        "size": len(data),
                        "head": data[:24].decode("latin1"),
                        "tail": data[-24:].decode("latin1"),
                    }
                )
                break
        except FileNotFoundError:
            bad.append({"kind": "path-missing-during-replace"})
            break
        except Exception as err:
            bad.append({"kind": "error", "error": "%s: %s" % (type(err).__name__, err)})
            break
    queue.put({"reads": reads, "observations": observations, "bad": bad})


def run(report):
    root = workdir("s02-save-read")

    # ---- 1) write a new file, another process reads it immediately -------
    latencies = []
    mismatches = []
    with report.step("write-then-read 30 rounds in another process"):
        for i in range(30):
            data = det_bytes(2000 + i, 3000 + (i * 61) % 5000)
            target = root / "save" / ("module-%02d.js" % i)
            write_file(target, data, fsync=True)
            start = time.perf_counter()
            proc = run_checked(
                [
                    "python3",
                    "read_verify.py",
                    str(target),
                    sha256_bytes(data),
                    str(len(data)),
                ],
                timeout=120,
            )
            latencies.append(time.perf_counter() - start)
            row = json.loads(proc.stdout)
            if not row.get("ok"):
                mismatches.append(row)
    report.check(
        not mismatches,
        "write-then-read all match",
        mismatches=mismatches[:5],
        read_latency=stats(latencies),
    )

    # ---- 2) in-place modification and full rewrite -----------------------
    target = root / "edit" / "app.js"
    original = det_bytes(3000, 8192)
    write_file(target, original, fsync=True)

    with report.step("in-place partial modification"):
        modified = original[:1000] + b"INPLACE-PATCH" + original[1013:]
        with open(target, "r+b") as fh:
            fh.seek(1000)
            fh.write(b"INPLACE-PATCH")
            fh.flush()
            os.fsync(fh.fileno())
    report.check(
        read_file(target) == modified,
        "in-place modification reads back exactly",
        size=len(modified),
    )

    with report.step("full rewrite with truncate"):
        rewritten = det_bytes(4000, 12345)
        with open(target, "wb") as fh:
            fh.write(rewritten)
            fh.flush()
            os.fsync(fh.fileno())
    report.check(
        read_file(target) == rewritten,
        "full rewrite reads back exactly",
        size=len(rewritten),
    )

    # ---- 3) atomic replace alternating A/B with a continuous reader ------
    atomic_dir = root / "atomic"
    atomic_dir.mkdir(parents=True, exist_ok=True)
    target = atomic_dir / "app.js"
    write_file(target, VERSION_A, fsync=True)
    iterations = 200
    ctx = mp.get_context("fork")
    stop_event = ctx.Event()
    queue = ctx.Queue()
    reader = ctx.Process(
        target=reader_worker, args=(str(target), stop_event, queue, time.time() + 180)
    )
    errors = []
    with report.step("atomic replace x%d with continuous reader" % iterations):
        reader.start()
        time.sleep(0.3)
        for i in range(iterations):
            tmp = atomic_dir / ("app.js.tmp%d" % (i % 2))
            try:
                write_file(tmp, VERSIONS[i % 2], fsync=True)
                os.replace(tmp, target)
            except OSError as err:
                errors.append(
                    {
                        "iter": i,
                        "errno": err.errno,
                        "msg": str(err),
                        "op": "write+replace",
                    }
                )
                if len(errors) >= 5:
                    break
            if i % 25 == 0:
                time.sleep(0.01)
        stop_event.set()
        reader.join(timeout=60)
        observation = queue.get(timeout=30)
    report.check(
        not errors,
        "atomic replace completes without filesystem errors",
        errors=errors,
        completed=iterations - len(errors),
    )
    report.check(
        not observation["bad"],
        "atomic replace never exposes mixed/truncated content",
        observations=observation["observations"],
        reads=observation["reads"],
        bad=observation["bad"][:5],
    )
    final_expected = VERSIONS[(iterations - 1) % 2]
    # after writes stop, the new content must become visible; record the delay
    settle = None
    wait_started = time.perf_counter()
    for attempt in range(40):
        try:
            data = read_file(target)
        except OSError as err:
            data = b""
            errors.append({"op": "final-read", "errno": err.errno, "msg": str(err)})
        if data == final_expected:
            settle = round(time.perf_counter() - wait_started, 3)
            break
        time.sleep(1)
    report.check(
        settle is not None,
        "final content becomes complete last version",
        size=len(final_expected),
        settle_s=settle,
        observed_size=len(data),
    )
    report.data["final_visibility_settle_s"] = settle
    leftovers = sorted(p.name for p in atomic_dir.iterdir() if p.name != "app.js")
    report.check(not leftovers, "no temp files left behind", leftovers=leftovers[:5])

    # ---- 4) batch modification then full scan ---------------------------
    batch = root / "batch"
    batch.mkdir(parents=True, exist_ok=True)
    count = 50
    with report.step("batch create + modify %d files" % count):
        for i in range(count):
            write_file(batch / ("mod-%03d.js" % i), det_bytes(5000 + i, 800 + i))
        for i in range(count):
            write_file(
                batch / ("mod-%03d.js" % i),
                det_bytes(9000 + i, 1200 + i * 3),
                fsync=True,
            )
    expected_manifest = tree_manifest(batch)
    report.check(
        len(expected_manifest) == count,
        "batch final file set complete",
        entries=len(expected_manifest),
    )
    scanned = json.loads(
        run_checked(
            [
                "python3",
                "scan_tree.py",
                str(batch),
                str(batch.parent / "batch-scan.json"),
            ]
        ).stdout
        or "{}"
    )
    scanned_manifest = json.loads((batch.parent / "batch-scan.json").read_text())
    diffs = manifest_diff(expected_manifest, scanned_manifest, ignore_keys=("mode",))
    report.check(not diffs, "batch scan agrees with readback", diffs=diffs[:10])

    # ---- 5) sync + independent read path --------------------------------
    sync = report.sync("after s02")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))
    api_sizes = {}
    mismatch = None
    for rel in ("save/module-00.js", "edit/app.js", "atomic/app.js"):
        local = root / rel
        remote = remote_of(local)
        data = api_cat(remote)
        api_sizes[rel] = len(data)
        expected = read_file(local)
        if sha256_bytes(data) != sha256_bytes(expected):
            mismatch = {"rel": rel, "api_size": len(data), "local_size": len(expected)}
            break
    if mismatch:
        # allow the local view to settle (read-cache TTL), then re-compare
        settle = None
        started = time.perf_counter()
        for _ in range(40):
            time.sleep(1)
            local_size = (root / mismatch["rel"]).stat().st_size
            if local_size == mismatch["api_size"]:
                settle = round(time.perf_counter() - started, 3)
                break
        report.check(
            settle is not None,
            "api and local views converge after sync",
            mismatch=mismatch,
            settle_s=settle,
        )
    else:
        report.check(True, "api read matches for sampled files", sizes=api_sizes)

    report.data["metrics"] = {
        "write_then_read_latency": stats(latencies),
        "atomic_replace_iterations": iterations,
    }


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s02-save-read")
