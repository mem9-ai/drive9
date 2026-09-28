"""Scenario 5: two processes race for the same asset name.

Acceptance checks:
- for two concurrent exclusive-create (O_EXCL) requests only one wins; the
  loser gets a conflict result, and the target holds the winner's content
- for no-replace rename (renameat2 RENAME_NOREPLACE) only one wins, the loser's
  source is untouched
- a pre-existing target is never modified by a no-replace operation
- unsupported operations are recorded as such (not silently treated as pass)
"""

from __future__ import annotations

import ctypes
import multiprocessing as mp
import os
import pathlib

from common import det_bytes, read_file, sha256_bytes, write_file, workdir

SYS_renameat2 = 316  # x86_64
RENAME_NOREPLACE = 1
AT_FDCWD = -100


def rename_noreplace(src, dst):
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    rc = libc.syscall(
        SYS_renameat2,
        AT_FDCWD,
        str(src).encode(),
        AT_FDCWD,
        str(dst).encode(),
        RENAME_NOREPLACE,
    )
    if rc != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return True


def excl_worker(path, marker, ready, gate, queue):
    ready.put(True)
    gate.wait()
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            os.write(fd, marker)
            os.fsync(fd)
        finally:
            os.close(fd)
        queue.put({"won": True, "marker": marker.decode()})
    except FileExistsError as err:
        queue.put({"won": False, "error": "FileExistsError", "detail": str(err)})
    except Exception as err:
        queue.put({"won": False, "error": "%s: %s" % (type(err).__name__, err)})


def rename_worker(src, dst, ready, gate, queue):
    ready.put(True)
    gate.wait()
    try:
        rename_noreplace(str(src), str(dst))
        queue.put({"won": True})
    except OSError as err:
        queue.put(
            {
                "won": False,
                "error": "OSError errno=%s" % err.errno,
                "detail": err.strerror,
            }
        )
    except Exception as err:
        queue.put({"won": False, "error": "%s: %s" % (type(err).__name__, err)})


def race(workers_args, target_workers):
    ctx = mp.get_context("fork")
    ready, gate, queue = ctx.Queue(), ctx.Event(), ctx.Queue()
    processes = []
    for args in workers_args:
        proc = ctx.Process(target=target_workers, args=(*args, ready, gate, queue))
        proc.start()
        processes.append(proc)
    for _ in processes:
        ready.get(timeout=60)
    gate.set()
    results = [queue.get(timeout=120) for _ in processes]
    for proc in processes:
        proc.join(timeout=30)
    return results


def run(report):
    root = workdir("s05-conflict")

    # ---- exclusive create race ------------------------------------------
    target = root / "cover.png"
    marker_a = b"A" * 4096 + b"||WINNER-A||"
    marker_b = b"B" * 6500 + b"||WINNER-B||"
    results = race([(str(target), marker_a), (str(target), marker_b)], excl_worker)
    winners = [row for row in results if row.get("won")]
    losers = [row for row in results if not row.get("won")]
    report.check(
        len(winners) == 1 and len(losers) == 1,
        "O_EXCL race: exactly one winner",
        results=results,
    )
    if len(winners) == 1:
        content = read_file(target)
        expected = marker_a if winners[0]["marker"] == marker_a.decode() else marker_b
        report.check(
            content == expected,
            "O_EXCL target holds winner content",
            size=len(content),
            winner=winners[0]["marker"],
        )
    report.check(
        all(
            "FileExistsError" in (row.get("error") or "") or row.get("won")
            for row in results
        ),
        "O_EXCL loser got a conflict result",
        losers=losers[:2],
    )

    # ---- no-replace rename race -----------------------------------------
    src_a, src_b = root / "install-a.png", root / "install-b.png"
    dst = root / "installed.png"
    write_file(src_a, b"A" * 3000 + b"||SRC-A||", fsync=True)
    write_file(src_b, b"B" * 5000 + b"||SRC-B||", fsync=True)
    sha_a, sha_b = sha256_bytes(read_file(src_a)), sha256_bytes(read_file(src_b))

    results = race([(src_a, dst), (src_b, dst)], rename_worker)
    winners = [row for row in results if row.get("won")]
    losers = [row for row in results if not row.get("won")]
    unsupported = all(not row.get("won") for row in results) and all(
        row.get("error", "").endswith(("errno=38", "errno=22", "errno=95"))
        for row in results
    )
    if unsupported:
        report.data["rename_noreplace_supported"] = False
        report.data["rename_noreplace_error"] = results
        report.check(
            True,
            "renameat2 RENAME_NOREPLACE unsupported (recorded as a limitation)",
            results=results,
            note="per doc: unsupported operations are recorded as such",
        )
    else:
        report.data["rename_noreplace_supported"] = True
        report.check(
            len(winners) == 1 and len(losers) == 1,
            "NOREPLACE race: exactly one winner",
            results=results,
        )
        if len(winners) == 1:
            got = sha256_bytes(read_file(dst))
            report.check(
                got in (sha_a, sha_b),
                "NOREPLACE target holds winner content",
                which="A" if got == sha_a else "B",
            )
            loser_src = src_b if got == sha_a else src_a
            report.check(loser_src.exists(), "loser source untouched")
            leftover = [p.name for p in (src_a, src_b) if p.exists()]
            report.data["noreplace_leftover_sources"] = leftover

    # ---- pre-existing target, no-replace must not modify it -------------
    preset = root / "preset.png"
    preset_content = det_bytes(777, 2222)
    write_file(preset, preset_content, fsync=True)
    staged = root / "staged.png"
    write_file(staged, b"NEW" * 900, fsync=True)
    try:
        rename_noreplace(str(staged), str(preset))
        report.check(
            False,
            "no-replace rename must refuse existing target",
            note="renameat2 NOREPLACE succeeded unexpectedly",
        )
    except OSError as err:
        unchanged = read_file(preset) == preset_content
        report.check(
            unchanged,
            "pre-existing target unchanged after refused rename",
            errno=err.errno,
            message=err.strerror,
            staged_still_exists=staged.exists(),
        )

    # ---- contrast: plain replace is allowed to overwrite -----------------
    plain = root / "plain.png"
    write_file(plain, b"OLD" * 1000, fsync=True)
    replacement = root / "replacement.png"
    new_content = b"NEW" * 1500
    write_file(replacement, new_content, fsync=True)
    os.replace(replacement, plain)
    report.check(
        read_file(plain) == new_content,
        "plain rename overwrites as expected (contrast)",
    )

    sync = report.sync("after s05")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s05-conflict")
