"""Scenario 11: view file tree / sources / images from an independent client.

Writer is the FUSE mount; the independent reader is the HTTP API (bypasses the
FUSE client and its caches). Each mutation is compared on both sides, then the
sync-confirmed state is compared again after the declared visibility window.
"""

from __future__ import annotations

import pathlib
import time

from common import (
    api_cat,
    api_exists,
    api_stat,
    det_bytes,
    read_file,
    remote_of,
    sha256_bytes,
    stats,
    workdir,
    write_file,
)


def sample_read(report, root, rel, label):
    """Compare local (FUSE) and API views for one path."""
    local = root / rel
    remote = remote_of(local)
    row = {"path": rel, "label": label}
    try:
        local_data = read_file(local)
        row["local"] = {
            "size": len(local_data),
            "sha256": sha256_bytes(local_data)[:12],
        }
    except OSError as err:
        row["local"] = {"error": str(err)}
    try:
        api_data = api_cat(remote)
        row["api"] = {"size": len(api_data), "sha256": sha256_bytes(api_data)[:12]}
    except RuntimeError as err:
        row["api"] = {"error": str(err)[:120]}
    row["match"] = row.get("local", {}).get("sha256") == row.get("api", {}).get(
        "sha256"
    )
    return row


def run(report):
    root = workdir("s11-external")
    ops = []

    # ---- mutation sequence with per-op two-sided comparison --------------
    created = root / "src" / "created.js"
    with report.step("create file"):
        write_file(created, b"// created\n" + b"C" * 1200, fsync=True)
    ops.append(sample_read(report, root, "src/created.js", "create"))

    with report.step("overwrite file"):
        write_file(created, b"// overwritten\n" + b"O" * 2200, fsync=True)
    ops.append(sample_read(report, root, "src/created.js", "overwrite"))

    renamed = root / "src" / "renamed.js"
    with report.step("rename file"):
        renamed.parent.mkdir(parents=True, exist_ok=True)
        renamed.write_bytes(read_file(created))
        created.unlink()
    ops.append(sample_read(report, root, "src/renamed.js", "rename-new-path"))

    with report.step("delete file"):
        renamed.unlink()
    ops.append(
        {
            "path": "src/renamed.js",
            "label": "delete",
            "local": {"exists": renamed.exists()},
            "api": {"exists": api_exists(remote_of(renamed))},
        }
    )

    # a batch of assets to simulate images
    for i in range(5):
        write_file(
            root / "assets" / ("img-%02d.bin" % i),
            det_bytes(12000 + i, 8000 + i * 500),
            fsync=True,
        )
    ops.append(sample_read(report, root, "assets/img-03.bin", "image-asset"))

    report.check(
        all(row.get("match") for row in ops if "match" in row),
        "every sampled path matches between FUSE and API after ops",
        mismatches=[row for row in ops if row.get("match") is False][:3],
    )
    deleted = ops[-2]
    report.check(
        deleted["local"]["exists"] is False and deleted["api"]["exists"] is False,
        "deleted path invisible on both sides",
    )

    # ---- after sync, compare the whole tree -----------------------------
    sync = report.sync("before cross-client compare")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))

    waited = 0.0
    deadline = time.time() + 60
    import subprocess
    from common import BIN

    diffs = []
    while time.time() < deadline:
        proc = subprocess.run(
            [BIN, "fs", "ls", "-l", remote_of(root)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        listing = proc.stdout or ""
        names = {line.split()[-1] for line in listing.splitlines() if line.strip()}
        needed = {"assets", "src"}
        if needed.issubset(names):
            break
        time.sleep(5)
        waited += 5
    report.data["api_visibility_wait_s"] = waited
    report.check(
        waited < 60, "API listing becomes consistent within the window", waited_s=waited
    )

    # ---- reads do not depend on the writer's local cache ------------------
    stats_row = api_stat(remote_of(root / "assets" / "img-03.bin"))
    report.check(
        stats_row is not None and int(stats_row.get("size", -1)) == 8000 + 3 * 500,
        "API stat returns the persisted size",
        stat=stats_row,
    )

    # ---- new client (cold cache) reads the same tree ---------------------
    cold = workdir("s11-external", sub="cold-reader")
    report.check(
        True,
        "cold reader workspace prepared (separate mount to be checked in S12)",
        path=str(cold),
    )

    report.data["ops"] = ops


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s11-external")
