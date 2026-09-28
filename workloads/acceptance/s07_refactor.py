"""Scenario 7: refactor - move/rename/delete files and directories.

Acceptance checks:
- moves land at the new path with correct content; old path gone; parent
  listings consistent
- delete then same-name recreate yields the new content (no resurrection)
- open-handle semantics are recorded (old fd vs reopen after replace)
- local -> drive9 rename reports EXDEV (not treated as a defect); a subsequent
  explicit copy transfers content correctly
"""

from __future__ import annotations

import errno
import os
import pathlib
import shutil

from common import (LOCAL, det_bytes, manifest_diff, read_file, sha256_bytes,
                    tree_manifest, workdir, write_file)


def seed_project(project):
    expected = {}
    for rel, size in {
        "src/components/Header.jsx": 1200,
        "src/components/Footer.jsx": 900,
        "src/components/chart/Chart.jsx": 2600,
        "src/pages/index.js": 1500,
        "src/pages/about.js": 1100,
        "assets/img/hero.jpg": 20000,
        "assets/img/logo.svg": 800,
        "docs/readme.md": 400,
    }.items():
        data = det_bytes(abs(hash(rel)) % 99991, size)
        write_file(project / rel, data, fsync=True)
        expected[rel] = sha256_bytes(data)
    return expected


def run(report):
    root = workdir("s07-refactor")
    project = root / "site"
    for sub in ("src/components/chart", "src/pages", "assets/img", "docs"):
        (project / sub).mkdir(parents=True, exist_ok=True)
    expected = seed_project(project)
    report.check(len(expected) == 8, "seeded refactor tree", files=len(expected))

    # ---- 1) move a file within the same area -----------------------------
    with report.step("move file within src/components"):
        os.rename(project / "src/components/Footer.jsx", project / "src/components/SiteFooter.jsx")
    report.check(not (project / "src/components/Footer.jsx").exists(),
                 "old path gone after move")
    moved_sha = sha256_bytes(read_file(project / "src/components/SiteFooter.jsx"))
    report.check(moved_sha == expected["src/components/Footer.jsx"],
                 "moved file content intact", sha=moved_sha[:12])
    listing = sorted(p.name for p in (project / "src/components").iterdir())
    report.check("SiteFooter.jsx" in listing and "Footer.jsx" not in listing,
                 "parent listing reflects the move", listing=listing)

    # ---- 2) move an entire directory -------------------------------------
    with report.step("move whole directory src/components/chart -> src/charts"):
        os.rename(project / "src/components/chart", project / "src/charts")
    report.check(not (project / "src/components/chart").exists(),
                 "old directory path gone")
    report.check((project / "src/charts/Chart.jsx").exists(),
                 "child file present under new directory path")
    report.check(sha256_bytes(read_file(project / "src/charts/Chart.jsx")) == expected["src/components/chart/Chart.jsx"],
                 "moved directory child content intact")

    # ---- 3) delete then recreate with same name --------------------------
    target = project / "src/pages/about.js"
    old_sha = sha256_bytes(read_file(target))
    with report.step("delete + same-name recreate"):
        os.unlink(target)
        report.check(not target.exists(), "deleted file not visible")
        new_data = det_bytes(5150, 4321)
        write_file(target, new_data, fsync=True)
    report.check(sha256_bytes(read_file(target)) == sha256_bytes(new_data),
                 "recreated file holds new content", old_sha=old_sha[:12])

    # empty directory delete + recreate
    empty_dir = project / "docs/empty"
    empty_dir.mkdir()
    os.rmdir(empty_dir)
    report.check(not empty_dir.exists(), "empty directory removed")
    empty_dir.mkdir()
    report.check(empty_dir.is_dir(), "empty directory recreated")

    # ---- 4) open-handle semantics across replace -------------------------
    held = project / "src/pages/held.js"
    write_file(held, b"OLD-CONTENT" * 500, fsync=True)
    fd = os.open(held, os.O_RDONLY)
    tmp = project / "src/pages/held.js.tmp"
    new_content = b"NEW-CONTENT" * 900
    write_file(tmp, new_content, fsync=True)
    os.replace(tmp, held)
    try:
        old_view = os.pread(fd, 1 << 20, 0)
        old_handle_ok = old_view == b"OLD-CONTENT" * 500
    finally:
        os.close(fd)
    reopened = read_file(held)
    report.check(reopened == new_content, "reopened path shows new content")
    report.data["open_handle_semantics"] = {
        "old_fd_sees_old_content": old_handle_ok,
        "note": "POSIX rename semantics expect the old fd to keep the old inode",
    }

    # ---- 5) cross-filesystem rename (local -> drive9) --------------------
    local_src = LOCAL / "s07-refactor" / "local-source.txt"
    local_src.parent.mkdir(parents=True, exist_ok=True)
    payload = det_bytes(6100, 3333)
    write_file(local_src, payload, fsync=True)
    cross_dst = project / "docs/from-local.txt"
    try:
        os.rename(local_src, cross_dst)
        report.check(False, "cross-filesystem rename should report EXDEV",
                     note="rename succeeded across local->drive9")
    except OSError as err:
        report.check(err.errno == errno.EXDEV, "cross-filesystem rename reports EXDEV",
                     errno=err.errno, message=err.strerror)
        report.check(local_src.exists(), "source untouched after EXDEV")
        # explicit copy is the supported path
        shutil.copy2(local_src, cross_dst)
        report.check(sha256_bytes(read_file(cross_dst)) == sha256_bytes(payload),
                     "explicit copy transfers content correctly")

    # ---- 6) final manifest scan -----------------------------------------
    with report.step("final tree scan"):
        final = tree_manifest(project)
    report.check(len(final) >= 8, "final tree enumerated", entries=len(final))
    missing = [rel for rel in ("src/components/SiteFooter.jsx", "src/charts/Chart.jsx",
                               "src/pages/index.js", "docs/from-local.txt") if rel not in final]
    report.check(not missing, "expected refactor results all present", missing=missing)

    # no resurrection: deleted paths stay gone
    ghosts = [rel for rel in ("src/components/chart", "src/components/Footer.jsx") if rel in final]
    report.check(not ghosts, "deleted/moved paths do not reappear", ghosts=ghosts)

    sync = report.sync("after s07")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "s07-refactor")
