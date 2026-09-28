"""Scenario 1: import a complete website project (tarball + git clone).

Acceptance checks:
- extracted/cloned files match the fixed input (path, size, sha256, mode)
- immediate post-import scan by another process agrees with readback
- local control run produces the same result
- after drain, an independent read path (HTTP API) sees the same content
"""

from __future__ import annotations

import json
import pathlib
import shutil

from common import (FIXTURES, LOCAL, api_download_tree, manifest_diff, remote_of,
                    run_checked, tree_manifest, workdir, localdir)
from fixtures_gen import ensure_project_tarball, ensure_git_repo


def scan_in_subprocess(root, out_json):
    run_checked(["python3", "scan_tree.py", str(root), str(out_json)], timeout=900)
    return json.loads(pathlib.Path(out_json).read_text())


def run(report):
    root = workdir("s01-import")
    local_root = localdir("s01-import")
    scan_dir = root.parent / (root.name + "-scans")
    scan_dir.mkdir(exist_ok=True)

    # ---- 1) tarball extraction, 100 and 1000 files -----------------------
    for count in (100, 1000):
        tar = ensure_project_tarball(count)
        fixture = FIXTURES / ("project-%d" % count)
        expected = tree_manifest(fixture)
        expected.pop(".fixture-ready", None)

        dest = root / ("extract-%d" % count)
        with report.step("extract tarball %d files" % count):
            dest.mkdir()
            run_checked(["tar", "-xzf", str(tar), "-C", str(dest)], timeout=900)

        with report.step("scan extracted tree %d" % count):
            actual = tree_manifest(dest)
        diffs = manifest_diff(expected, actual, ignore_keys=("mode",))
        report.check(not diffs, "extract-%d content matches input" % count,
                     entries=len(expected), diffs=diffs[:10])

        # independent process scan must agree with in-process readback
        scan_json = scan_dir / ("extract-%d.json" % count)
        with report.step("independent process scan %d" % count):
            scanned = scan_in_subprocess(dest, scan_json)
        diffs = manifest_diff(actual, scanned, ignore_keys=("mode",))
        report.check(not diffs, "extract-%d independent scan agrees" % count,
                     entries=len(scanned), diffs=diffs[:10])

        # permissions preserved
        mode_bad = [rel for rel, entry in expected.items()
                    if entry.get("mode") and actual.get(rel, {}).get("mode") != entry["mode"]]
        report.check(not mode_bad, "extract-%d permissions preserved" % count,
                     bad=[[rel, expected[rel]["mode"], actual.get(rel, {}).get("mode")] for rel in mode_bad[:10]])

        # local control run with the same input
        control = local_root / ("extract-%d" % count)
        control.mkdir()
        run_checked(["tar", "-xzf", str(tar), "-C", str(control)], timeout=900)
        control_manifest = tree_manifest(control)
        diffs = manifest_diff(control_manifest, actual, ignore_keys=("mode",))
        report.check(not diffs, "extract-%d matches local control run" % count, diffs=diffs[:10])

        # ---- after drain, independent read path sees the same content ----
        sync = report.sync("after extract-%d" % count)
        report.check(sync["ok"], "extract-%d drain succeeded" % count, drain=sync.get("result"))
        api_dest = local_root / ("api-extract-%d" % count)
        with report.step("api download extract-%d" % count):
            api_download_tree(remote_of(dest), api_dest)
        api_manifest = tree_manifest(api_dest)
        diffs = manifest_diff(expected, api_manifest, ignore_keys=("mode",))
        report.check(not diffs, "extract-%d independent api read matches input" % count,
                     entries=len(api_manifest), diffs=diffs[:10])

    # ---- 2) git clone import --------------------------------------------
    repo = ensure_git_repo()
    with report.step("resolve git fixture head"):
        head = run_checked(["git", "-C", str(repo), "rev-parse", "vA"], timeout=60).stdout.strip()

    clone = root / "git-clone"
    with report.step("git clone into drive9"):
        run_checked(["git", "clone", "-q", str(repo), str(clone)], timeout=900)
        run_checked(["git", "-C", str(clone), "checkout", "-q", "vA"], timeout=300)

    clone_manifest = tree_manifest(clone)
    clone_manifest = {k: v for k, v in clone_manifest.items() if not k.startswith(".git/") and k != ".git"}

    # reference: a local clone checked out at the same tag
    reference = local_root / "git-vA-reference"
    if reference.exists():
        shutil.rmtree(reference)
    run_checked(["git", "clone", "-q", str(repo), str(reference)], timeout=300)
    run_checked(["git", "-C", str(reference), "checkout", "-q", "vA"], timeout=120)
    fixture_manifest = tree_manifest(reference)
    fixture_manifest = {k: v for k, v in fixture_manifest.items() if not k.startswith(".git/") and k != ".git"}

    diffs = manifest_diff(fixture_manifest, clone_manifest, ignore_keys=("mode",))
    report.check(not diffs, "git clone worktree matches fixture repo", diffs=diffs[:10],
                 head=head, entries=len(clone_manifest))

    scan_json = scan_dir / "git-clone.json"
    scanned = scan_in_subprocess(clone, scan_json)
    scanned = {k: v for k, v in scanned.items() if not k.startswith(".git/") and k != ".git"}
    diffs = manifest_diff(clone_manifest, scanned, ignore_keys=("mode",))
    report.check(not diffs, "git clone independent scan agrees", diffs=diffs[:10])

    sync = report.sync("after git clone")
    report.check(sync["ok"], "git clone drain succeeded", drain=sync.get("result"))
    api_dest = local_root / "api-git-clone"
    api_download_tree(remote_of(clone), api_dest)
    api_manifest = tree_manifest(api_dest)
    api_manifest = {k: v for k, v in api_manifest.items() if not k.startswith(".git/") and k != ".git"}
    diffs = manifest_diff(clone_manifest, api_manifest, ignore_keys=("mode",))
    report.check(not diffs, "git clone independent api read matches", diffs=diffs[:10])

    report.data["inputs"] = {
        "project_100": str(FIXTURES / "project-100.tar.gz"),
        "project_1000": str(FIXTURES / "project-1000.tar.gz"),
        "git_repo": str(repo),
        "git_head": head,
    }


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "s01-import")
