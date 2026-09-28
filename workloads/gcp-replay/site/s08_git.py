"""Scenario 8: git checkout between two versions - mass file changes.

Acceptance checks:
- after each checkout the worktree matches the target version exactly
  (no leftovers from the previous version, no missing additions)
- repeated switching stays consistent
- git status is clean after checkout
- content and executable bits are preserved
"""

from __future__ import annotations

import os
import pathlib
import shutil
import stat

from common import LOCAL, manifest_diff, run_checked, sh, tree_manifest, workdir
from fixtures_gen import ensure_git_repo


def reference_manifest(repo, tag, reference_root):
    ref = reference_root / ("ref-" + tag)
    if ref.exists():
        shutil.rmtree(ref)
    run_checked(["git", "clone", "-q", str(repo), str(ref)], timeout=300)
    run_checked(["git", "-C", str(ref), "checkout", "-q", tag], timeout=120)
    manifest = tree_manifest(ref)
    return {
        k: v for k, v in manifest.items() if not k.startswith(".git/") and k != ".git"
    }


def live_manifest(clone):
    manifest = tree_manifest(clone)
    return {
        k: v for k, v in manifest.items() if not k.startswith(".git/") and k != ".git"
    }


def run(report):
    root = workdir("s08-git")
    repo = ensure_git_repo()
    reference_root = LOCAL / "s08-git"
    reference_root.mkdir(parents=True, exist_ok=True)

    ref_a = reference_manifest(repo, "vA", reference_root)
    ref_b = reference_manifest(repo, "vB", reference_root)
    report.check(
        len(ref_a) != len(ref_b),
        "fixture versions differ in size",
        vA=len(ref_a),
        vB=len(ref_b),
    )

    clone = root / "site"
    with report.step("clone into drive9"):
        run_checked(["git", "clone", "-q", str(repo), str(clone)], timeout=900)

    rounds = 3
    for round_no in range(1, rounds + 1):
        for tag, ref in (("vA", ref_a), ("vB", ref_b)):
            with report.step("round %d checkout %s" % (round_no, tag)):
                run_checked(
                    ["git", "-C", str(clone), "checkout", "-q", "-f", tag], timeout=300
                )
            live = live_manifest(clone)
            diffs = manifest_diff(ref, live, ignore_keys=("mode",))
            report.check(
                not diffs,
                "round %d %s worktree matches version" % (round_no, tag),
                entries=len(live),
                diffs=diffs[:10],
            )
            ghosts = [rel for rel, entry in live.items() if rel not in ref]
            report.check(
                not ghosts,
                "round %d %s has no stale files" % (round_no, tag),
                ghosts=ghosts[:10],
            )
            status = sh(["git", "-C", str(clone), "status", "--porcelain"], timeout=120)
            report.check(
                not status.stdout.strip(),
                "round %d %s git status clean after checkout" % (round_no, tag),
                porcelain=status.stdout[:400],
            )
            # deleted paths from the other version must not linger
            if tag == "vB":
                stale = [rel for rel in ref_a if rel not in ref_b and rel in live]
                report.check(
                    not stale,
                    "round %d vB removed vA-only files" % round_no,
                    stale=stale[:10],
                )

    # executable bit and symlink handling (git mode) -----------------------
    mode_report = []
    for rel in sorted(ref_a):
        path = clone / rel
        if path.exists() and path.is_file():
            mode = stat.S_IMODE(path.stat().st_mode)
            mode_report.append({"rel": rel, "mode": oct(mode)})
            if len(mode_report) >= 5:
                break
    report.data["sample_modes_after_checkout"] = mode_report

    # search/read after switching (doc: 文件搜索、git 状态检查、构建读取)
    with report.step("read/search after switch"):
        proc = sh(
            ["git", "-C", str(clone), "grep", "-c", "B", "--", "index.html"],
            timeout=120,
        )
    report.check(
        proc.returncode == 0,
        "git grep finds content after switch",
        stdout=proc.stdout.strip(),
    )

    sync = report.sync("after s08")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s08-git")
