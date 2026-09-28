"""Scenario 9: build - mass reads of sources, write and clean products.

Acceptance checks:
- sources read by the build match their input (bundle hash reproducible)
- outputs written by the build are complete and readable
- overwrite / truncate / delete of products are visible to later operations
- after an interrupted build, explicit cleanup + rerun succeeds
"""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess

from common import (
    det_bytes,
    manifest_diff,
    read_file,
    run_checked,
    sh,
    sha256_bytes,
    tree_manifest,
    workdir,
    write_file,
)

BUILD = "build_tool.py"


def seed_site(root, files=60):
    for sub in ("src", "assets/img", "assets/fonts"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    write_file(root / "src" / "index.js", b"console.log('entry');\n" * 30, fsync=True)
    for i in range(files):
        write_file(
            root / "src" / ("mod-%03d.js" % i),
            b"// module %d\n" % i + bytes([65 + (i % 26)]) * (200 + i * 7),
            fsync=True,
        )
    for i in range(6):
        write_file(
            root / "assets" / "img" / ("pic-%02d.bin" % i),
            det_bytes(8000 + i, 4096 + i * 100),
        )
    write_file(root / "assets" / "fonts" / "font.woff2", det_bytes(8100, 9000))


def run_build(root, extra=()):
    proc = run_checked(["python3", BUILD, str(root), *extra], timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def run(report):
    root = workdir("s09-build")
    site = root / "site"
    seed_site(site)

    # ---- 1) first build --------------------------------------------------
    with report.step("first build"):
        first = run_build(site)
    report.check(first["bundle_size"] > 0, "first build produced a bundle", **first)
    bundle = read_file(site / "dist" / "bundle.js")
    report.check(
        len(bundle) == first["bundle_size"]
        and sha256_bytes(bundle) == first["bundle_sha256"],
        "bundle content matches manifest",
    )
    manifest = json.loads((site / "dist" / "manifest.json").read_text())
    report.check(
        len(manifest["files"]) >= 60,
        "build read all inputs",
        inputs=len(manifest["files"]),
    )

    # reproducible: build again with cache present
    with report.step("repeat build with cache"):
        second = run_build(site)
    report.check(
        second["bundle_sha256"] == first["bundle_sha256"],
        "repeat build is reproducible",
        first=first["bundle_sha256"][:12],
        second=second["bundle_sha256"][:12],
    )

    # ---- 2) build after deleting the cache -------------------------------
    with report.step("build after removing cache"):
        shutil.rmtree(site / ".build-cache")
        third = run_build(site)
    report.check(
        third["bundle_sha256"] == first["bundle_sha256"],
        "cache-free build is reproducible",
    )

    # ---- 3) apply a fixed source change set and rebuild -------------------
    with report.step("apply source changes (add/modify/delete) then build"):
        write_file(
            site / "src" / "new-feature.js",
            b"// new feature\n" + b"N" * 900,
            fsync=True,
        )
        write_file(
            site / "src" / "mod-005.js", b"// modified\n" + b"M" * 1500, fsync=True
        )
        (site / "src" / "mod-007.js").unlink()
        (site / "assets" / "img" / "pic-03.bin").unlink()
        fourth = run_build(site)
    report.check(
        fourth["bundle_sha256"] != first["bundle_sha256"],
        "bundle changed after source change set",
    )
    names = {
        row["rel"]
        for row in json.loads((site / "dist" / "manifest.json").read_text())["files"]
    }
    report.check("src/new-feature.js" in names, "new source included in build")
    report.check("src/mod-007.js" not in names, "deleted source excluded from build")
    report.check(
        "assets/img/pic-03.bin" not in names, "deleted asset excluded from build"
    )

    # ---- 4) overwrite/truncate/delete products are visible ---------------
    with report.step("overwrite + truncate + delete build products"):
        dist = site / "dist"
        write_file(dist / "extra.txt", b"x" * 5000, fsync=True)
        report.check(
            (dist / "extra.txt").stat().st_size == 5000, "product overwrite visible"
        )
        with open(dist / "extra.txt", "r+b") as fh:
            fh.truncate(10)
        report.check(
            (dist / "extra.txt").stat().st_size == 10, "product truncate visible"
        )
        (dist / "extra.txt").unlink()
        report.check(not (dist / "extra.txt").exists(), "product delete visible")

    # ---- 5) interrupted build + explicit cleanup + rerun -----------------
    with report.step("interrupted build then cleanup and rerun"):
        proc = sh(["python3", BUILD, str(site), "--fail-after", "10"], timeout=300)
        report.check(
            proc.returncode != 0,
            "injected build failure reported",
            returncode=proc.returncode,
        )
        shutil.rmtree(site / "dist", ignore_errors=True)
        shutil.rmtree(site / ".build-cache", ignore_errors=True)
        report.check(
            not (site / "dist").exists(), "explicit cleanup removed partial output"
        )
        fifth = run_build(site)
        report.check(
            fifth["bundle_sha256"]
            == sha256_bytes(read_file(site / "dist" / "bundle.js")),
            "rerun after cleanup produced a complete bundle",
        )

    report.data["builds"] = {
        "first": first,
        "repeat": second,
        "cache_free": third,
        "changed": fourth,
        "after_cleanup": fifth,
    }

    sync = report.sync("after s09")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s09-build")
