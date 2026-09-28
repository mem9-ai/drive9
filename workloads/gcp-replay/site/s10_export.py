"""Scenario 10: package and export a fixed version of the project.

Acceptance checks:
- directory traversal is complete and free of duplicates
- tar and zip exports unpack to the same content
- a second export after source changes reflects the new state (no stale cache)
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import tarfile
import zipfile

from common import (
    LOCAL,
    manifest_diff,
    read_file,
    run_checked,
    sh,
    sha256_bytes,
    tree_manifest,
    workdir,
    write_file,
)
from fixtures_gen import ensure_project_tarball


def unpack_tar(tar_path, dest):
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    run_checked(["tar", "-xzf", str(tar_path), "-C", str(dest)], timeout=600)
    return dest


def make_zip(src, zip_path):
    src = pathlib.Path(src)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(src.rglob("*")):
            rel = str(path.relative_to(src))
            if path.is_dir():
                zf.writestr(rel + "/", "")
            else:
                zf.write(path, rel)
    return zip_path


def unpack_zip(zip_path, dest):
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    return dest


def run(report):
    root = workdir("s10-export")
    project = root / "site"
    project.mkdir()
    tar_fixture = ensure_project_tarball(100)
    with report.step("seed project from fixture tarball"):
        run_checked(["tar", "-xzf", str(tar_fixture), "-C", str(project)], timeout=600)

    exports = LOCAL / "s10-export"
    exports.mkdir(parents=True, exist_ok=True)

    def export_round(label):
        report.sync("before %s export" % label)
        expected = tree_manifest(project)
        # 1) copy to an independent local directory
        copied = exports / (label + "-copy")
        if copied.exists():
            shutil.rmtree(copied)
        with report.step("%s: copy tree out of drive9" % label):
            shutil.copytree(project, copied)
        copy_manifest = tree_manifest(copied)
        diffs = manifest_diff(expected, copy_manifest, ignore_keys=("mode",))
        report.check(
            not diffs,
            "%s: copied tree matches source" % label,
            entries=len(copy_manifest),
            diffs=diffs[:8],
        )

        # 2) tar export (writes have stopped and been confirmed before this)
        tar_path = exports / (label + ".tar.gz")
        with report.step("%s: tar export" % label):
            proc = sh(
                [
                    "tar",
                    "-czf",
                    str(tar_path),
                    "-C",
                    str(project),
                    "--warning=no-file-changed",
                    ".",
                ],
                timeout=900,
            )
            report.check(
                proc.returncode in (0, 1),
                "%s: tar export completed" % label,
                returncode=proc.returncode,
                stderr=(proc.stderr or "")[-300:],
            )
        tar_out = exports / (label + "-tar-extract")
        unpack_tar(tar_path, tar_out)
        tar_manifest = tree_manifest(tar_out)
        diffs = manifest_diff(expected, tar_manifest, ignore_keys=("mode",))
        report.check(
            not diffs,
            "%s: tar export unpacks to the same content" % label,
            entries=len(tar_manifest),
            diffs=diffs[:8],
        )

        # 3) zip export
        zip_path = exports / (label + ".zip")
        with report.step("%s: zip export" % label):
            make_zip(project, zip_path)
        zip_out = exports / (label + "-zip-extract")
        unpack_zip(zip_path, zip_out)
        zip_manifest = tree_manifest(zip_out)
        diffs = manifest_diff(expected, zip_manifest, ignore_keys=("mode",))
        report.check(
            not diffs,
            "%s: zip export unpacks to the same content" % label,
            entries=len(zip_manifest),
            diffs=diffs[:8],
        )

        # 4) no duplicate entries during traversal
        seen = {}
        duplicates = []
        for path in project.rglob("*"):
            key = str(path.relative_to(project))
            if key in seen:
                duplicates.append(key)
            seen[key] = True
        report.check(
            not duplicates,
            "%s: traversal has no duplicates" % label,
            duplicates=duplicates[:8],
        )
        return expected

    first = export_round("v1")

    # ---- source changes, then a second export ---------------------------
    with report.step("apply changes for v2"):
        write_file(
            project / "index.html", b"<html>v2</html>\n" + b"V2" * 800, fsync=True
        )
        write_file(project / "styles" / "new.css", b"/* new */\n" * 50, fsync=True)
        (project / "about.html").unlink()
        (project / "assets" / "empty.svg").unlink()
    second = export_round("v2")

    report.check(
        second["index.html"]["sha256"] != first["index.html"]["sha256"],
        "v2 export reflects replaced content",
    )
    report.check("about.html" not in second, "v2 export reflects deletion")
    report.check("styles/new.css" in second, "v2 export reflects addition")
    report.check(
        len(second) != len(first),
        "v2 file set differs from v1",
        v1=len(first),
        v2=len(second),
    )

    sync = report.sync("after s10")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s10-export")
