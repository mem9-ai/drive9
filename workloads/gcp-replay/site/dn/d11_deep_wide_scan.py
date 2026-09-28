"""d11 深宽目录遍历一致性：50 层深链 + 宽目录 + fixture 树，多次扫描比对."""

from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import FIXTURES, api_cat, api_stat, main_guard, remote_of, sha256_file  # noqa: E402
import dn_common as dn  # noqa: E402

DEPTH = 50
WIDE = 500


def scan(root):
    manifest = {}
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            p = pathlib.Path(dirpath) / name
            manifest[str(p.relative_to(root))] = sha256_file(p)
    return manifest


def run(report):
    base = dn.case_dir("d11-deep-wide-scan")
    root = base / "tree"
    root.mkdir(parents=True, exist_ok=True)

    expected = {}
    with report.step("build deep chain"):
        p = root / "deep"
        for i in range(1, DEPTH + 1):
            p = p / ("d%02d" % i)
        p.mkdir(parents=True, exist_ok=True)
        leaf = p / "leaf.txt"
        leaf.write_text("leaf\n")
        expected[str(leaf.relative_to(root))] = hashlib.sha256(b"leaf\n").hexdigest()
    with report.step("build wide dir (%d)" % WIDE):
        wide = root / "wide"
        wide.mkdir(exist_ok=True)
        for i in range(WIDE):
            data = (("%04d" % i) * 64).encode()
            f = wide / ("f%04d.dat" % i)
            f.write_bytes(data)
            expected[str(f.relative_to(root))] = hashlib.sha256(data).hexdigest()
    fixture = FIXTURES / "project-1000"
    if fixture.exists():
        with report.step("copy project-1000 fixture"):
            shutil.copytree(fixture, root / "project")
            for dirpath, _dirnames, filenames in os.walk(root / "project"):
                for name in filenames:
                    p2 = pathlib.Path(dirpath) / name
                    expected[str(p2.relative_to(root))] = sha256_file(p2)

    report.sync("d11 pre-scan drain")
    manifests = []
    for i in range(3):
        with report.step("scan %d" % (i + 1)):
            manifests.append(scan(root))
    report.check(
        manifests[0] == manifests[1] == manifests[2],
        "三次遍历结果一致",
        counts=[len(m) for m in manifests],
    )
    report.check(
        manifests[0] == expected,
        "遍历结果与预期清单一致",
        missing=len(set(expected) - set(manifests[0])),
        extra=len(set(manifests[0]) - set(expected)),
    )

    p = root / "deep"
    for i in range(1, DEPTH + 1):
        p = p / ("d%02d" % i)
    remote_leaf = remote_of(p / "leaf.txt")
    report.check(
        api_cat(remote_leaf) == b"leaf\n",
        "独立入口读取深层文件一致",
        remote=remote_leaf,
    )
    report.check(
        api_stat(remote_of(root / "wide" / "f0000.dat")) is not None,
        "独立入口可 stat 宽目录文件",
    )
    report.data.setdefault("metrics", {})["files_total"] = len(expected)


if __name__ == "__main__":
    main_guard(run, "d11-deep-wide-scan")
