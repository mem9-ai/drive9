"""n05 npm pack + tarball 安装往返（内容/权限/.bin）."""

from __future__ import annotations

import json
import os
import pathlib
import stat
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard, sha256_file  # noqa: E402
import dn_common as dn  # noqa: E402

CLI = '#!/usr/bin/env node\nconsole.log("dn-hello ok");\n'
DATA = "packed-data-0123456789\n"


def write(path, text, mode=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    if mode is not None:
        os.chmod(path, mode)


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n05-pack-tarball")
    pkg = base / "pkg"
    write(
        pkg / "package.json",
        json.dumps(
            {
                "name": "dn-pack-pkg",
                "version": "1.0.0",
                "bin": {"dn-hello": "./cli.js"},
                "files": ["cli.js", "lib"],
            },
            indent=2,
        )
        + "\n",
    )
    write(pkg / "cli.js", CLI, mode=0o755)
    write(pkg / "lib" / "data.txt", DATA)

    pack = dn.nrun(["npm", "pack", "--pack-destination", "."], cwd=pkg, timeout=300)
    tars = sorted(pkg.glob("dn-pack-pkg-*.tgz"))
    report.check(bool(tars), "npm pack 生成 tarball", out=(pack.stdout or "")[-200:])
    if not tars:
        return
    tgz = tars[0]
    listing = dn.nsh(["tar", "-tzf", str(tgz)], timeout=120)
    report.check(
        "package/cli.js" in listing.stdout and "package/lib/data.txt" in listing.stdout,
        "tarball 内容清单正确",
        listing=listing.stdout[-300:],
    )

    consumer = base / "consumer"
    consumer.mkdir(parents=True, exist_ok=True)
    dn.nrun(["npm", "init", "-y"], cwd=consumer, timeout=120)
    dn.nrun(
        ["npm", "install", "--no-audit", "--no-fund", str(tgz)],
        cwd=consumer,
        timeout=600,
    )

    installed = consumer / "node_modules" / "dn-pack-pkg"
    report.check((installed / "cli.js").exists(), "安装后文件存在")
    report.check(
        sha256_file(installed / "cli.js") == sha256_file(pkg / "cli.js"),
        "安装后 cli.js 内容一致",
    )
    report.check(
        (installed / "lib" / "data.txt").read_text() == DATA, "安装后 data.txt 一致"
    )

    bin_link = consumer / "node_modules" / ".bin" / "dn-hello"
    report.check(bin_link.exists(), ".bin/dn-hello 生成")
    proc = dn.nsh([str(bin_link)], cwd=consumer, timeout=120)
    report.check(
        proc.returncode == 0 and "dn-hello ok" in proc.stdout,
        "打包安装后的命令可执行",
        out=proc.stdout[-120:],
        err=proc.stderr[-200:],
    )
    report.check(
        bool(os.stat(installed / "cli.js").st_mode & stat.S_IXUSR), "安装后执行位保留"
    )
    report.sync("n05 final")


if __name__ == "__main__":
    main_guard(run, "n05-pack-tarball")
