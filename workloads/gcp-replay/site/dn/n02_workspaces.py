"""n02 npm workspaces：包间符号链接、跨包脚本、.bin 可执行."""

from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def write(path, text, mode=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    if mode is not None:
        os.chmod(path, mode)


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n02-workspaces")
    root = base / "repo"
    root.mkdir(parents=True, exist_ok=True)

    write(
        root / "package.json",
        json.dumps(
            {
                "name": "dn-root",
                "private": True,
                "workspaces": ["packages/*"],
            },
            indent=2,
        )
        + "\n",
    )
    write(
        root / "packages" / "lib" / "package.json",
        json.dumps(
            {
                "name": "dn-lib",
                "version": "1.0.0",
                "main": "index.js",
                "bin": {"dn-lib-cli": "cli.js"},
            },
            indent=2,
        )
        + "\n",
    )
    write(
        root / "packages" / "lib" / "index.js",
        'module.exports = { marker: "LIB_V1" };\n',
    )
    write(
        root / "packages" / "lib" / "cli.js",
        '#!/usr/bin/env node\nconsole.log("dn-lib-cli ok");\n',
        mode=0o755,
    )
    write(
        root / "packages" / "app" / "package.json",
        json.dumps(
            {
                "name": "dn-app",
                "version": "1.0.0",
                "dependencies": {"dn-lib": "*"},
                "scripts": {"build": "node build.js"},
            },
            indent=2,
        )
        + "\n",
    )
    write(
        root / "packages" / "app" / "index.js",
        'const lib = require("dn-lib");\nconsole.log("APP sees " + lib.marker);\n',
    )
    write(
        root / "packages" / "app" / "build.js",
        'const fs = require("fs");\nconst lib = require("dn-lib");\n'
        'fs.writeFileSync("out.txt", "build:" + lib.marker + "\\n");\n',
    )

    with report.step("npm install (workspaces)"):
        dn.nrun(["npm", "install", "--no-audit", "--no-fund"], cwd=root, timeout=900)

    linked = root / "node_modules" / "dn-lib"
    report.check(
        linked.is_symlink(),
        "workspace 包以符号链接安装",
        readlink=os.readlink(linked) if linked.is_symlink() else None,
    )

    out1 = dn.nsh(["node", "packages/app/index.js"], cwd=root)
    report.check(
        out1.returncode == 0 and "APP sees LIB_V1" in out1.stdout,
        "跨包 require 正常（V1）",
        out=out1.stdout[-120:],
        err=out1.stderr[-200:],
    )

    write(
        root / "packages" / "lib" / "index.js",
        'module.exports = { marker: "LIB_V2" };\n',
    )
    out2 = dn.nsh(["node", "packages/app/index.js"], cwd=root)
    report.check(
        "APP sees LIB_V2" in out2.stdout,
        "修改 lib 后无需重装即可见（V2）",
        out=out2.stdout[-120:],
    )

    build = dn.nsh(["npm", "run", "-w", "dn-app", "build"], cwd=root, timeout=300)
    out_txt = root / "packages" / "app" / "out.txt"
    report.check(
        build.returncode == 0
        and out_txt.exists()
        and "build:LIB_V2" in out_txt.read_text(),
        "workspace 脚本构建输出正确",
        rc=build.returncode,
    )

    cli = root / "node_modules" / ".bin" / "dn-lib-cli"
    report.check(cli.exists(), ".bin shim 存在")
    cli_run = dn.nsh([str(cli)], cwd=root, timeout=120)
    report.check(
        cli_run.returncode == 0 and "dn-lib-cli ok" in cli_run.stdout,
        ".bin 可执行",
        out=cli_run.stdout[-120:],
        err=cli_run.stderr[-200:],
    )
    report.sync("n02 final")


if __name__ == "__main__":
    main_guard(run, "n02-workspaces")
