"""n01 pnpm 布局：store + node_modules 符号链接，冷/热安装，包可加载."""

from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

PKG = {
    "name": "dn-pnpm-app",
    "version": "1.0.0",
    "private": True,
    "dependencies": {"is-odd": "3.0.1", "left-pad": "1.3.0"},
}


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n01-pnpm-layout")
    proj = base / "app"
    proj.mkdir(parents=True, exist_ok=True)
    (proj / "package.json").write_text(json.dumps(PKG, indent=2) + "\n")
    store = dn.STATE / "pnpm-store"

    with report.step("pnpm install (cold)"):
        dn.nrun(
            ["pnpm", "install", "--store-dir", str(store), "--reporter", "append-only"],
            cwd=proj,
            timeout=900,
        )
    check = dn.nsh(
        ["node", "-e", "const o=require('is-odd');console.log(o(3),o(4))"], cwd=proj
    )
    report.check(
        check.returncode == 0 and check.stdout.strip() == "true false",
        "is-odd 可 require",
        out=check.stdout[-120:],
        err=check.stderr[-200:],
    )

    nm = proj / "node_modules"
    report.check((nm / ".pnpm").is_dir(), "node_modules/.pnpm 存在")
    link = nm / "is-odd"
    is_link = link.is_symlink()
    dn.record_compat(report, "node_modules_uses_symlinks", bool(is_link))
    report.check(
        is_link,
        "依赖以符号链接安装（pnpm 布局）",
        readlink=os.readlink(link) if is_link else None,
    )
    resolved = dn.nsh(
        ["node", "-e", "console.log(require.resolve('is-odd'))"], cwd=proj
    )
    report.check(
        resolved.returncode == 0 and "is-odd" in resolved.stdout,
        "require.resolve 解析正常",
        out=resolved.stdout.strip()[-120:],
    )

    with report.step("pnpm install (hot, frozen lockfile)"):
        dn.nrun(
            [
                "pnpm",
                "install",
                "--store-dir",
                str(store),
                "--frozen-lockfile",
                "--reporter",
                "append-only",
            ],
            cwd=proj,
            timeout=900,
        )
    report.check((proj / "pnpm-lock.yaml").exists(), "pnpm-lock.yaml 存在")

    with report.step("rm node_modules + reinstall"):
        dn.nrun(["rm", "-rf", "node_modules"], cwd=proj, timeout=600)
        dn.nrun(
            [
                "pnpm",
                "install",
                "--store-dir",
                str(store),
                "--frozen-lockfile",
                "--reporter",
                "append-only",
            ],
            cwd=proj,
            timeout=900,
        )
    check2 = dn.nsh(
        ["node", "-e", "console.log(require('left-pad')('x',3,'0'))"], cwd=proj
    )
    report.check(
        check2.returncode == 0 and check2.stdout.strip() == "00x",
        "重装后 left-pad 可用",
        out=check2.stdout[-120:],
        err=check2.stderr[-200:],
    )
    report.sync("n01 final")


if __name__ == "__main__":
    main_guard(run, "n01-pnpm-layout")
