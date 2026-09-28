"""n03 TypeScript 增量构建 + watch 重编译."""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n03-tsc-incremental")
    proj = base / "proj"
    proj.mkdir(parents=True, exist_ok=True)
    write(
        proj / "tsconfig.json",
        json.dumps(
            {
                "compilerOptions": {
                    "target": "ES2020",
                    "module": "CommonJS",
                    "strict": False,
                    "outDir": "dist",
                    "incremental": True,
                },
                "include": ["src"],
            },
            indent=2,
        )
        + "\n",
    )
    write(proj / "src" / "a.ts", 'export const a = "A1";\n')
    write(proj / "src" / "b.ts", 'import { a } from "./a";\nconsole.log("B:" + a);\n')

    with report.step("tsc first build"):
        dn.nrun(["tsc", "-p", "."], cwd=proj, timeout=600)
    a_js = proj / "dist" / "a.js"
    b_js = proj / "dist" / "b.js"
    report.check(a_js.exists() and b_js.exists(), "首次构建产物存在")
    report.check("A1" in a_js.read_text(), "a.js 内容正确")
    report.check(
        any(proj.rglob("*.tsbuildinfo")),
        "增量信息文件生成",
        found=[str(p.relative_to(proj)) for p in proj.rglob("*.tsbuildinfo")],
    )

    with report.step("tsc rebuild (no changes)"):
        dn.nrun(["tsc", "-p", "."], cwd=proj, timeout=600)
    report.check("A1" in a_js.read_text(), "无变化重建正常")

    write(proj / "src" / "a.ts", 'export const a = "A2";\n')
    with report.step("tsc incremental rebuild"):
        dn.nrun(["tsc", "-p", "."], cwd=proj, timeout=600)
    report.check("A2" in a_js.read_text(), "增量重建后产物更新")

    watch = dn.spawn_bg(
        ["tsc", "-p", ".", "--watch", "--preserveWatchOutput"],
        log_path=base / "watch.log",
        cwd=proj,
    )
    try:
        dn.wait_for(
            lambda: (
                "Watching for file changes"
                in (base / "watch.log").read_text(errors="replace")
            ),
            timeout=60,
            what="tsc watch start",
        )
        write(proj / "src" / "a.ts", 'export const a = "A3";\n')
        dn.wait_for(
            lambda: "A3" in a_js.read_text(errors="replace"),
            timeout=60,
            what="watch 编译 A3",
        )
        report.check(True, "tsc --watch 检测到修改并重编译")
    finally:
        dn.kill9(watch)
    report.sync("n03 final")


if __name__ == "__main__":
    main_guard(run, "n03-tsc-incremental")
