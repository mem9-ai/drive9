"""n04 vitest：并行运行 + 快照写入/更新/稳定性."""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

SNAP_BODY_V1 = (
    'import { expect, test } from "vitest";\n'
    'import { sum } from "../src/sum.js";\n'
    'test("sum", () => { expect(sum(1, 2)).toBe(3); });\n'
    'test("snap", () => { expect({ v: 1 }).toMatchSnapshot(); });\n'
)
SNAP_BODY_V2 = SNAP_BODY_V1.replace("{ v: 1 }", "{ v: 2 }")


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n04-vitest")
    proj = base / "proj"
    proj.mkdir(parents=True, exist_ok=True)
    write(
        proj / "package.json",
        json.dumps({"name": "dn-vitest", "private": True, "type": "module"}, indent=2)
        + "\n",
    )
    write(proj / "src" / "sum.js", "export const sum = (a, b) => a + b;\n")
    write(proj / "tests" / "sum.test.js", SNAP_BODY_V1)

    run1 = dn.nsh(["vitest", "run"], cwd=proj, timeout=900)
    report.check(
        run1.returncode == 0,
        "vitest 首跑通过",
        out=(run1.stdout or "")[-400:],
        err=(run1.stderr or "")[-300:],
    )
    snaps = list(proj.glob("tests/__snapshots__/*.snap"))
    report.check(
        bool(snaps), "快照文件生成", found=[str(p.relative_to(proj)) for p in snaps]
    )
    snap = snaps[0] if snaps else proj / "tests/__snapshots__" / "sum.test.js.snap"

    write(proj / "tests" / "sum.test.js", SNAP_BODY_V2)
    run2 = dn.nsh(["vitest", "run"], cwd=proj, timeout=900)
    report.check(run2.returncode != 0, "快照不一致时运行失败", rc=run2.returncode)

    run3 = dn.nsh(["vitest", "run", "-u"], cwd=proj, timeout=900)
    report.check(
        run3.returncode == 0,
        "vitest -u 更新快照成功",
        out=(run3.stdout or "")[-300:],
        err=(run3.stderr or "")[-300:],
    )
    content = snap.read_text() if snap.exists() else ""
    report.check('"v": 2' in content, "快照内容已更新", content=content[:300])

    run4 = dn.nsh(["vitest", "run"], cwd=proj, timeout=900)
    report.check(run4.returncode == 0, "更新后重复运行稳定", rc=run4.returncode)
    report.sync("n04 final")


if __name__ == "__main__":
    main_guard(run, "n04-vitest")
