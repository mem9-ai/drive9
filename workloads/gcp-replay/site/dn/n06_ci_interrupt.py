"""n06 npm ci 中途 kill -9 后重跑收敛（两轮）."""

from __future__ import annotations

import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

DEPS = {
    "dependencies": {
        "is-odd": "3.0.1",
        "left-pad": "1.3.0",
        "ms": "2.1.3",
        "once": "1.4.0",
        "minimist": "1.2.8",
        "semver": "7.6.3",
    }
}


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n06-ci-interrupt")
    proj = base / "proj"
    proj.mkdir(parents=True, exist_ok=True)
    (proj / "package.json").write_text(
        json.dumps(
            {"name": "dn-ci", "version": "1.0.0", "private": True, **DEPS}, indent=2
        )
        + "\n"
    )
    dn.nrun(
        ["npm", "install", "--package-lock-only", "--no-audit", "--no-fund"],
        cwd=proj,
        timeout=600,
    )
    report.check((proj / "package-lock.json").exists(), "锁文件已生成")

    for r in (1, 2):
        proc = dn.spawn_bg(
            ["npm", "ci", "--no-audit", "--no-fund"],
            log_path=base / ("ci-kill-%d.log" % r),
            cwd=proj,
        )
        killed_at = None
        try:
            dn.wait_for(
                lambda: (
                    (proj / "node_modules").exists()
                    and any((proj / "node_modules").iterdir())
                ),
                timeout=90,
                what="ci 开始写入 node_modules",
            )
            killed_at = len(list((proj / "node_modules").iterdir()))
        finally:
            dn.kill_group(proc)
        report.data.setdefault("metrics", {})["kill_%d_entries" % r] = killed_at
        rerun = dn.nsh(["npm", "ci", "--no-audit", "--no-fund"], cwd=proj, timeout=1200)
        report.check(
            rerun.returncode == 0,
            "第 %d 轮：kill 后重跑 npm ci 成功" % r,
            out=(rerun.stdout or "")[-300:],
            err=(rerun.stderr or "")[-300:],
        )
        check = dn.nsh(["node", "-e", "console.log(require('is-odd')(3))"], cwd=proj)
        report.check(
            check.returncode == 0 and check.stdout.strip() == "true",
            "第 %d 轮：重跑后依赖可用" % r,
            out=(check.stdout or "")[-120:],
            err=(check.stderr or "")[-200:],
        )
    report.sync("n06 final")


if __name__ == "__main__":
    main_guard(run, "n06-ci-interrupt")
