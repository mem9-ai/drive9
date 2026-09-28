"""n07 并发安装共享 npm cache + npm cache verify."""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

DEPS = {"dependencies": {"is-odd": "3.0.1", "left-pad": "1.3.0"}}


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n07-cache-concurrency")
    cache = dn.STATE / "n07-cache-outside-mount"
    cache.mkdir(parents=True, exist_ok=True)
    procs = []
    for name in ("p1", "p2"):
        proj = base / name
        proj.mkdir(parents=True, exist_ok=True)
        (proj / "package.json").write_text(
            json.dumps(
                {"name": "dn-" + name, "version": "1.0.0", "private": True, **DEPS},
                indent=2,
            )
            + "\n"
        )
        procs.append(
            (
                name,
                proj,
                dn.spawn_bg(
                    [
                        "npm",
                        "install",
                        "--cache",
                        str(cache),
                        "--no-audit",
                        "--no-fund",
                    ],
                    log_path=base / (name + ".log"),
                    cwd=proj,
                ),
            )
        )

    rcs = {}
    for name, proj, proc in procs:
        rcs[name] = dn.wait_proc(proc, timeout=1200)
    report.check(
        all(rc == 0 for rc in rcs.values()),
        "两个并发安装都成功",
        rcs=rcs,
        logs={n: (base / (n + ".log")).read_text()[-200:] for n in rcs if rcs[n] != 0},
    )

    for name, proj, _ in procs:
        check = dn.nsh(
            ["node", "-e", "console.log(require('left-pad')('x',3,'0'))"], cwd=proj
        )
        report.check(
            check.returncode == 0 and check.stdout.strip() == "00x",
            "%s 依赖可用" % name,
            out=(check.stdout or "")[-60:],
        )

    verify = dn.nsh(["npm", "cache", "verify", "--cache", str(cache)], timeout=900)
    report.check(
        verify.returncode == 0,
        "cache verify 通过",
        out=(verify.stdout or "")[-300:],
        err=(verify.stderr or "")[-300:],
    )
    ev = report.sync("n07 final")
    report.check(ev["ok"], "drain 无残留冲突", exit_code=ev.get("exit_code"))


if __name__ == "__main__":
    main_guard(run, "n07-cache-concurrency")
