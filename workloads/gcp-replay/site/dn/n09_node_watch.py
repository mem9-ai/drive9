"""n09 node --watch 与 fs.watch 跨进程事件."""

from __future__ import annotations

import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

WATCHER_JS = """const fs = require("fs");
const d = process.argv[2];
fs.watch(d, (evt, name) => { console.log("EV " + evt + " " + name); });
setInterval(() => {}, 60000);
"""


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n09-node-watch")
    proj = base / "proj"
    proj.mkdir(parents=True, exist_ok=True)
    app = proj / "app.js"
    app.write_text('console.log("APP V1");\nsetInterval(() => {}, 60000);\n')

    log = base / "watch.log"
    proc = dn.spawn_bg(["node", "--watch", "app.js"], log_path=log, cwd=proj)
    ok = False
    try:
        dn.wait_for(
            lambda: "APP V1" in log.read_text(errors="replace"),
            timeout=60,
            what="node --watch start",
        )
        app.write_text('console.log("APP V2");\nsetInterval(() => {}, 60000);\n')
        dn.wait_for(
            lambda: "APP V2" in log.read_text(errors="replace"),
            timeout=60,
            what="node --watch restart",
        )
        ok = True
    except TimeoutError:
        pass
    finally:
        dn.kill_group(proc)
    dn.record_compat(report, "node_watch_restart", ok)
    report.check(
        ok, "node --watch 检测到修改并重启", log=log.read_text(errors="replace")[-300:]
    )

    watcher_js = proj / "watcher.js"
    watcher_js.write_text(WATCHER_JS)
    wdir = proj / "watched"
    wdir.mkdir(exist_ok=True)
    wlog = base / "fswatch.log"
    wproc = dn.spawn_bg(["node", "watcher.js", str(wdir)], log_path=wlog, cwd=proj)
    seen = False
    try:
        time.sleep(1.5)
        (wdir / "hit.txt").write_text("hit\n")
        deadline = time.time() + 20
        while time.time() < deadline:
            if "EV " in wlog.read_text(errors="replace"):
                seen = True
                break
            time.sleep(0.2)
    finally:
        dn.kill_group(wproc)
    dn.record_compat(report, "fs_watch_cross_process", seen)
    report.check(
        seen, "fs.watch 收到跨进程写入事件", log=wlog.read_text(errors="replace")[-300:]
    )
    report.sync("n09 final")


if __name__ == "__main__":
    main_guard(run, "n09-node-watch")
