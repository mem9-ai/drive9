"""d04 多 agent 并发编辑同一仓库 + 一个进程持续扫描.

atomic 阶段：3 个 worker 各自用 temp+fsync+rename 反复替换自己的文件，
扫描进程必须永远读到合法完整的 JSON（原子替换不得暴露半成品）。
mixed 阶段：额外加入原地重写、创建、删除，扫描允许约定的竞态（消失），
最终状态在 drain 后核对。
"""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

WORKERS = 3
ROUNDS = 10


def phase(report, base, name, mode):
    d = base / name
    d.mkdir(parents=True, exist_ok=True)
    stop = d / "STOP"
    scanner_out = d / "scanner.json"
    scanner = dn.spawn_worker(
        "scanner",
        [d, scanner_out, 240, "--stop-file", stop],
        log_path=d / "scanner.log",
    )
    workers = [
        dn.spawn_worker(
            "editor", [d, "w%d" % i, mode, ROUNDS], log_path=d / ("editor-w%d.log" % i)
        )
        for i in range(WORKERS)
    ]
    for i, w in enumerate(workers):
        rc = dn.wait_proc(w, timeout=300)
        report.check(rc == 0, "%s editor-w%d 正常退出" % (name, i), rc=rc)
    stop.write_text("stop")
    dn.wait_proc(scanner, timeout=120)
    sc = dn.read_json(scanner_out)
    report.check(
        sc["bad_count"] == 0,
        "%s 扫描到原子文件始终是完整合法内容" % name,
        bad_count=sc["bad_count"],
        bad=sc["bad"][:5],
    )
    report.check(
        sc["error_count"] == 0,
        "%s 扫描无错误" % name,
        error_count=sc["error_count"],
        errors=sc["errors"][:5],
    )
    report.data.setdefault("metrics", {})[name] = {
        "scans": len(sc["scans"]),
        "vanished": sc["vanished"],
        "files_last": sc["scans"][-1]["files"] if sc["scans"] else 0,
    }
    report.sync("%s drain" % name)
    for i in range(WORKERS):
        p = d / ("atomic-w%d.dat" % i)
        if report.check(p.exists(), "%s atomic-w%d 存在" % (name, i)):
            obj = json.loads(p.read_text())
            report.check(
                obj.get("file") == p.name,
                "%s atomic-w%d 终态内容合法" % (name, i),
                got=obj,
            )
    leftovers = [x.name for x in d.glob("tmp-*")]
    report.check(not leftovers, "%s 无 tmp 残留" % name, leftovers=leftovers[:5])


def run(report):
    base = dn.case_dir("d04-multi-agent")
    phase(report, base, "atomic", "atomic")
    phase(report, base, "mixed", "mixed")


if __name__ == "__main__":
    main_guard(run, "d04-multi-agent")
