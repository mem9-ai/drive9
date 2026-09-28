"""d08 临时文件风暴 + 幽灵条目检查 + 句柄稳定性."""

from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import api_stat, main_guard, remote_of  # noqa: E402
import dn_common as dn  # noqa: E402

COUNT = 500


def run(report):
    base = dn.case_dir("d08-temp-storm")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)
    stop1 = base / "STOP1"
    checker1_out = base / "checker-create.json"
    checker1 = dn.spawn_worker(
        "ghost-checker",
        [d, checker1_out, 600, "--stop-file", stop1, "--prefix", "file-"],
        log_path=base / "checker-create.log",
    )

    fds_before = len(os.listdir("/proc/self/fd"))
    created = []
    with report.step("create %d via temp+rename" % COUNT):
        for i in range(COUNT):
            tmp = d / (".tmp-%04d" % i)
            final = d / ("file-%04d.dat" % i)
            with open(tmp, "wb") as fh:
                fh.write(b"z" * 1024)
                fh.flush()
            os.replace(tmp, final)
            created.append(final.name)
    last = d / created[-1]
    report.check(last.exists() and last.stat().st_size == 1024, "重命名后的文件可读")

    stop1.write_text("stop")
    dn.wait_proc(checker1, timeout=120)
    chk = dn.read_json(checker1_out)
    report.check(chk["vanished"] == 0, "创建阶段无幽灵条目（列出即可打开）", got=chk)
    report.check(chk["error_count"] == 0, "检查进程无错误", got=chk["errors"][:5])
    report.data.setdefault("metrics", {})["checker_max_entries"] = chk["max_entries"]

    with report.step("delete all"):
        for name in created:
            os.unlink(d / name)

    report.sync("d08 final")
    leftovers = [x.name for x in d.iterdir()]
    report.check(not leftovers, "删除后目录为空", leftovers=leftovers[:10])
    remote = remote_of(last)
    report.check(api_stat(remote) is None, "独立入口确认文件已删除", remote=remote)

    checker2_out = base / "checker-after.json"
    checker2 = dn.spawn_worker(
        "ghost-checker",
        [d, checker2_out, 6, "--prefix", "file-"],
        log_path=base / "checker-after.log",
    )
    dn.wait_proc(checker2, timeout=60)
    chk2 = dn.read_json(checker2_out)
    report.check(
        chk2["max_entries"] == 0 and chk2["vanished"] == 0, "删除后无条目复现", got=chk2
    )

    fds_after = len(os.listdir("/proc/self/fd"))
    report.check(
        abs(fds_after - fds_before) <= 4,
        "无文件句柄泄漏",
        before=fds_before,
        after=fds_after,
    )


if __name__ == "__main__":
    main_guard(run, "d08-temp-storm")
