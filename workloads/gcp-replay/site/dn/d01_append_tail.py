"""d01 追加日志 + 跟随读（tail -f 语义）.

Agent 模式：一个进程以 O_APPEND 持续追加 JSONL（会话日志），另一个进程反复
从偏移量增量读（会话恢复 / tail -f）。校验：不丢行、不重复、不读半行，
最终内容一致。
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard, sha256_file  # noqa: E402
import dn_common as dn  # noqa: E402

ITER = 3
LINES = 600


def expected_bytes(lines=LINES):
    body = (
        "\n".join(
            json.dumps({"seq": i, "pad": "x" * 60}, separators=(",", ":"))
            for i in range(lines)
        )
        + "\n"
    )
    return body.encode()


def one_iter(report, base, it):
    d = base / ("iter%d" % it)
    d.mkdir(parents=True, exist_ok=True)
    log = d / "session.jsonl"
    reader_out = d / "reader.json"
    writer = dn.spawn_worker(
        "append-writer", [log, LINES, 3], log_path=d / "writer.log"
    )
    reader = dn.spawn_worker(
        "follow-reader",
        [log, LINES, reader_out, "--timeout", "90", "--reopen"],
        log_path=d / "reader.log",
    )
    with report.step("iter%d writer+reader" % it):
        dn.wait_proc(writer, timeout=180)
        dn.wait_proc(reader, timeout=180)
    robj = dn.read_json(reader_out)
    report.check(
        writer.returncode == 0, "iter%d writer 正常退出" % it, rc=writer.returncode
    )
    report.check(
        robj["complete"] == LINES, "iter%d 全部 %d 行被读到" % (it, LINES), got=robj
    )
    report.check(
        robj["bad_lines"] == 0 and robj["dups"] == 0,
        "iter%d 无坏行/无重复行" % it,
        got=robj,
    )
    report.check(robj["tail_bytes"] == 0, "iter%d 读完后无残留半行" % it, got=robj)
    report.check(
        sha256_file(log) == hashlib.sha256(expected_bytes()).hexdigest(),
        "iter%d 最终内容哈希一致" % it,
    )


def run(report):
    base = dn.case_dir("d01-append-tail")
    for it in range(1, ITER + 1):
        one_iter(report, base, it)
    report.sync("d01 final")


if __name__ == "__main__":
    main_guard(run, "d01-append-tail")
