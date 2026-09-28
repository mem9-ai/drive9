"""d02 写入进程被 kill -9 后：内容前缀、O_APPEND 续写、截断重写、目录 fsync."""

from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import det_bytes, main_guard, sha256_bytes, sha256_file  # noqa: E402
import dn_common as dn  # noqa: E402

ITER = 2
KILL_AT = 2 * 1024 * 1024
CHUNK = 64 * 1024


def expected_prefix(size):
    out = bytearray()
    k = 0
    while len(out) < size:
        out += (("%08d" % k).encode() * (CHUNK // 8 + 1))[:CHUNK]
        k += 1
    return bytes(out[:size])


def run(report):
    base = dn.case_dir("d02-kill-resume")
    for it in range(1, ITER + 1):
        d = base / ("iter%d" % it)
        d.mkdir(parents=True, exist_ok=True)
        target = d / "partial.bin"
        writer = dn.spawn_worker(
            "chunk-writer",
            [target, 8, 64, "--sleep-ms", 100],
            log_path=d / "writer.log",
        )
        with report.step("iter%d 部分写入后 kill -9" % it):
            dn.wait_for(
                lambda: target.exists() and target.stat().st_size >= KILL_AT,
                timeout=120,
                what="partial write",
            )
            size_at_kill = target.stat().st_size
            dn.kill9(writer)
        size_after = target.stat().st_size
        data = target.read_bytes()
        report.check(
            data == expected_prefix(len(data)),
            "iter%d 被杀后内容是完整块前缀" % it,
            size_at_kill=size_at_kill,
            size_after=size_after,
        )
        with open(target, "ab") as fh:
            fh.write(b"TAIL-1\n")
            fh.flush()
            os.fsync(fh.fileno())
        report.check(
            target.read_bytes()[-7:] == b"TAIL-1\n", "iter%d O_APPEND 续写正确" % it
        )
        payload = det_bytes(42, 1 << 20)
        with open(target, "r+b") as fh:
            fh.truncate(0)
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        report.check(
            sha256_file(target) == sha256_bytes(payload), "iter%d 截断重写一致" % it
        )
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
            dir_result = True
        except OSError as err:
            dir_result = "errno=%s %s" % (err.errno, err.strerror)
        finally:
            os.close(dfd)
        dn.record_compat(report, "dir_fsync_iter%d" % it, dir_result)
        report.check(True, "iter%d 目录 fsync 结果已记录" % it, result=dir_result)
    report.sync("d02 final")


if __name__ == "__main__":
    main_guard(run, "d02-kill-resume")
