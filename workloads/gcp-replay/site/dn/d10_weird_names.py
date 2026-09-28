"""d10 名称边界：Unicode / 空格 / 引号 / 长名 / 大小写."""

from __future__ import annotations

import os
import pathlib
import sys
import unicodedata

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

NAMES = [
    "中文-文件.txt",
    "emoji-🚀-文件.txt",
    "space name.txt",
    "quote'\"name.txt",
    "hash#name.txt",
    "dollar$name.txt",
    "semi;colon.txt",
    "-leading-dash.txt",
    "trailing-space .txt",
    "dot.in.name.txt",
    "UPPER.txt",
    "upper.txt",
    "café-nfc.txt",
    unicodedata.normalize("NFD", "café") + "-nfd.txt",
    "长" * 60 + ".txt",
]


def run(report):
    base = dn.case_dir("d10-weird-names")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)
    failures = []
    for name in NAMES:
        p = d / name
        body = ("content:" + name + "\n").encode()
        try:
            p.write_bytes(body)
            if p.read_bytes() != body:
                failures.append("%r: content mismatch" % name)
            if name not in os.listdir(d):
                failures.append("%r: not in listdir" % name)
            if p.stat().st_size != len(body):
                failures.append("%r: size mismatch" % name)
            renamed = d / (name + ".renamed")
            os.replace(p, renamed)
            if renamed.read_bytes() != body:
                failures.append("%r: renamed content mismatch" % name)
            os.unlink(renamed)
        except OSError as err:
            failures.append("%r: %s" % (name, err))
    report.check(
        not failures,
        "全部 %d 个边界名称完成 创建/列出/读取/改名/删除" % len(NAMES),
        failures=failures[:8],
    )

    over = d / ("x" * 300 + ".txt")
    try:
        over.write_text("over\n")
        dn.record_compat(report, "overlong_name_rejected", False)
        report.check(False, "超长文件名（300 字节）应被拒绝")
    except OSError as err:
        import errno as _errno

        dn.record_compat(report, "overlong_name_errno", err.errno)
        report.check(
            err.errno == _errno.ENAMETOOLONG,
            "超长文件名返回 ENAMETOOLONG",
            errno=err.errno,
            error=err.strerror,
        )
    ev = report.sync("d10 final")
    report.check(ev["ok"], "drain 无残留冲突", exit_code=ev.get("exit_code"))


if __name__ == "__main__":
    main_guard(run, "d10-weird-names")
