"""d07 写完立即执行脚本：写（含 temp+rename 变体）→ chmod → exec."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard, stats  # noqa: E402
import dn_common as dn  # noqa: E402

ROUNDS = 50


def run(report):
    base = dn.case_dir("d07-exec-after-write")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)
    bad = []
    latencies = []
    for k in range(1, ROUNDS + 1):
        name = "run-%02d.sh" % k
        target = d / name
        body = "#!/bin/sh\necho ok-%02d\n" % k
        if k % 2:
            target.write_text(body)
        else:
            tmp = d / (name + ".tmp")
            tmp.write_text(body)
            os.replace(tmp, target)
        os.chmod(target, 0o755)
        if not (os.stat(target).st_mode & 0o100):
            bad.append("%s: exec bit missing" % name)
        if target.read_text() != body:
            bad.append("%s: content mismatch" % name)
        t0 = time.time()
        proc = subprocess.run([str(target)], capture_output=True, text=True, timeout=60)
        latencies.append(time.time() - t0)
        if proc.returncode != 0 or proc.stdout.strip() != "ok-%02d" % k:
            bad.append(
                "%s: rc=%s out=%r err=%r"
                % (name, proc.returncode, proc.stdout[:60], proc.stderr[-120:])
            )
    report.check(not bad, "%d 轮写完立即执行全部成功" % ROUNDS, bad=bad[:5])
    report.data.setdefault("metrics", {})["exec_latency_s"] = stats(latencies)
    report.sync("d07 final")


if __name__ == "__main__":
    main_guard(run, "d07-exec-after-write")
