"""d05 锁语义：O_EXCL 争用（硬性）+ flock / fcntl（支持性记录）."""

from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402

ROUNDS = 20


def read_state(path):
    try:
        return json.loads(pathlib.Path(path).read_text()).get("state")
    except Exception:
        return None


def lock_scenario(report, base, kind):
    lk = base / ("%s.lock" % kind)
    holder_out = base / ("%s-holder.json" % kind)
    holder = dn.spawn_worker(
        "lock-holder",
        [lk, kind, 2.0, holder_out],
        log_path=base / ("%s-holder.log" % kind),
    )
    try:
        dn.wait_for(
            lambda: read_state(holder_out) == "holding",
            timeout=30,
            what=kind + " holder",
        )
    except TimeoutError:
        dn.kill9(holder)
        dn.record_compat(report, kind + "_supported", False)
        return
    t1 = base / ("%s-try1.json" % kind)
    dn.wait_proc(
        dn.spawn_worker(
            "lock-try", [lk, kind, t1], log_path=base / ("%s-try1.log" % kind)
        ),
        timeout=30,
    )
    dn.wait_proc(holder, timeout=30)
    dn.wait_for(
        lambda: read_state(holder_out) == "released", timeout=20, what=kind + " release"
    )
    t2 = base / ("%s-try2.json" % kind)
    dn.wait_proc(
        dn.spawn_worker(
            "lock-try", [lk, kind, t2], log_path=base / ("%s-try2.log" % kind)
        ),
        timeout=30,
    )
    row1 = dn.read_json(t1)
    row2 = dn.read_json(t2)
    dn.record_compat(report, kind + "_supported", True)
    dn.record_compat(report, kind + "_while_held", row1)
    dn.record_compat(report, kind + "_after_release", row2)
    report.check(row2.get("acquired") is True, "%s 释放后可获取" % kind, got=row2)


def run(report):
    base = dn.case_dir("d05-lock-semantics")
    bad = []
    for r in range(ROUNDS):
        d = base / ("excl%02d" % r)
        d.mkdir(parents=True, exist_ok=True)
        lock = d / "app.lock"
        outs = [d / "r0.json", d / "r1.json"]
        procs = [
            dn.spawn_worker("excl-racer", [lock, outs[i]], log_path=d / ("r%d.log" % i))
            for i in range(2)
        ]
        for p in procs:
            dn.wait_proc(p, timeout=60)
        rows = [dn.read_json(o) for o in outs]
        won = [x for x in rows if x.get("won")]
        lost = [x for x in rows if not x.get("won")]
        if not (len(won) == 1 and len(lost) == 1 and lost[0].get("errno") == 17):
            bad.append({"round": r, "rows": rows})
    report.check(
        len(bad) == 0,
        "O_EXCL 争用 %d 轮均只有一个赢家（输家 EEXIST）" % ROUNDS,
        bad=bad[:3],
    )

    lk = base / "reuse-excl.lock"
    fd = os.open(lk, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.close(fd)
    os.unlink(lk)
    fd = os.open(lk, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.close(fd)
    os.unlink(lk)
    report.check(True, "O_EXCL 删除后可重新获取")

    lock_scenario(report, base, "flock")
    lock_scenario(report, base, "fcntl")
    report.sync("d05 final")


if __name__ == "__main__":
    main_guard(run, "d05-lock-semantics")
