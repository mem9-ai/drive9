"""d06 watcher 驱动闭环：写 → 通知 → 响应写 → 再通知.

模拟 agent/dev-server 闭环：对每次检测到的变更写一个 .done 响应文件，
检查是否有遗漏、事件风暴和追加（O_APPEND）的可达性。
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard, stats  # noqa: E402
import dn_common as dn  # noqa: E402

ROUNDS = 20


def load_events(jsonl):
    p = pathlib.Path(jsonl)
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out


def run(report):
    base = dn.case_dir("d06-watcher-loop")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)
    stop = base / "STOP"
    events_path = base / "events.json"
    jsonl = pathlib.Path(str(events_path) + ".jsonl")
    watcher = dn.spawn_worker(
        "watcher",
        [d, events_path, 300, "--stop-file", stop],
        log_path=base / "watcher.log",
    )
    dn.wait_for(
        lambda: jsonl.exists() or events_path.exists(), timeout=30, what="watcher ready"
    )
    if not jsonl.exists():
        dn.kill9(watcher)
        summary = dn.read_json(events_path)
        dn.record_compat(report, "inotify_supported", False)
        report.check(False, "inotify 不可用", got=summary)
        return

    latencies = []
    missed = []
    for k in range(1, ROUNDS + 1):
        name = "round-%02d.txt" % k
        tmp = d / (name + ".tmp")
        t0 = time.time()
        tmp.write_text("payload-%02d\n" % k)
        os.replace(tmp, d / name)
        deadline = t0 + 5
        seen = False
        while time.time() < deadline:
            hits = [
                e
                for e in load_events(jsonl)
                if e.get("name") == name and (e.get("mask", 0) & 0x88)
            ]
            if hits:
                latencies.append(time.time() - t0)
                seen = True
                break
            time.sleep(0.05)
        if not seen:
            missed.append(name)
        (d / ("%s.done" % name)).write_text("done\n")

    log = d / "session.log"
    log.write_text("")
    for i in range(10):
        with open(log, "a") as fh:
            fh.write("line-%02d\n" % i)
            fh.flush()
        time.sleep(0.05)
    time.sleep(1.0)
    stop.write_text("stop")
    dn.wait_proc(watcher, timeout=120)

    events = load_events(jsonl)
    summary = dn.read_json(events_path)
    report.data["inotify_supported"] = bool(summary.get("supported"))
    report.check(not missed, "每一轮的最终写入都被通知发现", missed=missed)
    if latencies:
        report.data.setdefault("metrics", {})["notify_latency_s"] = stats(latencies)
    total = len(events)
    report.data.setdefault("metrics", {})["inotify_event_count"] = total
    report.data["inotify_event_count"] = total
    report.check(total < 1000, "无事件风暴（事件数有界）", count=total)
    appends = [e for e in events if e.get("name") == "session.log"]
    report.data.setdefault("metrics", {})["append_events"] = len(appends)
    report.sync("d06 final")


if __name__ == "__main__":
    main_guard(run, "d06-watcher-loop")
