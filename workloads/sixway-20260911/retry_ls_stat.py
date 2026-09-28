"""Sequential diagnostic retries; preserve original case and original failure."""

import fcntl
import json
import os
import pathlib
import stat
import subprocess
import time
import traceback

from cases import Clock, payload, write_file
from run_matrix import BASE, BIN, MOUNTS, drain, save

GROUP = "none-b"
RUN = "ls-stat-retry-20260911-01"
OUT = BASE / RUN


def snapshot(root, paths):
    names = os.listdir(root)
    attrs = []
    for path in paths:
        try:
            info = os.stat(path)
            attrs.append({"name": path.name, "size": info.st_size, "mode": info.st_mode,
                          "regular": stat.S_ISREG(info.st_mode)})
        except OSError as error:
            attrs.append({"name": path.name, "errno": error.errno})
    return {"names": names, "attrs": attrs}


def main():
    with (BASE / "coordinator.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert not OUT.exists(), "A retry run already exists; inspect instead of overwriting"
        OUT.mkdir()
        metadata = json.loads((BASE / "mounts.json").read_text())
        mount = next(row for row in metadata if row["group"] == GROUP)
        assert mount["profile"] == "none" and mount["durability"] == "interactive"
        assert "788ec769866185c6c3d694206172d8f5009781e7" in subprocess.check_output([f"/proc/{mount['pid']}/exe", "version"], text=True)
        save(OUT / "mount.json", mount)
        content = payload(4096)
        rows = []
        for attempt in range(1, 21):
            before = drain(GROUP)
            root = MOUNTS[GROUP] / "sixway-20260911" / f"{RUN}-r{attempt:02d}"
            root.mkdir(mode=0o755, parents=True, exist_ok=False)
            paths = [root / f"file-{i:04d}.dat" for i in range(4)]
            clock = Clock()
            started = time.time()
            row = {"attempt": attempt, "files": 4, "size_bytes": 4096, "root": str(root),
                   "group": GROUP, "start_epoch": started, "drain_before": before}
            try:
                # Exact original operation sequence. No extra observation before
                # the initial list and all four stat calls finish.
                clock.call("create_fsync", lambda: [write_file(path, content) for path in paths])
                names = clock.call("list", lambda: os.listdir(root))
                attrs = clock.call("stat", lambda: [os.stat(path) for path in paths])
                initial = {"names": names, "attrs": [
                    {"name": path.name, "size": info.st_size, "mode": info.st_mode,
                     "regular": stat.S_ISREG(info.st_mode)} for path, info in zip(paths, attrs)]}
                expected = {path.name for path in paths}
                row.update(initial=initial, missing_names=sorted(expected - set(names)),
                           unexpected_names=sorted(set(names) - expected),
                           stat_mismatches=[item for item in initial["attrs"] if item["size"] != 4096 or not item["regular"]])
                row["passed"] = set(names) == expected and not row["stat_mismatches"]
            except Exception as error:
                row.update(passed=False, error_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
            row.update(initial_end_epoch=time.time(), phases_s=clock.phases)
            save(OUT / f"r{attempt:02d}.json", row)
            # Observe recovery only after preserving the actual initial values.
            row["drain_after"] = drain(GROUP)
            row["after_drain"] = snapshot(root, paths)
            row["after_drain_ok"] = (set(row["after_drain"]["names"]) == {p.name for p in paths}
                                     and all(item.get("size") == 4096 and item.get("regular") for item in row["after_drain"]["attrs"]))
            save(OUT / f"r{attempt:02d}.json", row)
            rows.append(row)
            summary = {"run": RUN, "group": GROUP, "attempted": len(rows),
                       "passed": sum(r["passed"] for r in rows), "failed": sum(not r["passed"] for r in rows),
                       "after_drain_failed": sum(not r["after_drain_ok"] for r in rows), "complete": attempt == 20,
                       "failure_attempts": [r["attempt"] for r in rows if not r["passed"]]}
            save(OUT / "summary.json", summary)
            print(json.dumps({k: row.get(k) for k in ("attempt", "passed", "missing_names", "unexpected_names", "stat_mismatches", "after_drain_ok", "error")}), flush=True)
        print("SUMMARY " + json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
