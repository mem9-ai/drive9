"""Run the site acceptance scenarios in order; one JSON per scenario.

usage: python3 run_all.py [scenario.py ...]     (no args = all 12)
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import time

SCENARIOS = [
    "s01_import.py", "s02_save_read.py", "s03_watch.py", "s04_assets.py",
    "s05_conflict.py", "s06_npm.py", "s07_refactor.py", "s08_git.py",
    "s09_build.py", "s10_export.py", "s11_external.py", "s12_recovery.py",
]


def main():
    only = set(sys.argv[1:])
    summary = []
    for script in SCENARIOS:
        if only and script not in only:
            continue
        print("=== START %s (%s) ===" % (script, time.strftime("%H:%M:%S")), flush=True)
        start = time.time()
        try:
            proc = subprocess.run(["python3", script], capture_output=True, text=True, timeout=7200)
            code, out, err = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as expired:
            code, out, err = -1, (expired.stdout or b"").decode() if isinstance(expired.stdout, bytes) else (expired.stdout or ""), "timeout"
        elapsed = time.time() - start
        row = {"scenario": script, "exit": code, "wall_s": round(elapsed, 2),
               "stdout_tail": (out or "")[-1200:], "stderr_tail": (err or "")[-600:]}
        summary.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
    pathlib.Path("results/run_all.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print("RUN_ALL_DONE", flush=True)


if __name__ == "__main__":
    main()
