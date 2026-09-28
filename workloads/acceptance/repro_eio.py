"""Focused reproducer: EIO on os.replace after write+fsync (from S2).

usage: python3 repro_eio.py <workdir> [iterations] [--same-tmp]
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

A = b"A" * 4096 + b"||END-A||"
B = b"B" * 7000 + b"||END-B||"
VERSIONS = (A, B)


def main():
    root = pathlib.Path(sys.argv[1])
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    same_tmp = "--same-tmp" in sys.argv
    if root.exists():
        import shutil
        shutil.rmtree(root)
    root.mkdir(parents=True)
    target = root / "app.js"

    write_errors, replace_errors = [], []
    started = time.time()
    for i in range(iterations):
        tmp = root / ("app.js.tmp" if same_tmp else "app.js.tmp%d" % (i % 2))
        data = VERSIONS[i % 2]
        try:
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as err:
            write_errors.append({"iter": i, "stage": "write", "errno": err.errno, "msg": str(err)})
            if len(write_errors) >= 3:
                break
        try:
            os.replace(tmp, target)
        except OSError as err:
            replace_errors.append({"iter": i, "stage": "replace", "errno": err.errno, "msg": str(err)})
            if len(replace_errors) >= 3:
                break
    elapsed = time.time() - started
    result = {
        "iterations": i + 1,
        "elapsed_s": round(elapsed, 3),
        "same_tmp": same_tmp,
        "write_errors": write_errors,
        "replace_errors": replace_errors,
    }
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if not write_errors and not replace_errors else 1)


if __name__ == "__main__":
    main()
