"""Focused check: is an atomic replace immediately visible to a re-open?

Sequence: write target=A, stage tmp=B, os.replace(tmp, target), then re-open
target and report which version is observed. Repeats with a delay and via the
API to separate "rename not visible" from "read cache stale".

usage: python3 repro_replace_visibility.py <workdir>
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import sys
import time

A = b"A" * 4096 + b"||END-A||"
B = b"B" * 7000 + b"||END-B||"


def version_of(data):
    if data == A:
        return "A"
    if data == B:
        return "B"
    return "mixed(%d)" % len(data)


def main():
    root = pathlib.Path(sys.argv[1])
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    target = root / "app.js"
    tmp = root / "app.js.tmp"
    results = []

    for round_no in range(6):
        # reset to A
        with open(target, "wb") as fh:
            fh.write(A)
            fh.flush()
            os.fsync(fh.fileno())
        time.sleep(0.2)
        before = version_of(open(target, "rb").read())

        # stage B and replace
        with open(tmp, "wb") as fh:
            fh.write(B)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)

        immediate = version_of(open(target, "rb").read())
        time.sleep(0.5)
        after_500ms = version_of(open(target, "rb").read())
        time.sleep(3)
        after_3s = version_of(open(target, "rb").read())

        results.append({"round": round_no, "before": before, "immediate": immediate,
                        "after_500ms": after_500ms, "after_3s": after_3s})

    # does an explicit drain change the picture?
    from common import drain, MOUNT, api_cat, remote_of
    view_before_drain = version_of(open(target, "rb").read())
    sync = drain(MOUNT)
    view_after_drain = version_of(open(target, "rb").read())
    api_view = version_of(api_cat(remote_of(target)))

    summary = {
        "results": results,
        "stale_immediate_count": sum(1 for r in results if r["immediate"] != "B"),
        "stale_after_500ms": sum(1 for r in results if r["after_500ms"] != "B"),
        "stale_after_3s": sum(1 for r in results if r["after_3s"] != "B"),
        "view_before_drain": view_before_drain,
        "drain_ok": sync["ok"],
        "view_after_drain": view_after_drain,
        "api_view": api_view,
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
