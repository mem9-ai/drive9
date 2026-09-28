"""Sequential, untimed concurrent-reader atomic replacement checks."""

import fcntl
import hashlib
import multiprocessing as mp
import os
import pathlib
import time

from cases import payload, write_file
from run_matrix import BASE, GROUPS, LOCK_PATH, MOUNTS, REMOTE_ROOT, RUN_PREFIX, drain, request, save


def reader(target, allowed, ready, stop, result):
    samples = 0
    error = None
    try:
        while not stop.is_set():
            value = target.read_bytes()
            if value not in allowed:
                raise AssertionError("reader saw incomplete or unknown generation: bytes=" + str(len(value)))
            samples += 1
            ready.set()
    except Exception as exc:
        error = repr(exc)
        ready.set()
    result.put({"samples": samples, "error": error})


def main():
    result_file = BASE / "atomic-results.json"
    assert not result_file.exists(), "Preserve previous atomic proof"
    rows = []
    with LOCK_PATH.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for group in GROUPS:
            drain(group)
            root = MOUNTS[group] / RUN_PREFIX / "atomic-proof"
            root.mkdir(parents=True, exist_ok=False)
            target = root / "target.dat"
            generations = [payload(4096, i) for i in range(11)]
            write_file(target, generations[0])
            drain(group)
            ctx = mp.get_context("fork")
            ready, stop, result = ctx.Event(), ctx.Event(), ctx.Queue()
            observer = ctx.Process(target=reader, args=(target, set(generations), ready, stop, result))
            observer.start()
            try:
                assert ready.wait(30), "reader did not become ready"
                for i in range(1, 11):
                    staged = root / f"staged-{i}.new"
                    write_file(staged, generations[i])
                    os.replace(staged, target)
                stop.set()
                read_result = result.get(timeout=30)
                observer.join(timeout=10)
                assert observer.exitcode == 0 and read_result["error"] is None and read_result["samples"] > 0, read_result
                assert target.read_bytes() == generations[-1]
                drained = drain(group)
                if group in GROUPS[:4]:
                    remote_data = request(group, f"{REMOTE_ROOT}/{RUN_PREFIX}/atomic-proof/target.dat")
                    assert remote_data == generations[-1]
                rows.append({"group": group, "passed": True, "replacements": 10, **read_result})
                save(result_file, {"complete": len(rows) == 6, "rows": rows})
                print("ATOMIC PASS " + group + " samples=" + str(read_result["samples"]), flush=True)
            finally:
                stop.set()
                if observer.is_alive():
                    observer.terminate(); observer.join(timeout=5)


if __name__ == "__main__":
    main()
