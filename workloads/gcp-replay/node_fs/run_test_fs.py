"""Run each pinned official test-fs-* case with its temporary tree on Drive9."""

import argparse
import csv
import fcntl
import hashlib
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time

from prepare_source import MANIFEST, source_root, verify


def test_id(source_path):
    path = pathlib.PurePosixPath(source_path)
    return str(path.relative_to("test").with_suffix(""))


def drain(binary, mount):
    started = time.perf_counter()
    proc = subprocess.run(
        [binary, "mount", "drain", "--timeout", "120s", "--json", str(mount)],
        capture_output=True,
        text=True,
        timeout=150,
    )
    result = json.loads(proc.stdout or "{}")
    return dict(
        ok=proc.returncode == 0 and result.get("ok") is True,
        wall_s=time.perf_counter() - started,
        result=result,
    )


def run_test(command, source):
    started = time.perf_counter()
    proc = subprocess.Popen(
        command,
        cwd=source,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=1200)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        stdout, stderr = proc.communicate()
        return -1, stdout + stderr, time.perf_counter() - started
    # Node's official runner can exit while a test-spawned writer continues.
    # Stop remaining members of this test's private process group before drain.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    else:
        for _ in range(20):
            try:
                os.killpg(proc.pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.1)
        else:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    return proc.returncode, stdout + stderr, time.perf_counter() - started


def summarize(rows, path):
    payload = {
        "node_version": MANIFEST["node_version"],
        "source_sha256": MANIFEST["source_sha256"],
        "expected_tests": len(MANIFEST["tests"]),
        "rows": rows,
    }
    (path / "summary.json").write_text(json.dumps(payload, indent=2) + "\n")
    with (path / "durations.csv").open("w", newline="") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=(
                "test",
                "status",
                "node_duration_ms",
                "wall_s",
                "drain_after_s",
                "wall_plus_drain_s",
            ),
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in writer.fieldnames})


def parse_tap(output, exit_code):
    statuses = re.findall(r"(?m)^(ok|not ok) \d+ .*$", output)
    skipped = "No tests to run." in output or bool(
        re.search(r"(?im)^ok \d+ .*# skip", output)
    )
    durations = [
        float(value)
        for value in re.findall(r"(?m)^\s*duration_ms:\s*([0-9.]+)", output)
    ]
    status = (
        "skip"
        if skipped and (not statuses or all(item == "ok" for item in statuses))
        else "pass"
        if exit_code == 0 and statuses and all(item == "ok" for item in statuses)
        else "fail"
    )
    return status, sum(durations) if durations else None, len(statuses)


def run(args):
    if sys.platform != "linux":
        raise RuntimeError("Official Node filesystem tests require Linux FUSE")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "site"))
    from case_guard import boundary

    node = pathlib.Path(os.environ["D9_NODE_BIN"]) / "node"
    version = subprocess.check_output([str(node), "--version"], text=True).strip()
    assert version == MANIFEST["node_version"], version
    source = source_root(pathlib.Path(os.environ["D9_STATE"]).expanduser())
    verify(source)
    binary = os.environ["D9_BIN"]
    mount = pathlib.Path(os.environ["D9_MOUNT"])
    if not os.path.ismount(mount):
        raise RuntimeError("Expected Drive9 mount is absent: " + str(mount))
    run_id = os.environ["D9_RUN_ID"]
    results = pathlib.Path(os.environ["D9_RESULTS"]) / "node-fs"
    # Node's Unix-socket tests have a short sockaddr_un path limit on Linux.
    # Keep this path short; long descriptive names belong in result filenames.
    work = mount / ".node-fs" / hashlib.sha256(run_id.encode()).hexdigest()[:8]
    lock_path = pathlib.Path(os.environ["D9_REPLAY_LOCK"])
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    selected = [
        item
        for item in MANIFEST["tests"]
        if not args.only or test_id(item) in args.only
    ]
    if len(selected) != len(args.only) and args.only:
        raise ValueError("Unknown or duplicate --only value")
    indexes = {test_id(item): index for index, item in enumerate(MANIFEST["tests"])}
    rows = []
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        results.mkdir(parents=True, exist_ok=False)
        work.mkdir(parents=True, exist_ok=False)
        for item in selected:
            name = test_id(item)
            label = name.replace("/", "-")
            boundary(mount, results / (label + "-boundary-before.json"))
            before = drain(binary, mount)
            if not before["ok"]:
                raise RuntimeError("Dirty mount before " + name)
            tempdir = work / f"{indexes[name]:03d}"
            tempdir.mkdir()
            command = [
                sys.executable,
                str(source / "tools" / "test.py"),
                "--shell",
                str(node),
                "--mode=release",
                "--progress=tap",
                "--temp-dir",
                str(tempdir),
                "-j1",
                name,
            ]
            code, output, wall_s = run_test(command, source)
            (results / (label + ".tap")).write_text(output)
            after = drain(binary, mount)
            status, node_duration_ms, selected_subtests = parse_tap(output, code)
            row = dict(
                test=name,
                source_file=item,
                node_test_dir=str(tempdir),
                configured_durability=os.environ.get("D9_DURABILITY", "auto"),
                actual_sync_mode=os.environ.get("D9_ACTUAL_SYNC_MODE", "unknown"),
                status=status,
                exit_code=code,
                node_duration_ms=node_duration_ms,
                selected_subtests=selected_subtests,
                wall_s=wall_s,
                drain_before=before,
                drain_after=after,
                drain_after_s=after["wall_s"],
                wall_plus_drain_s=wall_s + after["wall_s"],
            )
            rows.append(row)
            (results / (label + ".json")).write_text(json.dumps(row, indent=2) + "\n")
            summarize(rows, results)
            print(name, status.upper(), flush=True)
            boundary(mount, results / (label + "-boundary-after.json"))
            if not after["ok"]:
                raise RuntimeError("Unresolved drain after " + name)
    return (
        0
        if len(rows) == len(selected) and all(r["status"] != "fail" for r in rows)
        else 1
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--only", action="append", default=[], metavar="SUITE/TEST")
    args = parser.parse_args()
    if args.list:
        print(json.dumps([test_id(item) for item in MANIFEST["tests"]], indent=2))
        return
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
