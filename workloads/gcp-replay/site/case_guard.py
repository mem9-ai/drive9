"""Check Linux case boundaries and stop descendants of a finished case."""

import json
import os
from pathlib import Path
import signal
import subprocess
import time


def boundary(mount, output):
    expected = str(Path(mount).resolve()) if mount else None
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        if right.split()[0] == "fuse.drive9":
            mounts.append(left.split()[4])
    process_list = subprocess.check_output(
        ["ps", "-eo", "pid,ppid,stat,args"], text=True
    )
    zombies = []
    for line in process_list.splitlines()[1:]:
        columns = line.split(None, 3)
        if len(columns) < 4 or "Z" not in columns[2]:
            continue
        pid, parent = map(int, columns[:2])
        if parent == os.getpid():
            try:
                os.waitpid(pid, os.WNOHANG)
                continue
            except ChildProcessError:
                pass
        zombies.append(line.strip())
    rules = []
    ips = os.environ["D9_ENDPOINT_IPS"].split(",")
    if not any(ip.strip() for ip in ips):
        raise ValueError("Set D9_ENDPOINT_IPS before checking firewall boundaries")
    for chain in ("INPUT", "OUTPUT"):
        result = subprocess.run(
            ["sudo", "-n", "iptables", "-S", chain],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        rules += [
            line
            for line in result.stdout.splitlines()
            if "DROP" in line and any(ip.strip() in line for ip in ips if ip.strip())
        ]
    expected_mounts = [expected] if expected else []
    row = dict(
        epoch=time.time(),
        mounts=mounts,
        expected_mounts=expected_mounts,
        zombies=zombies,
        endpoint_block_rules=rules,
    )
    row["ok"] = sorted(mounts) == expected_mounts and not zombies and not rules
    Path(output).write_text(json.dumps(row, indent=2) + "\n")
    if not row["ok"]:
        raise RuntimeError("Case boundary is not clean; inspect " + str(output))
    return row


def run_case(argv, timeout, **kwargs):
    # Become the reaper for grandchildren left by a case on Linux. Mount
    # processes run in separate groups and are not signalled by this helper.
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "Cannot enable case child reaping")
    started = time.perf_counter()
    proc = subprocess.Popen(argv, start_new_session=True, **kwargs)
    try:
        code = proc.wait(timeout=timeout)
        elapsed = time.perf_counter() - started
    finally:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                break
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    pid, _ = os.waitpid(-proc.pid, os.WNOHANG)
                except ChildProcessError:
                    break
                if pid == 0:
                    time.sleep(0.05)
        proc.wait(timeout=10)
    return subprocess.CompletedProcess(argv, code), elapsed
