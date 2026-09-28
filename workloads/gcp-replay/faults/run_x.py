"""Run five GCP-local Drive9 fault categories with independent FUSE mounts."""

import argparse
import fcntl
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

CASES = {
    "x1": "hang_end_reset",
    "x2": "reset_after_delay",
    "x3": "injected_429",
    "x4": "reset_during_mount",
    "x5": "upstream_success_response_dropped",
}


def command(argv, timeout=180):
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def drain(binary, mount):
    started = time.perf_counter()
    proc = command(
        [binary, "mount", "drain", "--timeout", "120s", "--json", str(mount)], 150
    )
    try:
        result = json.loads(proc.stdout)
    except json.JSONDecodeError:
        result = {"stderr": proc.stderr[-700:], "stdout": proc.stdout[-700:]}
    return dict(
        ok=proc.returncode == 0 and result.get("ok") is True,
        wall_s=time.perf_counter() - started,
        result=result,
    )


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_proxy(port, proc):
    url = f"http://127.0.0.1:{port}/__fault_proxy_healthz"
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError("fault proxy exited before readiness")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise RuntimeError("fault proxy readiness timeout")


def mount_client(binary, server, remote, mount, cache, log):
    cache.mkdir(parents=True, exist_ok=False)
    argv = [
        binary,
        "mount",
        "--foreground",
        "--mode=fuse",
        "--server",
        server,
        "--profile",
        os.environ["D9_PROFILE"],
        "--durability",
        os.environ["D9_DURABILITY"],
        "--cache-dir",
        str(cache),
        "--allow-other",
        "--gvisor-compat=false",
        "--dir-ttl",
        "30s",
        "--attr-ttl",
        "30s",
        "--entry-ttl",
        "30s",
        ":" + remote,
        str(mount),
    ]
    if os.environ["D9_PROFILE"] == "coding-agent":
        argv[2:2] = ["--local-root", str(cache.parent / (cache.name + "-overlay"))]
    (log.parent / (log.stem + "-command.json")).write_text(json.dumps(argv) + "\n")
    output = log.open("w")
    proc = subprocess.Popen(
        argv,
        stdout=output,
        stderr=output,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    output.close()
    threading.Thread(target=proc.wait, daemon=True).start()
    for _ in range(120):
        if os.path.ismount(mount):
            return proc
        if proc.poll() is not None:
            return proc
        time.sleep(0.5)
    raise RuntimeError("mount readiness timeout: " + str(log))


def write_fsync(path, data):
    started = time.perf_counter()
    try:
        with path.open("wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        return dict(ok=True, duration_s=time.perf_counter() - started)
    except OSError as error:
        return dict(
            ok=False,
            duration_s=time.perf_counter() - started,
            errno=error.errno,
            error=str(error),
        )


def remote_bytes(binary, remote):
    proc = subprocess.run(
        [binary, "fs", "cat", ":" + remote], capture_output=True, timeout=90
    )
    return proc.stdout if proc.returncode == 0 else None


def read_events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def run_case(case, args):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "site"))
    from case_guard import boundary

    case_started = time.perf_counter()
    binary = os.environ["D9_BIN"]
    upstream = os.environ["D9_SERVER"]
    os.environ["DRIVE9_SERVER"] = upstream
    base = pathlib.Path(os.environ["D9_RESULTS"]) / "faults" / case
    base.mkdir(parents=True, exist_ok=False)
    boundary(None, base / "boundary-before.json")
    remote = (
        os.environ["D9_REMOTE_ROOT"].rstrip("/") + "/faults/" + args.run_id + "/" + case
    )
    created = command([binary, "fs", "mkdir", ":" + remote])
    if created.returncode:
        raise RuntimeError("remote mkdir failed: " + created.stderr[-500:])
    mount = base / "mount"
    mount.mkdir()
    port = unused_port()
    local_server = f"http://127.0.0.1:{port}"
    events = base / "events.jsonl"
    proxy_argv = [
        os.environ["D9_FAULT_PROXY"],
        "--listen",
        f"127.0.0.1:{port}",
        "--upstream",
        upstream,
        "--mode",
        case,
        "--match-method",
        "" if case == "x4" else "PUT",
        "--match-path",
        "/" if case == "x4" else remote,
        "--events",
        str(events),
    ]
    proxy_log = (base / "proxy.log").open("w")
    proxy = subprocess.Popen(
        proxy_argv,
        stdout=proxy_log,
        stderr=proxy_log,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    proxy_log.close()
    mount_proc = None
    row = dict(
        case=case,
        proxy=local_server,
        upstream=upstream,
        remote_root=remote,
        profile=os.environ["D9_PROFILE"],
        durability=os.environ["D9_DURABILITY"],
        verified=False,
        writes=[],
    )
    try:
        wait_proxy(port, proxy)
        mount_started = time.perf_counter()
        mount_proc = mount_client(
            binary,
            local_server,
            remote,
            mount,
            base / "cache-first",
            base / "mount-first.log",
        )
        row["first_mount_ready"] = os.path.ismount(mount)
        row["first_mount_exit"] = mount_proc.poll()
        row["first_mount_wall_s"] = time.perf_counter() - mount_started
        if not row["first_mount_ready"]:
            if case != "x4" or CASES[case] not in [
                e["event"] for e in read_events(events)
            ]:
                raise RuntimeError("first mount failed outside X4 injection")
            mount_proc.wait(timeout=30)
            retry_started = time.perf_counter()
            mount_proc = mount_client(
                binary,
                local_server,
                remote,
                mount,
                base / "cache-retry",
                base / "mount-retry.log",
            )
            row["retry_mount_wall_s"] = time.perf_counter() - retry_started
        if not os.path.ismount(mount):
            raise RuntimeError("mount unavailable after recovery")
        mount_log = base / (
            "mount-retry.log" if "retry_mount_wall_s" in row else "mount-first.log"
        )
        row["actual_sync_mode"] = next(
            (
                line.rsplit(": ", 1)[-1]
                for line in mount_log.read_text(errors="replace").splitlines()
                if "sync mode:" in line
            ),
            "unknown",
        )
        list(mount.iterdir())
        for index in range(2 if case == "x2" else 1):
            name = f"payload-{index}.dat"
            data = hashlib.sha256(f"{case}-{index}".encode()).digest() * 256
            outcome = write_fsync(mount / name, data)
            outcome["file"] = name
            row["writes"].append(outcome)
            location = remote + "/" + name
            after = remote_bytes(binary, location)
            if after != data:
                outcome["retry"] = write_fsync(mount / name, data)
                after = remote_bytes(binary, location)
            outcome["remote_sha_match"] = after == data
        row["drain"] = drain(binary, mount)
        row["trigger_events"] = [
            e for e in read_events(events) if e["event"] == CASES[case]
        ]
        row["duration_s"] = sum(item["duration_s"] for item in row["writes"])
        row["verified"] = bool(
            row["trigger_events"]
            and row["drain"]["ok"]
            and all(item["remote_sha_match"] for item in row["writes"])
        )
    except Exception as error:
        row["error"] = f"{type(error).__name__}: {error}"
    finally:
        row["case_wall_s"] = time.perf_counter() - case_started
        row["cleanup_ok"] = True
        if os.path.ismount(mount):
            try:
                unmount = command([binary, "umount", "--no-auto-pack", str(mount)], 210)
                row["unmount_exit"] = unmount.returncode
                if unmount.returncode:
                    row["unmount_error"] = unmount.stderr[-500:]
                    row["cleanup_ok"] = False
                    row["verified"] = False
            except subprocess.TimeoutExpired:
                row["unmount_error"] = "unmount timeout"
                row["cleanup_ok"] = False
                row["verified"] = False
        if mount_proc and mount_proc.poll() is None:
            mount_proc.terminate()
        if mount_proc:
            try:
                mount_proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                row["error"] = "mount process did not stop"
                row["verified"] = False
        proxy.terminate()
        try:
            proxy.wait(timeout=20)
        except subprocess.TimeoutExpired:
            row["error"] = "proxy did not stop"
            row["verified"] = False
            os.killpg(proxy.pid, signal.SIGKILL)
            proxy.wait(timeout=10)
        row["all_events"] = read_events(events)
        try:
            boundary(None, base / "boundary-after.json")
        except Exception as error:
            row["cleanup_ok"] = False
            row["verified"] = False
            row["cleanup_error"] = str(error)
        (base / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    print(case, "PASS" if row["verified"] else "FAIL", flush=True)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--only", choices=CASES)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.list:
        print(json.dumps(CASES, indent=2))
        return
    if sys.platform != "linux":
        parser.error("Live fault cases require Linux FUSE")
    if not args.run_id or not args.run_id.replace("-", "").isalnum():
        parser.error("--run-id must be a fresh alphanumeric/hyphen ID")
    if not os.path.isfile(os.environ["D9_FAULT_PROXY"]):
        parser.error("D9_FAULT_PROXY must point to the Linux helper binary")
    lock_path = pathlib.Path(os.environ["D9_REPLAY_LOCK"])
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    selected = [args.only] if args.only else list(CASES)
    rows = []
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for case in selected:
            row = run_case(case, args)
            rows.append(row)
            summary = pathlib.Path(os.environ["D9_RESULTS"]) / "faults/summary.json"
            summary.write_text(json.dumps(rows, indent=2) + "\n")
            if not row["cleanup_ok"] or not row.get("drain", {}).get("ok", True):
                break
    raise SystemExit(
        0 if len(rows) == len(selected) and all(r["verified"] for r in rows) else 1
    )


if __name__ == "__main__":
    main()
