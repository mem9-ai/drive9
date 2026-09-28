"""Run one B/C/Node/X collection serially, resuming after dirty-cache segments."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
GROUPS = {
    "G01": ("none", "interactive"),
    "G02": ("none", "close-sync"),
    "G03": ("none", "write-sync"),
    "G04": ("coding-agent", "interactive"),
    "G05": ("coding-agent", "close-sync"),
    "G06": ("coding-agent", "write-sync"),
}


def call(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, timeout=240, **kw)


def inventory(suite):
    path = {
        "B": "site/run_all.py",
        "C": "site/run_dn.py",
        "Node": "node_fs/run_test_fs.py",
        "X": "faults/run_x.py",
    }[suite]
    rows = json.loads(
        subprocess.check_output([sys.executable, str(ROOT / path), "--list"])
    )
    if suite == "C":
        rows = [r.split("_")[0] for r in rows]
    return path, list(rows)


def ensure_idle():
    ips = [ip.strip() for ip in os.environ.get("D9_ENDPOINT_IPS", "").split(",") if ip.strip()]
    if not ips:
        raise ValueError("Set D9_ENDPOINT_IPS before checking firewall boundaries")
    mounts = call(["findmnt", "-rn", "-t", "fuse.drive9", "-o", "TARGET"])
    if mounts.stdout.strip():
        raise RuntimeError("Drive9 mounts still present: " + mounts.stdout)
    for chain in ("INPUT", "OUTPUT"):
        rules = call(["sudo", "-n", "iptables", "-S", chain])
        if rules.returncode:
            raise RuntimeError("Cannot inspect firewall")
        for line in rules.stdout.splitlines():
            if any(ip in line for ip in ips) and "DROP" in line:
                raise RuntimeError("PSC block rule remains: " + line)


def start_mount(env, segment):
    mount = Path(env["D9_MOUNT"])
    mount.mkdir(parents=True, exist_ok=True)
    if os.path.ismount(mount):
        raise RuntimeError("Mount already active")
    argv = [
        env["D9_BIN"],
        "mount",
        "--foreground",
        "--mode=fuse",
        "--server",
        env["D9_SERVER"],
        "--profile",
        env["D9_PROFILE"],
        "--durability",
        env["D9_DURABILITY"],
        "--cache-dir",
        str(segment / "state/cache/main"),
        "--allow-other",
        "--gvisor-compat=false",
        "--dir-ttl",
        "30s",
        "--attr-ttl",
        "30s",
        "--entry-ttl",
        "30s",
    ]
    if env["D9_PROFILE"] == "coding-agent":
        argv += ["--local-root", str(segment / "state/main-overlay")]
    argv += [":" + env["D9_REMOTE_ROOT"], str(mount)]
    (segment / "mount-command.json").write_text(json.dumps(argv) + "\n")
    with (segment / "mount.log").open("w") as log:
        proc = subprocess.Popen(
            argv,
            env=env,
            stdout=log,
            stderr=log,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    threading.Thread(target=proc.wait, daemon=True).start()
    for _ in range(120):
        if os.path.ismount(mount):
            return proc
        if proc.poll() is not None:
            raise RuntimeError("Mount exited; inspect " + str(segment))
        time.sleep(1)
    proc.terminate()
    raise RuntimeError("Mount readiness timeout")


def stop_mount(env, segment, proc):
    mount = env["D9_MOUNT"]
    if os.path.ismount(mount):
        drained = call(
            [env["D9_BIN"], "mount", "drain", "--json", "--timeout", "120s", mount],
            env=env,
        )
        (segment / "final-drain.json").write_text(drained.stdout)
        unmount = call(
            [env["D9_BIN"], "umount", "--no-auto-pack", "--timeout", "90s", mount],
            env=env,
        )
        (segment / "umount.log").write_text(unmount.stdout + unmount.stderr)
        if unmount.returncode or os.path.ismount(mount):
            raise RuntimeError("Unmount incomplete; preserve segment and inspect")
    if proc and proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=45)
    ensure_idle()


def run(args):
    profile, durability = GROUPS[args.group]
    entry, pending = inventory(args.suite)
    if args.only:
        if not set(args.only) <= set(pending):
            raise ValueError("Unknown case")
        pending = [name for name in pending if name in args.only]
    base = Path.home() / "drive9-replays" / args.run_id
    base.mkdir(parents=True, exist_ok=False)
    original = Path.home() / "drive9-replays/gcp-20260924-cc0159f6-r01/state"
    payload = dict(
        run_id=args.run_id,
        group=args.group,
        suite=args.suite,
        profile=profile,
        durability=durability,
        expected=len(pending),
        rows=[],
        details={},
        status="running",
    )
    summary = base / "suite-summary.json"
    summary.write_text(json.dumps(payload, indent=2) + "\n")
    with (base.parent / "formal-suite.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ensure_idle()
        segment_no = 0
        try:
            while pending:
                segment_no += 1
                segment = base / f"segment-{segment_no:02d}"
                state, results = segment / "state", segment / "results"
                state.mkdir(parents=True)
                results.mkdir()
                for name in ("tools", "node-source"):
                    (state / name).symlink_to(original / name, target_is_directory=True)
                env = dict(
                    os.environ,
                    D9_PROFILE=profile,
                    D9_DURABILITY=durability,
                    D9_ACTUAL_SYNC_MODE="interactive"
                    if durability == "interactive"
                    else "strict",
                    D9_RUN_ID=f"{args.run_id}-s{segment_no}",
                    D9_STATE=str(state),
                    D9_RESULTS=str(results),
                    D9_LOCAL=str(state / "local"),
                    D9_FIXTURES=str(original / "fixtures"),
                    D9_MOUNT=str(Path.home() / "drive9"),
                    D9_MOUNT_BASE=str(Path.home()),
                    D9_REMOTE_ROOT=f"/gcp-replay-20260924-r01/formal/{args.run_id}/s{segment_no}",
                    D9_FAULT_PROXY=str(ROOT / "faults/proxy-linux-amd64"),
                    npm_config_cache=str(state / "npm-cache"),
                    COREPACK_HOME=str(state / "corepack"),
                )
                env["DRIVE9_SERVER"] = env["D9_SERVER"]
                env["PATH"] = env["D9_NODE_BIN"] + ":/usr/sbin:/sbin:" + env["PATH"]
                created = call(
                    [env["D9_BIN"], "fs", "mkdir", ":" + env["D9_REMOTE_ROOT"]], env=env
                )
                if created.returncode:
                    raise RuntimeError("Remote mkdir failed: " + created.stderr)
                argv = [sys.executable, str(ROOT / entry)]
                if args.suite == "X":
                    argv += ["--run-id", env["D9_RUN_ID"], "--only", pending[0]]
                elif args.suite == "Node":
                    for name in pending:
                        argv += ["--only", name]
                else:
                    argv += pending
                proc = None
                try:
                    if args.suite != "X":
                        proc = start_mount(env, segment)
                    with (segment / "runner.log").open("w") as log:
                        worker = subprocess.Popen(
                            argv,
                            cwd=ROOT / "site" if args.suite in ("B", "C") else ROOT,
                            env=env,
                            stdout=log,
                            stderr=log,
                            start_new_session=True,
                        )
                        try:
                            code = worker.wait(timeout=21600)
                        except subprocess.TimeoutExpired:
                            os.killpg(worker.pid, signal.SIGKILL)
                            worker.wait()
                            raise RuntimeError(
                                "Suite timed out; inspect processes and firewall"
                            )
                        finally:
                            try:
                                os.killpg(worker.pid, signal.SIGTERM)
                            except ProcessLookupError:
                                pass
                            try:
                                worker.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                os.killpg(worker.pid, signal.SIGKILL)
                                worker.wait(timeout=10)
                finally:
                    stop_mount(env, segment, proc)
                index = (
                    results
                    / {
                        "B": "run_all.json",
                        "C": "run_dn.json",
                        "Node": "node-fs/summary.json",
                        "X": "faults/summary.json",
                    }[args.suite]
                )
                found = json.loads(index.read_text()) if index.exists() else []
                rows = found["rows"] if args.suite == "Node" and found else found
                if not rows:
                    raise RuntimeError(
                        f"No case results: segment {segment_no}, exit {code}"
                    )
                for row in rows:
                    row["source_results"] = str(results)
                    if args.suite in ("B", "C"):
                        name = row["scenario"]
                        case = json.loads((results / (name + ".json")).read_text())
                        payload["details"][name] = case
                        selected = (
                            name.replace("-", "_") + ".py"
                            if args.suite == "B"
                            else name.split("-")[0]
                        )
                    else:
                        selected = row["test"] if args.suite == "Node" else row["case"]
                    pending.remove(selected)
                    payload["rows"].append(row)
                summary.write_text(json.dumps(payload, indent=2) + "\n")
                print(
                    args.group,
                    args.suite,
                    len(payload["rows"]),
                    "/",
                    payload["expected"],
                    flush=True,
                )
            payload["status"] = "completed"
        except BaseException as error:
            payload["status"] = "blocked"
            payload["error"] = type(error).__name__ + ": " + str(error)
            raise
        finally:
            summary.write_text(json.dumps(payload, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=GROUPS)
    parser.add_argument("suite", choices=("B", "C", "Node", "X"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--only", action="append", default=[])
    run(parser.parse_args())
