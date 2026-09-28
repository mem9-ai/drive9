"""Six policy/profile groups, one mounted at a time; ten-case timing contract."""

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time

from cases import CASES

GROUPS = {
    "G01": ("none", "interactive"),
    "G02": ("none", "close-sync"),
    "G03": ("none", "write-sync"),
    "G04": ("coding-agent", "interactive"),
    "G05": ("coding-agent", "close-sync"),
    "G06": ("coding-agent", "write-sync"),
}


def schedule():
    return [
        dict(group=g, case=c, profile=p, durability=d)
        for g, (p, d) in GROUPS.items()
        for c in CASES
    ]


def command(args, **kw):
    return subprocess.run(args, check=True, capture_output=True, timeout=180, **kw)


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


def run(args):
    if sys.platform != "linux":
        raise RuntimeError("FUSE workload execution requires Linux")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "site"))
    from case_guard import boundary

    if not args.run_id or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.run_id):
        raise ValueError(
            "--run-id must be a new alphanumeric/hyphen/underscore identifier"
        )
    import fcntl

    lock_path = pathlib.Path(
        os.environ.get(
            "D9_REPLAY_LOCK",
            str(pathlib.Path.home() / "drive9-replays/coordinator.lock"),
        )
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = lock_path.open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    binary = str(pathlib.Path(os.environ["D9_BIN"]).expanduser().resolve())
    server = os.environ["D9_SERVER"]
    os.environ["DRIVE9_SERVER"] = server
    base = pathlib.Path(args.runs_dir).expanduser().resolve() / args.run_id
    base.mkdir(parents=True, exist_ok=False)
    results = base / "results"
    results.mkdir()
    groups = {args.group: GROUPS[args.group]} if args.group else GROUPS
    metadata = dict(
        run_id=args.run_id,
        server=server,
        groups=groups,
        version=command([binary, "version"]).stdout.decode(),
        scale=0.02 if args.mode == "serial-smoke" else 1,
        remote_parent=args.remote_parent,
    )
    (base / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    for group, (profile, durability) in groups.items():
        remote = args.remote_parent.rstrip("/") + "/" + args.run_id + "/" + group
        command([binary, "fs", "mkdir", ":" + remote])
        mount = pathlib.Path(args.mount_root) / ("d9-six-" + group)
        command(["sudo", "-n", "mkdir", "-p", str(mount)])
        command(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(mount)])
        if os.path.ismount(mount) or any(mount.iterdir()):
            raise RuntimeError("Mount path is in use: " + str(mount))
        cache = base / "cache" / group
        cache.mkdir(parents=True)
        mount_args = [
            binary,
            "mount",
            "--foreground",
            "--mode=fuse",
            "--server",
            server,
            "--profile",
            profile,
            "--durability",
            durability,
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
        ]
        if profile == "coding-agent":
            mount_args += ["--local-root", str(base / "local" / group)]
        mount_args += [":" + remote, str(mount)]
        with (base / (group + "-mount.log")).open("w") as log:
            proc = subprocess.Popen(
                mount_args,
                stdout=log,
                stderr=log,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        threading.Thread(target=proc.wait, daemon=True).start()
        try:
            for _ in range(120):
                if os.path.ismount(mount):
                    break
                if proc.poll() is not None:
                    raise RuntimeError("Mount exited: " + group)
                time.sleep(1)
            else:
                raise RuntimeError("Mount readiness timeout: " + group)
            for case in CASES:
                label = f"{args.mode}-r01-{group}-{case}"
                boundary(mount, results / (label + "-boundary-before.json"))
                before = drain(binary, mount)
                if not before["ok"]:
                    (results / (label + "-preflight.json")).write_text(
                        json.dumps(before, indent=2)
                    )
                    raise RuntimeError("Dirty mount before " + label)
                raw = results / (label + ".workload.json")
                row = dict(
                    case=case,
                    group=group,
                    profile=profile,
                    durability=durability,
                    verified=False,
                    drain_before=before,
                )
                started = time.perf_counter()
                with (results / (label + ".log")).open("w") as log:
                    worker = subprocess.Popen(
                        [
                            sys.executable,
                            str(pathlib.Path(__file__).with_name("cases.py")),
                            case,
                            str(mount / label),
                            str(raw),
                            "--scale",
                            str(metadata["scale"]),
                        ],
                        stdout=log,
                        stderr=log,
                        start_new_session=True,
                    )
                    try:
                        row["exit_code"] = worker.wait(timeout=1800)
                    except subprocess.TimeoutExpired:
                        import signal

                        os.killpg(worker.pid, signal.SIGKILL)
                        worker.wait()
                        row["exit_code"] = -1
                        row["error"] = "case timeout"
                row["case_wall_s"] = time.perf_counter() - started
                if raw.exists():
                    row.update(json.loads(raw.read_text()))
                row["drain_after"] = drain(binary, mount)
                if "duration_s" in row:
                    row["duration_plus_drain_s"] = (
                        row["duration_s"] + row["drain_after"]["wall_s"]
                    )
                if row.get("verified") and row["drain_after"]["ok"]:
                    try:
                        sample = row.get("sample")
                        if sample:
                            data = command(
                                [
                                    binary,
                                    "fs",
                                    "cat",
                                    ":" + remote + "/" + label + "/" + sample["path"],
                                ]
                            ).stdout
                            if (
                                len(data) != sample["size"]
                                or hashlib.sha256(data).hexdigest() != sample["sha256"]
                            ):
                                raise RuntimeError("remote sample mismatch")
                            row["remote_sample_verified"] = True
                    except Exception as err:
                        row["postcheck_error"] = str(err)
                        row["verified"] = False
                row["ok"] = bool(
                    row.get("verified")
                    and row["exit_code"] == 0
                    and row["drain_after"]["ok"]
                )
                (results / (label + ".json")).write_text(
                    json.dumps(row, indent=2) + "\n"
                )
                rows.append(row)
                (base / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
                print(label, "PASS" if row["ok"] else "FAIL", flush=True)
                boundary(mount, results / (label + "-boundary-after.json"))
                if not row["drain_after"]["ok"]:
                    raise RuntimeError(
                        "Unresolved drain; preserve cache and stop: " + label
                    )
                if args.mode == "serial-smoke" and not row["ok"]:
                    raise RuntimeError("Smoke failed: " + label)
        finally:
            if os.path.ismount(mount):
                command([binary, "umount", "--no-auto-pack", str(mount)])
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=30)
    return (
        0 if len(rows) == len(groups) * len(CASES) and all(r["ok"] for r in rows) else 1
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", nargs="?", choices=["serial", "serial-smoke"], default="serial"
    )
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--group", choices=GROUPS)
    parser.add_argument(
        "--runs-dir", default=str(pathlib.Path.home() / "drive9-replays")
    )
    parser.add_argument("--mount-root", default="/mnt")
    parser.add_argument("--remote-parent", default="/sixway")
    options = parser.parse_args()
    if options.list:
        print(json.dumps(schedule(), indent=2))
    else:
        raise SystemExit(run(options))
