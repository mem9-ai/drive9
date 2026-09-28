"""Drive9 site acceptance framework - shared helpers.

Target: fe581b9f FUSE (latest) on https://drive9.example.invalid
Independent read path: `drive9 fs ...` HTTP API (bypasses FUSE cache).
Sync confirmation:      `drive9 mount drain --json`.

Layout on EC2:
  /mnt/d9-dev-gcp-new/site-acceptance/<scenario>/   test workspace (on FUSE)
  ~/d9work/site-acceptance/local/                   local-only ground truth
  ~/d9work/site-acceptance/results/                 JSON evidence per scenario
  ~/d9work/site-acceptance/fixtures/                deterministic inputs
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import pathlib
import random
import shutil
import stat
import subprocess
import threading
import time

# ------------------------------------------------------------------ config

from config import (
    BIN,
    CACHE_ROOT,
    D9_ROOT,
    DURABILITY,
    ENDPOINT_HOST,
    FIXTURES,
    LOCAL,
    MOUNT,
    MOUNT_BASE,
    MOUNT_FLAGS,
    NODE_BIN,
    PROFILE,
    REMOTE_ROOT,
    RESULT_DIR,
    SERVER,
    STATE_ROOT,
    TARGET_VERSION,
    TTL_FLAGS,
    endpoint_ips,
    ensure_dirs,
    live_connection_ips,
    remote_root_spec,
)

TARGET = TARGET_VERSION
WORK_ROOT = MOUNT / "site-acceptance"

ensure_dirs()


# ----------------------------------------------------------------- helpers


def sh(args, timeout=300, **kw):
    kw.setdefault("capture_output", True)
    kw.setdefault("text", True)
    return subprocess.run(args, timeout=timeout, **kw)


def run_checked(args, timeout=300, **kw):
    proc = sh(args, timeout=timeout, **kw)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed (%s): %s\nstdout=%s\nstderr=%s"
            % (
                proc.returncode,
                args,
                (proc.stdout or "")[-1500:],
                (proc.stderr or "")[-1500:],
            )
        )
    return proc


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def det_bytes(seed: int, size: int) -> bytes:
    return random.Random(seed).randbytes(size)


def write_file(path, data: bytes, *, mode=None, fsync=False):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        if fsync:
            os.fsync(fh.fileno())
    if mode is not None:
        os.chmod(path, mode)


def read_file(path) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def tree_manifest(root, *, with_mode=True, with_mtime=False):
    """Return {relpath: entry} for a directory tree."""
    root = pathlib.Path(root)
    out = {}
    for path in sorted(root.rglob("*")):
        rel = str(path.relative_to(root))
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            out[rel] = {"type": "symlink", "target": os.readlink(path)}
        elif stat.S_ISDIR(st.st_mode):
            entry = {"type": "dir"}
            if with_mode:
                entry["mode"] = oct(stat.S_IMODE(st.st_mode))
            out[rel] = entry
        else:
            entry = {"type": "file", "size": st.st_size, "sha256": sha256_file(path)}
            if with_mode:
                entry["mode"] = oct(stat.S_IMODE(st.st_mode))
            if with_mtime:
                entry["mtime_ns"] = st.st_mtime_ns
            out[rel] = entry
    return out


def manifest_diff(
    expected: dict, actual: dict, *, ignore_keys=("mode",), ignore_paths=()
):
    diffs = []
    exp_keys, act_keys = set(expected), set(actual)
    for rel in sorted(exp_keys - act_keys):
        diffs.append("missing: " + rel)
    for rel in sorted(act_keys - exp_keys):
        diffs.append("unexpected: " + rel)
    for rel in sorted(exp_keys & act_keys):
        if rel in ignore_paths:
            continue
        e, a = expected[rel], actual[rel]
        for key in sorted(set(e) | set(a)):
            if key in ignore_keys:
                continue
            if e.get(key) != a.get(key):
                diffs.append(
                    "%s: %s expected=%r actual=%r" % (rel, key, e.get(key), a.get(key))
                )
    return diffs


# ------------------------------------------------------------- sync confirm


def stats(samples):
    """Timing summary: n / min / median / p95 / max (seconds)."""
    if not samples:
        return {}
    ordered = sorted(samples)

    def pct(q):
        index = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
        return ordered[index]

    return {
        "n": len(ordered),
        "min": round(ordered[0], 4),
        "median": round(pct(0.5), 4),
        "p95": round(pct(0.95), 4),
        "max": round(ordered[-1], 4),
    }


def drain(mount=None, timeout="120s"):
    """Sync confirmation. Returns parsed drain evidence."""
    mount = pathlib.Path(mount or MOUNT)
    started = time.perf_counter()
    proc = sh(
        [BIN, "mount", "drain", "--timeout", timeout, "--json", str(mount)], timeout=180
    )
    payload = None
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        payload = {
            "raw_stdout": (proc.stdout or "")[-1500:],
            "raw_stderr": (proc.stderr or "")[-1500:],
        }
    ok = proc.returncode == 0 and bool(payload.get("ok"))
    return {
        "ok": ok,
        "exit_code": proc.returncode,
        "result": payload,
        "wall_s": time.perf_counter() - started,
    }


# --------------------------------------------------- independent read path


def remote_of(local_path, mount=None) -> str:
    """Map a mounted path to its remote spec.

    Uses D9_REMOTE_ROOT, so it works for both a :/benchmark mount and a
    client mounted with --remote-root / (PSC setups often mount the root).
    """
    root = pathlib.Path(mount or MOUNT)
    rel = pathlib.Path(local_path).resolve().relative_to(root.resolve())
    base = REMOTE_ROOT.strip("/")
    if base:
        return ":/%s/%s" % (base, rel)
    return ":/%s" % rel


# ------------------------------------------------------- secondary clients


def is_mountpoint(path) -> bool:
    return sh(["mountpoint", "-q", str(path)]).returncode == 0


def wait_mount_ready(mount=None, timeout=300):
    """Wait until the mount answers readdir (a degraded client returns EAGAIN)."""
    mount = pathlib.Path(mount or MOUNT)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            list(mount.iterdir())
            return True
        except OSError:
            time.sleep(3)
    return False


def mount_client(mountpoint, cache_dir, durability=None, extra=()):
    """Start another fe581b9f FUSE client; returns the Popen handle."""
    mountpoint = pathlib.Path(mountpoint)
    durability = durability or DURABILITY
    if mountpoint != MOUNT and os.path.ismount(MOUNT):
        if not drain(MOUNT)["ok"]:
            raise RuntimeError("Main mount dirty before auxiliary mount")
        run_checked([BIN, "umount", "--no-auto-pack", str(MOUNT)], timeout=180)
    extra = list(extra)
    sh(["sudo", "-n", "mkdir", "-p", str(mountpoint)])
    sh(["sudo", "-n", "chown", "%s:%s" % (os.getuid(), os.getgid()), str(mountpoint)])
    cache_dir = pathlib.Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    if PROFILE == "coding-agent" and "--local-root" not in extra:
        extra += ["--local-root", str(cache_dir.parent / (cache_dir.name + "-overlay"))]
    args = [
        BIN,
        "mount",
        "--server",
        SERVER,
        "--durability",
        durability,
        "--cache-dir",
        str(cache_dir),
        *MOUNT_FLAGS,
        *TTL_FLAGS,
        "--foreground",
        *extra,
        remote_root_spec(),
        str(mountpoint),
    ]
    log = cache_dir.parent / (mountpoint.name + ".log")
    with (RESULT_DIR / "mount-commands.jsonl").open("a") as audit:
        audit.write(json.dumps({"argv": args, "epoch": time.time()}) + "\n")
    with open(log, "w") as handle:
        proc = subprocess.Popen(
            args,
            stdout=handle,
            stderr=handle,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    # Reap an exited client while the CLI waits for its PID during unmount.
    threading.Thread(target=proc.wait, daemon=True).start()
    for _ in range(60):
        if proc.poll() is not None:
            raise RuntimeError("mount failed; inspect %s" % log)
        if is_mountpoint(mountpoint):
            return proc
        time.sleep(1)
    raise RuntimeError("mount readiness timeout: %s" % mountpoint)


def unmount_client(mountpoint, timeout=120):
    return sh(["sudo", "-n", "umount", str(mountpoint)], timeout=timeout)


def kill_client(pid):
    return sh(["sudo", "-n", "kill", "-9", str(pid)], timeout=60)


# ------------------------------------------------------- network injection


def live_endpoint_ips():
    """Alias kept for callers; IP discovery lives in config.endpoint_ips."""
    return live_connection_ips() or endpoint_ips()


def net_block(ips=None):
    """Drop traffic to the endpoint; returns the blocked IP list."""
    ips = ips or endpoint_ips()
    for ip in ips:
        sh(["sudo", "-n", "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP"])
        sh(["sudo", "-n", "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP"])
    return ips


def net_unblock(ips):
    for ip in ips:
        sh(["sudo", "-n", "iptables", "-D", "OUTPUT", "-d", ip, "-j", "DROP"])
        sh(["sudo", "-n", "iptables", "-D", "INPUT", "-s", ip, "-j", "DROP"])


def net_is_blocked(ips):
    proc = sh(["sudo", "-n", "iptables", "-S", "OUTPUT"])
    return all(
        ("-d %s/32 -j DROP" % ip) in proc.stdout
        or ("-d %s -j DROP" % ip) in proc.stdout
        for ip in ips
    )


def net_interface():
    proc = sh(
        ["bash", "-c", "ip route get 8.8.8.8 2>/dev/null | head -1 | awk '{print $5}'"]
    )
    return (proc.stdout or "").strip() or "eth0"


def rate_limit(rate="1mbit"):
    """Throttle the egress interface so write-back uploads fall behind."""
    dev = net_interface()
    sh(
        [
            "sudo",
            "-n",
            "tc",
            "qdisc",
            "add",
            "dev",
            dev,
            "root",
            "tbf",
            "rate",
            rate,
            "burst",
            "32kbit",
            "latency",
            "400ms",
        ]
    )
    return dev


def rate_limit_clear(dev):
    sh(["sudo", "-n", "tc", "qdisc", "del", "dev", dev, "root"])


def net_block_responses(ips=None):
    """Drop only inbound packets from the endpoint: requests still reach the
    server (the operation executes) but the success response never arrives."""
    ips = ips or endpoint_ips()
    for ip in ips:
        sh(["sudo", "-n", "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP"])
    return ips


def net_unblock_responses(ips):
    for ip in ips:
        sh(["sudo", "-n", "iptables", "-D", "INPUT", "-s", ip, "-j", "DROP"])


def api(*args, timeout=300):
    return sh([BIN, *args], timeout=timeout)


def api_cat(remote_path):
    """Read a remote file through the API; returns raw bytes."""
    proc = subprocess.run(
        [BIN, "fs", "cat", remote_path], capture_output=True, timeout=180
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "api cat failed: %s\n%s" % (remote_path, (proc.stderr or b"")[-500:])
        )
    return proc.stdout


def api_stat(remote_path):
    proc = sh([BIN, "fs", "stat", "-o", "json", remote_path], timeout=120)
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except Exception:
        return {"raw": proc.stdout}


def api_exists(remote_path) -> bool:
    return api_stat(remote_path) is not None


def api_download_tree(remote_dir, dest_dir):
    """Download a remote tree via API (independent of FUSE) and extract it.

    `drive9 fs archive` packs the remote directory itself as the tar top-level
    entry, so unwrap a single leading directory when present.
    """
    dest_dir = pathlib.Path(dest_dir)
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    dest_dir.mkdir(parents=True)
    tar_path = dest_dir.parent / (dest_dir.name + ".tar.gz")
    if tar_path.exists():
        tar_path.unlink()
    run_checked([BIN, "fs", "archive", remote_dir, str(tar_path)], timeout=900)
    stage = dest_dir.parent / (dest_dir.name + ".stage")
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    run_checked(["tar", "-xzf", str(tar_path), "-C", str(stage)], timeout=900)
    entries = sorted(stage.iterdir(), key=lambda p: p.name)
    source = entries[0] if len(entries) == 1 and entries[0].is_dir() else stage
    for entry in source.iterdir():
        shutil.move(str(entry), str(dest_dir / entry.name))
    shutil.rmtree(stage, ignore_errors=True)
    return dest_dir


# -------------------------------------------------------------- workspaces


def workdir(scenario, *, sub=""):
    path = WORK_ROOT / scenario / sub if sub else WORK_ROOT / scenario
    if path.exists():
        removed = False
        for _ in range(3):
            try:
                shutil.rmtree(path)
                removed = True
                break
            except OSError:
                time.sleep(1.0)
        if not removed:
            # FUSE rmtree can hit stale namespace entries (ENOTEMPTY);
            # fall back to a fresh directory name instead of failing the run
            path = path.parent / (path.name + "-" + time.strftime("%H%M%S"))
    for attempt in range(12):
        try:
            path.mkdir(parents=True)
            return path
        except BlockingIOError:
            # mount degraded (e.g. after a network injection); wait for recovery
            time.sleep(5)
    path.mkdir(parents=True)
    return path


def localdir(scenario, *, sub=""):
    path = LOCAL / scenario / sub if sub else LOCAL / scenario
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


# ---------------------------------------------------------------- report


class Report:
    def __init__(self, scenario):
        self.path = RESULT_DIR / (scenario + ".json")
        self.data = {
            "scenario": scenario,
            "target_binary": BIN,
            "target_version": TARGET,
            "mount": str(MOUNT),
            "durability": DURABILITY,
            "profile": PROFILE,
            "server": SERVER,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "steps": [],
            "checks": [],
            "sync_evidence": [],
            "failures": [],
            "verified": None,
        }
        self._t0 = time.perf_counter()

    @contextlib.contextmanager
    def step(self, name):
        record = {"step": name, "start_epoch": time.time()}
        start = time.perf_counter()
        try:
            yield record
            record["ok"] = True
        except Exception as err:
            record["ok"] = False
            record["error"] = "%s: %s" % (type(err).__name__, err)
            raise
        finally:
            record["duration_s"] = round(time.perf_counter() - start, 4)
            self.data["steps"].append(record)

    def check(self, ok, name, **evidence):
        row = {"name": name, "ok": bool(ok)}
        row.update(evidence)
        self.data["checks"].append(row)
        if not ok:
            self.data["failures"].append(name)
        return bool(ok)

    def sync(self, label, mount=None, timeout="120s"):
        evidence = drain(mount, timeout)
        evidence["label"] = label
        self.data["sync_evidence"].append(evidence)
        return evidence

    def finish(self):
        self.data["duration_s"] = round(time.perf_counter() - self._t0, 3)
        self.data["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.data["verified"] = not self.data["failures"]
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False))
        summary = {
            "scenario": self.data["scenario"],
            "verified": self.data["verified"],
            "duration_s": self.data["duration_s"],
            "failures": self.data["failures"][:5],
        }
        print("RESULT " + json.dumps(summary, ensure_ascii=False), flush=True)
        return self.data["verified"]


def main_guard(fn, scenario):
    """Wrap a scenario entrypoint: always persist a report, exit 0/1."""
    report = Report(scenario)
    try:
        fn(report)
    except Exception as err:
        import traceback

        report.check(
            False,
            "scenario completed without exception",
            error="%s: %s" % (type(err).__name__, err),
            traceback=traceback.format_exc()[-3000:],
        )
    ok = report.finish()
    raise SystemExit(0 if ok else 1)
