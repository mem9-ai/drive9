"""Create four explicitly isolated Drive9 mounts; retain keys only in private files."""

import json
import os
import pathlib
import subprocess
import time
import urllib.error
import urllib.request

BASE = pathlib.Path(__file__).resolve().parent
BIN = BASE / "drive9-official"
ENDPOINT = os.environ.get("DRIVE9_BENCH_SERVER", "https://drive9.example.invalid")
GROUPS = ("none-a", "none-b", "coding-a", "coding-b")


def api(credentials, path, method="GET"):
    req = urllib.request.Request(ENDPOINT + path, method=method,
                                 headers={"Authorization": "Bearer " + credentials["api_key"]})
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            data = response.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        if error.code == 409 and method == "POST":
            return None
        raise RuntimeError("API HTTP " + str(error.code)) from None


rows = []
for group in GROUPS:
    credentials = json.loads((BASE / "credentials" / (group + ".json")).read_text())
    assert credentials["server"] == ENDPOINT
    status = api(credentials, "/v1/status")
    assert status["tenant_id"] == credentials["tenant_id"] and status["status"] == "active"
    api(credentials, "/v1/fs/benchmark/?mkdir", "POST")
    mount = pathlib.Path("/mnt/d9-six-" + group)
    subprocess.run(["sudo", "-n", "mkdir", "-p", str(mount)], check=True)
    subprocess.run(["sudo", "-n", "chown", "1000:1000", str(mount)], check=True)
    assert subprocess.run(["mountpoint", "-q", str(mount)]).returncode != 0, "Mount already exists; inspect it"
    cache = BASE / "cache" / group
    cache.mkdir(parents=True, exist_ok=True)
    profile = "coding-agent" if group.startswith("coding") else "none"
    durability = "fsync" if group.endswith("a") else "interactive"
    args = [str(BIN), "mount", "--foreground", "--mode=fuse", "--server", ENDPOINT,
            "--profile", profile, "--durability", durability, "--cache-dir", str(cache),
            "--dir-ttl", "30s", "--attr-ttl", "30s", "--entry-ttl", "30s", "--allow-other",
            "--gvisor-compat=false"]
    if profile == "coding-agent":
        local = BASE / "local" / group
        local.mkdir(parents=True, exist_ok=True)
        args += ["--local-root", str(local)]
    args += [":/benchmark", str(mount)]
    env = {k: v for k, v in os.environ.items() if not k.startswith("DRIVE9_")}
    env["DRIVE9_API_KEY"] = credentials["api_key"]
    with (BASE / ("mount-" + group + ".log")).open("w") as log:
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
    for attempt in range(60):
        if proc.poll() is not None:
            raise RuntimeError("Mount failed for " + group + "; inspect sanitized logs")
        if subprocess.run(["mountpoint", "-q", str(mount)]).returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Mount readiness timeout " + group)
    info = json.loads(subprocess.check_output(["findmnt", "--json", "--target", str(mount)], text=True))["filesystems"][0]
    assert info["fstype"] == "fuse.drive9" and info["target"] == str(mount)
    version = subprocess.check_output([f"/proc/{proc.pid}/exe", "version"], text=True)
    assert "788ec769866185c6c3d694206172d8f5009781e7" in version
    rows.append({"group": group, "pid": proc.pid, "tenant_id": credentials["tenant_id"],
                 "profile": profile, "durability": durability, "mount": info, "args": args, "version": version})
    (BASE / "mounts.json").write_text(json.dumps(rows, indent=2))
    print("MOUNTED " + group + " " + credentials["tenant_id"], flush=True)
assert len({row["tenant_id"] for row in rows}) == 4
