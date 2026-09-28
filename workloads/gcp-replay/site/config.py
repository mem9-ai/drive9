"""Central configuration for the site acceptance suite.

Every environment-specific value can be overridden with an environment variable
(see config.example.env). Defaults assume: the drive9 client binary is at
~/drive9, the endpoint must be configured with D9_SERVER, and all local state lives
under ./state next to this file.
"""

from __future__ import annotations

import os
import pathlib

HOME = pathlib.Path.home()


def _env(name, default=""):
    value = os.environ.get(name)
    return value.strip() if value else default


def _path(name, default):
    value = _env(name)
    if value:
        return pathlib.Path(value).expanduser()
    return pathlib.Path(default).expanduser()


# ------------------------------------------------ target under test

# drive9 client binary (the version being tested). Required.
BIN = _env("D9_BIN") or str(HOME / "drive9")

# drive9 endpoint. Works for public endpoints and for PSC/private-link setups,
# as long as the client can resolve the host name.
SERVER = _env("D9_SERVER") or "https://drive9.example.invalid"
ENDPOINT_HOST = _env("D9_ENDPOINT_HOST") or SERVER.split("://")[-1].split("/")[0]

# Main FUSE mount point used by every scenario.
MOUNT = _path("D9_MOUNT", "/mnt/d9-work")

# Remote root the mount exposes. "/benchmark" for a :/benchmark mount,
# "/" when the client was mounted with --remote-root / (or mounted the root).
REMOTE_ROOT = _env("D9_REMOTE_ROOT") or "/benchmark"


def remote_root_spec():
    """Remote spec string used when the suite starts its own clients."""
    base = REMOTE_ROOT.strip("/")
    return ":/" + base if base else ":/"


# Secondary mounts (recovery / cache-cap / mount-retry) are created under this
# base with a fixed name prefix so they never collide with the main mount.
MOUNT_BASE = _path("D9_MOUNT_BASE", str(pathlib.Path(MOUNT).parent))

# ------------------------------------------------------------ local state

D9_ROOT = pathlib.Path(__file__).resolve().parent
STATE_ROOT = _path("D9_STATE", D9_ROOT / "state")
CACHE_ROOT = STATE_ROOT / "cache"
RESULT_DIR = _path("D9_RESULTS", D9_ROOT / "results")
FIXTURES = _path("D9_FIXTURES", D9_ROOT / "fixtures")
LOCAL = _path("D9_LOCAL", STATE_ROOT / "local")

# ------------------------------------------------------ optional toolchain

# Node toolchain directory (containing node and npm). S6 records a
# "not available" result instead of failing when it is unset.
NODE_BIN = _env("D9_NODE_BIN")

# -------------------------------------------------------- network injection

# Comma-separated endpoint IPs. When unset the suite discovers the IPs from the
# live drive9 connections first (correct for both public and PSC endpoints),
# then falls back to DNS.
ENDPOINT_IPS_OVERRIDE = _env("D9_ENDPOINT_IPS")

# Mount flags shared by every client this suite starts.
PROFILE = _env("D9_PROFILE") or "none"
MOUNT_FLAGS = [
    "--profile",
    PROFILE,
    "--allow-other",
    "--gvisor-compat=false",
    "--mode=fuse",
]
DURABILITY = _env("D9_DURABILITY") or "interactive"
DURABILITY_CONTROL = _env("D9_DURABILITY_CONTROL") or "fsync"
TTL_FLAGS = ["--dir-ttl", "30s", "--attr-ttl", "30s", "--entry-ttl", "30s"]

TARGET_VERSION = _env("D9_TARGET_VERSION") or "(unspecified)"


def endpoint_ips(retries=5, delay=1.0):
    """Endpoint IPs for network-injection tests.

    Order: explicit override -> IPs observed on live drive9 connections
    (works for PSC where DNS may only resolve inside the VPC) -> DNS.
    """
    if ENDPOINT_IPS_OVERRIDE:
        return [ip.strip() for ip in ENDPOINT_IPS_OVERRIDE.split(",") if ip.strip()]

    ips = live_connection_ips()
    if ips:
        return ips

    import socket
    import time as _time

    for _ in range(retries):
        try:
            hosts = socket.getaddrinfo(ENDPOINT_HOST, 443, type=socket.SOCK_STREAM)
            found = sorted({row[4][0] for row in hosts})
            if found:
                return found
        except socket.gaierror:
            pass
        _time.sleep(delay)
    return []


def live_connection_ips():
    """IPs that running drive9 mounts are actually connected to."""
    import subprocess

    try:
        proc = subprocess.run(
            [
                "bash",
                "-c",
                "ss -tnp 2>/dev/null | grep '\\\"drive9\\\"' | awk '{print $5}' | "
                "cut -d: -f1 | sort -u",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return []
    return [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]


def ensure_dirs():
    for path in (STATE_ROOT, CACHE_ROOT, RESULT_DIR, FIXTURES, LOCAL):
        path.mkdir(parents=True, exist_ok=True)
