from __future__ import annotations

import os
import platform
import shutil
from pathlib import Path
from typing import Any

from harness.capabilities import detect_capabilities as detect_host_capabilities


FUSE_TOOLS = (
    "fio",
    "java",
    "mdtest",
    "perl",
    "prove",
    "runltp",
    "umount",
    "vdbench",
)

DARWIN_FUSE_HELPERS = (
    ("mount_macfuse", Path("/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse"), "macFUSE"),
    ("mount_fusefs", Path("/Library/Filesystems/fuse-t.fs/Contents/Resources/mount_fusefs"), "FUSE-T"),
)


def ensure_user_allow_other() -> None:
    """Ensure /etc/fuse.conf enables user_allow_other so --allow-other mounts work.

    Some blackbox modules (e.g. community.ltp.*) mount with --allow-other, which
    fusermount rejects unless 'user_allow_other' is set in /etc/fuse.conf. When
    passwordless sudo is available, enable it automatically so the module does
    not fail on an environment misconfiguration.
    """
    import subprocess

    fuse_conf = Path("/etc/fuse.conf")
    if fuse_conf.exists():
        try:
            text = fuse_conf.read_text(encoding="utf-8")
        except OSError:
            return
        has_directive = any(
            line.strip() == "user_allow_other" or line.strip().startswith("user_allow_other ")
            for line in text.splitlines()
            if not line.strip().startswith("#")
        )
        if has_directive:
            return

    sudo = shutil.which("sudo")
    if sudo is None:
        return
    # Probe passwordless sudo before attempting any write.
    probe = subprocess.run([sudo, "-n", "true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if probe.returncode != 0:
        return
    cmd = [
        "sh",
        "-c",
        "mkdir -p /etc && touch /etc/fuse.conf && "
        "if ! grep -qE '^[[:space:]]*user_allow_other' /etc/fuse.conf; then "
        "printf '\\nuser_allow_other\\n' >> /etc/fuse.conf; fi",
    ]
    subprocess.run([sudo, "-n"] + cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def detect_capabilities() -> dict[str, Any]:
    caps = detect_host_capabilities(extra_tools=FUSE_TOOLS)
    caps["fuse"] = detect_fuse(str(caps.get("os") or platform.system()))
    return caps


def detect_fuse(system: str) -> dict[str, str | bool]:
    if system == "Linux":
        if not Path("/dev/fuse").exists():
            return {"ok": False, "detail": "/dev/fuse is missing"}
        if not shutil.which("fusermount3") and not shutil.which("fusermount"):
            return {"ok": False, "detail": "fusermount3/fusermount is missing"}
        ensure_user_allow_other()
        return {"ok": True, "detail": "Linux FUSE prerequisites are present"}
    if system == "Darwin":
        for name, fallback, label in DARWIN_FUSE_HELPERS:
            helper = shutil.which(name)
            if helper:
                return {"ok": True, "detail": f"{label} mount helper is present: {helper}"}
            if fallback.exists() and os.access(fallback, os.X_OK):
                return {"ok": True, "detail": f"{label} mount helper is present: {fallback}"}
        return {"ok": False, "detail": "macFUSE/FUSE-T mount helper is missing"}
    return {"ok": False, "detail": f"unsupported OS for FUSE suite: {system}"}
