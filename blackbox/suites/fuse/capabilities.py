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
