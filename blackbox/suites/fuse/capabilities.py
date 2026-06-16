from __future__ import annotations

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
        if shutil.which("mount_macfuse"):
            return {"ok": True, "detail": "macFUSE mount helper is present"}
        if shutil.which("mount_fusefs"):
            return {"ok": True, "detail": "FUSE-T mount helper is present"}
        return {"ok": False, "detail": "macFUSE/FUSE-T mount helper is missing"}
    return {"ok": False, "detail": f"unsupported OS for FUSE suite: {system}"}
