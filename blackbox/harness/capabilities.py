from __future__ import annotations

import os
import platform
import shutil
from pathlib import Path
from typing import Any


def detect_capabilities() -> dict[str, Any]:
    system = platform.system()
    caps: dict[str, Any] = {
        "os": system,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "is_root": hasattr(os, "geteuid") and os.geteuid() == 0,
        "tools": {},
        "fuse": {"ok": False, "detail": ""},
        "features": {
            "symlink": hasattr(os, "symlink"),
            "hardlink": hasattr(os, "link"),
            "xattr": hasattr(os, "setxattr") and hasattr(os, "getxattr"),
            "fcntl_lock": system in ("Linux", "Darwin"),
            "case_sensitive_host_tmp": None,
            "linux_only": system == "Linux",
            "macos": system == "Darwin",
        },
    }
    for tool in (
        "bash",
        "docker",
        "podman",
        "fio",
        "git",
        "java",
        "make",
        "mdtest",
        "perl",
        "prove",
        "rg",
        "runltp",
        "umount",
        "vdbench",
    ):
        caps["tools"][tool] = shutil.which(tool) or ""
    caps["fuse"] = detect_fuse(system)
    caps["features"]["case_sensitive_host_tmp"] = detect_case_sensitive(Path(os.environ.get("TMPDIR", "/tmp")))
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


def detect_case_sensitive(tmp_root: Path) -> bool | None:
    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        probe = tmp_root / f"drive9-case-probe-{os.getpid()}"
        probe.mkdir(exist_ok=True)
        upper = probe / "A"
        lower = probe / "a"
        upper.write_text("upper", encoding="utf-8")
        lower.write_text("lower", encoding="utf-8")
        return upper.read_text(encoding="utf-8") != lower.read_text(encoding="utf-8")
    except Exception:
        return None
    finally:
        shutil.rmtree(tmp_root / f"drive9-case-probe-{os.getpid()}", ignore_errors=True)
