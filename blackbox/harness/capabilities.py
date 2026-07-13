from __future__ import annotations

import os
import platform
import shutil
from pathlib import Path
from typing import Any, Iterable

from .core import progress


FUSE_TOOLS = (
    "fio",
    "java",
    "kirk",
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


def detect_capabilities(extra_tools: Iterable[str] = ()) -> dict[str, Any]:
    system = platform.system()
    caps: dict[str, Any] = {
        "os": system,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "is_root": hasattr(os, "geteuid") and os.geteuid() == 0,
        "tools": {},
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
    tools = {
        "bash",
        "docker",
        "podman",
        "git",
        "make",
        "rg",
        *extra_tools,
    }
    for tool in sorted(tools):
        caps["tools"][tool] = shutil.which(tool) or ""
    caps["features"]["case_sensitive_host_tmp"] = detect_case_sensitive(Path(os.environ.get("TMPDIR", "/tmp")))
    return caps


def detect_case_sensitive(tmp_root: Path) -> bool | None:
    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        probe = tmp_root / f"blackbox-case-probe-{os.getpid()}"
        probe.mkdir(exist_ok=True)
        upper = probe / "A"
        lower = probe / "a"
        upper.write_text("upper", encoding="utf-8")
        lower.write_text("lower", encoding="utf-8")
        return upper.read_text(encoding="utf-8") != lower.read_text(encoding="utf-8")
    except Exception:
        return None
    finally:
        shutil.rmtree(tmp_root / f"blackbox-case-probe-{os.getpid()}", ignore_errors=True)


def detect_fuse_capabilities() -> dict[str, Any]:
    caps = detect_capabilities(extra_tools=FUSE_TOOLS)
    caps["fuse"] = detect_fuse(str(caps.get("os") or platform.system()))
    return caps


def detect_fuse(system: str) -> dict[str, str | bool]:
    if system == "Linux":
        if not Path("/dev/fuse").exists():
            return {"ok": False, "detail": "/dev/fuse is missing"}
        if not shutil.which("fusermount3") and not shutil.which("fusermount"):
            return {"ok": False, "detail": "fusermount3/fusermount is missing"}
        allow_other_ok = ensure_user_allow_other()
        if not allow_other_ok:
            return {
                "ok": False,
                "detail": "Linux FUSE present but user_allow_other is not enabled in /etc/fuse.conf and could not be auto-enabled (no passwordless sudo). Modules requiring --allow-other will fail.",
            }
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


def ensure_user_allow_other() -> bool:
    """Ensure /etc/fuse.conf enables user_allow_other so --allow-other mounts work.

    Some blackbox modules (e.g. community.ltp.*) mount with --allow-other, which
    fusermount rejects unless 'user_allow_other' is set in /etc/fuse.conf. When
    passwordless sudo is available, enable it automatically so the module does
    not fail on an environment misconfiguration.

    Returns True if the directive is present (or was successfully added), False
    if it could not be enabled.
    """
    import subprocess

    fuse_conf = Path("/etc/fuse.conf")
    if fuse_conf.exists():
        try:
            text = fuse_conf.read_text(encoding="utf-8")
        except OSError:
            return False
        has_directive = any(
            line.strip() == "user_allow_other" or line.strip().startswith("user_allow_other ")
            for line in text.splitlines()
            if not line.strip().startswith("#")
        )
        if has_directive:
            return True

    sudo = shutil.which("sudo")
    if sudo is None:
        return False
    # Probe passwordless sudo before attempting any write.
    probe = subprocess.run([sudo, "-n", "true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if probe.returncode != 0:
        return False
    cmd = [
        "sh",
        "-c",
        "mkdir -p /etc && touch /etc/fuse.conf && "
        "if ! grep -qE '^[[:space:]]*user_allow_other' /etc/fuse.conf; then "
        "printf '\\nuser_allow_other\\n' >> /etc/fuse.conf; fi",
    ]
    result = subprocess.run([sudo, "-n"] + cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return result.returncode == 0
