from __future__ import annotations

import os
import platform
import shutil
from pathlib import Path
from typing import Any, Iterable


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
