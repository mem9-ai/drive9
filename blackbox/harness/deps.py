from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .core import CACHE_ROOT, DependencyUnavailable, REPO_ROOT, progress


class DependencyManager:
    def __init__(self, cache_root: Path | None = None, *, auto_fetch: bool = True, recorder: Any | None = None) -> None:
        self.cache_root = cache_root or CACHE_ROOT
        self.tools_root = self.cache_root / "tools"
        self.auto_fetch = auto_fetch
        self.recorder = recorder
        self.tools_root.mkdir(parents=True, exist_ok=True)

    def event(self, name: str, status: str, detail: str, metadata: dict[str, Any] | None = None) -> None:
        if self.recorder is not None:
            self.recorder.event({"type": "dependency", "name": name, "status": status, "detail": detail, "metadata": metadata or {}})

    def require_tool(self, name: str, env_var: str = "") -> str:
        if env_var and os.environ.get(env_var):
            path = os.environ[env_var]
            if Path(path).exists():
                progress(f"dependency tool: {name} from ${env_var} -> {path}")
                return path
        found = shutil.which(name)
        if found:
            progress(f"dependency tool: {name} -> {found}")
            return found
        progress(f"dependency missing: {name}")
        raise DependencyUnavailable(f"{name} is required")

    def run(self, name: str, cmd: list[str], cwd: Path | None = None, timeout: int = 1800) -> None:
        log_dir = self.cache_root / "logs" / name
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout = log_dir / "stdout.log"
        stderr = log_dir / "stderr.log"
        progress(f"dependency command start: {name} (timeout={timeout}s, logs={log_dir})")
        with stdout.open("ab") as out, stderr.open("ab") as err:
            out.write(f"\n# {' '.join(cmd)}\n".encode())
            out.flush()
            proc = subprocess.run(cmd, cwd=str(cwd or REPO_ROOT), stdout=out, stderr=err, timeout=timeout, check=False)
        if proc.returncode != 0:
            progress(f"dependency command failed: {name} exit={proc.returncode} stderr={stderr}")
            raise DependencyUnavailable(f"dependency command failed for {name}; see {stderr}")
        progress(f"dependency command done: {name}")

    def ensure_git_clone(self, name: str, url: str, ref: str) -> Path:
        dest = self.tools_root / name / ref
        marker = dest / ".drive9-blackbox-ready"
        if marker.exists():
            progress(f"dependency cached: {name}@{ref} -> {dest}")
            return dest
        if not self.auto_fetch:
            raise DependencyUnavailable(f"{name} is not cached and auto-fetch is disabled")
        parent = dest.parent
        parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            progress(f"dependency fetch: cloning {name}@{ref} from {url}")
            self.run(f"{name}-clone", ["git", "clone", "--depth", "1", "--branch", ref, url, str(dest)], timeout=1800)
        else:
            progress(f"dependency fetch: updating {name}@{ref} in {dest}")
            self.run(f"{name}-fetch", ["git", "fetch", "--depth", "1", "origin", ref], cwd=dest, timeout=1800)
            self.run(f"{name}-checkout", ["git", "checkout", "FETCH_HEAD"], cwd=dest, timeout=600)
        marker.write_text("ready\n", encoding="utf-8")
        progress(f"dependency ready: {name}@{ref} -> {dest}")
        return dest
