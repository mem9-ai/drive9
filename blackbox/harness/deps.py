from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .core import CACHE_ROOT, DependencyUnavailable, REPO_ROOT, env_value, progress, write_json

APT_AUTO_INSTALL_TRUTHY = {"1", "true", "yes", "on"}


class DependencyManager:
    def __init__(self, cache_root: Path | None = None, *, auto_fetch: bool = True, recorder: Any | None = None) -> None:
        self.cache_root = cache_root or CACHE_ROOT
        self.tools_root = self.cache_root / "tools"
        self.auto_fetch = auto_fetch
        self.recorder = recorder
        # tools_root is created lazily on first use so the runner can override
        # cache_root/tools_root before any dependency is prepared.

    def _ensure_tools_root(self) -> None:
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
        self._ensure_tools_root()
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


class Drive9DependencyManager(DependencyManager):
    """Shared dependency helpers used across all suite modules.

    Suite-specific fetchers (ensure_pjdfstest, ensure_ltp, etc.) live in each
    module's own ``deps.py`` and call the shared methods here.
    """

    def ensure_system_packages(self, *packages: str) -> None:
        requested = tuple(dict.fromkeys(package for package in packages if package))
        if not requested:
            return
        if env_value("AUTO_INSTALL_SYSTEM_DEPS", "1").lower() not in APT_AUTO_INSTALL_TRUTHY:
            return
        if not sys.platform.startswith("linux"):
            return
        if not shutil.which("apt-get") or not shutil.which("sudo"):
            return
        probe = subprocess.run(["sudo", "-n", "true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if probe.returncode != 0:
            return
        attempted: set[str] = getattr(self, "_system_packages_attempted", set())
        missing = [package for package in requested if package not in attempted]
        if not missing:
            return
        if not getattr(self, "_apt_updated", False):
            self.run("system-apt-update", ["sudo", "apt-get", "update"], timeout=1800)
            self._apt_updated = True
        command_name = "system-apt-install-" + "-".join(missing)
        if len(command_name) > 120:
            command_name = "system-apt-install-" + str(abs(hash(tuple(missing))))
        self.run(command_name, ["sudo", "apt-get", "install", "-y", *missing], timeout=1800)
        attempted.update(missing)
        self._system_packages_attempted = attempted

    def ensure_node_version(self, required: str = ">=24.15.0") -> str:
        """Return a Node binary path that satisfies ``required``.

        If the system Node already satisfies the constraint, return it. Otherwise
        fetch the matching Node binary tarball into the tools cache and return
        the cached ``node`` path.
        """
        sys_node = shutil.which("node")
        if sys_node:
            try:
                out = subprocess.run([sys_node, "--version"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=20, check=False)
                sys_version = out.stdout.strip().lstrip("v")
            except Exception:
                sys_version = ""
            if sys_version and _node_satisfies(sys_version, required):
                progress(f"dependency tool: node ({sys_version}) satisfies {required} -> {sys_node}")
                return sys_node

        target_version = _resolve_node_version(required)
        node_dir = self.tools_root / "node" / target_version
        node_bin = node_dir / "bin" / "node"
        if node_bin.exists():
            progress(f"dependency tool: node ({target_version}) from cache -> {node_bin}")
            return str(node_bin)
        if not self.auto_fetch:
            raise DependencyUnavailable(f"node {required} is required and auto-fetch is disabled")
        self._ensure_tools_root()
        arch = _node_arch()
        tarball = f"node-v{target_version}-linux-{arch}.tar.xz"
        url = f"https://nodejs.org/dist/v{target_version}/{tarball}"
        download_dir = self.tools_root / "node" / "_downloads"
        download_dir.mkdir(parents=True, exist_ok=True)
        archive = download_dir / tarball
        self.run(f"node-download-{target_version}", ["curl", "-fsSL", "-o", str(archive), url], timeout=600)
        self.run(f"node-extract-{target_version}", ["tar", "-xJf", str(archive), "-C", str(node_dir.parent)], timeout=300)
        extracted = node_dir.parent / f"node-v{target_version}-linux-{arch}"
        extracted.rename(node_dir)
        if not node_bin.exists():
            raise DependencyUnavailable(f"node {target_version} download did not produce {node_bin}")
        progress(f"dependency tool: node ({target_version}) fetched -> {node_bin}")
        return str(node_bin)

    def node_env(self, node_bin: str) -> dict[str, str]:
        """Build an env with the given node's bin dir prepended to PATH."""
        env = dict(os.environ)
        node_bin_dir = str(Path(node_bin).resolve().parent)
        env["PATH"] = f"{node_bin_dir}:{env.get('PATH', '')}"
        return env

    def ensure_git_tool(self) -> str:
        found = shutil.which("git")
        if found:
            progress(f"dependency tool: git -> {found}")
            return found
        self.ensure_system_packages("git")
        found = shutil.which("git")
        if found:
            progress(f"dependency tool: git -> {found}")
            return found
        raise DependencyUnavailable("git is required")

    def ensure_prove(self) -> str:
        found = shutil.which("prove")
        if found:
            progress(f"dependency tool: prove -> {found}")
            return found
        self.ensure_system_packages("perl")
        found = shutil.which("prove")
        if found:
            progress(f"dependency tool: prove -> {found}")
            return found
        raise DependencyUnavailable("prove is required")


def _node_version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for piece in version.split("."):
        try:
            parts.append(int(piece))
        except ValueError:
            break
    return tuple(parts)


def _node_satisfies(actual: str, constraint: str) -> bool:
    constraint = constraint.strip()
    actual_t = _node_version_tuple(actual)
    if constraint.startswith(">="):
        return actual_t >= _node_version_tuple(constraint[2:].strip())
    if constraint.startswith(">"):
        return actual_t > _node_version_tuple(constraint[1:].strip())
    if constraint.startswith("<="):
        return actual_t <= _node_version_tuple(constraint[2:].strip())
    if constraint.startswith("<"):
        return actual_t < _node_version_tuple(constraint[1:].strip())
    if constraint.startswith("="):
        return actual_t == _node_version_tuple(constraint[1:].strip())
    return True


def _resolve_node_version(constraint: str) -> str:
    constraint = constraint.strip()
    if constraint.startswith(">="):
        base = constraint[2:].strip()
        major = _node_version_tuple(base)[0]
        latest = {
            24: "24.15.0",
            22: "22.22.1",
            20: "20.19.0",
        }
        return latest.get(major, base)
    for prefix in (">", "<", "<=", "="):
        if constraint.startswith(prefix):
            return constraint[len(prefix):].strip()
    return constraint


def _node_arch() -> str:
    import platform

    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        return "x64"
    if machine in ("arm64", "aarch64"):
        return "arm64"
    return "x64"
