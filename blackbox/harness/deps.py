from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .core import CACHE_ROOT, DependencyUnavailable, REPO_ROOT, env_value, write_json


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
                return path
        found = shutil.which(name)
        if found:
            return found
        raise DependencyUnavailable(f"{name} is required")

    def run(self, name: str, cmd: list[str], cwd: Path | None = None, timeout: int = 1800) -> None:
        log_dir = self.cache_root / "logs" / name
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout = log_dir / "stdout.log"
        stderr = log_dir / "stderr.log"
        with stdout.open("ab") as out, stderr.open("ab") as err:
            out.write(f"\n# {' '.join(cmd)}\n".encode())
            out.flush()
            proc = subprocess.run(cmd, cwd=str(cwd or REPO_ROOT), stdout=out, stderr=err, timeout=timeout, check=False)
        if proc.returncode != 0:
            raise DependencyUnavailable(f"dependency command failed for {name}; see {stderr}")

    def ensure_git_clone(self, name: str, url: str, ref: str) -> Path:
        dest = self.tools_root / name / ref
        marker = dest / ".drive9-blackbox-ready"
        if marker.exists():
            return dest
        if not self.auto_fetch:
            raise DependencyUnavailable(f"{name} is not cached and auto-fetch is disabled")
        parent = dest.parent
        parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            self.run(f"{name}-clone", ["git", "clone", "--depth", "1", "--branch", ref, url, str(dest)], timeout=1800)
        else:
            self.run(f"{name}-fetch", ["git", "fetch", "--depth", "1", "origin", ref], cwd=dest, timeout=1800)
            self.run(f"{name}-checkout", ["git", "checkout", "FETCH_HEAD"], cwd=dest, timeout=600)
        marker.write_text("ready\n", encoding="utf-8")
        return dest

    def ensure_pjdfstest(self) -> tuple[Path, Path]:
        tests = os.environ.get("PJDFSTEST_TESTS", "")
        root = os.environ.get("PJDFSTEST_DIR", "")
        candidates: list[Path] = []
        if tests:
            candidates.append(Path(tests).expanduser())
        if root:
            candidates.append(Path(root).expanduser() / "tests")
        cached_root = self.tools_root / "pjdfstest" / os.environ.get("PJDFSTEST_REF", "master")
        candidates.append(cached_root / "tests")
        tests_dir = next((path.resolve() for path in candidates if path.is_dir()), None)
        bin_candidates: list[Path] = []
        if os.environ.get("PJDFSTEST_BIN"):
            bin_candidates.append(Path(os.environ["PJDFSTEST_BIN"]).expanduser())
        if tests_dir is not None:
            bin_candidates.append(tests_dir.parent / "pjdfstest")
        if shutil.which("pjdfstest"):
            bin_candidates.append(Path(shutil.which("pjdfstest") or ""))
        bin_path = next((path.resolve() for path in bin_candidates if path.exists() and os.access(path, os.X_OK)), None)
        if tests_dir is not None and bin_path is not None:
            return tests_dir, bin_path

        if not self.auto_fetch:
            raise DependencyUnavailable("pjdfstest is not available and auto-fetch is disabled")
        ref = os.environ.get("PJDFSTEST_REF", "master")
        root_dir = self.ensure_git_clone("pjdfstest", "https://github.com/pjd/pjdfstest.git", ref)
        if not (root_dir / "pjdfstest").exists():
            if (root_dir / "autogen.sh").exists():
                self.run("pjdfstest-autogen", ["sh", "autogen.sh"], cwd=root_dir, timeout=600)
            elif (root_dir / "configure.ac").exists() and shutil.which("autoreconf"):
                self.run("pjdfstest-autoreconf", ["autoreconf", "-fi"], cwd=root_dir, timeout=600)
            if (root_dir / "configure").exists():
                self.run("pjdfstest-configure", ["./configure"], cwd=root_dir, timeout=600)
            self.run("pjdfstest-make", ["make"], cwd=root_dir, timeout=1200)
        metadata = {
            "name": "pjdfstest",
            "source": "https://github.com/pjd/pjdfstest",
            "ref": ref,
            "license": "BSD-2-Clause",
        }
        write_json(root_dir / ".drive9-blackbox-dependency.json", metadata)
        return root_dir / "tests", root_dir / "pjdfstest"

    def ensure_git_source(self) -> Path:
        if os.environ.get("GIT_TEST_SOURCE_DIR"):
            path = Path(os.environ["GIT_TEST_SOURCE_DIR"]).expanduser().resolve()
            if (path / "t").is_dir():
                return path
        ref = env_value("GIT_TEST_REF", "v2.46.2")
        root_dir = self.ensure_git_clone("git", "https://github.com/git/git.git", ref)
        write_json(
            root_dir / ".drive9-blackbox-dependency.json",
            {"name": "git", "source": "https://github.com/git/git", "ref": ref, "license": "GPL-2.0-only"},
        )
        return root_dir

    def ensure_ltp(self) -> Path:
        if os.environ.get("LTP_ROOT"):
            path = Path(os.environ["LTP_ROOT"]).expanduser().resolve()
            if path.exists():
                return path
        if shutil.which("runltp"):
            return Path(shutil.which("runltp") or "").resolve().parent.parent
        ref = env_value("LTP_REF", "20240129")
        if not self.auto_fetch:
            raise DependencyUnavailable("LTP is not available and auto-fetch is disabled")
        root_dir = self.ensure_git_clone("ltp", "https://github.com/linux-test-project/ltp.git", ref)
        if not (root_dir / "runltp").exists():
            if not (root_dir / "configure").exists() and (root_dir / "Makefile").exists():
                self.run("ltp-autotools", ["make", "autotools"], cwd=root_dir, timeout=1200)
            if (root_dir / "configure").exists():
                self.run("ltp-configure", ["./configure"], cwd=root_dir, timeout=1200)
            self.run("ltp-make", ["make", "-j2"], cwd=root_dir, timeout=3600)
        write_json(root_dir / ".drive9-blackbox-dependency.json", {"name": "ltp", "source": "https://github.com/linux-test-project/ltp", "ref": ref, "license": "GPL-2.0-or-later"})
        return root_dir

    def ensure_fio(self) -> str:
        return self.require_tool("fio", "FIO_BIN")

    def ensure_mdtest(self) -> str:
        if os.environ.get("MDTEST_BIN") and Path(os.environ["MDTEST_BIN"]).exists():
            return os.environ["MDTEST_BIN"]
        found = shutil.which("mdtest")
        if found:
            return found
        raise DependencyUnavailable("mdtest is required; install IOR/mdtest or set MDTEST_BIN")

    def ensure_vdbench(self) -> str:
        if os.environ.get("VDBENCH_BIN") and Path(os.environ["VDBENCH_BIN"]).exists():
            return os.environ["VDBENCH_BIN"]
        found = shutil.which("vdbench")
        if found:
            return found
        raise DependencyUnavailable("vdbench is required; set VDBENCH_BIN or put vdbench on PATH")

    def ensure_fsx(self) -> str:
        if os.environ.get("FSX_BIN") and Path(os.environ["FSX_BIN"]).exists():
            return os.environ["FSX_BIN"]
        found = shutil.which("fsx")
        if found:
            return found
        ref = env_value("SECFS_TEST_REF", "master")
        root_dir = self.ensure_git_clone("secfs.test", "https://github.com/billziss-gh/secfs.test.git", ref)
        candidate = root_dir / "tools" / "bin" / "fsx"
        if not candidate.exists():
            self.run("secfs-test-tools", ["make", "tools"], cwd=root_dir, timeout=1200)
        if candidate.exists():
            write_json(root_dir / ".drive9-blackbox-dependency.json", {"name": "secfs.test", "source": "https://github.com/billziss-gh/secfs.test", "ref": ref, "license": "Apache-2.0"})
            return str(candidate)
        raise DependencyUnavailable("fsx binary not found after preparing secfs.test")

    def ensure_pyxattr(self) -> None:
        proc = subprocess.run(["python3", "-c", "import xattr"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if proc.returncode == 0:
            return
        if not self.auto_fetch:
            raise DependencyUnavailable("python xattr module is missing and auto-fetch is disabled")
        target = self.tools_root / "python" / "pyxattr"
        target.mkdir(parents=True, exist_ok=True)
        self.run("pyxattr-pip", ["python3", "-m", "pip", "install", "--target", str(target), "pyxattr"], timeout=1200)
        os.environ["PYTHONPATH"] = f"{target}:{os.environ.get('PYTHONPATH', '')}"

    def ensure_all_for_module(self, module_id: str) -> None:
        if module_id == "community.pjdfstest":
            self.ensure_pjdfstest()
        elif module_id.startswith("git.official."):
            self.ensure_git_source()
        elif module_id == "community.ltp.fs" or module_id == "community.ltp.syscalls":
            self.ensure_ltp()
        elif module_id == "community.fio":
            self.ensure_fio()
        elif module_id == "community.mdtest":
            self.ensure_mdtest()
        elif module_id == "community.vdbench":
            self.ensure_vdbench()
        elif module_id == "community.fsx":
            self.ensure_fsx()
        elif module_id == "community.pyxattr":
            self.ensure_pyxattr()
