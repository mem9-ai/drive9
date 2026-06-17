from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from harness.core import DependencyUnavailable, env_value, write_json
from harness.deps import DependencyManager


class FuseDependencyManager(DependencyManager):
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
                self.ensure_git_test_build(path)
                return path
        ref = env_value("GIT_TEST_REF", "v2.46.2")
        root_dir = self.ensure_git_clone("git", "https://github.com/git/git.git", ref)
        self.ensure_git_test_build(root_dir)
        write_json(
            root_dir / ".drive9-blackbox-dependency.json",
            {"name": "git", "source": "https://github.com/git/git", "ref": ref, "license": "GPL-2.0-only"},
        )
        return root_dir

    def ensure_git_test_build(self, root_dir: Path) -> None:
        if (root_dir / "GIT-BUILD-OPTIONS").exists() and (root_dir / "bin-wrappers" / "git").exists() and (root_dir / "t" / "helper" / "test-tool").exists():
            return
        if not self.auto_fetch:
            raise DependencyUnavailable("Git source is not built and auto-fetch is disabled")
        self.run("git-build", ["make", "-j2"], cwd=root_dir, timeout=int(os.environ.get("GIT_TEST_BUILD_TIMEOUT_S", "1800")))

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
