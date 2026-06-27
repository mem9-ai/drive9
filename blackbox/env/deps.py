from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from harness.core import DependencyUnavailable, env_value, progress, write_json
from harness.deps import DependencyManager


DEFAULT_LTP_FS_SCENARIO = "drive9-fs-smoke"
DEFAULT_LTP_SYSCALL_SCENARIO = "drive9-syscalls-fs"
DEFAULT_FIO_REF = "fio-3.42"
DEFAULT_IOR_REF = "4.0.0"

LTP_BUILD_ONLY_TARGETS = (
    "lib",
    "libs",
)

LTP_REQUIRED_INSTALL_TARGETS = (
    "pan",
    "tools",
    "testcases/kernel/fs",
    "testcases/kernel/io/writetest",
)

DEFAULT_LTP_SYSCALL_DIRS = (
    "access",
    "chmod",
    "chown",
    "close",
    "ftruncate",
    "getcwd",
    "getdents",
    "getxattr",
    "link",
    "listxattr",
    "lseek",
    "lstat",
    "mkdir",
    "open",
    "read",
    "rename",
    "rmdir",
    "setxattr",
    "stat",
    "symlink",
    "truncate",
    "unlink",
    "write",
)

DEFAULT_LTP_SYSCALL_CASES = (
    "access01",
    "chmod01",
    "chown01",
    "close01",
    "ftruncate01",
    "getcwd01",
    "getdents01",
    "getxattr01",
    "link02",
    "listxattr01",
    "lseek01",
    "lstat01",
    "mkdir02",
    "open01",
    "read01",
    "rename01",
    "rmdir01",
    "setxattr01",
    "stat01",
    "symlink01",
    "truncate02",
    "unlink01",
    "write01",
)

DEFAULT_LTP_FS_CASES = (
    "openfile01",
    "stream01",
    "ftest01",
    "lftest01",
    "writetest01",
)

LTP_TARGET_FILTER_OUT = {
    # LTP 20240129 getxattr05 includes sched helpers that collide with newer
    # glibc/linux headers. Keep the rest of getxattr coverage.
    "testcases/kernel/syscalls/getxattr": "getxattr05",
}

APT_AUTO_INSTALL_TRUTHY = {"1", "true", "yes", "on"}


class Drive9DependencyManager(DependencyManager):
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
        self.ensure_system_packages("git", "build-essential", "autoconf", "automake", "libtool", "pkg-config", "perl")
        self.ensure_git_tool()
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
        self.ensure_git_tool()
        if os.environ.get("GIT_TEST_SOURCE_DIR"):
            path = Path(os.environ["GIT_TEST_SOURCE_DIR"]).expanduser().resolve()
            if (path / "t").is_dir():
                self.ensure_git_test_build(path)
                return path
        ref = env_value("GIT_TEST_REF", "v2.49.0")
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
        self.ensure_system_packages("build-essential", "gettext", "libcurl4-openssl-dev", "libssl-dev", "make", "perl", "zlib1g-dev")
        self.run("git-build", ["make", "-j2"], cwd=root_dir, timeout=int(os.environ.get("GIT_TEST_BUILD_TIMEOUT_S", "1800")))

    def ensure_ltp(self) -> Path:
        if os.environ.get("LTP_ROOT"):
            path = Path(os.environ["LTP_ROOT"]).expanduser().resolve()
            if self.ltp_install_ready(path):
                return path
            raise DependencyUnavailable(f"LTP_ROOT is not an installed LTP tree: {path}")
        if shutil.which("runltp"):
            path = Path(shutil.which("runltp") or "").resolve().parent
            if self.ltp_install_ready(path):
                return path
        ref = env_value("LTP_REF", "20240129")
        if not self.auto_fetch:
            raise DependencyUnavailable("LTP is not available and auto-fetch is disabled")
        self.ensure_system_packages("git")
        self.ensure_git_tool()
        source_dir = self.ensure_git_clone("ltp", "https://github.com/linux-test-project/ltp.git", ref)
        install_dir = Path(env_value("LTP_INSTALL_ROOT", str(self.tools_root / "ltp-install" / ref))).expanduser().resolve()
        if not self.ltp_install_ready(install_dir):
            self.build_ltp(source_dir, install_dir)
        self.ensure_ltp_fs_scenario(source_dir, install_dir)
        self.ensure_ltp_syscall_scenario(source_dir, install_dir)
        metadata = {
            "name": "ltp",
            "source": "https://github.com/linux-test-project/ltp",
            "ref": ref,
            "license": "GPL-2.0-or-later",
            "source_dir": str(source_dir),
            "install_dir": str(install_dir),
        }
        write_json(source_dir / ".drive9-blackbox-dependency.json", metadata)
        write_json(install_dir / ".drive9-blackbox-dependency.json", metadata)
        return install_dir

    def ltp_install_ready(self, root: Path) -> bool:
        runltp = root / "runltp"
        pan = root / "bin" / "ltp-pan"
        return (
            runltp.exists()
            and os.access(runltp, os.X_OK)
            and pan.exists()
            and os.access(pan, os.X_OK)
            and (root / "runtest").is_dir()
            and (root / "testcases" / "bin").is_dir()
        )

    def build_ltp(self, source_dir: Path, install_dir: Path) -> None:
        self.ensure_system_packages("build-essential", "autoconf", "automake", "bison", "flex", "libacl1-dev", "libtool", "m4", "pkg-config", "perl")
        if not (source_dir / "configure").exists():
            if (source_dir / "Makefile").exists():
                self.run("ltp-autotools", ["make", "autotools"], cwd=source_dir, timeout=1200)
            elif shutil.which("autoreconf") and (source_dir / "configure.ac").exists():
                self.run("ltp-autoreconf", ["autoreconf", "-fi"], cwd=source_dir, timeout=1200)
        if not (source_dir / "configure").exists():
            raise DependencyUnavailable(f"LTP configure script not found after autotools step: {source_dir}")
        shutil.rmtree(install_dir, ignore_errors=True)
        install_dir.mkdir(parents=True, exist_ok=True)
        jobs = env_value("LTP_MAKE_JOBS", "2")
        self.run("ltp-configure", ["./configure", f"--prefix={install_dir}"], cwd=source_dir, timeout=1200)
        for target in LTP_BUILD_ONLY_TARGETS:
            self.ltp_make_target(source_dir, target, jobs=jobs, install=False, required=True)
        for target in LTP_REQUIRED_INSTALL_TARGETS:
            self.ltp_make_target(source_dir, target, jobs=jobs, install=True, required=True)
        skipped_syscalls: list[str] = []
        for name in self.ltp_syscall_dirs():
            target = f"testcases/kernel/syscalls/{name}"
            if not (source_dir / target).is_dir():
                continue
            if not self.ltp_make_target(source_dir, target, jobs=jobs, install=True, required=False):
                skipped_syscalls.append(name)
        self.install_ltp_runtime_files(source_dir, install_dir)
        fs_count = self.ensure_ltp_fs_scenario(source_dir, install_dir)
        if fs_count == 0:
            raise DependencyUnavailable(f"LTP filesystem scenario contains no runnable tests: {install_dir}")
        scenario_count = self.ensure_ltp_syscall_scenario(source_dir, install_dir)
        if scenario_count == 0:
            raise DependencyUnavailable(f"LTP syscall scenario contains no runnable tests: {install_dir}")
        if skipped_syscalls:
            progress(f"dependency warning: skipped LTP syscall dirs: {', '.join(skipped_syscalls)}")
        if not self.ltp_install_ready(install_dir):
            raise DependencyUnavailable(f"LTP install did not produce a runnable tree: {install_dir}")

    def ltp_syscall_dirs(self) -> tuple[str, ...]:
        value = env_value("LTP_SYSCALL_DIRS")
        if not value:
            return DEFAULT_LTP_SYSCALL_DIRS
        return tuple(item for item in value.replace(",", " ").split() if item)

    def ltp_syscall_cases(self) -> tuple[str, ...]:
        value = env_value("LTP_SYSCALL_CASES")
        if not value:
            return DEFAULT_LTP_SYSCALL_CASES
        return tuple(item for item in value.replace(",", " ").split() if item)

    def ltp_fs_cases(self) -> tuple[str, ...]:
        value = env_value("LTP_FS_CASES")
        if not value:
            return DEFAULT_LTP_FS_CASES
        return tuple(item for item in value.replace(",", " ").split() if item)

    def ltp_make_target(self, source_dir: Path, target: str, *, jobs: str, install: bool, required: bool) -> bool:
        name = "ltp-" + target.replace("/", "-")
        filter_out = LTP_TARGET_FILTER_OUT.get(target)
        build_cmd = ["make", f"-j{jobs}", "-C", target]
        if filter_out:
            build_cmd.append(f"FILTER_OUT_MAKE_TARGETS={filter_out}")
        build_cmd.append("all")
        try:
            self.run(f"{name}-all", build_cmd, cwd=source_dir, timeout=int(env_value("LTP_BUILD_TIMEOUT_S", "1800")))
            if install:
                install_cmd = ["make", "-C", target]
                if filter_out:
                    install_cmd.append(f"FILTER_OUT_MAKE_TARGETS={filter_out}")
                install_cmd.append("install")
                self.run(f"{name}-install", install_cmd, cwd=source_dir, timeout=int(env_value("LTP_BUILD_TIMEOUT_S", "1800")))
            return True
        except DependencyUnavailable:
            if required:
                raise
            progress(f"dependency warning: optional LTP target skipped: {target}")
            return False

    def install_ltp_runtime_files(self, source_dir: Path, install_dir: Path) -> None:
        for name in ("runltp", "IDcheck.sh", "ver_linux"):
            src = source_dir / name
            if src.exists():
                dst = install_dir / name
                shutil.copy2(src, dst)
                dst.chmod(0o755)
        self.run("ltp-version", ["make", "Version"], cwd=source_dir, timeout=300)
        if (source_dir / "Version").exists():
            shutil.copy2(source_dir / "Version", install_dir / "Version")
        elif (source_dir / "VERSION").exists():
            shutil.copy2(source_dir / "VERSION", install_dir / "Version")
        for dirname in ("runtest", "scenario_groups", "testscripts"):
            src = source_dir / dirname
            if src.exists():
                shutil.copytree(src, install_dir / dirname, dirs_exist_ok=True)
        (install_dir / "output").mkdir(parents=True, exist_ok=True)
        (install_dir / "results").mkdir(parents=True, exist_ok=True)

    def ensure_ltp_syscall_scenario(self, source_dir: Path, install_dir: Path) -> int:
        runtest_dir = install_dir / "runtest"
        source = source_dir / "runtest" / "syscalls"
        destination = runtest_dir / DEFAULT_LTP_SYSCALL_SCENARIO
        bin_dir = install_dir / "testcases" / "bin"
        if not source.exists() or not bin_dir.is_dir():
            return 0
        installed = {path.name for path in bin_dir.iterdir() if path.is_file() and os.access(path, os.X_OK)}
        wanted = set(self.ltp_syscall_cases())
        lines: list[str] = []
        for raw in source.read_text(encoding="utf-8").splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split()
            if len(parts) >= 2 and parts[0] in wanted and parts[1] in installed:
                lines.append(raw)
        runtest_dir.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            "# Auto-generated by drive9 blackbox from LTP runtest/syscalls.\n"
            "# It keeps filesystem-sensitive syscall tests whose binaries were built.\n"
            + "\n".join(lines)
            + "\n",
            encoding="utf-8",
        )
        return len(lines)

    def ensure_ltp_fs_scenario(self, source_dir: Path, install_dir: Path) -> int:
        runtest_dir = install_dir / "runtest"
        source = source_dir / "runtest" / "fs"
        destination = runtest_dir / DEFAULT_LTP_FS_SCENARIO
        if not source.exists():
            return 0
        wanted = set(self.ltp_fs_cases())
        lines: list[str] = []
        for raw in source.read_text(encoding="utf-8").splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            tag = stripped.split(maxsplit=1)[0]
            if tag in wanted:
                lines.append(raw)
        runtest_dir.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            "# Auto-generated by drive9 blackbox from LTP runtest/fs.\n"
            "# It keeps a bounded filesystem smoke subset suitable for remote FUSE runs.\n"
            + "\n".join(lines)
            + "\n",
            encoding="utf-8",
        )
        return len(lines)

    def ensure_fio(self) -> str:
        if os.environ.get("FIO_BIN") and Path(os.environ["FIO_BIN"]).exists():
            return os.environ["FIO_BIN"]
        found = shutil.which("fio")
        if found:
            progress(f"dependency tool: fio -> {found}")
            return found
        if not self.auto_fetch:
            raise DependencyUnavailable("fio is required and auto-fetch is disabled")
        self.ensure_system_packages("git", "build-essential", "pkg-config")
        self.ensure_git_tool()
        ref = env_value("FIO_REF", DEFAULT_FIO_REF)
        root_dir = self.ensure_git_clone("fio", "https://github.com/axboe/fio.git", ref)
        candidate = root_dir / "fio"
        if not (candidate.exists() and os.access(candidate, os.X_OK)):
            if (root_dir / "configure").exists():
                self.run("fio-configure", ["./configure"], cwd=root_dir, timeout=600)
            jobs = env_value("FIO_MAKE_JOBS", env_value("MAKE_JOBS", "2"))
            self.run("fio-make", ["make", f"-j{jobs}"], cwd=root_dir, timeout=int(env_value("FIO_BUILD_TIMEOUT_S", "1800")))
        if candidate.exists() and os.access(candidate, os.X_OK):
            write_json(
                root_dir / ".drive9-blackbox-dependency.json",
                {"name": "fio", "source": "https://github.com/axboe/fio", "ref": ref, "license": "GPL-2.0-only"},
            )
            return str(candidate)
        raise DependencyUnavailable(f"fio binary not found after build: {candidate}")

    def ensure_mdtest(self) -> str:
        if os.environ.get("MDTEST_BIN") and Path(os.environ["MDTEST_BIN"]).exists():
            return os.environ["MDTEST_BIN"]
        found = shutil.which("mdtest")
        if found:
            progress(f"dependency tool: mdtest -> {found}")
            return found
        if not self.auto_fetch:
            raise DependencyUnavailable("mdtest is required and auto-fetch is disabled")
        self.ensure_system_packages("git", "build-essential", "autoconf", "automake", "libtool", "m4", "pkg-config", "perl")
        self.ensure_git_tool()
        mpicc = self.ensure_mpi_compiler()
        ref = env_value("IOR_REF", DEFAULT_IOR_REF)
        root_dir = self.ensure_git_clone("ior", "https://github.com/hpc/ior.git", ref)
        self.patch_ior_source(root_dir)
        candidates = [root_dir / "src" / "mdtest", root_dir / "mdtest"]
        candidate = next((path for path in candidates if path.exists() and os.access(path, os.X_OK)), None)
        if candidate is None:
            if not (root_dir / "configure").exists():
                if (root_dir / "bootstrap").exists():
                    self.run("ior-bootstrap", ["./bootstrap"], cwd=root_dir, timeout=1200)
                elif (root_dir / "autogen.sh").exists():
                    self.run("ior-autogen", ["./autogen.sh"], cwd=root_dir, timeout=1200)
            if (root_dir / "configure").exists():
                self.run(
                    "ior-configure",
                    ["./configure", f"MPICC={mpicc}", "--with-mpiio=no", "--with-hdf5=no", "--with-ncmpi=no"],
                    cwd=root_dir,
                    timeout=1200,
                )
                jobs = env_value("IOR_MAKE_JOBS", env_value("MAKE_JOBS", "2"))
                self.run("ior-make", ["make", f"-j{jobs}"], cwd=root_dir, timeout=int(env_value("IOR_BUILD_TIMEOUT_S", "1800")))
            else:
                raise DependencyUnavailable(f"IOR configure script not found after bootstrap: {root_dir}")
            candidate = next((path for path in candidates if path.exists() and os.access(path, os.X_OK)), None)
        if candidate is not None:
            write_json(
                root_dir / ".drive9-blackbox-dependency.json",
                {"name": "IOR/mdtest", "source": "https://github.com/hpc/ior", "ref": ref, "license": "GPL-2.0-only"},
            )
            return str(candidate)
        raise DependencyUnavailable(f"mdtest binary not found after building IOR: {root_dir}")

    def patch_ior_source(self, root_dir: Path) -> None:
        option = root_dir / "src" / "option.c"
        if not option.exists():
            return
        old = "void(*fp)() = o->variable;\n                  fp(arg);"
        new = "void(*fp)(char *) = o->variable;\n                  fp(arg);"
        text = option.read_text(encoding="utf-8")
        if old not in text:
            return
        progress("dependency patch: applying IOR option callback compatibility patch")
        option.write_text(text.replace(old, new), encoding="utf-8")

    def ensure_mpi_compiler(self) -> str:
        if os.environ.get("MPICC") and Path(os.environ["MPICC"]).exists():
            return os.environ["MPICC"]
        found = shutil.which("mpicc")
        if found:
            progress(f"dependency tool: mpicc -> {found}")
            return found
        if env_value("AUTO_INSTALL_SYSTEM_DEPS", "1").lower() not in APT_AUTO_INSTALL_TRUTHY:
            raise DependencyUnavailable("mpicc is required to build IOR/mdtest; install MPICH/OpenMPI or set MPICC")
        self.ensure_system_packages("mpich", "libmpich-dev")
        found = shutil.which("mpicc")
        if found:
            progress(f"dependency tool: mpicc -> {found}")
            return found
        raise DependencyUnavailable("mpicc is required to build IOR/mdtest; install MPICH/OpenMPI or set MPICC")

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
        try:
            ltp = self.ensure_ltp()
            candidate = ltp / "testcases" / "bin" / "fsx-linux"
            if candidate.exists() and os.access(candidate, os.X_OK):
                return str(candidate)
        except DependencyUnavailable:
            pass
        ref = env_value("SECFS_TEST_REF", "master")
        self.ensure_system_packages("git", "build-essential", "make")
        self.ensure_git_tool()
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
        self.ensure_system_packages("python3-pip", "python3-dev", "build-essential")
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
