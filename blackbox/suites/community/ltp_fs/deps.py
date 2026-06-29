"""Self-contained LTP dependency fetcher and builder."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json

DEFAULT_LTP_FS_SCENARIO = "drive9-fs-smoke"
DEFAULT_LTP_SYSCALL_SCENARIO = "drive9-syscalls-fs"

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


def ensure_dependencies(ctx: Context) -> None:
    ensure_ltp(ctx)


def ensure_ltp(ctx: Context) -> Path:
    """Resolve or fetch+build LTP. Returns the install tree root."""
    if os.environ.get("LTP_ROOT"):
        path = Path(os.environ["LTP_ROOT"]).expanduser().resolve()
        if _ltp_install_ready(path):
            return path
        raise DependencyUnavailable(f"LTP_ROOT is not an installed LTP tree: {path}")
    if shutil.which("runltp"):
        path = Path(shutil.which("runltp") or "").resolve().parent
        if _ltp_install_ready(path):
            return path
    ref = env_value("LTP_REF", "20240129")
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("LTP is not available and auto-fetch is disabled")
    ctx.deps.ensure_system_packages("git")
    ctx.deps.ensure_git_tool()
    source_dir = ctx.deps.ensure_git_clone("ltp", "https://github.com/linux-test-project/ltp.git", ref)
    install_dir = Path(env_value("LTP_INSTALL_ROOT", str(ctx.deps.tools_root / "ltp-install" / ref))).expanduser().resolve()
    if not _ltp_install_ready(install_dir):
        _build_ltp(ctx, source_dir, install_dir)
    _ensure_ltp_fs_scenario(source_dir, install_dir)
    _ensure_ltp_syscall_scenario(source_dir, install_dir)
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


def _ltp_install_ready(root: Path) -> bool:
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


def _build_ltp(ctx: Context, source_dir: Path, install_dir: Path) -> None:
    ctx.deps.ensure_system_packages("build-essential", "autoconf", "automake", "bison", "flex", "libacl1-dev", "libtool", "m4", "pkg-config", "perl")
    if not (source_dir / "configure").exists():
        if (source_dir / "Makefile").exists():
            ctx.deps.run("ltp-autotools", ["make", "autotools"], cwd=source_dir, timeout=1200)
        elif shutil.which("autoreconf") and (source_dir / "configure.ac").exists():
            ctx.deps.run("ltp-autoreconf", ["autoreconf", "-fi"], cwd=source_dir, timeout=1200)
    if not (source_dir / "configure").exists():
        raise DependencyUnavailable(f"LTP configure script not found after autotools step: {source_dir}")
    shutil.rmtree(install_dir, ignore_errors=True)
    install_dir.mkdir(parents=True, exist_ok=True)
    jobs = env_value("LTP_MAKE_JOBS", "2")
    ctx.deps.run("ltp-configure", ["./configure", f"--prefix={install_dir}"], cwd=source_dir, timeout=1200)
    for target in LTP_BUILD_ONLY_TARGETS:
        _ltp_make_target(ctx, source_dir, target, jobs=jobs, install=False, required=True)
    for target in LTP_REQUIRED_INSTALL_TARGETS:
        _ltp_make_target(ctx, source_dir, target, jobs=jobs, install=True, required=True)
    skipped_syscalls: list[str] = []
    for name in _ltp_syscall_dirs():
        target = f"testcases/kernel/syscalls/{name}"
        if not (source_dir / target).is_dir():
            continue
        if not _ltp_make_target(ctx, source_dir, target, jobs=jobs, install=True, required=False):
            skipped_syscalls.append(name)
    _install_ltp_runtime_files(ctx, source_dir, install_dir)
    fs_count = _ensure_ltp_fs_scenario(source_dir, install_dir)
    if fs_count == 0:
        raise DependencyUnavailable(f"LTP filesystem scenario contains no runnable tests: {install_dir}")
    scenario_count = _ensure_ltp_syscall_scenario(source_dir, install_dir)
    if scenario_count == 0:
        raise DependencyUnavailable(f"LTP syscall scenario contains no runnable tests: {install_dir}")
    if skipped_syscalls:
        progress(f"dependency warning: skipped LTP syscall dirs: {', '.join(skipped_syscalls)}")
    if not _ltp_install_ready(install_dir):
        raise DependencyUnavailable(f"LTP install did not produce a runnable tree: {install_dir}")


def _ltp_syscall_dirs() -> tuple[str, ...]:
    value = env_value("LTP_SYSCALL_DIRS")
    if not value:
        return DEFAULT_LTP_SYSCALL_DIRS
    return tuple(item for item in value.replace(",", " ").split() if item)


def _ltp_syscall_cases() -> tuple[str, ...]:
    value = env_value("LTP_SYSCALL_CASES")
    if not value:
        return DEFAULT_LTP_SYSCALL_CASES
    return tuple(item for item in value.replace(",", " ").split() if item)


def _ltp_fs_cases() -> tuple[str, ...]:
    value = env_value("LTP_FS_CASES")
    if not value:
        return DEFAULT_LTP_FS_CASES
    return tuple(item for item in value.replace(",", " ").split() if item)


def _ltp_make_target(ctx: Context, source_dir: Path, target: str, *, jobs: str, install: bool, required: bool) -> bool:
    name = "ltp-" + target.replace("/", "-")
    filter_out = LTP_TARGET_FILTER_OUT.get(target)
    build_cmd = ["make", f"-j{jobs}", "-C", target]
    if filter_out:
        build_cmd.append(f"FILTER_OUT_MAKE_TARGETS={filter_out}")
    build_cmd.append("all")
    try:
        ctx.deps.run(f"{name}-all", build_cmd, cwd=source_dir, timeout=int(env_value("LTP_BUILD_TIMEOUT_S", "1800")))
        if install:
            install_cmd = ["make", "-C", target]
            if filter_out:
                install_cmd.append(f"FILTER_OUT_MAKE_TARGETS={filter_out}")
            install_cmd.append("install")
            ctx.deps.run(f"{name}-install", install_cmd, cwd=source_dir, timeout=int(env_value("LTP_BUILD_TIMEOUT_S", "1800")))
        return True
    except DependencyUnavailable:
        if required:
            raise
        progress(f"dependency warning: optional LTP target skipped: {target}")
        return False


def _install_ltp_runtime_files(ctx: Context, source_dir: Path, install_dir: Path) -> None:
    for name in ("runltp", "IDcheck.sh", "ver_linux"):
        src = source_dir / name
        if src.exists():
            dst = install_dir / name
            shutil.copy2(src, dst)
            dst.chmod(0o755)
    ctx.deps.run("ltp-version", ["make", "Version"], cwd=source_dir, timeout=300)
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


def _ensure_ltp_syscall_scenario(source_dir: Path, install_dir: Path) -> int:
    runtest_dir = install_dir / "runtest"
    source = source_dir / "runtest" / "syscalls"
    destination = runtest_dir / DEFAULT_LTP_SYSCALL_SCENARIO
    bin_dir = install_dir / "testcases" / "bin"
    if not source.exists() or not bin_dir.is_dir():
        return 0
    installed = {path.name for path in bin_dir.iterdir() if path.is_file() and os.access(path, os.X_OK)}
    wanted = set(_ltp_syscall_cases())
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


def _ensure_ltp_fs_scenario(source_dir: Path, install_dir: Path) -> int:
    runtest_dir = install_dir / "runtest"
    source = source_dir / "runtest" / "fs"
    destination = runtest_dir / DEFAULT_LTP_FS_SCENARIO
    if not source.exists():
        return 0
    wanted = set(_ltp_fs_cases())
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