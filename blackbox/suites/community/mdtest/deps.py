from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json

DEFAULT_IOR_REF = "4.0.0"

APT_AUTO_INSTALL_TRUTHY = {"1", "true", "yes", "on"}


def ensure_dependencies(ctx: Context) -> None:
    ensure_mdtest(ctx)


def ensure_mdtest(ctx: Context) -> str:
    """Resolve or fetch+build mdtest (via IOR). Returns the mdtest binary path."""
    if os.environ.get("MDTEST_BIN") and Path(os.environ["MDTEST_BIN"]).exists():
        return os.environ["MDTEST_BIN"]
    found = shutil.which("mdtest")
    if found:
        progress(f"dependency tool: mdtest -> {found}")
        return found
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("mdtest is required and auto-fetch is disabled")
    ctx.deps.ensure_system_packages("git", "build-essential", "autoconf", "automake", "libtool", "m4", "pkg-config", "perl")
    ctx.deps.ensure_git_tool()
    mpicc = _ensure_mpi_compiler(ctx)
    ref = env_value("IOR_REF", DEFAULT_IOR_REF)
    root_dir = ctx.deps.ensure_git_clone("ior", "https://github.com/hpc/ior.git", ref)
    _patch_ior_source(root_dir)
    candidates = [root_dir / "src" / "mdtest", root_dir / "mdtest"]
    candidate = next((path for path in candidates if path.exists() and os.access(path, os.X_OK)), None)
    if candidate is None:
        if not (root_dir / "configure").exists():
            if (root_dir / "bootstrap").exists():
                ctx.deps.run("ior-bootstrap", ["./bootstrap"], cwd=root_dir, timeout=1200)
            elif (root_dir / "autogen.sh").exists():
                ctx.deps.run("ior-autogen", ["./autogen.sh"], cwd=root_dir, timeout=1200)
        if (root_dir / "configure").exists():
            ctx.deps.run(
                "ior-configure",
                ["./configure", f"MPICC={mpicc}", "--with-mpiio=no", "--with-hdf5=no", "--with-ncmpi=no"],
                cwd=root_dir,
                timeout=1200,
            )
            jobs = env_value("IOR_MAKE_JOBS", env_value("MAKE_JOBS", "2"))
            ctx.deps.run("ior-make", ["make", f"-j{jobs}"], cwd=root_dir, timeout=int(env_value("IOR_BUILD_TIMEOUT_S", "1800")))
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


def _patch_ior_source(root_dir: Path) -> None:
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


def _ensure_mpi_compiler(ctx: Context) -> str:
    if os.environ.get("MPICC") and Path(os.environ["MPICC"]).exists():
        return os.environ["MPICC"]
    found = shutil.which("mpicc")
    if found:
        progress(f"dependency tool: mpicc -> {found}")
        return found
    if env_value("AUTO_INSTALL_SYSTEM_DEPS", "1").lower() not in APT_AUTO_INSTALL_TRUTHY:
        raise DependencyUnavailable("mpicc is required to build IOR/mdtest; install MPICH/OpenMPI or set MPICC")
    ctx.deps.ensure_system_packages("mpich", "libmpich-dev")
    found = shutil.which("mpicc")
    if found:
        progress(f"dependency tool: mpicc -> {found}")
        return found
    raise DependencyUnavailable("mpicc is required to build IOR/mdtest; install MPICH/OpenMPI or set MPICC")