from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json

DEFAULT_SQLITE_REF = "master"
SQLITE_SOURCE = "https://github.com/sqlite/sqlite.git"
SQLITE_LICENSE = "public domain (SQLite blessing)"

REQUIRED_SCRIPTS = ("multiwrite01.test", "crash01.test")


@dataclass(frozen=True)
class SqliteTools:
    """Resolved official SQLite probe binaries plus the mptest script directory."""

    speedtest1: Path
    mptester: Path
    kvtest: Path
    threadtest3: Path
    scripts_dir: Path
    source_root: Path


def ensure_dependencies(ctx: Context) -> None:
    ensure_sqlite_tools(ctx)


def ensure_sqlite_tools(ctx: Context) -> SqliteTools:
    """Resolve or fetch+build official SQLite speedtest1, mptester, kvtest, and threadtest3."""
    source_root = _env_path("SQLITE_SRC", "SQLITE_DIR")
    if source_root is None:
        if not ctx.deps.auto_fetch:
            raise DependencyUnavailable("sqlite tools are required and auto-fetch is disabled")
        ctx.deps.ensure_system_packages("git", "build-essential", "zlib1g-dev")
        ctx.deps.ensure_git_tool()
        ref = os.environ.get("SQLITE_REF") or env_value("SQLITE_REF", DEFAULT_SQLITE_REF) or DEFAULT_SQLITE_REF
        source_root = ctx.deps.ensure_git_clone("sqlite", SQLITE_SOURCE, ref)
        tools = _resolve_tools(source_root)
        if not _binaries_ready(tools):
            _build_sqlite_tools(ctx, source_root)
            tools = _resolve_tools(source_root)
        _validate_tools(tools)
        write_json(
            source_root / ".drive9-blackbox-dependency.json",
            {
                "name": "sqlite",
                "source": SQLITE_SOURCE,
                "ref": ref,
                "license": SQLITE_LICENSE,
                "binaries": {
                    "speedtest1": str(tools.speedtest1),
                    "mptester": str(tools.mptester),
                    "kvtest": str(tools.kvtest),
                    "threadtest3": str(tools.threadtest3),
                },
            },
        )
        return tools

    tools = _resolve_tools(source_root)
    if not _binaries_ready(tools):
        if not ctx.deps.auto_fetch:
            raise DependencyUnavailable(
                f"sqlite tools missing under SQLITE_SRC={source_root} and auto-fetch is disabled"
            )
        _build_sqlite_tools(ctx, source_root)
        tools = _resolve_tools(source_root)
    _validate_tools(tools)
    return tools


def _env_path(*names: str) -> Path | None:
    for name in names:
        raw = os.environ.get(name, "").strip()
        if not raw:
            continue
        path = Path(raw).expanduser()
        if path.exists():
            return path.resolve()
    return None


def _is_exe(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _resolve_tools(source_root: Path) -> SqliteTools:
    return SqliteTools(
        speedtest1=_env_path("SPEEDTEST1_BIN", "SQLITE_SPEEDTEST1_BIN") or source_root / "speedtest1",
        mptester=_env_path("MPTEST_BIN", "MPTESTER_BIN", "SQLITE_MPTEST_BIN") or source_root / "mptester",
        kvtest=_env_path("KVTEST_BIN", "SQLITE_KVTEST_BIN") or source_root / "kvtest",
        threadtest3=_env_path("THREADTEST3_BIN", "SQLITE_THREADTEST3_BIN") or source_root / "threadtest3",
        scripts_dir=source_root / "mptest",
        source_root=source_root,
    )


def _binaries_ready(tools: SqliteTools) -> bool:
    return all(_is_exe(path) for path in (tools.speedtest1, tools.mptester, tools.kvtest, tools.threadtest3))


def _validate_tools(tools: SqliteTools) -> None:
    for label, path in (
        ("speedtest1", tools.speedtest1),
        ("mptester", tools.mptester),
        ("kvtest", tools.kvtest),
        ("threadtest3", tools.threadtest3),
    ):
        if not _is_exe(path):
            raise DependencyUnavailable(f"{label} is not executable: {path}")
    if not tools.scripts_dir.is_dir():
        raise DependencyUnavailable(f"sqlite mptest scripts directory missing: {tools.scripts_dir}")
    missing = [name for name in REQUIRED_SCRIPTS if not (tools.scripts_dir / name).is_file()]
    if missing:
        raise DependencyUnavailable(
            f"sqlite mptest scripts missing under {tools.scripts_dir}: {', '.join(missing)}"
        )


def _build_sqlite_tools(ctx: Context, root: Path) -> None:
    """Configure the tree, generate sqlite3.c, then compile the four probe binaries."""
    configure = root / "configure"
    if not configure.is_file():
        raise DependencyUnavailable(f"sqlite configure script not found in {root}")
    jobs = env_value("SQLITE_MAKE_JOBS", os.environ.get("SQLITE_MAKE_JOBS", env_value("MAKE_JOBS", "2"))) or "2"
    timeout = int(env_value("SQLITE_BUILD_TIMEOUT_S", os.environ.get("SQLITE_BUILD_TIMEOUT_S", "1800")) or "1800")
    ctx.deps.run("sqlite-configure", [str(configure)], cwd=root, timeout=600)
    ctx.deps.run("sqlite-amalgamation", ["make", f"-j{jobs}", "sqlite3.h", "sqlite3.c"], cwd=root, timeout=timeout)
    _compile_sqlite_tools(ctx, root)


def _compile_sqlite_tools(ctx: Context, root: Path) -> None:
    amalgamation = root / "sqlite3.c"
    header = root / "sqlite3.h"
    if not amalgamation.is_file() or not header.is_file():
        raise DependencyUnavailable(f"sqlite amalgamation missing after build: {amalgamation}")
    cc = os.environ.get("CC", "cc")
    libs = ["-lpthread", "-lm"]
    if sys.platform.startswith("linux"):
        libs.append("-ldl")
    common = [cc, "-O2", "-DHAVE_USLEEP", "-I", str(root)]
    targets: list[tuple[str, list[Path], list[str]]] = [
        ("speedtest1", [root / "test" / "speedtest1.c"], []),
        ("mptester", [root / "mptest" / "mptest.c"], []),
        ("kvtest", [root / "test" / "kvtest.c"], []),
        (
            "threadtest3",
            [root / "test" / "threadtest3.c", root / "src" / "test_multiplex.c"],
            ["-I", str(root / "src"), "-I", str(root / "test")],
        ),
    ]
    for name, sources, extra_cflags in targets:
        for path in sources:
            if not path.is_file():
                raise DependencyUnavailable(f"sqlite source file missing after amalgamation build: {path}")
        progress(f"dependency compile: sqlite {name} from amalgamation")
        ctx.deps.run(
            f"sqlite-cc-{name}",
            [*common, *extra_cflags, "-o", str(root / name), *[str(path) for path in sources], str(amalgamation), *libs],
            cwd=root,
            timeout=600,
        )
