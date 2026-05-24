#!/usr/bin/env python3
"""Run drive9 FUSE versus native repo clone/build benchmarks.

The harness intentionally uses only the Python standard library so it can run
early on a fresh EC2 host after the shell bootstrap installs language tools.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import errno
import fnmatch
import json
import os
import platform
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class BenchError(Exception):
    """Benchmark setup or execution failed."""


@dataclass(frozen=True)
class RepoSpec:
    id: str
    language: str
    url: str
    ref: str
    build_dir: str
    prewarm: list[str]
    build: list[str]
    clean: list[str]
    commit: str | None = None


@dataclass(frozen=True)
class BenchCase:
    name: str
    runs: int
    storages: list[str]
    repos: list[RepoSpec]


@dataclass
class PhaseResult:
    session: str
    case: str
    repo: str
    language: str
    storage: str
    run: int
    phase: str
    status: str
    duration_seconds: float
    started_at: str
    ended_at: str
    exit_code: int
    cwd: str
    stdout: str
    stderr: str
    commands: list[str]
    error: str = ""
    commit: str = ""

    def as_event(self) -> dict[str, Any]:
        return {
            "type": "phase",
            "session": self.session,
            "case": self.case,
            "repo": self.repo,
            "language": self.language,
            "storage": self.storage,
            "run": self.run,
            "phase": self.phase,
            "status": self.status,
            "duration_seconds": round(self.duration_seconds, 6),
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "exit_code": self.exit_code,
            "cwd": self.cwd,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "commands": self.commands,
            "error": self.error,
            "commit": self.commit,
        }


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_case_path() -> Path:
    return repo_root() / "bench" / "cases" / "repo-build.json"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def session_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_case(path: Path) -> BenchCase:
    raw = json.loads(path.read_text(encoding="utf-8"))
    repos = []
    for item in raw.get("repos", []):
        repos.append(
            RepoSpec(
                id=required(item, "id"),
                language=required(item, "language"),
                url=required(item, "url"),
                ref=required(item, "ref"),
                build_dir=item.get("build_dir", "."),
                prewarm=list(item.get("prewarm", [])),
                build=list(item.get("build", [])),
                clean=list(item.get("clean", [])),
                commit=item.get("commit"),
            )
        )
    if not repos:
        raise BenchError(f"case {path} has no repos")
    runs = int(raw.get("runs", 3))
    if runs <= 0:
        raise BenchError("case runs must be positive")
    storages = list(raw.get("storages", ["native", "fuse"]))
    validate_storages(storages)
    return BenchCase(name=required(raw, "name"), runs=runs, storages=storages, repos=repos)


def required(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise BenchError(f"missing required string field {key!r}")
    return value


def validate_storages(storages: list[str]) -> None:
    valid = {"native", "fuse"}
    for storage in storages:
        if storage not in valid:
            raise BenchError(f"unsupported storage {storage!r}; expected one of {sorted(valid)}")


def parse_storages(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    storages = [item.strip() for item in value.split(",") if item.strip()]
    validate_storages(storages)
    return storages


def filter_repos(case: BenchCase, value: str | None) -> BenchCase:
    if not value:
        return case
    wanted = [item.strip() for item in value.split(",") if item.strip()]
    if not wanted:
        return case
    by_id = {repo.id: repo for repo in case.repos}
    missing = [repo_id for repo_id in wanted if repo_id not in by_id]
    if missing:
        raise BenchError(f"unknown repo(s) in --repos/BENCH_REPOS: {', '.join(missing)}")
    return BenchCase(
        name=case.name,
        runs=case.runs,
        storages=case.storages,
        repos=[by_id[repo_id] for repo_id in wanted],
    )


def bench_home_from_args(value: str | None) -> Path:
    raw = value or os.environ.get("BENCH_HOME") or "/tmp/drive9-bench"
    return Path(raw).expanduser().resolve()


def rel_or_abs(path: str) -> str:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    return str((repo_root() / candidate).resolve())


def drive9_cli() -> str:
    configured = os.environ.get("BENCH_DRIVE9_CLI")
    if configured:
        return rel_or_abs(configured)
    found = shutil.which("drive9")
    if found:
        return found
    local = repo_root() / "bin" / "drive9"
    if local.exists():
        return str(local)
    return "drive9"


def command_display(command: str | list[str]) -> str:
    if isinstance(command, str):
        return command
    return shlex.join(str(part) for part in command)


def append_log_header(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")


def run_one_command(
    command: str | list[str],
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    *,
    shell: bool = False,
    dry_run: bool = False,
    timeout: float | None = None,
) -> int:
    display = command_display(command)
    append_log_header(stdout_path, f"$ {display}")
    if dry_run:
        append_log_header(stdout_path, "(dry-run: command not executed)")
        return 0
    with stdout_path.open("ab") as out, stderr_path.open("ab") as err:
        try:
            proc = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                stdout=out,
                stderr=err,
                shell=shell,
                timeout=timeout,
                check=False,
            )
            return int(proc.returncode)
        except subprocess.TimeoutExpired:
            err.write(f"\nbenchmark command timed out after {timeout} seconds\n".encode())
            return 124
        except OSError as exc:
            err.write(f"\nbenchmark command failed to start: {exc}\n".encode())
            return 127


def run_phase(
    *,
    session: str,
    case_name: str,
    repo: RepoSpec,
    storage: str,
    run_index: int,
    phase: str,
    commands: list[str | list[str]],
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    commit: str,
    dry_run: bool = False,
    shell: bool = False,
) -> PhaseResult:
    started_at = utc_now()
    start = time.monotonic()
    exit_code = 0
    error = ""
    for command in commands:
        exit_code = run_one_command(
            command,
            cwd,
            env,
            stdout_path,
            stderr_path,
            shell=shell,
            dry_run=dry_run,
            timeout=phase_timeout_seconds(env, phase),
        )
        if exit_code != 0:
            error = f"command failed with exit code {exit_code}: {command_display(command)}"
            break
    duration = time.monotonic() - start
    ended_at = utc_now()
    status = "ok" if exit_code == 0 else "failed"
    return PhaseResult(
        session=session,
        case=case_name,
        repo=repo.id,
        language=repo.language,
        storage=storage,
        run=run_index,
        phase=phase,
        status=status,
        duration_seconds=duration,
        started_at=started_at,
        ended_at=ended_at,
        exit_code=exit_code,
        cwd=str(cwd),
        stdout=str(stdout_path),
        stderr=str(stderr_path),
        commands=[command_display(command) for command in commands],
        error=error,
        commit=commit,
    )


def bool_env(env: dict[str, str], key: str, default: bool = False) -> bool:
    raw = env.get(key, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def phase_timeout_seconds(env: dict[str, str], phase: str) -> float | None:
    phase_key = f"BENCH_{phase.upper()}_TIMEOUT_SECONDS"
    raw = env.get(phase_key, "").strip() or env.get("BENCH_COMMAND_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return None
    try:
        timeout = float(raw)
    except ValueError as exc:
        raise BenchError(f"{phase_key} or BENCH_COMMAND_TIMEOUT_SECONDS must be a number, got {raw!r}") from exc
    if timeout <= 0:
        raise BenchError(f"{phase_key} or BENCH_COMMAND_TIMEOUT_SECONDS must be > 0")
    return timeout


def int_env(env: dict[str, str], key: str, default: int) -> int:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise BenchError(f"{key} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise BenchError(f"{key} must be >= 0")
    return value


def float_env(env: dict[str, str], key: str, default: float) -> float:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise BenchError(f"{key} must be a number, got {raw!r}") from exc
    if value < 0:
        raise BenchError(f"{key} must be >= 0")
    return value


def append_log_line(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")


def fsync_one(path: Path, *, directory: bool = False) -> None:
    flags = os.O_RDONLY
    if directory and hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def is_vanished_error(exc: OSError) -> bool:
    return isinstance(exc, FileNotFoundError) or exc.errno == errno.ENOENT


def run_fsync_tree(path: Path, stdout_path: Path, stderr_path: Path, *, dry_run: bool = False) -> int:
    append_log_header(stdout_path, f"$ bench-fsync-tree {shlex.quote(str(path))}")
    if dry_run:
        append_log_header(stdout_path, "(dry-run: fsync not executed)")
        return 0
    if not path.exists():
        append_log_line(stderr_path, f"bench-fsync-tree: path does not exist: {path}")
        return 1

    start = time.monotonic()
    file_count = 0
    dir_count = 0
    errors = 0
    try:
        if path.is_file():
            fsync_one(path)
            file_count += 1
        else:
            for root, dirs, files in os.walk(path, topdown=False, followlinks=False):
                root_path = Path(root)
                for name in files:
                    candidate = root_path / name
                    try:
                        if not candidate.is_symlink():
                            fsync_one(candidate)
                            file_count += 1
                    except OSError as exc:
                        if is_vanished_error(exc):
                            append_log_line(stderr_path, f"bench-fsync-tree: skipped vanished file {candidate}")
                            continue
                        append_log_line(stderr_path, f"bench-fsync-tree: fsync file {candidate}: {exc}")
                        errors += 1
                for name in dirs:
                    candidate = root_path / name
                    try:
                        if not candidate.is_symlink():
                            fsync_one(candidate, directory=True)
                            dir_count += 1
                    except OSError as exc:
                        if is_vanished_error(exc):
                            append_log_line(stderr_path, f"bench-fsync-tree: skipped vanished dir {candidate}")
                            continue
                        append_log_line(stderr_path, f"bench-fsync-tree: fsync dir {candidate}: {exc}")
                        errors += 1
                try:
                    fsync_one(root_path, directory=True)
                    dir_count += 1
                except OSError as exc:
                    if is_vanished_error(exc):
                        append_log_line(stderr_path, f"bench-fsync-tree: skipped vanished dir {root_path}")
                        continue
                    append_log_line(stderr_path, f"bench-fsync-tree: fsync dir {root_path}: {exc}")
                    errors += 1
    finally:
        duration = time.monotonic() - start
        append_log_line(
            stdout_path,
            f"bench-fsync-tree: files={file_count} dirs={dir_count} errors={errors} duration_seconds={duration:.6f}",
        )
    return 0 if errors == 0 else 1


def run_syncfs(path: Path, cwd: Path, env: dict[str, str], stdout_path: Path, stderr_path: Path, *, dry_run: bool) -> int:
    if platform.system() != "Linux" or shutil.which("sync") is None:
        return 0
    return run_one_command(["sync", "-f", str(path)], cwd, env, stdout_path, stderr_path, dry_run=dry_run, timeout=120)


def settle_after_fsync(env: dict[str, str]) -> None:
    delay = float_env(env, "BENCH_GIT_FSYNC_SETTLE_SECONDS", 0.0)
    if delay > 0:
        time.sleep(delay)


def run_fsync_boundary(
    checkout_dir: Path,
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    *,
    dry_run: bool,
) -> int:
    git_dir = checkout_dir / ".git"
    code = run_fsync_tree(git_dir, stdout_path, stderr_path, dry_run=dry_run)
    if code != 0:
        return code
    code = run_syncfs(git_dir, cwd, env, stdout_path, stderr_path, dry_run=dry_run)
    if code != 0:
        return code
    settle_after_fsync(env)
    return 0


def run_write_detached_head(checkout_dir: Path, commit: str, stdout_path: Path, stderr_path: Path, *, dry_run: bool) -> int:
    head_path = checkout_dir / ".git" / "HEAD"
    append_log_header(stdout_path, f"$ bench-write-detached-head {commit}")
    if dry_run:
        append_log_header(stdout_path, "(dry-run: HEAD not written)")
        return 0
    data = f"{commit}\n".encode("ascii")
    try:
        head_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(head_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        fsync_one(head_path.parent, directory=True)
    except OSError as exc:
        append_log_line(stderr_path, f"bench-write-detached-head: {exc}")
        return 1
    return 0


def git_checkout_mode(args: argparse.Namespace, env: dict[str, str]) -> str:
    mode = args.git_checkout_mode or env.get("BENCH_GIT_CHECKOUT_MODE", "standard")
    if mode not in {"standard", "read-tree"}:
        raise BenchError("BENCH_GIT_CHECKOUT_MODE/--git-checkout-mode must be standard or read-tree")
    return mode


def run_clone_phase(
    *,
    session: str,
    case_name: str,
    repo: RepoSpec,
    storage: str,
    run_index: int,
    commit: str,
    checkout_dir: Path,
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    dry_run: bool,
    args: argparse.Namespace,
) -> PhaseResult:
    mode = git_checkout_mode(args, env)
    fsync_boundaries = bool(args.git_fsync or bool_env(env, "BENCH_GIT_FSYNC", False))
    retries = args.git_checkout_retries
    if retries is None:
        retries = int_env(env, "BENCH_GIT_CHECKOUT_RETRIES", 0)
    if mode == "standard" and not fsync_boundaries and retries == 0:
        return run_phase(
            session=session,
            case_name=case_name,
            repo=repo,
            storage=storage,
            run_index=run_index,
            phase="clone",
            commands=clone_checkout_commands(repo, commit, checkout_dir),
            cwd=cwd,
            env=env,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            commit=commit,
            dry_run=dry_run,
        )

    started_at = utc_now()
    start = time.monotonic()
    commands: list[str] = []
    exit_code = 0
    error = ""

    clone_cmd = ["git", "clone", "--no-checkout", repo.url, str(checkout_dir)]
    commands.append(command_display(clone_cmd))
    exit_code = run_one_command(
        clone_cmd,
        cwd,
        env,
        stdout_path,
        stderr_path,
        dry_run=dry_run,
        timeout=phase_timeout_seconds(env, "clone"),
    )
    if exit_code == 0 and fsync_boundaries:
        commands.append(f"bench-fsync-tree {checkout_dir / '.git'}")
        exit_code = run_fsync_boundary(checkout_dir, cwd, env, stdout_path, stderr_path, dry_run=dry_run)

    if exit_code == 0:
        if mode == "read-tree":
            read_tree_cmd = ["git", "-C", str(checkout_dir), "read-tree", "-mu", commit]
            commands.append(command_display(read_tree_cmd))
            exit_code = run_one_command(
                read_tree_cmd,
                cwd,
                env,
                stdout_path,
                stderr_path,
                dry_run=dry_run,
                timeout=phase_timeout_seconds(env, "clone"),
            )
            if exit_code == 0 and fsync_boundaries:
                commands.append(f"bench-fsync-tree {checkout_dir}")
                exit_code = run_fsync_tree(checkout_dir, stdout_path, stderr_path, dry_run=dry_run)
                if exit_code == 0:
                    exit_code = run_syncfs(checkout_dir, cwd, env, stdout_path, stderr_path, dry_run=dry_run)
                    settle_after_fsync(env)
            if exit_code == 0:
                commands.append(f"bench-write-detached-head {commit}")
                exit_code = run_write_detached_head(checkout_dir, commit, stdout_path, stderr_path, dry_run=dry_run)
            if exit_code == 0:
                verify_cmd = ["git", "-C", str(checkout_dir), "rev-parse", "--verify", "HEAD"]
                commands.append(command_display(verify_cmd))
                exit_code = run_one_command(
                    verify_cmd,
                    cwd,
                    env,
                    stdout_path,
                    stderr_path,
                    dry_run=dry_run,
                    timeout=phase_timeout_seconds(env, "clone"),
                )
        else:
            checkout_cmd = ["git", "-C", str(checkout_dir), "checkout", "--detach", commit]
            commands.append(command_display(checkout_cmd))
            attempt = 0
            while True:
                exit_code = run_one_command(
                    checkout_cmd,
                    cwd,
                    env,
                    stdout_path,
                    stderr_path,
                    dry_run=dry_run,
                    timeout=phase_timeout_seconds(env, "clone"),
                )
                if exit_code == 0 or attempt >= retries:
                    break
                attempt += 1
                append_log_header(stdout_path, f"# checkout failed; fsyncing .git before retry {attempt}/{retries}")
                if fsync_boundaries:
                    fsync_code = run_fsync_boundary(checkout_dir, cwd, env, stdout_path, stderr_path, dry_run=dry_run)
                    if fsync_code != 0:
                        exit_code = fsync_code
                        break

    if exit_code != 0:
        last = commands[-1] if commands else "clone"
        error = f"command failed with exit code {exit_code}: {last}"

    duration = time.monotonic() - start
    return PhaseResult(
        session=session,
        case=case_name,
        repo=repo.id,
        language=repo.language,
        storage=storage,
        run=run_index,
        phase="clone",
        status="ok" if exit_code == 0 else "failed",
        duration_seconds=duration,
        started_at=started_at,
        ended_at=utc_now(),
        exit_code=exit_code,
        cwd=str(cwd),
        stdout=str(stdout_path),
        stderr=str(stderr_path),
        commands=commands,
        error=error,
        commit=commit,
    )


def emit_event(result_dir: Path, event: dict[str, Any]) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    with (result_dir / "events.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, sort_keys=True) + "\n")


def load_completed_samples(result_dir: Path) -> set[tuple[int, str, str]]:
    events_path = result_dir / "events.jsonl"
    if not events_path.exists():
        return set()
    completed: set[tuple[int, str, str]] = set()
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("type") != "phase":
                continue
            phase = event.get("phase")
            if phase == "build" or (phase == "clone" and event.get("status") != "ok"):
                completed.add((int(event["run"]), str(event["repo"]), str(event["storage"])))
    return completed


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def make_cache_env(bench_home: Path, *, sample_key: str | None = None, language: str | None = None) -> dict[str, str]:
    env = {key: str(value) for key, value in os.environ.items()}
    cache_root = bench_home / "cache"
    defaults = {
        "BUN_INSTALL_CACHE_DIR": cache_root / "bun",
        "UV_CACHE_DIR": cache_root / "uv",
        "CARGO_HOME": cache_root / "cargo-home",
        "GOMODCACHE": cache_root / "go" / "pkg" / "mod",
        "npm_config_cache": cache_root / "npm",
    }
    for key, value in defaults.items():
        env.setdefault(key, str(value))
        Path(env[key]).mkdir(parents=True, exist_ok=True)
    if language == "go":
        if sample_key:
            gocache = cache_root / "go" / "build" / sample_key
        else:
            gocache = cache_root / "go" / "build-prewarm"
        if gocache.exists() and sample_key:
            safe_rmtree(gocache, bench_home)
        gocache.mkdir(parents=True, exist_ok=True)
        env["GOCACHE"] = str(gocache)
    return env


def resolve_commit(repo: RepoSpec, *, dry_run: bool = False, no_resolve: bool = False) -> str:
    if repo.commit:
        return repo.commit
    if dry_run or no_resolve:
        return "dry-run-" + repo.ref.replace("/", "-")
    candidates = [repo.ref]
    if not repo.ref.startswith("refs/"):
        candidates.extend([f"refs/heads/{repo.ref}", f"refs/tags/{repo.ref}"])
    last_error = ""
    for ref in candidates:
        proc = subprocess.run(
            ["git", "ls-remote", "--exit-code", repo.url, ref],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            for line in proc.stdout.splitlines():
                if line.strip() and not line.endswith("^{}"):
                    return line.split()[0]
        last_error = proc.stderr.strip()
    raise BenchError(f"could not resolve {repo.id} {repo.ref} from {repo.url}: {last_error}")


def resolve_case_commits(case: BenchCase, *, dry_run: bool = False, no_resolve: bool = False) -> dict[str, str]:
    return {repo.id: resolve_commit(repo, dry_run=dry_run, no_resolve=no_resolve) for repo in case.repos}


def clone_checkout_commands(repo: RepoSpec, commit: str, checkout_dir: Path) -> list[list[str]]:
    return [
        ["git", "clone", "--no-checkout", repo.url, str(checkout_dir)],
        ["git", "-C", str(checkout_dir), "checkout", "--detach", commit],
    ]


def clean_declared_outputs(checkout_dir: Path, repo: RepoSpec, bench_home: Path) -> None:
    for pattern in repo.clean:
        matches = list(checkout_dir.glob(pattern))
        if not matches and any(ch in pattern for ch in "*?["):
            matches = [p for p in checkout_dir.rglob("*") if fnmatch.fnmatch(str(p.relative_to(checkout_dir)), pattern)]
        for path in matches:
            if path.exists() or path.is_symlink():
                safe_remove(path, bench_home)


def safe_remove(path: Path, bench_home: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        safe_rmtree(path, bench_home)
    else:
        assert_inside(path, bench_home)
        path.unlink(missing_ok=True)


def safe_rmtree(path: Path, bench_home: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    assert_inside(path, bench_home)
    for attempt in range(3):
        try:
            if path.is_symlink() or path.is_file():
                path.unlink(missing_ok=True)
            else:
                shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)


def assert_inside(path: Path, parent: Path) -> None:
    resolved_path = path.resolve(strict=False)
    resolved_parent = parent.resolve(strict=False)
    if resolved_path == resolved_parent:
        raise BenchError(f"refusing to remove bench root {resolved_parent}")
    try:
        resolved_path.relative_to(resolved_parent)
    except ValueError as exc:
        raise BenchError(f"refusing to remove path outside BENCH_HOME: {resolved_path}") from exc


def build_sample_plan(
    case: BenchCase,
    runs: int,
    storages: list[str],
    *,
    repo_major: bool = False,
) -> list[tuple[int, RepoSpec, str]]:
    plan = []
    if repo_major:
        for repo in case.repos:
            for run_index in range(1, runs + 1):
                for storage in storages:
                    plan.append((run_index, repo, storage))
    else:
        for run_index in range(1, runs + 1):
            for repo in case.repos:
                for storage in storages:
                    plan.append((run_index, repo, storage))
    return plan


def prewarm(case: BenchCase, bench_home: Path, commits: dict[str, str], *, force: bool = False, dry_run: bool = False) -> None:
    prewarm_root = bench_home / "prewarm"
    prewarm_root.mkdir(parents=True, exist_ok=True)
    for repo in case.repos:
        commit = commits[repo.id]
        repo_dir = prewarm_root / f"{repo.id}-{commit[:12]}"
        marker = repo_dir / ".bench-prewarm-ok"
        if marker.exists() and not force:
            print(f"prewarm: {repo.id} already warm at {commit[:12]}")
            continue
        if repo_dir.exists():
            safe_rmtree(repo_dir, bench_home)
        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        logs = bench_home / "results" / "prewarm" / repo.id
        env = make_cache_env(bench_home, language=repo.language)
        clone_phase = run_phase(
            session="prewarm",
            case_name=case.name,
            repo=repo,
            storage="native",
            run_index=0,
            phase="clone",
            commands=clone_checkout_commands(repo, commit, repo_dir),
            cwd=prewarm_root,
            env=env,
            stdout_path=logs / "clone.out",
            stderr_path=logs / "clone.err",
            commit=commit,
            dry_run=dry_run,
        )
        if clone_phase.exit_code != 0:
            raise BenchError(clone_phase.error)
        build_dir = repo_dir / repo.build_dir
        prewarm_phase = run_phase(
            session="prewarm",
            case_name=case.name,
            repo=repo,
            storage="native",
            run_index=0,
            phase="prewarm",
            commands=repo.prewarm,
            cwd=build_dir,
            env=env,
            stdout_path=logs / "prewarm.out",
            stderr_path=logs / "prewarm.err",
            commit=commit,
            dry_run=dry_run,
            shell=True,
        )
        if prewarm_phase.exit_code != 0:
            raise BenchError(prewarm_phase.error)
        if not dry_run:
            marker.write_text(json.dumps({"commit": commit, "completed_at": utc_now()}) + "\n", encoding="utf-8")
        print(f"prewarm: {repo.id} ready at {commit[:12]}")


def collect_environment(bench_home: Path, commits: dict[str, str]) -> dict[str, Any]:
    return {
        "captured_at": utc_now(),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": sys.version.split()[0],
        },
        "bench_home": str(bench_home),
        "repo_root": str(repo_root()),
        "drive9_repo_commit": command_output(["git", "rev-parse", "HEAD"], cwd=repo_root()),
        "drive9_repo_branch": command_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root()),
        "tool_versions": {
            "git": command_output(["git", "--version"]),
            "python3": command_output(["python3", "--version"]),
            "node": command_output(["node", "--version"]),
            "npm": command_output(["npm", "--version"]),
            "bun": command_output(["bun", "--version"]),
            "uv": command_output(["uv", "--version"]),
            "cargo": command_output(["cargo", "--version"]),
            "rustc": command_output(["rustc", "--version"]),
            "go": command_output(["go", "version"]),
            "make": first_line(command_output(["make", "--version"])),
            "drive9": command_output([drive9_cli(), "--version"]),
        },
        "system": {
            "uname": command_output(["uname", "-a"]),
            "lscpu": command_output(["lscpu"]),
            "free": command_output(["free", "-h"]),
            "df": command_output(["df", "-h"]),
            "mount": command_output(["mount"]),
        },
        "env": redact_env(
            {
                "BENCH_HOME": str(bench_home),
                "BENCH_STORAGES": os.environ.get("BENCH_STORAGES", ""),
                "DRIVE9_SERVER": os.environ.get("DRIVE9_SERVER", ""),
                "DRIVE9_API_KEY": os.environ.get("DRIVE9_API_KEY", ""),
                "BENCH_DRIVE9_CLI": os.environ.get("BENCH_DRIVE9_CLI", ""),
                "BENCH_DRIVE9_MOUNT_FLAGS": os.environ.get("BENCH_DRIVE9_MOUNT_FLAGS", ""),
            }
        ),
        "repo_commits": commits,
    }


def command_output(command: list[str], cwd: Path | None = None) -> str:
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=20,
            check=False,
        )
        text = proc.stdout.strip()
        if proc.returncode != 0 and not text:
            return f"unavailable (exit {proc.returncode})"
        return text
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"unavailable ({exc})"


def first_line(text: str) -> str:
    return text.splitlines()[0] if text else text


def redact_env(values: dict[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in values.items():
        if "KEY" in key or "TOKEN" in key or "SECRET" in key:
            redacted[key] = "<set>" if value else ""
        else:
            redacted[key] = value
    return redacted


def mount_drive9(
    *,
    bench_home: Path,
    result_dir: Path,
    session: str,
    repo: RepoSpec,
    run_index: int,
    dry_run: bool,
) -> tuple[Path, subprocess.Popen[bytes] | None, Path]:
    mountpoint = bench_home / "mounts" / "drive9"
    ensure_mountpoint_ready(mountpoint)
    remote = os.environ.get("BENCH_DRIVE9_REMOTE", "/")
    if remote == "/":
        remote_arg = ":/"
    elif remote.startswith("/"):
        remote_arg = f":{remote}"
    else:
        remote_arg = f":/{remote}"
    flags = shlex.split(os.environ.get("BENCH_DRIVE9_MOUNT_FLAGS", "--mode=fuse --durability=interactive --perf-counters"))
    timeout = float(os.environ.get("BENCH_DRIVE9_MOUNT_TIMEOUT_SECONDS", "60"))
    log_dir = result_dir / "logs" / "fuse-mount" / repo.id / f"run-{run_index}"
    stdout_path = log_dir / "mount.out"
    stderr_path = log_dir / "mount.err"
    command = [drive9_cli(), "mount", *flags, remote_arg, str(mountpoint)]
    append_log_header(stdout_path, "$ " + command_display(command))
    if dry_run:
        append_log_header(stdout_path, "(dry-run: mount not executed)")
        return mountpoint, None, stderr_path

    env = {key: str(value) for key, value in os.environ.items()}
    out = stdout_path.open("ab")
    err = stderr_path.open("ab")
    proc = subprocess.Popen(command, cwd=str(repo_root()), env=env, stdout=out, stderr=err)
    out.close()
    err.close()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise BenchError(f"drive9 mount exited early with code {proc.returncode}; see {stderr_path}")
        if os.path.ismount(mountpoint):
            return mountpoint, proc, stderr_path
        time.sleep(0.25)
    terminate_process(proc)
    raise BenchError(f"timed out waiting for drive9 mount at {mountpoint}; see {stderr_path}")


def ensure_mountpoint_ready(mountpoint: Path) -> None:
    try:
        mountpoint.mkdir(parents=True, exist_ok=True)
        if os.path.ismount(mountpoint):
            cleanup_mountpoint(mountpoint)
        return
    except OSError as exc:
        if exc.errno != errno.ENOTCONN:
            raise
    cleanup_mountpoint(mountpoint)
    mountpoint.mkdir(parents=True, exist_ok=True)


def cleanup_mountpoint(mountpoint: Path) -> None:
    commands = [
        [drive9_cli(), "umount", "--timeout", "30s", str(mountpoint)],
        [shutil.which("fusermount3") or shutil.which("fusermount") or "fusermount3", "-uz", str(mountpoint)],
        ["umount", "-l", str(mountpoint)],
    ]
    for command in commands:
        try:
            subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=35, check=False)
        except (OSError, subprocess.TimeoutExpired):
            continue


def unmount_drive9(
    *,
    proc: subprocess.Popen[bytes] | None,
    mountpoint: Path,
    stderr_path: Path,
    result_dir: Path,
    session: str,
    repo: RepoSpec,
    run_index: int,
    dry_run: bool,
) -> None:
    log_dir = result_dir / "logs" / "fuse-mount" / repo.id / f"run-{run_index}"
    stdout_path = log_dir / "umount.out"
    umount_stderr = log_dir / "umount.err"
    timeout_value = os.environ.get("BENCH_DRIVE9_UMOUNT_TIMEOUT_SECONDS", "300s")
    if timeout_value.isdigit():
        timeout_value += "s"
    command = [drive9_cli(), "umount", "--timeout", timeout_value, str(mountpoint)]
    append_log_header(stdout_path, "$ " + command_display(command))
    if dry_run:
        append_log_header(stdout_path, "(dry-run: umount not executed)")
        return
    try:
        run_one_command(command, repo_root(), {key: str(value) for key, value in os.environ.items()}, stdout_path, umount_stderr)
    finally:
        if proc is not None:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                terminate_process(proc)
        emit_fuse_perf_events(stderr_path, result_dir, session, repo, run_index)


def emit_fuse_perf_events(stderr_path: Path, result_dir: Path, session: str, repo: RepoSpec, run_index: int) -> None:
    if not stderr_path.exists():
        return
    lines = []
    try:
        for line in stderr_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "drive9: FUSE perf summary" in line or "drive9: perf " in line:
                lines.append(line)
    except OSError:
        return
    if lines:
        emit_event(
            result_dir,
            {
                "type": "fuse_perf",
                "session": session,
                "repo": repo.id,
                "run": run_index,
                "stderr": str(stderr_path),
                "lines": lines,
            },
        )


def terminate_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def run_benchmark(args: argparse.Namespace) -> Path:
    case = filter_repos(load_case(Path(args.case)), args.repos or os.environ.get("BENCH_REPOS"))
    bench_home = bench_home_from_args(args.bench_home)
    bench_home.mkdir(parents=True, exist_ok=True)
    runs = int(args.runs or os.environ.get("BENCH_RUNS") or case.runs)
    storages = parse_storages(args.storages or os.environ.get("BENCH_STORAGES"), case.storages)
    if args.native_only:
        storages = ["native"]
    session = args.session or session_id()
    result_dir = bench_home / "results" / session
    result_dir.mkdir(parents=True, exist_ok=True)
    commits = resolve_case_commits(case, dry_run=args.dry_run, no_resolve=args.no_resolve)
    manifest = {
        "session": session,
        "case": case.name,
        "started_at": utc_now(),
        "runs": runs,
        "storages": storages,
        "dry_run": bool(args.dry_run),
        "command_timeout_seconds": os.environ.get("BENCH_COMMAND_TIMEOUT_SECONDS", ""),
        "clone_timeout_seconds": os.environ.get("BENCH_CLONE_TIMEOUT_SECONDS", ""),
        "build_timeout_seconds": os.environ.get("BENCH_BUILD_TIMEOUT_SECONDS", ""),
        "prewarm_timeout_seconds": os.environ.get("BENCH_PREWARM_TIMEOUT_SECONDS", ""),
        "clone_only": bool(args.clone_only),
        "repo_major": bool(args.repo_major),
        "stop_after_first_repo_timeout": bool(args.stop_after_first_repo_timeout),
        "stop_after_first_repo_failure": bool(args.stop_after_first_repo_failure),
        "clean_before_build": bool_env(os.environ, "BENCH_CLEAN_BEFORE_BUILD", False),
        "git_fsync": bool(args.git_fsync or bool_env(os.environ, "BENCH_GIT_FSYNC", False)),
        "git_checkout_mode": args.git_checkout_mode or os.environ.get("BENCH_GIT_CHECKOUT_MODE", "standard"),
        "git_checkout_retries": args.git_checkout_retries
        if args.git_checkout_retries is not None
        else os.environ.get("BENCH_GIT_CHECKOUT_RETRIES", ""),
        "repo_commits": commits,
        "case_file": str(Path(args.case).resolve()),
    }
    write_json(result_dir / "manifest.json", manifest)
    write_json(result_dir / "environment.json", collect_environment(bench_home, commits))
    emit_event(result_dir, {"type": "manifest", **manifest})
    if not args.skip_prewarm:
        prewarm(case, bench_home, commits, force=args.force_prewarm, dry_run=args.dry_run)

    failures = 0
    completed_samples = load_completed_samples(result_dir) if args.resume else set()
    first_repo_id = case.repos[0].id
    for run_index, repo, storage in build_sample_plan(case, runs, storages, repo_major=args.repo_major):
        if (run_index, repo.id, storage) in completed_samples:
            print(f"resume: skip completed {repo.id} {storage} run {run_index}")
            continue
        commit = commits[repo.id]
        sample_key = f"{session}/{storage}/{repo.id}/run-{run_index}"
        env = make_cache_env(bench_home, sample_key=sample_key, language=repo.language)
        checkout_dir: Path
        sample_failed = False
        mount_proc: subprocess.Popen[bytes] | None = None
        mount_stderr = Path()
        break_after_sample = False
        if storage == "native":
            sample_root = bench_home / "work" / "native" / session
        else:
            mountpoint, mount_proc, mount_stderr = mount_drive9(
                bench_home=bench_home,
                result_dir=result_dir,
                session=session,
                repo=repo,
                run_index=run_index,
                dry_run=args.dry_run,
            )
            sample_root = mountpoint / "bench" / session
        checkout_dir = sample_root / f"{repo.id}-run-{run_index}"
        stdout_base = result_dir / "logs" / storage / repo.id / f"run-{run_index}"
        try:
            if checkout_dir.exists():
                safe_rmtree(checkout_dir, bench_home)
            sample_root.mkdir(parents=True, exist_ok=True)
            clone_result = run_clone_phase(
                session=session,
                case_name=case.name,
                repo=repo,
                storage=storage,
                run_index=run_index,
                commit=commit,
                checkout_dir=checkout_dir,
                cwd=sample_root,
                env=env,
                stdout_path=stdout_base / "clone.out",
                stderr_path=stdout_base / "clone.err",
                dry_run=args.dry_run,
                args=args,
            )
            emit_event(result_dir, clone_result.as_event())
            clone_failed = clone_result.exit_code != 0
            if clone_failed:
                sample_failed = True
                failures += 1
                break_after_sample = (
                    bool(args.stop_after_first_repo_failure)
                    and repo.id == first_repo_id
                    and storage == "fuse"
                ) or (
                    bool(args.stop_after_first_repo_timeout)
                    and repo.id == first_repo_id
                    and storage == "fuse"
                    and clone_result.exit_code == 124
                )
                if break_after_sample:
                    emit_event(
                        result_dir,
                        {
                            "type": "stop",
                            "session": session,
                            "reason": "first_repo_fuse_clone_failure"
                            if clone_result.exit_code != 124
                            else "first_repo_fuse_clone_timeout",
                            "repo": repo.id,
                            "storage": storage,
                            "run": run_index,
                            "phase": "clone",
                            "exit_code": clone_result.exit_code,
                            "ended_at": utc_now(),
                        },
                    )
                if not args.continue_on_error and not break_after_sample:
                    raise BenchError(clone_result.error)
            elif not args.clone_only:
                if bool_env(env, "BENCH_CLEAN_BEFORE_BUILD", False):
                    clean_declared_outputs(checkout_dir, repo, bench_home)
                build_result = run_phase(
                    session=session,
                    case_name=case.name,
                    repo=repo,
                    storage=storage,
                    run_index=run_index,
                    phase="build",
                    commands=repo.build,
                    cwd=checkout_dir / repo.build_dir,
                    env=env,
                    stdout_path=stdout_base / "build.out",
                    stderr_path=stdout_base / "build.err",
                    commit=commit,
                    dry_run=args.dry_run,
                    shell=True,
                )
                emit_event(result_dir, build_result.as_event())
                if build_result.exit_code != 0:
                    sample_failed = True
                    failures += 1
                    if not args.continue_on_error:
                        raise BenchError(build_result.error)
        finally:
            cleanup_started = utc_now()
            cleanup_error = ""
            cleanup_status = "ok"
            try:
                if storage == "fuse" and sample_failed:
                    cleanup_status = "skipped_after_failure"
                elif checkout_dir.exists():
                    safe_rmtree(checkout_dir, bench_home)
            except Exception as exc:  # noqa: BLE001 - event should preserve cleanup failures.
                cleanup_error = str(exc)
                cleanup_status = "failed"
            emit_event(
                result_dir,
                {
                    "type": "cleanup",
                    "session": session,
                    "repo": repo.id,
                    "storage": storage,
                    "run": run_index,
                    "started_at": cleanup_started,
                    "ended_at": utc_now(),
                    "status": cleanup_status,
                    "error": cleanup_error,
                    "path": str(checkout_dir),
                },
            )
            if storage == "fuse":
                unmount_drive9(
                    proc=mount_proc,
                    mountpoint=bench_home / "mounts" / "drive9",
                    stderr_path=mount_stderr,
                    result_dir=result_dir,
                    session=session,
                    repo=repo,
                    run_index=run_index,
                    dry_run=args.dry_run,
                )
        if break_after_sample:
            print(f"stop: first repo {repo.id} FUSE clone failed on run {run_index}; skipping remaining repos")
            break
    summary_paths = summarize_session(result_dir)
    print(f"benchmark session: {session}")
    print(f"results: {result_dir}")
    print(f"summary csv: {summary_paths['csv']}")
    print(f"summary md: {summary_paths['markdown']}")
    if failures:
        raise BenchError(f"{failures} measured phase(s) failed; see {result_dir / 'events.jsonl'}")
    return result_dir


def summarize_session(result_dir: Path) -> dict[str, Path]:
    events_path = result_dir / "events.jsonl"
    if not events_path.exists():
        raise BenchError(f"events file not found: {events_path}")
    rows = []
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("type") == "phase":
                rows.append(event)
    summary_rows: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["repo"], row["language"], row["storage"], row["phase"])
        groups.setdefault(key, []).append(row)
    for (repo_id, language, storage, phase), values in sorted(groups.items()):
        durations = [float(v["duration_seconds"]) for v in values if v.get("status") == "ok"]
        failed = sum(1 for v in values if v.get("status") != "ok")
        summary_rows.append(
            {
                "repo": repo_id,
                "language": language,
                "storage": storage,
                "phase": phase,
                "count": len(values),
                "ok": len(durations),
                "failed": failed,
                "mean_seconds": round(statistics.mean(durations), 6) if durations else "",
                "median_seconds": round(statistics.median(durations), 6) if durations else "",
                "min_seconds": round(min(durations), 6) if durations else "",
                "max_seconds": round(max(durations), 6) if durations else "",
            }
        )

    csv_path = result_dir / "summary.csv"
    fieldnames = [
        "repo",
        "language",
        "storage",
        "phase",
        "count",
        "ok",
        "failed",
        "mean_seconds",
        "median_seconds",
        "min_seconds",
        "max_seconds",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    md_path = result_dir / "summary.md"
    md_path.write_text(render_summary_markdown(summary_rows), encoding="utf-8")
    return {"csv": csv_path, "markdown": md_path}


def render_summary_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Repo Build Benchmark Summary",
        "",
        "| repo | language | storage | phase | ok/count | mean s | median s | min s | max s |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        ok_count = f"{row['ok']}/{row['count']}"
        lines.append(
            "| {repo} | {language} | {storage} | {phase} | {ok_count} | {mean} | {median} | {minv} | {maxv} |".format(
                repo=row["repo"],
                language=row["language"],
                storage=row["storage"],
                phase=row["phase"],
                ok_count=ok_count,
                mean=row["mean_seconds"],
                median=row["median_seconds"],
                minv=row["min_seconds"],
                maxv=row["max_seconds"],
            )
        )
    lines.extend(["", "## FUSE/native mean ratios", ""])
    ratio_lines = render_ratio_lines(rows)
    lines.extend(ratio_lines or ["No paired native and FUSE rows were available."])
    lines.append("")
    return "\n".join(lines)


def render_ratio_lines(rows: list[dict[str, Any]]) -> list[str]:
    by_key = {(row["repo"], row["phase"], row["storage"]): row for row in rows if row["mean_seconds"] != ""}
    output = []
    for repo_id, phase in sorted({(row["repo"], row["phase"]) for row in rows}):
        native = by_key.get((repo_id, phase, "native"))
        fuse = by_key.get((repo_id, phase, "fuse"))
        if not native or not fuse:
            continue
        native_mean = float(native["mean_seconds"])
        fuse_mean = float(fuse["mean_seconds"])
        ratio = fuse_mean / native_mean if native_mean else 0.0
        output.append(f"- {repo_id} {phase}: FUSE/native mean ratio {ratio:.2f}x")
    return output


def doctor(args: argparse.Namespace) -> int:
    case = filter_repos(load_case(Path(args.case)), args.repos or os.environ.get("BENCH_REPOS"))
    bench_home = bench_home_from_args(args.bench_home)
    storages = parse_storages(args.storages or os.environ.get("BENCH_STORAGES"), case.storages)
    if args.native_only:
        storages = ["native"]
    required_tools = ["git", "python3", "node", "npm", "bun", "uv", "cargo", "rustc", "go", "make"]
    if "fuse" in storages and not args.skip_drive9:
        required_tools.append(drive9_cli())
    print(f"bench home: {bench_home}")
    print(f"case: {Path(args.case).resolve()}")
    print(f"storages: {','.join(storages)}")
    missing = []
    for tool in required_tools:
        found = shutil.which(tool) if os.path.basename(tool) == tool else (tool if Path(tool).exists() else None)
        status = found or "missing"
        print(f"tool {tool}: {status}")
        if not found:
            missing.append(tool)
    if args.dry_run:
        print("dry-run: skipped FUSE and drive9 server checks")
        return 0
    if "fuse" in storages:
        if not args.skip_drive9:
            check_drive9_connectivity()
        check_fuse()
    return 0 if not missing else 1


def check_drive9_connectivity() -> None:
    env = {key: str(value) for key, value in os.environ.items()}
    command = [drive9_cli(), "fs", "ls", ":/"]
    proc = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30, check=False)
    if proc.returncode != 0:
        raise BenchError(f"drive9 connectivity check failed: {proc.stderr.strip()}")
    print("drive9 connectivity: ok")


def check_fuse() -> None:
    system = platform.system()
    if system == "Linux":
        if not Path("/dev/fuse").exists():
            raise BenchError("/dev/fuse is missing")
        helper = shutil.which("fusermount3") or shutil.which("fusermount")
        print(f"fuse helper: {helper or 'not found'}")
    elif system == "Darwin":
        macfuse = Path("/Library/Filesystems/macfuse.fs").exists() or bool(shutil.which("mount_macfuse"))
        if not macfuse:
            raise BenchError("macFUSE does not appear to be installed")
        print("macFUSE: ok")
    else:
        raise BenchError(f"FUSE check is not implemented for {system}")


def command_prewarm(args: argparse.Namespace) -> None:
    case = filter_repos(load_case(Path(args.case)), args.repos or os.environ.get("BENCH_REPOS"))
    bench_home = bench_home_from_args(args.bench_home)
    bench_home.mkdir(parents=True, exist_ok=True)
    commits = resolve_case_commits(case, dry_run=args.dry_run, no_resolve=args.no_resolve)
    prewarm(case, bench_home, commits, force=args.force, dry_run=args.dry_run)


def command_summarize(args: argparse.Namespace) -> None:
    result_dir = Path(args.result_dir).expanduser().resolve()
    paths = summarize_session(result_dir)
    print(paths["csv"])
    print(paths["markdown"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="drive9 repo clone/build benchmark harness")
    parser.add_argument("--case", default=str(default_case_path()), help="case JSON file")
    parser.add_argument("--bench-home", default=None, help="mutable benchmark root")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor_p = sub.add_parser("doctor", help="check local prerequisites")
    doctor_p.add_argument("--dry-run", action="store_true")
    doctor_p.add_argument("--storages", default=None)
    doctor_p.add_argument("--repos", default=None)
    doctor_p.add_argument("--native-only", action="store_true")
    doctor_p.add_argument("--skip-drive9", action="store_true")
    doctor_p.set_defaults(func=doctor)

    prewarm_p = sub.add_parser("prewarm", help="download dependency caches outside measured timings")
    prewarm_p.add_argument("--force", action="store_true")
    prewarm_p.add_argument("--repos", default=None)
    prewarm_p.add_argument("--dry-run", action="store_true")
    prewarm_p.add_argument("--no-resolve", action="store_true")
    prewarm_p.set_defaults(func=command_prewarm)

    run_p = sub.add_parser("run", help="run measured clone/build samples")
    run_p.add_argument("--runs", default=None)
    run_p.add_argument("--storages", default=None)
    run_p.add_argument("--repos", default=None, help="comma-separated repo ids to run")
    run_p.add_argument("--native-only", action="store_true")
    run_p.add_argument("--session", default=None)
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--no-resolve", action="store_true")
    run_p.add_argument("--skip-prewarm", action="store_true")
    run_p.add_argument("--force-prewarm", action="store_true")
    run_p.add_argument("--clone-only", action="store_true", help="measure clone phases only and skip builds")
    run_p.add_argument("--repo-major", action="store_true", help="run all repeats for each repo before moving to the next repo")
    run_p.add_argument("--git-fsync", action="store_true", help="fsync Git metadata/worktree boundaries during clone phases")
    run_p.add_argument("--git-checkout-mode", choices=["standard", "read-tree"], default=None)
    run_p.add_argument("--git-checkout-retries", type=int, default=None)
    run_p.add_argument(
        "--stop-after-first-repo-timeout",
        action="store_true",
        help="stop the session after the first repo's FUSE clone times out",
    )
    run_p.add_argument(
        "--stop-after-first-repo-failure",
        action="store_true",
        help="stop the session after the first repo's FUSE clone fails for any reason",
    )
    run_p.add_argument("--continue-on-error", action="store_true")
    run_p.add_argument("--resume", action="store_true", help="skip samples already completed in events.jsonl")
    run_p.set_defaults(func=run_benchmark)

    summarize_p = sub.add_parser("summarize", help="rebuild summary files for a result directory")
    summarize_p.add_argument("result_dir")
    summarize_p.set_defaults(func=command_summarize)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = args.func(args)
        if isinstance(result, int):
            return result
        return 0
    except BenchError as exc:
        print(f"bench: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
