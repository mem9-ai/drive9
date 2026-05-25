#!/usr/bin/env python3
"""Run the production/dev Drive9 FUSE benchmark with local dependency/output binds.

This harness is intentionally narrower than run-repo-build.py. It captures the
successful workaround used for large real-world repo builds where third-party
install trees and known generated-output trees are bind-mounted from the native
disk, while the source checkout itself remains on either native disk or Drive9
FUSE. Each repo gets a fresh `drive9 create` context before its FUSE sample.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BENCH_HOME = Path(os.environ.get("BENCH_HOME", "/mnt/drive9-bench"))
RUNS = int(os.environ.get("BENCH_RUNS", "1"))
DRIVE9_ENV = os.environ.get("BENCH_DRIVE9_ENV", "prod")
DRIVE9_SERVER = os.environ.get("BENCH_DRIVE9_SERVER", "https://api.drive9.ai")
RUN_LABEL = os.environ.get("BENCH_RUN_LABEL", f"{DRIVE9_ENV}-localdeps-outputs-newinst")
SESSION = os.environ.get("BENCH_SESSION") or time.strftime(f"ec2-%Y%m%dT%H%M%SZ-{RUN_LABEL}", time.gmtime())
RESULT_DIR = BENCH_HOME / "results" / SESSION
WORK_DIR = BENCH_HOME / "work" / RUN_LABEL / SESSION
LOCAL_DEPS_DIR = BENCH_HOME / "local-deps" / SESSION
MOUNTPOINT = BENCH_HOME / "mounts" / RUN_LABEL
TOOLS_DIR = BENCH_HOME / "tools"
DEFAULT_PROD_CONFIGS = (
    Path.home() / ".drive9" / "config_prod_bak",
    Path.home() / ".drive9" / "config_prod_bak_codex",
)
CONFIG_SOURCE = os.environ.get("BENCH_DRIVE9_CONFIG")
CONFIG = Path.home() / ".drive9" / "config"
CONFIG_BACKUP = Path.home() / ".drive9" / f"config.before-{SESSION}"

CLONE_TIMEOUT = int(os.environ.get("BENCH_CLONE_TIMEOUT_SECONDS", "1800"))
BUILD_TIMEOUT = int(os.environ.get("BENCH_BUILD_TIMEOUT_SECONDS", "1800"))
PREWARM_TIMEOUT = int(os.environ.get("BENCH_PREWARM_TIMEOUT_SECONDS", "1800"))
UMOUNT_TIMEOUT = os.environ.get("BENCH_DRIVE9_UMOUNT_TIMEOUT_SECONDS", "900s")
REPO_FILTER = tuple(
    repo.strip()
    for repo in os.environ.get("BENCH_REPOS", "").split(",")
    if repo.strip()
)


@dataclass(frozen=True)
class Repo:
    repo_id: str
    language: str
    url: str
    ref: str
    build_dir: str
    prewarm: tuple[str, ...]
    build: tuple[str, ...]


REPOS = (
    Repo(
        repo_id="drive9",
        language="go",
        url="https://github.com/mem9-ai/drive9.git",
        ref="main",
        build_dir=".",
        prewarm=("go mod download",),
        build=("make build",),
    ),
    Repo(
        repo_id="kimi-cli",
        language="python",
        url="https://github.com/MoonshotAI/kimi-cli.git",
        ref="main",
        build_dir=".",
        prewarm=(
            "uv sync --frozen --all-extras --all-packages",
            "npm --prefix web ci --include=dev",
            "npm --prefix vis ci --include=dev",
        ),
        build=("uv sync --frozen --all-extras --all-packages", "make build"),
    ),
    Repo(
        repo_id="kimi-code",
        language="typescript",
        url="https://github.com/MoonshotAI/kimi-code.git",
        ref="main",
        build_dir=".",
        prewarm=("corepack pnpm install --frozen-lockfile --store-dir \"$PNPM_STORE_DIR\"",),
        build=(
            "corepack pnpm install --frozen-lockfile --store-dir \"$PNPM_STORE_DIR\"",
            "corepack pnpm run build",
        ),
    ),
)


KIMI_CODE_NODE_MODULE_DIRS = (
    ".",
    "apps/kimi-code",
    "apps/vis",
    "apps/vis/server",
    "apps/vis/web",
    "docs",
    "packages/agent-core",
    "packages/kaos",
    "packages/kosong",
    "packages/migration-legacy",
    "packages/node-sdk",
    "packages/oauth",
    "packages/telemetry",
)

KIMI_CODE_OUTPUT_DIRS = (
    "apps/kimi-code/dist",
    "apps/vis/server/dist",
    "apps/vis/web/dist",
    "docs/.vitepress/dist",
    "packages/agent-core/dist",
    "packages/kaos/dist",
    "packages/kosong/dist",
    "packages/migration-legacy/dist",
    "packages/oauth/dist",
    "packages/telemetry/dist",
)

KIMI_CODE_PARENT_BIND_DIRS = (
    "packages/node-sdk",
)


def selected_repos() -> tuple[Repo, ...]:
    if not REPO_FILTER:
        return REPOS
    known = {repo.repo_id for repo in REPOS}
    unknown = sorted(set(REPO_FILTER) - known)
    if unknown:
        raise RuntimeError(f"unknown BENCH_REPOS values: {', '.join(unknown)}")
    return tuple(repo for repo in REPOS if repo.repo_id in REPO_FILTER)


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def base_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PATH"] = (
        f"{TOOLS_DIR / 'node24' / 'bin'}:"
        f"{Path.home() / '.local' / 'bin'}:"
        f"{Path.home() / '.bun' / 'bin'}:"
        f"{Path.home() / '.cargo' / 'bin'}:"
        f"/usr/local/go/bin:"
        f"{env.get('PATH', '')}"
    )
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_COUNT"] = "1"
    env["GIT_CONFIG_KEY_0"] = "safe.directory"
    env["GIT_CONFIG_VALUE_0"] = "*"
    env["COREPACK_HOME"] = str(BENCH_HOME / "cache" / "corepack")
    env["BUN_INSTALL_CACHE_DIR"] = str(BENCH_HOME / "cache" / "bun")
    env["UV_CACHE_DIR"] = str(BENCH_HOME / "cache" / "uv")
    env["npm_config_cache"] = str(BENCH_HOME / "cache" / "npm")
    env["GOMODCACHE"] = str(BENCH_HOME / "cache" / "go" / "pkg" / "mod")
    env["CARGO_HOME"] = str(BENCH_HOME / "cache" / "cargo-home")
    for key in (
        "COREPACK_HOME",
        "BUN_INSTALL_CACHE_DIR",
        "UV_CACHE_DIR",
        "npm_config_cache",
        "GOMODCACHE",
        "CARGO_HOME",
    ):
        Path(env[key]).mkdir(parents=True, exist_ok=True)
    return env


def run(
    command: str | list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    stdout: Path,
    stderr: Path,
    timeout: int | None,
    shell: bool = False,
) -> int:
    stdout.parent.mkdir(parents=True, exist_ok=True)
    stderr.parent.mkdir(parents=True, exist_ok=True)
    with stdout.open("ab") as out, stderr.open("ab") as err:
        display = command if isinstance(command, str) else " ".join(command)
        out.write(f"\n# {utc_now()} $ {display}\n".encode())
        out.flush()
        try:
            proc = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                stdout=out,
                stderr=err,
                timeout=timeout,
                shell=shell,
                executable="/bin/bash" if shell else None,
                check=False,
            )
            return proc.returncode
        except subprocess.TimeoutExpired:
            err.write(f"\n# {utc_now()} timed out after {timeout}s\n".encode())
            return 124


def capture(command: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None, timeout: int = 60) -> str:
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
        return proc.stdout.strip()
    except Exception as exc:  # noqa: BLE001
        return f"unavailable: {exc}"


def emit(event: dict[str, Any]) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with (RESULT_DIR / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def redact_secrets(text: str) -> str:
    text = re.sub(r"dat9_[A-Za-z0-9_.-]+", "dat9_[redacted]", text)
    text = re.sub(r"drive9_[A-Za-z0-9_.-]+", "drive9_[redacted]", text)
    text = re.sub(r"(?i)(api[_ -]?key\s*[:=]\s*)\S+", r"\1[redacted]", text)
    return text


def resolve_commit(repo: Repo, env: dict[str, str]) -> str:
    proc = subprocess.run(
        ["git", "ls-remote", "--exit-code", repo.url, repo.ref],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        proc = subprocess.run(
            ["git", "ls-remote", "--exit-code", repo.url, f"refs/heads/{repo.ref}"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
            check=False,
        )
    if proc.returncode != 0:
        raise RuntimeError(f"could not resolve {repo.repo_id}: {proc.stderr.strip()}")
    return proc.stdout.split()[0]


def version_tuple(version: str) -> tuple[int, int, int]:
    parts = version.strip().lstrip("v").split(".")
    nums = [int(part) for part in parts[:3]]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def ensure_node24(env: dict[str, str]) -> dict[str, str]:
    current = capture(["node", "-p", "process.versions.node"], env=env)
    try:
        if version_tuple(current) >= (24, 15, 0):
            return env
    except Exception:
        pass

    node_home = TOOLS_DIR / "node24"
    if not (node_home / "bin" / "node").exists():
        TOOLS_DIR.mkdir(parents=True, exist_ok=True)
        index = json.loads(urllib.request.urlopen("https://nodejs.org/dist/index.json", timeout=30).read())
        candidates = [
            item["version"]
            for item in index
            if version_tuple(item["version"]) >= (24, 15, 0)
            and version_tuple(item["version"])[0] == 24
            and "linux-x64" in item.get("files", [])
        ]
        if not candidates:
            candidates = [
                item["version"]
                for item in index
                if version_tuple(item["version"])[0] == 24 and "linux-x64" in item.get("files", [])
            ]
        if not candidates:
            raise RuntimeError("could not find a Node.js 24 linux-x64 release")
        version = candidates[0]
        archive = TOOLS_DIR / f"node-{version}-linux-x64.tar.xz"
        url = f"https://nodejs.org/dist/{version}/node-{version}-linux-x64.tar.xz"
        print(f"installing Node.js {version} from {url}", flush=True)
        urllib.request.urlretrieve(url, archive)
        tmp = TOOLS_DIR / f"node-{version}-extract"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        with tarfile.open(archive, "r:xz") as tar:
            tar.extractall(tmp)
        extracted = tmp / f"node-{version}-linux-x64"
        if node_home.exists():
            shutil.rmtree(node_home)
        extracted.rename(node_home)
        shutil.rmtree(tmp)

    env = dict(env)
    env["PATH"] = f"{node_home / 'bin'}:{env['PATH']}"
    return env


def prepare_drive9_config() -> None:
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    if CONFIG.exists() and not CONFIG_BACKUP.exists():
        shutil.copy2(CONFIG, CONFIG_BACKUP)
    source = Path(CONFIG_SOURCE).expanduser() if CONFIG_SOURCE else None
    if source is None and DRIVE9_ENV == "prod":
        source = next((path for path in DEFAULT_PROD_CONFIGS if path.exists()), DEFAULT_PROD_CONFIGS[0])
    if source is not None:
        if not source.exists():
            raise RuntimeError(f"missing drive9 config at {source}")
        shutil.copy2(source, CONFIG)
        CONFIG.chmod(0o600)


def restore_config() -> None:
    if CONFIG_BACKUP.exists():
        shutil.copy2(CONFIG_BACKUP, CONFIG)
        CONFIG.chmod(0o600)


def sudo_drive9_command(env: dict[str, str], *args: str) -> list[str]:
    return ["sudo", "env", f"HOME={Path.home()}", f"PATH={env['PATH']}", "drive9", *args]


def create_repo_context(repo: Repo, env: dict[str, str]) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    short = {"drive9": "d9", "kimi-cli": "kcli", "kimi-code": "kcode"}[repo.repo_id]
    ctx_prefix = os.environ.get("BENCH_DRIVE9_CONTEXT_PREFIX", "bpld")
    ctx = f"{ctx_prefix}{short}{stamp}"
    log_dir = RESULT_DIR / "logs" / "contexts" / repo.repo_id
    log_dir.mkdir(parents=True, exist_ok=True)
    command = ["drive9", "create", "--name", ctx, "--server", DRIVE9_SERVER]
    started = utc_now()
    start = time.monotonic()
    proc = subprocess.run(
        command,
        env=env,
        cwd=str(Path.home()),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=300,
        check=False,
    )
    duration = time.monotonic() - start
    (log_dir / "create.out").write_text(redact_secrets(proc.stdout), encoding="utf-8")
    (log_dir / "create.err").write_text(redact_secrets(proc.stderr), encoding="utf-8")
    emit(
        {
            "type": "drive9_context",
            "session": SESSION,
            "repo": repo.repo_id,
            "context": ctx,
            "server": DRIVE9_SERVER,
            "status": "ok" if proc.returncode == 0 else "failed",
            "exit_code": proc.returncode,
            "duration_seconds": duration,
            "started_at": started,
            "ended_at": utc_now(),
            "stdout": str(log_dir / "create.out"),
            "stderr": str(log_dir / "create.err"),
            "commands": [" ".join(command)],
        }
    )
    if proc.returncode != 0:
        raise RuntimeError(f"drive9 create failed for {repo.repo_id}; see {log_dir / 'create.err'}")
    use_code = run(
        ["drive9", "ctx", "use", ctx],
        cwd=Path.home(),
        env=env,
        stdout=log_dir / "ctx-use.out",
        stderr=log_dir / "ctx-use.err",
        timeout=60,
    )
    if use_code != 0:
        raise RuntimeError(f"drive9 ctx use failed for {ctx}; see {log_dir / 'ctx-use.err'}")
    return ctx


def mount_drive9(repo: Repo, run_index: int, env: dict[str, str]) -> subprocess.Popen[bytes]:
    if MOUNTPOINT.exists() and os.path.ismount(MOUNTPOINT):
        subprocess.run(sudo_drive9_command(env, "umount", "--timeout", "30s", str(MOUNTPOINT)), env=env, check=False)
        subprocess.run(["sudo", "umount", "-l", str(MOUNTPOINT)], env=env, check=False)
    if MOUNTPOINT.exists():
        try:
            MOUNTPOINT.rmdir()
        except OSError:
            pass
    MOUNTPOINT.mkdir(parents=True, exist_ok=True)
    log_dir = RESULT_DIR / "logs" / "fuse-mount" / repo.repo_id / f"run-{run_index}"
    stdout = log_dir / "mount.out"
    stderr = log_dir / "mount.err"
    cache_dir = BENCH_HOME / "cache" / f"drive9-{RUN_LABEL}" / SESSION
    cache_dir.mkdir(parents=True, exist_ok=True)
    command = sudo_drive9_command(
        env,
        "mount",
        "--mode=fuse",
        "--allow-other",
        "--profile=interactive",
        "--durability=interactive",
        "--perf-counters",
        "--cache-dir",
        str(cache_dir),
        ":/",
        str(MOUNTPOINT),
    )
    stdout.parent.mkdir(parents=True, exist_ok=True)
    with stdout.open("ab") as out, stderr.open("ab") as err:
        out.write(f"# {utc_now()} $ {' '.join(command)}\n".encode())
        out.flush()
        proc = subprocess.Popen(command, env=env, stdout=out, stderr=err)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"drive9 mount exited early with {proc.returncode}; see {stderr}")
        if os.path.ismount(MOUNTPOINT):
            return proc
        time.sleep(0.25)
    proc.terminate()
    raise RuntimeError(f"timed out waiting for drive9 mount at {MOUNTPOINT}; see {stderr}")


def unmount_drive9(repo: Repo, run_index: int, proc: subprocess.Popen[bytes] | None, env: dict[str, str]) -> None:
    log_dir = RESULT_DIR / "logs" / "fuse-mount" / repo.repo_id / f"run-{run_index}"
    stdout = log_dir / "umount.out"
    stderr = log_dir / "umount.err"
    code = run(
        sudo_drive9_command(env, "umount", "--timeout", UMOUNT_TIMEOUT, str(MOUNTPOINT)),
        cwd=Path.home(),
        env=env,
        stdout=stdout,
        stderr=stderr,
        timeout=1200,
    )
    if code != 0:
        run(["sudo", "umount", "-l", str(MOUNTPOINT)], cwd=Path.home(), env=env, stdout=stdout, stderr=stderr, timeout=60)
        run(["fusermount3", "-uz", str(MOUNTPOINT)], cwd=Path.home(), env=env, stdout=stdout, stderr=stderr, timeout=60)
    if proc is not None:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    mount_err = RESULT_DIR / "logs" / "fuse-mount" / repo.repo_id / f"run-{run_index}" / "mount.err"
    if mount_err.exists():
        lines = [
            line
            for line in mount_err.read_text(encoding="utf-8", errors="replace").splitlines()
            if "drive9: FUSE perf summary" in line or "drive9: perf " in line
        ]
        if lines:
            emit({"type": "fuse_perf", "session": SESSION, "repo": repo.repo_id, "run": run_index, "lines": lines})


def local_dep_env(repo: Repo, storage: str, run_index: int, env: dict[str, str]) -> dict[str, str]:
    sample = LOCAL_DEPS_DIR / storage / repo.repo_id / f"run-{run_index}"
    env = dict(env)
    env["UV_CACHE_DIR"] = str(BENCH_HOME / "cache" / "uv")
    env["npm_config_cache"] = str(BENCH_HOME / "cache" / "npm")
    env["PNPM_STORE_DIR"] = str(BENCH_HOME / "cache" / "pnpm-store")
    env["npm_config_store_dir"] = env["PNPM_STORE_DIR"]
    env["GOMODCACHE"] = str(BENCH_HOME / "cache" / "go" / "pkg" / "mod")
    env["GOCACHE"] = str(sample / "go-build-cache")
    for key in ("UV_CACHE_DIR", "npm_config_cache", "PNPM_STORE_DIR", "GOMODCACHE", "GOCACHE"):
        Path(env[key]).mkdir(parents=True, exist_ok=True)
    return env


def bind_mounts_for(repo: Repo, checkout: Path, storage: str, run_index: int) -> list[tuple[Path, Path]]:
    base = LOCAL_DEPS_DIR / storage / repo.repo_id / f"run-{run_index}" / "binds"
    mounts: list[tuple[Path, Path]] = []
    if repo.repo_id == "kimi-cli":
        rels = (
            ".venv",
            "web/node_modules",
            "vis/node_modules",
            "src/kimi_cli/deps/bin",
            "src/kimi_cli/deps/tmp",
            "web/dist",
            "vis/dist",
            "src/kimi_cli/web",
            "src/kimi_cli/vis",
            "dist",
            "build",
        )
    elif repo.repo_id == "kimi-code":
        parent_bound = set(KIMI_CODE_PARENT_BIND_DIRS)
        rels = tuple(
            f"{rel}/node_modules" if rel != "." else "node_modules"
            for rel in KIMI_CODE_NODE_MODULE_DIRS
            if rel not in parent_bound
        )
        rels = rels + KIMI_CODE_OUTPUT_DIRS
        rels = rels + KIMI_CODE_PARENT_BIND_DIRS
    else:
        rels = ()
    for rel in rels:
        source = base / rel
        target = checkout / rel
        mounts.append((source, target))
    return mounts


def setup_bind_mounts(mounts: list[tuple[Path, Path]], env: dict[str, str], log_dir: Path) -> list[Path]:
    mounted: list[Path] = []
    stdout = log_dir / "bind-mount.out"
    stderr = log_dir / "bind-mount.err"
    for source, target in mounts:
        source.mkdir(parents=True, exist_ok=True)
        target.mkdir(parents=True, exist_ok=True)
        try:
            if target.exists() and not any(source.iterdir()):
                shutil.copytree(target, source, dirs_exist_ok=True, symlinks=True)
        except OSError:
            pass
        code = run(["sudo", "mount", "--bind", str(source), str(target)], cwd=Path.home(), env=env, stdout=stdout, stderr=stderr, timeout=60)
        if code != 0:
            raise RuntimeError(f"bind mount failed: {source} -> {target}; see {stderr}")
        mounted.append(target)
    return mounted


def teardown_bind_mounts(mounted: list[Path], env: dict[str, str], log_dir: Path) -> None:
    stdout = log_dir / "bind-umount.out"
    stderr = log_dir / "bind-umount.err"
    for target in reversed(mounted):
        run(["sudo", "umount", "-l", str(target)], cwd=Path.home(), env=env, stdout=stdout, stderr=stderr, timeout=60)


def clone_phase(repo: Repo, storage: str, run_index: int, checkout: Path, commit: str, env: dict[str, str]) -> dict[str, Any]:
    log_dir = RESULT_DIR / "logs" / storage / repo.repo_id / f"run-{run_index}"
    stdout = log_dir / "clone.out"
    stderr = log_dir / "clone.err"
    started = utc_now()
    start = time.monotonic()
    checkout.parent.mkdir(parents=True, exist_ok=True)
    commands = [
        ["git", "clone", "--no-checkout", repo.url, str(checkout)],
        ["git", "-C", str(checkout), "checkout", "--detach", commit],
    ]
    exit_code = 0
    for command in commands:
        exit_code = run(command, cwd=checkout.parent, env=env, stdout=stdout, stderr=stderr, timeout=CLONE_TIMEOUT)
        if exit_code != 0:
            break
    duration = time.monotonic() - start
    event = {
        "type": "phase",
        "session": SESSION,
        "repo": repo.repo_id,
        "language": repo.language,
        "storage": storage,
        "run": run_index,
        "phase": "clone",
        "status": "ok" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "duration_seconds": duration,
        "started_at": started,
        "ended_at": utc_now(),
        "cwd": str(checkout.parent),
        "stdout": str(stdout),
        "stderr": str(stderr),
        "commands": [" ".join(command) for command in commands],
        "commit": commit,
        "error": "" if exit_code == 0 else f"clone failed with exit code {exit_code}",
    }
    emit(event)
    return event


def build_phase(repo: Repo, storage: str, run_index: int, checkout: Path, commit: str, env: dict[str, str]) -> dict[str, Any]:
    log_dir = RESULT_DIR / "logs" / storage / repo.repo_id / f"run-{run_index}"
    stdout = log_dir / "build.out"
    stderr = log_dir / "build.err"
    started = utc_now()
    start = time.monotonic()
    exit_code = 0
    cwd = checkout / repo.build_dir
    for command in repo.build:
        exit_code = run(command, cwd=cwd, env=env, stdout=stdout, stderr=stderr, timeout=BUILD_TIMEOUT, shell=True)
        if exit_code != 0:
            break
    duration = time.monotonic() - start
    event = {
        "type": "phase",
        "session": SESSION,
        "repo": repo.repo_id,
        "language": repo.language,
        "storage": storage,
        "run": run_index,
        "phase": "build",
        "status": "ok" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "duration_seconds": duration,
        "started_at": started,
        "ended_at": utc_now(),
        "cwd": str(cwd),
        "stdout": str(stdout),
        "stderr": str(stderr),
        "commands": list(repo.build),
        "commit": commit,
        "error": "" if exit_code == 0 else f"build failed with exit code {exit_code}",
    }
    emit(event)
    return event


def prewarm_repo(repo: Repo, commit: str, env: dict[str, str]) -> None:
    marker = BENCH_HOME / "prewarm" / f"{repo.repo_id}-{commit[:12]}.{RUN_LABEL}-ok"
    if marker.exists():
        print(f"prewarm: {repo.repo_id} already warm", flush=True)
        return
    prewarm_root = BENCH_HOME / "prewarm" / f"{repo.repo_id}-{commit[:12]}.{RUN_LABEL}"
    if prewarm_root.exists():
        shutil.rmtree(prewarm_root)
    prewarm_root.parent.mkdir(parents=True, exist_ok=True)
    logs = RESULT_DIR / "logs" / "prewarm" / repo.repo_id
    code = run(["git", "clone", "--no-checkout", repo.url, str(prewarm_root)], cwd=prewarm_root.parent, env=env, stdout=logs / "clone.out", stderr=logs / "clone.err", timeout=CLONE_TIMEOUT)
    if code == 0:
        code = run(["git", "-C", str(prewarm_root), "checkout", "--detach", commit], cwd=prewarm_root.parent, env=env, stdout=logs / "clone.out", stderr=logs / "clone.err", timeout=CLONE_TIMEOUT)
    if code != 0:
        raise RuntimeError(f"prewarm clone failed for {repo.repo_id}")
    prewarm_env = local_dep_env(repo, "prewarm", 0, env)
    if repo.repo_id == "kimi-code":
        prewarm_env = ensure_node24(prewarm_env)
        run("corepack enable", cwd=prewarm_root, env=prewarm_env, stdout=logs / "prewarm.out", stderr=logs / "prewarm.err", timeout=300, shell=True)
        run("corepack prepare pnpm@10.33.0 --activate", cwd=prewarm_root, env=prewarm_env, stdout=logs / "prewarm.out", stderr=logs / "prewarm.err", timeout=300, shell=True)
    for command in repo.prewarm:
        code = run(command, cwd=prewarm_root / repo.build_dir, env=prewarm_env, stdout=logs / "prewarm.out", stderr=logs / "prewarm.err", timeout=PREWARM_TIMEOUT, shell=True)
        if code != 0:
            raise RuntimeError(f"prewarm failed for {repo.repo_id}: {command}")
    marker.write_text(json.dumps({"commit": commit, "completed_at": utc_now()}) + "\n", encoding="utf-8")
    shutil.rmtree(prewarm_root, ignore_errors=True)
    print(f"prewarm: {repo.repo_id} ready", flush=True)


def write_manifest(commits: dict[str, str], env: dict[str, str]) -> None:
    repos = selected_repos()
    manifest = {
        "session": SESSION,
        "started_at": utc_now(),
        "runs": RUNS,
        "storages": ["native", "fuse"],
        "repos": [repo.repo_id for repo in repos],
        "repo_commits": commits,
        "drive9_env": DRIVE9_ENV,
        "drive9_server": DRIVE9_SERVER,
        "dependency_policy": "fresh per-sample dependency directories bind-mounted from native disk; shared package caches under BENCH_HOME/cache",
        "output_policy": "known build output directories are also bind-mounted from native disk so generated artifacts do not go through Drive9 FUSE",
        "drive9_context_strategy": f"create one fresh {DRIVE9_ENV} drive9 context per repo; context creation is not timed in clone/build phases",
        "git_safe_directory": "GIT_CONFIG_COUNT=1, safe.directory=* for root-started allow_other FUSE mounts",
        "mount_flags": "--mode=fuse --allow-other --profile=interactive --durability=interactive --perf-counters",
        "timeouts": {"clone": CLONE_TIMEOUT, "build": BUILD_TIMEOUT, "prewarm": PREWARM_TIMEOUT},
        "tool_versions": {
            "git": capture(["git", "--version"], env=env),
            "node": capture(["node", "--version"], env=env),
            "npm": capture(["npm", "--version"], env=env),
            "corepack": capture(["corepack", "--version"], env=env),
            "uv": capture(["uv", "--version"], env=env),
            "go": capture(["go", "version"], env=env),
            "drive9": capture(["drive9", "--version"], env=env),
            "mount_ctx": capture(["drive9", "ctx"], env=env),
        },
    }
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    (RESULT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    emit({"type": "manifest", **manifest})


def summarize() -> None:
    events = []
    with (RESULT_DIR / "events.jsonl").open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                event = json.loads(line)
                if event.get("type") == "phase":
                    events.append(event)

    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        key = (event["repo"], event["storage"], event["phase"])
        groups.setdefault(key, []).append(event)

    rows = []
    for repo in [repo.repo_id for repo in selected_repos()]:
        for storage in ("native", "fuse"):
            for phase in ("clone", "build"):
                samples = groups.get((repo, storage, phase), [])
                ok = [sample["duration_seconds"] for sample in samples if sample["status"] == "ok"]
                rows.append(
                    {
                        "repo": repo,
                        "storage": storage,
                        "phase": phase,
                        "ok": len(ok),
                        "count": len(samples),
                        "mean": statistics.mean(ok) if ok else None,
                        "median": statistics.median(ok) if ok else None,
                        "min": min(ok) if ok else None,
                        "max": max(ok) if ok else None,
                    }
                )

    csv_lines = ["repo,storage,phase,ok,count,mean_s,median_s,min_s,max_s"]
    for row in rows:
        nums = [
            "" if row[key] is None else f"{row[key]:.6f}"
            for key in ("mean", "median", "min", "max")
        ]
        csv_lines.append(f"{row['repo']},{row['storage']},{row['phase']},{row['ok']},{row['count']}," + ",".join(nums))
    (RESULT_DIR / "summary.csv").write_text("\n".join(csv_lines) + "\n", encoding="utf-8")

    md = [
        f"# {DRIVE9_ENV.capitalize()} Drive9 Local-Deps Repo Build Benchmark",
        "",
        f"- Session: `{SESSION}`",
        f"- Drive9 endpoint: `{DRIVE9_SERVER}`",
        f"- Runs per repo/storage: `{RUNS}`",
        f"- Context strategy: one fresh {DRIVE9_ENV} drive9 context per repo via `drive9 create`; context creation time is not included in clone/build.",
        "- Dependency policy: dependency caches, install directories, and known build output directories are bind-mounted from native disk under `/mnt/drive9-bench`; remaining repo source stays under the tested storage.",
        "- FUSE mount flags: `--mode=fuse --allow-other --profile=interactive --durability=interactive --perf-counters`; mount is started via `sudo env HOME=/home/ubuntu ...` so bind mounts work without changing `/etc/fuse.conf`.",
        "- Git safety: benchmark env sets `safe.directory=*` because root-started `allow_other` FUSE mounts otherwise trigger Git dubious-ownership checks.",
        "",
        "## Summary",
        "",
        "| repo | storage | phase | ok/count | mean s | median s | min s | max s |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        fmt = lambda value: "" if value is None else f"{value:.3f}"
        md.append(
            f"| {row['repo']} | {row['storage']} | {row['phase']} | "
            f"{row['ok']}/{row['count']} | {fmt(row['mean'])} | {fmt(row['median'])} | {fmt(row['min'])} | {fmt(row['max'])} |"
        )

    md.extend(["", "## FUSE / Native Ratios", ""])
    for repo in [repo.repo_id for repo in selected_repos()]:
        for phase in ("clone", "build"):
            native = next((row for row in rows if row["repo"] == repo and row["storage"] == "native" and row["phase"] == phase), None)
            fuse = next((row for row in rows if row["repo"] == repo and row["storage"] == "fuse" and row["phase"] == phase), None)
            if native and fuse and native["mean"] and fuse["mean"]:
                md.append(f"- `{repo}` {phase}: `{fuse['mean'] / native['mean']:.2f}x`")
            else:
                md.append(f"- `{repo}` {phase}: unavailable")

    md.extend(["", "## Failures", ""])
    failures = [event for event in events if event["status"] != "ok"]
    if not failures:
        md.append("- None")
    else:
        for event in failures:
            md.append(
                f"- `{event['repo']}` `{event['storage']}` `{event['phase']}` run {event['run']}: "
                f"exit `{event['exit_code']}`, see `{event['stderr']}`"
            )
    md.extend(["", "## Raw Artifacts", "", f"- Result directory: `{RESULT_DIR}`"])
    (RESULT_DIR / "summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def main() -> int:
    env = base_env()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"session: {SESSION}", flush=True)
    prepare_drive9_config()
    try:
        env = base_env()
        env = ensure_node24(env)
        run("corepack enable", cwd=Path.home(), env=env, stdout=RESULT_DIR / "logs" / "tooling.out", stderr=RESULT_DIR / "logs" / "tooling.err", timeout=300, shell=True)
        run("corepack prepare pnpm@10.33.0 --activate", cwd=Path.home(), env=env, stdout=RESULT_DIR / "logs" / "tooling.out", stderr=RESULT_DIR / "logs" / "tooling.err", timeout=300, shell=True)
        repos = selected_repos()
        commits = {repo.repo_id: resolve_commit(repo, env) for repo in repos}
        write_manifest(commits, env)
        for repo in repos:
            prewarm_repo(repo, commits[repo.repo_id], env)

        for repo in repos:
            ctx = create_repo_context(repo, env)
            print(f"{utc_now()} repo={repo.repo_id} context={ctx}", flush=True)
            for run_index in range(1, RUNS + 1):
                for storage in ("native", "fuse"):
                    print(f"{utc_now()} run={run_index} repo={repo.repo_id} storage={storage}", flush=True)
                    sample_env = local_dep_env(repo, storage, run_index, env)
                    if repo.repo_id == "kimi-code":
                        sample_env = ensure_node24(sample_env)
                    mount_proc: subprocess.Popen[bytes] | None = None
                    mounted: list[Path] = []
                    if storage == "native":
                        root = WORK_DIR / "native"
                    else:
                        mount_proc = mount_drive9(repo, run_index, sample_env)
                        root = MOUNTPOINT / "bench-localdeps" / SESSION
                    checkout = root / f"{repo.repo_id}-run-{run_index}"
                    log_dir = RESULT_DIR / "logs" / storage / repo.repo_id / f"run-{run_index}"
                    try:
                        if checkout.exists():
                            shutil.rmtree(checkout, ignore_errors=True)
                        clone = clone_phase(repo, storage, run_index, checkout, commits[repo.repo_id], sample_env)
                        if clone["status"] == "ok":
                            mounts = bind_mounts_for(repo, checkout, storage, run_index)
                            mounted = setup_bind_mounts(mounts, sample_env, log_dir)
                            build_phase(repo, storage, run_index, checkout, commits[repo.repo_id], sample_env)
                    except Exception as exc:  # noqa: BLE001
                        emit(
                            {
                                "type": "sample_error",
                                "session": SESSION,
                                "repo": repo.repo_id,
                                "storage": storage,
                                "run": run_index,
                                "error": str(exc),
                                "ended_at": utc_now(),
                            }
                        )
                        print(f"sample error: {repo.repo_id} {storage}: {exc}", flush=True)
                    finally:
                        teardown_bind_mounts(mounted, sample_env, log_dir)
                        if checkout.exists():
                            shutil.rmtree(checkout, ignore_errors=True)
                        if storage == "fuse":
                            unmount_drive9(repo, run_index, mount_proc, sample_env)
        summarize()
        print(f"summary: {RESULT_DIR / 'summary.md'}", flush=True)
        return 0
    finally:
        restore_config()


if __name__ == "__main__":
    raise SystemExit(main())
