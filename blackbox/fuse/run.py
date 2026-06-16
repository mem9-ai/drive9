#!/usr/bin/env python3
"""Self-contained Drive9 FUSE blackbox runner.

The runner intentionally does not call repository e2e or bench scripts. It may
build and execute Drive9 product binaries from this checkout, but all blackbox
case orchestration, reporting, pjdfstests parsing, and performance aggregation
live here.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import platform
import random
import shutil
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


SCHEMA = "drive9-fuse-blackbox/v1"
CONFIG_DIR = Path(__file__).resolve().parent / "config"
REPO_ROOT = Path(__file__).resolve().parents[2]
RESULT_ROOT = REPO_ROOT / "blackbox" / "results" / "fuse"


class BlackboxError(RuntimeError):
    pass


def utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def file_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def load_json(name: str) -> dict[str, Any]:
    return json.loads((CONFIG_DIR / name).read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_bytes(size: int, seed: int = 42) -> bytes:
    rng = random.Random(seed)
    block = bytearray(size)
    for idx in range(size):
        block[idx] = rng.randrange(0, 256)
    return bytes(block)


def ensure_empty(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def pick_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return int(port)


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def http_json(method: str, url: str, token: str = "", body: dict[str, Any] | None = None, timeout: int = 60) -> tuple[int, Any, str]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(text) if text else None
            except json.JSONDecodeError:
                parsed = text
            return int(resp.status), parsed, text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(text) if text else None
        except json.JSONDecodeError:
            parsed = text
        return int(exc.code), parsed, text


@dataclass
class CommandResult:
    code: int
    seconds: float
    stdout: Path
    stderr: Path
    ok: bool


@dataclass
class CaseRecord:
    suite: str
    case: str
    status: str
    seconds: float
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    stdout: str = ""
    stderr: str = ""


class Recorder:
    def __init__(self, result_dir: Path) -> None:
        self.result_dir = result_dir
        self.records: list[CaseRecord] = []
        self.events_path = result_dir / "functional-results.jsonl"
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def case(self, record: CaseRecord) -> None:
        self.records.append(record)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record.__dict__, sort_keys=True) + "\n")

    def event(self, event: dict[str, Any]) -> None:
        path = self.result_dir / "events.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")


@dataclass
class MountHandle:
    mountpoint: Path
    remote_root: str
    proc: subprocess.Popen[bytes]
    log_dir: Path
    cache_dir: Path


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.presets = load_json("presets.json")
        self.suites = load_json("suites.json")
        self.functional_cfg = load_json("cases-functional.json")
        self.perf_cfg = load_json("cases-perf.json")
        self.allowlist = load_json("pjdfstests-allowlist.json")
        self.session = args.session or f"fuse-{file_ts()}"
        self.result_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else RESULT_ROOT / self.session
        self.logs_dir = self.result_dir / "logs"
        self.mount_logs_dir = self.result_dir / "mount-logs"
        self.tmp_dir = self.result_dir / "tmp"
        self.bin_dir = self.result_dir / "bin"
        self.recorder = Recorder(self.result_dir)
        self.cli = Path(args.drive9_cli).expanduser().resolve() if args.drive9_cli else self.bin_dir / "drive9"
        self.server_bin = self.bin_dir / "drive9-server-local"
        self.server_url = os.environ.get("DRIVE9_BASE", "")
        self.api_key = os.environ.get("DRIVE9_API_KEY", "")
        self.local_api_key = os.environ.get("DRIVE9_LOCAL_API_KEY", "local-dev-key")
        self.server_proc: subprocess.Popen[bytes] | None = None
        self.db_container = ""
        self.db_runtime = ""
        self.mounts: list[MountHandle] = []
        self.run_id = self.session.replace("/", "-")
        self.env_home = self.tmp_dir / "home"
        self.strict_prereqs = bool(args.strict_prereqs)
        self.report_daily = False
        self.skipped_for_prereq = False

    def setup_dirs(self) -> None:
        for path in (self.logs_dir, self.mount_logs_dir, self.tmp_dir, self.bin_dir, self.env_home):
            path.mkdir(parents=True, exist_ok=True)

    def base_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["HOME"] = str(self.env_home)
        env["DRIVE9_SERVER"] = self.server_url
        if self.api_key:
            env["DRIVE9_API_KEY"] = self.api_key
        env.setdefault("GIT_AUTHOR_NAME", "Drive9 Blackbox")
        env.setdefault("GIT_AUTHOR_EMAIL", "blackbox@drive9.local")
        env.setdefault("GIT_COMMITTER_NAME", "Drive9 Blackbox")
        env.setdefault("GIT_COMMITTER_EMAIL", "blackbox@drive9.local")
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        env.setdefault("GIT_CONFIG_COUNT", "1")
        env.setdefault("GIT_CONFIG_KEY_0", "safe.directory")
        env.setdefault("GIT_CONFIG_VALUE_0", "*")
        env["PATH"] = os.environ.get("PATH", "")
        return env

    def run_cmd(
        self,
        name: str,
        cmd: list[str] | str,
        cwd: Path | None = None,
        timeout: int = 120,
        env: dict[str, str] | None = None,
        shell: bool = False,
        ok_codes: tuple[int, ...] = (0,),
    ) -> CommandResult:
        log_dir = self.logs_dir / name
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout = log_dir / "stdout.log"
        stderr = log_dir / "stderr.log"
        display = cmd if isinstance(cmd, str) else " ".join(cmd)
        start = time.monotonic()
        with stdout.open("ab") as out, stderr.open("ab") as err:
            out.write(f"\n# {utc_ts()} $ {display}\n".encode())
            out.flush()
            proc = subprocess.Popen(
                cmd,
                cwd=str(cwd or REPO_ROOT),
                env=env or self.base_env(),
                stdout=out,
                stderr=err,
                shell=shell,
                executable="/bin/bash" if shell else None,
                start_new_session=True,
            )
            try:
                code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                err.write(f"\n# {utc_ts()} timeout after {timeout}s\n".encode())
                err.flush()
                self.kill_process_group(proc)
                code = 124
        seconds = time.monotonic() - start
        return CommandResult(code=code, seconds=seconds, stdout=stdout, stderr=stderr, ok=code in ok_codes)

    def capture(self, cmd: list[str], cwd: Path | None = None, timeout: int = 120, env: dict[str, str] | None = None) -> str:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd or REPO_ROOT),
            env=env or self.base_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0:
            raise BlackboxError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout[-2000:]}")
        return proc.stdout

    @staticmethod
    def kill_process_group(proc: subprocess.Popen[bytes]) -> None:
        if proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)

    def build_cli(self) -> None:
        if self.args.drive9_cli:
            if not self.cli.exists():
                raise BlackboxError(f"drive9 CLI not found: {self.cli}")
            return
        env = dict(os.environ)
        env["CGO_ENABLED"] = "0"
        result = self.run_cmd("build-cli", ["go", "build", "-o", str(self.cli), "./cmd/drive9"], timeout=600, env=env)
        if not result.ok:
            raise BlackboxError(f"build drive9 CLI failed; see {result.stderr}")

    def build_server_local(self) -> None:
        env = dict(os.environ)
        env["CGO_ENABLED"] = "0"
        result = self.run_cmd("build-server-local", ["go", "build", "-o", str(self.server_bin), "./cmd/drive9-server-local"], timeout=600, env=env)
        if not result.ok:
            raise BlackboxError(f"build drive9-server-local failed; see {result.stderr}")

    def detect_fuse(self) -> tuple[bool, str]:
        system = platform.system()
        if system == "Linux":
            if not Path("/dev/fuse").exists():
                return False, "/dev/fuse is missing"
            if not shutil.which("fusermount3") and not shutil.which("fusermount"):
                return False, "fusermount3/fusermount is missing"
            return True, "linux fuse prerequisites ok"
        if system == "Darwin":
            if shutil.which("mount_macfuse") or shutil.which("mount_fusefs"):
                return True, "macOS FUSE prerequisites ok"
            return False, "macFUSE/FUSE-T mount helper missing"
        return False, f"unsupported OS for FUSE blackbox: {system}"

    def maybe_skip_for_fuse(self) -> bool:
        ok, detail = self.detect_fuse()
        if ok:
            self.recorder.event({"type": "prereq", "name": "fuse", "status": "PASS", "detail": detail})
            return False
        self.recorder.event({"type": "prereq", "name": "fuse", "status": "SKIP", "detail": detail})
        self.recorder.case(CaseRecord(suite="prereq", case="fuse", status="SKIP", seconds=0, detail=detail))
        self.skipped_for_prereq = True
        self.write_manifest()
        self.write_daily_report()
        if self.strict_prereqs:
            raise BlackboxError(detail)
        return True

    def detect_runtime(self) -> str:
        runtime = os.environ.get("DRIVE9_LOCAL_E2E_DB_RUNTIME", "")
        if runtime:
            if shutil.which(runtime):
                return runtime
            raise BlackboxError(f"configured container runtime not found: {runtime}")
        if shutil.which("docker"):
            return "docker"
        if shutil.which("podman"):
            return "podman"
        raise BlackboxError("docker or podman is required when DRIVE9_LOCAL_DSN is not set")

    def start_mysql_if_needed(self) -> str:
        dsn = os.environ.get("DRIVE9_LOCAL_DSN", "")
        if dsn:
            return dsn
        runtime = self.detect_runtime()
        self.db_runtime = runtime
        image = os.environ.get("DRIVE9_LOCAL_E2E_DB_IMAGE", "mysql:8.4")
        password = os.environ.get("DRIVE9_LOCAL_E2E_DB_PASSWORD", "drive9pass")
        db_name = os.environ.get("DRIVE9_LOCAL_E2E_DB_NAME", "drive9_local")
        self.db_container = f"drive9-blackbox-{int(time.time())}-{os.getpid()}"
        result = self.run_cmd(
            "mysql-container-run",
            [
                runtime,
                "run",
                "-d",
                "--name",
                self.db_container,
                "-e",
                f"MYSQL_ROOT_PASSWORD={password}",
                "-e",
                f"MYSQL_DATABASE={db_name}",
                "-p",
                "127.0.0.1::3306",
                image,
            ],
            timeout=180,
        )
        if not result.ok:
            raise BlackboxError(f"start MySQL container failed; see {result.stderr}")
        deadline = time.monotonic() + 120
        port = ""
        while time.monotonic() < deadline:
            try:
                out = self.capture([runtime, "port", self.db_container, "3306/tcp"], timeout=20)
                port = out.strip().rsplit(":", 1)[-1]
            except Exception:
                port = ""
            if port:
                ping = subprocess.run(
                    [runtime, "exec", self.db_container, "mysqladmin", "ping", "-uroot", f"-p{password}", "--silent"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                if ping.returncode == 0:
                    return f"root:{password}@tcp(127.0.0.1:{port})/{db_name}?parseTime=true"
            time.sleep(1)
        raise BlackboxError("timed out waiting for MySQL container readiness")

    def start_server(self) -> None:
        mode = self.args.server_mode
        if mode == "auto":
            mode = "existing" if self.server_url else "local"
        if mode == "existing":
            if not self.server_url:
                raise BlackboxError("DRIVE9_BASE is required for --server-mode existing")
            if not self.api_key:
                self.provision_existing_server()
            return
        if mode != "local":
            raise BlackboxError(f"unknown server mode: {mode}")
        self.build_server_local()
        dsn = self.start_mysql_if_needed()
        listen_addr = f"127.0.0.1:{pick_port()}"
        self.server_url = f"http://{listen_addr}"
        self.api_key = self.local_api_key
        log = self.logs_dir / "drive9-server-local.log"
        env = dict(os.environ)
        env.update(
            {
                "DRIVE9_LISTEN_ADDR": listen_addr,
                "DRIVE9_PUBLIC_URL": self.server_url,
                "DRIVE9_LOCAL_DSN": dsn,
                "DRIVE9_LOCAL_INIT_SCHEMA": os.environ.get("DRIVE9_LOCAL_INIT_SCHEMA", "true"),
                "DRIVE9_LOCAL_EMBEDDING_MODE": os.environ.get("DRIVE9_LOCAL_EMBEDDING_MODE", "none"),
                "DRIVE9_LOCAL_API_KEY": self.api_key,
                "DRIVE9_S3_DIR": os.environ.get("DRIVE9_S3_DIR", str(self.tmp_dir / "s3")),
                "DRIVE9_LOG_LEVEL": os.environ.get("DRIVE9_LOG_LEVEL", "warn"),
            }
        )
        with log.open("ab") as handle:
            handle.write(f"# {utc_ts()} starting drive9-server-local at {self.server_url}\n".encode())
            self.server_proc = subprocess.Popen([str(self.server_bin)], cwd=str(REPO_ROOT), env=env, stdout=handle, stderr=handle, start_new_session=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.server_proc.poll() is not None:
                raise BlackboxError(f"drive9-server-local exited early; see {log}")
            try:
                code, parsed, _ = http_json("GET", f"{self.server_url}/healthz", timeout=5)
                if code == 200:
                    self.recorder.event({"type": "server", "mode": "local", "url": self.server_url, "health": parsed})
                    return
            except Exception:
                pass
            time.sleep(1)
        raise BlackboxError(f"timed out waiting for drive9-server-local; see {log}")

    def provision_existing_server(self) -> None:
        code, parsed, raw = http_json("POST", f"{self.server_url}/v1/provision", timeout=60)
        if code not in (200, 202) or not isinstance(parsed, dict):
            raise BlackboxError(f"provision failed: code={code} body={raw}")
        key = str(parsed.get("api_key") or "")
        if not key:
            raise BlackboxError(f"provision response missing api_key: {raw}")
        self.api_key = key
        deadline = time.monotonic() + int(os.environ.get("POLL_TIMEOUT_S", "120"))
        while time.monotonic() < deadline:
            status_code, status_body, _ = http_json("GET", f"{self.server_url}/v1/status", token=self.api_key, timeout=20)
            if status_code == 200 and isinstance(status_body, dict) and status_body.get("status") == "active":
                return
            time.sleep(float(os.environ.get("POLL_INTERVAL_S", "2")))
        raise BlackboxError("provisioned tenant did not become active")

    def drive9(self, name: str, args: list[str], timeout: int = 120, ok_codes: tuple[int, ...] = (0,)) -> CommandResult:
        return self.run_cmd(name, [str(self.cli), *args], timeout=timeout, env=self.base_env(), ok_codes=ok_codes)

    def drive9_capture(self, args: list[str], timeout: int = 120) -> str:
        return self.capture([str(self.cli), *args], timeout=timeout, env=self.base_env())

    def remote_root(self, case: str) -> str:
        return f"/blackbox-fuse/{self.run_id}/{case}"

    def mkdir_remote(self, remote: str) -> None:
        result = self.drive9(f"mkdir-{remote.strip('/').replace('/', '-')}", ["fs", "mkdir", f":{remote}"], timeout=120, ok_codes=(0,))
        if not result.ok:
            raise BlackboxError(f"remote mkdir failed for {remote}; see {result.stderr}")

    def mount(
        self,
        case: str,
        remote_root: str,
        *,
        read_only: bool = False,
        profile: str = "none",
        durability: str = "interactive",
        cache_key: str = "",
        extra: list[str] | None = None,
    ) -> MountHandle:
        mountpoint = self.tmp_dir / "mounts" / case / (cache_key or "primary")
        local_root = self.tmp_dir / "local-roots" / case / (cache_key or "primary")
        cache_dir = self.tmp_dir / "caches" / case / (cache_key or "primary")
        for path in (mountpoint, local_root, cache_dir):
            path.mkdir(parents=True, exist_ok=True)
        log_dir = self.mount_logs_dir / case / (cache_key or "primary")
        log_dir.mkdir(parents=True, exist_ok=True)
        command = [
            str(self.cli),
            "mount",
            "--mode=fuse",
            "--foreground",
            "--profile",
            profile,
            "--durability",
            durability,
            "--perf-counters",
            "--cache-dir",
            str(cache_dir),
            f":{remote_root}",
            str(mountpoint),
        ]
        if profile and profile not in ("none", "interactive"):
            command[6:6] = ["--local-root", str(local_root)]
        if read_only:
            command.insert(3, "--read-only")
        if extra:
            command[3:3] = extra
        env = self.base_env()
        out = (log_dir / "mount.out").open("ab")
        err = (log_dir / "mount.err").open("ab")
        out.write(f"\n# {utc_ts()} $ {' '.join(command)}\n".encode())
        out.flush()
        proc = subprocess.Popen(command, cwd=str(REPO_ROOT), env=env, stdout=out, stderr=err, start_new_session=True)
        deadline = time.monotonic() + int(os.environ.get("MOUNT_READY_TIMEOUT_S", "30"))
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise BlackboxError(f"mount exited early for {case}; see {log_dir / 'mount.err'}")
            if self.is_mounted(mountpoint):
                handle = MountHandle(mountpoint=mountpoint, remote_root=remote_root, proc=proc, log_dir=log_dir, cache_dir=cache_dir)
                self.mounts.append(handle)
                return handle
            time.sleep(0.25)
        self.kill_process_group(proc)
        raise BlackboxError(f"mount did not become ready for {case}; see {log_dir / 'mount.err'}")

    def is_mounted(self, mountpoint: Path) -> bool:
        if shutil.which("mountpoint"):
            return subprocess.run(["mountpoint", "-q", str(mountpoint)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0
        if os.path.ismount(mountpoint):
            return True
        try:
            physical = mountpoint.resolve()
            mount_text = self.capture(["mount"], timeout=20)
            return str(mountpoint) in mount_text or str(physical) in mount_text
        except Exception:
            return False

    def unmount(self, handle: MountHandle, *, force: bool = False) -> None:
        if self.is_mounted(handle.mountpoint):
            self.drive9(f"umount-{handle.mountpoint.name}", ["umount", "--timeout", os.environ.get("FUSE_UMOUNT_TIMEOUT", "60s"), str(handle.mountpoint)], timeout=90, ok_codes=(0, 1))
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and self.is_mounted(handle.mountpoint):
            time.sleep(0.25)
        if force and self.is_mounted(handle.mountpoint):
            if platform.system() == "Linux":
                for cmd in (["fusermount3", "-uz", str(handle.mountpoint)], ["fusermount", "-uz", str(handle.mountpoint)], ["umount", "-l", str(handle.mountpoint)]):
                    if shutil.which(cmd[0]):
                        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                        if not self.is_mounted(handle.mountpoint):
                            break
            elif platform.system() == "Darwin":
                subprocess.run(["umount", "-f", str(handle.mountpoint)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        self.kill_process_group(handle.proc)

    def cleanup(self) -> None:
        for handle in reversed(self.mounts):
            try:
                self.unmount(handle, force=True)
            except Exception:
                pass
        self.mounts.clear()
        if self.server_proc is not None:
            self.kill_process_group(self.server_proc)
        if self.db_container and self.db_runtime and os.environ.get("DRIVE9_LOCAL_E2E_KEEP_DB", "0") != "1":
            subprocess.run([self.db_runtime, "rm", "-f", self.db_container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if not self.args.keep_artifacts and not any(record.status.startswith("FAIL") for record in self.recorder.records):
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def run_case(self, suite: str, case: str, fn: Callable[[], dict[str, Any] | None]) -> None:
        start = time.monotonic()
        try:
            metrics = fn() or {}
            self.recorder.case(CaseRecord(suite=suite, case=case, status="PASS", seconds=time.monotonic() - start, metrics=metrics))
        except SkipCase as exc:
            self.recorder.case(CaseRecord(suite=suite, case=case, status="SKIP", seconds=time.monotonic() - start, detail=str(exc)))
        except Exception as exc:
            self.recorder.case(CaseRecord(suite=suite, case=case, status="FAIL", seconds=time.monotonic() - start, detail=f"{type(exc).__name__}: {exc}"))
            if self.args.fail_fast:
                raise

    def write_manifest(self) -> None:
        manifest = {
            "schema": SCHEMA,
            "session": self.session,
            "timestamp": utc_ts(),
            "repo_root": str(REPO_ROOT),
            "result_dir": str(self.result_dir),
            "server_url": self.server_url,
            "server_mode": self.args.server_mode,
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "drive9_cli": str(self.cli),
            "preset": self.args.preset,
            "suite": self.args.suite,
            "runs": self.args.runs,
            "strict_prereqs": self.strict_prereqs,
            "skipped_for_prereq": self.skipped_for_prereq,
        }
        try:
            manifest["git_sha"] = self.capture(["git", "rev-parse", "HEAD"], timeout=20).strip()
        except Exception:
            manifest["git_sha"] = "unknown"
        try:
            if self.cli.exists():
                manifest["drive9_version"] = self.capture([str(self.cli), "--version"], timeout=20).strip()
        except Exception as exc:
            manifest["drive9_version_error"] = str(exc)
        (self.result_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def write_daily_report(self) -> None:
        lines = [
            "# Drive9 FUSE Blackbox Report",
            "",
            f"- Session: `{self.session}`",
            f"- Timestamp: `{utc_ts()}`",
            f"- Platform: `{platform.platform()}`",
            f"- Server: `{self.server_url or 'not-started'}`",
            f"- Result dir: `{self.result_dir}`",
            "",
            "## Summary",
            "",
            "| Suite | Case | Status | Seconds | Detail |",
            "|---|---|---:|---:|---|",
        ]
        for record in self.recorder.records:
            detail = record.detail.replace("|", "\\|")[:240]
            lines.append(f"| `{record.suite}` | `{record.case}` | {record.status} | {record.seconds:.3f} | {detail} |")
        perf_path = self.result_dir / "perf-results.json"
        if perf_path.exists():
            lines.extend(["", "## Performance", ""])
            perf = json.loads(perf_path.read_text(encoding="utf-8"))
            lines.append("| Workload | Unit | Mean | Median | Runs |")
            lines.append("|---|---|---:|---:|---|")
            for name, data in sorted(perf.get("results", {}).items()):
                lines.append(f"| `{name}` | {data.get('unit', '')} | {float(data.get('mean', 0)):.3f} | {float(data.get('median', 0)):.3f} | {len(data.get('values', []))} |")
        posix_path = self.result_dir / "pjdfstests.json"
        if posix_path.exists():
            posix = json.loads(posix_path.read_text(encoding="utf-8"))
            lines.extend(
                [
                    "",
                    "## POSIX",
                    "",
                    f"- Raw pass rate: `{posix.get('raw_pass_rate', 0):.4f}`",
                    f"- Effective pass rate: `{posix.get('effective_pass_rate', 0):.4f}`",
                    f"- PASS: `{posix.get('passed_cases', 0)}`",
                    f"- FAIL_REGRESSION: `{posix.get('fail_regression_cases', 0)}`",
                    f"- XFAIL_KNOWN: `{posix.get('xfail_known_cases', 0)}`",
                ]
            )
        (self.result_dir / "daily-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def selected_suites(self) -> list[tuple[str, str]]:
        if self.args.preset:
            preset = self.presets[self.args.preset]
            self.strict_prereqs = bool(preset.get("strict_prereqs", False)) or bool(self.args.strict_prereqs)
            self.report_daily = bool(preset.get("report_daily", False))
            return [(item["name"], item.get("tier", "daily")) for item in preset["suites"]]
        if not self.args.suite:
            raise BlackboxError("one of --preset or --suite is required")
        return [(self.args.suite, self.args.tier or "daily")]

    def run(self) -> int:
        self.setup_dirs()
        suites = self.selected_suites()
        if self.maybe_skip_for_fuse():
            return 0
        self.build_cli()
        self.start_server()
        self.write_manifest()
        for suite, tier in suites:
            if suite == "functional":
                self.run_functional_suite(tier)
            elif suite == "posix":
                self.run_posix_suite(tier)
            elif suite == "perf":
                self.run_perf_suite(tier)
            else:
                raise BlackboxError(f"unknown suite: {suite}")
        self.write_manifest()
        self.write_daily_report()
        failures = [record for record in self.recorder.records if record.status == "FAIL"]
        return 1 if failures else 0

    # Functional cases.

    def run_functional_suite(self, tier: str) -> None:
        all_cases = {item["id"] for item in self.functional_cfg["cases"]}
        selected = self.suites["functional"]["tiers"][tier]
        case_ids = list(all_cases if selected == "all" else selected)
        order = [item["id"] for item in self.functional_cfg["cases"] if item["id"] in case_ids]
        case_map: dict[str, Callable[[], dict[str, Any] | None]] = {
            "mount_rw_basic": self.case_mount_rw_basic,
            "cross_channel_visibility": self.case_cross_channel_visibility,
            "file_semantics": self.case_file_semantics,
            "metadata_ops": self.case_metadata_ops,
            "links": self.case_links,
            "readonly_mount": self.case_readonly_mount,
            "subtree_mount": self.case_subtree_mount,
            "concurrency": self.case_concurrency,
            "sqlite_wal": self.case_sqlite_wal,
            "git_workspace": self.case_git_workspace,
            "crash_recovery": self.case_crash_recovery,
        }
        for case_id in order:
            self.run_case("functional", case_id, case_map[case_id])

    def case_mount_rw_basic(self) -> dict[str, Any]:
        remote = self.remote_root("mount-rw-basic")
        self.mkdir_remote(remote)
        handle = self.mount("mount_rw_basic", remote)
        path = handle.mountpoint / "hello.txt"
        data = "hello from drive9 fuse\n"
        with path.open("w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        got = path.read_text(encoding="utf-8")
        if got != data:
            raise BlackboxError("mounted read mismatch")
        self.unmount(handle)
        remote_got = self.drive9_capture(["fs", "cat", f":{remote}/hello.txt"], timeout=60)
        if remote_got != data:
            raise BlackboxError("remote read after unmount mismatch")
        return {"bytes": len(data)}

    def case_cross_channel_visibility(self) -> dict[str, Any]:
        remote = self.remote_root("cross-channel")
        self.mkdir_remote(remote)
        local_seed = self.tmp_dir / "cross-channel-seed.txt"
        local_seed.write_text("cli to fuse\n", encoding="utf-8")
        cp = self.drive9("cross-channel-cli-cp", ["fs", "cp", str(local_seed), f":{remote}/cli.txt"], timeout=120)
        if not cp.ok:
            raise BlackboxError(f"CLI seed upload failed; see {cp.stderr}")
        handle = self.mount("cross_channel_visibility", remote)
        if (handle.mountpoint / "cli.txt").read_text(encoding="utf-8") != "cli to fuse\n":
            raise BlackboxError("CLI write not visible through FUSE")
        (handle.mountpoint / "fuse.txt").write_text("fuse to cli\n", encoding="utf-8")
        self.unmount(handle)
        got = self.drive9_capture(["fs", "cat", f":{remote}/fuse.txt"], timeout=60)
        if got != "fuse to cli\n":
            raise BlackboxError("FUSE write not visible through CLI")
        code, body, raw = http_json("GET", f"{self.server_url}/v1/fs/{urllib.parse.quote((remote + '/fuse.txt').lstrip('/'))}", token=self.api_key)
        if code != 200 or raw != "fuse to cli\n":
            raise BlackboxError(f"FUSE write not visible through API: code={code} body={raw}")
        return {}

    def case_file_semantics(self) -> dict[str, Any]:
        remote = self.remote_root("file-semantics")
        self.mkdir_remote(remote)
        handle = self.mount("file_semantics", remote, durability="write-sync")
        root = handle.mountpoint
        (root / "empty.bin").write_bytes(b"")
        small = stable_bytes(4096, 1)
        (root / "small.bin").write_bytes(small)
        large_mib = int(self.functional_cfg["defaults"]["large_file_mib"])
        large_data = stable_bytes(large_mib * 1024 * 1024, 2)
        large_path = root / "large.bin"
        with large_path.open("wb") as f:
            f.write(large_data)
            f.flush()
            os.fsync(f.fileno())
        with (root / "small.bin").open("r+b") as f:
            f.seek(128)
            f.write(b"OVERWRITE")
            f.truncate(1024)
            f.flush()
            os.fsync(f.fileno())
        with (root / "sparse.bin").open("wb") as f:
            f.seek(1024 * 1024)
            f.write(b"tail")
            f.flush()
            os.fsync(f.fileno())
        if (root / "sparse.bin").stat().st_size != 1024 * 1024 + 4:
            raise BlackboxError("sparse file size mismatch")
        large_hash = sha256_file(large_path)
        self.unmount(handle)
        downloaded = self.tmp_dir / "large-downloaded.bin"
        cp = self.drive9("file-semantics-download", ["fs", "cp", f":{remote}/large.bin", str(downloaded)], timeout=300)
        if not cp.ok:
            raise BlackboxError(f"download large file failed; see {cp.stderr}")
        if sha256_file(downloaded) != large_hash:
            raise BlackboxError("large file checksum mismatch after remote download")
        return {"large_mib": large_mib, "large_sha256": large_hash}

    def case_metadata_ops(self) -> dict[str, Any]:
        remote = self.remote_root("metadata")
        self.mkdir_remote(remote)
        handle = self.mount("metadata_ops", remote)
        root = handle.mountpoint
        (root / "dir").mkdir()
        file_path = root / "dir" / "a.txt"
        file_path.write_text("metadata\n", encoding="utf-8")
        os.chmod(file_path, 0o600)
        renamed = root / "dir" / "renamed.txt"
        file_path.rename(renamed)
        if not renamed.exists() or file_path.exists():
            raise BlackboxError("rename did not update local tree")
        mode = renamed.stat().st_mode & 0o777
        if mode != 0o600:
            raise BlackboxError(f"chmod mode={oct(mode)}, want 0o600")
        renamed.unlink()
        (root / "dir").rmdir()
        self.unmount(handle)
        return {}

    def case_links(self) -> dict[str, Any]:
        remote = self.remote_root("links")
        self.mkdir_remote(remote)
        handle = self.mount("links", remote)
        root = handle.mountpoint
        target = root / "target.txt"
        target.write_text("link target\n", encoding="utf-8")
        os.symlink("target.txt", root / "symlink.txt")
        if os.readlink(root / "symlink.txt") != "target.txt":
            raise BlackboxError("readlink target mismatch")
        os.link(target, root / "hardlink.txt")
        if (root / "hardlink.txt").read_text(encoding="utf-8") != "link target\n":
            raise BlackboxError("hardlink content mismatch")
        nlink = target.stat().st_nlink
        if nlink < 2:
            raise BlackboxError(f"nlink={nlink}, want >= 2")
        self.unmount(handle)
        return {"nlink": nlink}

    def case_readonly_mount(self) -> dict[str, Any]:
        remote = self.remote_root("readonly")
        self.mkdir_remote(remote)
        seed = self.tmp_dir / "readonly-seed.txt"
        seed.write_text("readonly seed\n", encoding="utf-8")
        cp = self.drive9("readonly-seed", ["fs", "cp", str(seed), f":{remote}/seed.txt"], timeout=120)
        if not cp.ok:
            raise BlackboxError(f"readonly seed upload failed; see {cp.stderr}")
        handle = self.mount("readonly_mount", remote, read_only=True)
        if (handle.mountpoint / "seed.txt").read_text(encoding="utf-8") != "readonly seed\n":
            raise BlackboxError("read-only seed read mismatch")
        try:
            (handle.mountpoint / "new.txt").write_text("should fail", encoding="utf-8")
        except OSError:
            pass
        else:
            raise BlackboxError("read-only mount accepted write")
        self.unmount(handle)
        return {}

    def case_subtree_mount(self) -> dict[str, Any]:
        remote = self.remote_root("subtree")
        self.mkdir_remote(f"{remote}/sub")
        seed = self.tmp_dir / "subtree-seed.txt"
        seed.write_text("subtree seed\n", encoding="utf-8")
        cp = self.drive9("subtree-seed", ["fs", "cp", str(seed), f":{remote}/sub/seed.txt"], timeout=120)
        if not cp.ok:
            raise BlackboxError(f"subtree seed upload failed; see {cp.stderr}")
        handle = self.mount("subtree_mount", f"{remote}/sub")
        if (handle.mountpoint / "seed.txt").read_text(encoding="utf-8") != "subtree seed\n":
            raise BlackboxError("subtree seed read mismatch")
        (handle.mountpoint / "new.txt").write_text("subtree new\n", encoding="utf-8")
        self.unmount(handle)
        got = self.drive9_capture(["fs", "cat", f":{remote}/sub/new.txt"], timeout=60)
        if got != "subtree new\n":
            raise BlackboxError("subtree write not visible at remote path")
        return {}

    def case_concurrency(self) -> dict[str, Any]:
        remote = self.remote_root("concurrency")
        self.mkdir_remote(remote)
        workers = int(self.functional_cfg["defaults"]["concurrency_workers"])
        files_per_worker = int(self.functional_cfg["defaults"]["concurrency_files_per_worker"])
        handle = self.mount("concurrency", remote)
        root = handle.mountpoint
        errors: list[str] = []

        def worker(idx: int) -> None:
            try:
                d = root / f"w{idx:02d}"
                d.mkdir()
                for n in range(files_per_worker):
                    p = d / f"f{n:03d}.txt"
                    payload = f"worker={idx} file={n}\n"
                    p.write_text(payload, encoding="utf-8")
                    if p.read_text(encoding="utf-8") != payload:
                        raise BlackboxError(f"readback mismatch {p}")
                    p.rename(d / f"r{n:03d}.txt")
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if errors:
            raise BlackboxError("; ".join(errors[:5]))
        count = sum(1 for _ in root.rglob("*.txt"))
        want = workers * files_per_worker
        if count != want:
            raise BlackboxError(f"concurrency file count={count}, want={want}")
        self.unmount(handle)
        return {"files": count}

    def case_sqlite_wal(self) -> dict[str, Any]:
        remote = self.remote_root("sqlite-wal")
        self.mkdir_remote(remote)
        rows = int(self.functional_cfg["defaults"]["sqlite_rows"])
        handle = self.mount("sqlite_wal", remote, durability="write-sync")
        db_path = handle.mountpoint / "test.db"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
            conn.executemany("INSERT INTO t(payload) VALUES (?)", [(f"row-{idx}",) for idx in range(rows)])
            conn.commit()
            got = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            if got != rows:
                raise BlackboxError(f"sqlite count={got}, want={rows}")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
        self.unmount(handle)
        handle2 = self.mount("sqlite_wal_remount", remote, durability="write-sync")
        conn = sqlite3.connect(str(handle2.mountpoint / "test.db"))
        try:
            got = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            if got != rows:
                raise BlackboxError(f"sqlite remount count={got}, want={rows}")
        finally:
            conn.close()
            self.unmount(handle2)
        return {"rows": rows}

    def create_git_fixture(self) -> Path:
        fixture = self.tmp_dir / "git-fixture"
        work = fixture / "work"
        bare = fixture / "repo.git"
        ensure_empty(fixture)
        work.mkdir()
        self.capture(["git", "init"], cwd=work)
        self.capture(["git", "config", "user.name", "Drive9 Blackbox"], cwd=work)
        self.capture(["git", "config", "user.email", "blackbox@drive9.local"], cwd=work)
        (work / "README.md").write_text("# fixture\n", encoding="utf-8")
        (work / "src").mkdir()
        (work / "src" / "app.py").write_text("print('fixture')\n", encoding="utf-8")
        (work / "script.sh").write_text("#!/bin/sh\necho fixture\n", encoding="utf-8")
        os.chmod(work / "script.sh", 0o755)
        os.symlink("README.md", work / "link-to-readme")
        self.capture(["git", "add", "."], cwd=work)
        self.capture(["git", "commit", "-m", "fixture"], cwd=work)
        self.capture(["git", "clone", "--bare", str(work), str(bare)], cwd=fixture)
        return bare

    def case_git_workspace(self) -> dict[str, Any]:
        remote = self.remote_root("git-workspace")
        self.mkdir_remote(remote)
        bare = self.create_git_fixture()
        handle = self.mount("git_workspace", remote, profile="coding-agent")
        repo = handle.mountpoint / "repo"
        clone = self.drive9(
            "git-workspace-fast-blobless",
            ["git", "clone", "--fast", "--blobless", "--hydrate=sync", str(bare), str(repo)],
            timeout=300,
        )
        if not clone.ok:
            raise BlackboxError(f"drive9 git clone failed; see {clone.stderr}")
        self.capture(["git", "config", "user.name", "Drive9 Blackbox"], cwd=repo)
        self.capture(["git", "config", "user.email", "blackbox@drive9.local"], cwd=repo)
        status = self.capture(["git", "status", "--porcelain"], cwd=repo).strip()
        if status:
            raise BlackboxError(f"clean repo status not empty: {status}")
        (repo / "README.md").write_text("# fixture\n\nedited\n", encoding="utf-8")
        (repo / "new.txt").write_text("new\n", encoding="utf-8")
        self.capture(["git", "add", "-A"], cwd=repo)
        self.capture(["git", "commit", "-m", "blackbox edit"], cwd=repo)
        self.unmount(handle)
        return {}

    def case_crash_recovery(self) -> dict[str, Any]:
        remote = self.remote_root("crash-recovery")
        self.mkdir_remote(remote)
        handle = self.mount("crash_recovery", remote, durability="interactive", cache_key="shared")
        (handle.mountpoint / "before-crash.txt").write_text("before crash\n", encoding="utf-8")
        # Simulate a hard client loss. The forced detach path is intentionally
        # best-effort because macOS and Linux expose different unmount tools.
        self.kill_process_group(handle.proc)
        self.unmount(handle, force=True)
        handle2 = self.mount("crash_recovery", remote, durability="interactive", cache_key="shared")
        (handle2.mountpoint / "after-remount.txt").write_text("after remount\n", encoding="utf-8")
        self.unmount(handle2)
        got = self.drive9_capture(["fs", "cat", f":{remote}/after-remount.txt"], timeout=60)
        if got != "after remount\n":
            raise BlackboxError("post-remount write not durable")
        return {}

    # POSIX / pjdfstests.

    def run_posix_suite(self, tier: str) -> None:
        self.run_case("posix", "pjdfstests", lambda: self.case_pjdfstests(tier))

    def resolve_pjdfstests(self) -> tuple[Path, Path]:
        tests = os.environ.get("PJDFSTEST_TESTS", "")
        root = os.environ.get("PJDFSTEST_DIR", "")
        candidates: list[Path] = []
        if tests:
            candidates.append(Path(tests).expanduser())
        if root:
            candidates.append(Path(root).expanduser() / "tests")
        candidates.extend(
            [
                REPO_ROOT / "third_party" / "pjdfstest" / "tests",
                REPO_ROOT / "pjdfstest" / "tests",
                Path("/usr/local/share/pjdfstest/tests"),
                Path("/opt/pjdfstest/tests"),
            ]
        )
        tests_dir = next((path.resolve() for path in candidates if path.is_dir()), None)
        if tests_dir is None:
            raise SkipCase("pjdfstests tests directory not found; set PJDFSTEST_DIR or PJDFSTEST_TESTS")
        bin_candidates = []
        if os.environ.get("PJDFSTEST_BIN"):
            bin_candidates.append(Path(os.environ["PJDFSTEST_BIN"]).expanduser())
        bin_candidates.append(tests_dir.parent / "pjdfstest")
        if shutil.which("pjdfstest"):
            bin_candidates.append(Path(shutil.which("pjdfstest") or ""))
        bin_path = next((path.resolve() for path in bin_candidates if path and path.exists() and os.access(path, os.X_OK)), None)
        if bin_path is None:
            raise SkipCase("pjdfstest binary not found; set PJDFSTEST_BIN")
        return tests_dir, bin_path

    def case_pjdfstests(self, tier: str) -> dict[str, Any]:
        if not shutil.which("prove"):
            raise SkipCase("prove is required for pjdfstests")
        if os.geteuid() != 0 and os.environ.get("PJDFSTEST_ALLOW_NONROOT", "0") != "1":
            raise SkipCase("pjdfstests requires root; set PJDFSTEST_ALLOW_NONROOT=1 to run anyway")
        tests_dir, bin_path = self.resolve_pjdfstests()
        remote = self.remote_root("pjdfstests")
        self.mkdir_remote(remote)
        handle = self.mount("pjdfstests", remote, durability="write-sync")
        work_dir = handle.mountpoint / "work"
        work_dir.mkdir()
        selected = self.suites["posix"]["tiers"][tier]
        if selected == "all":
            test_args = [str(tests_dir)]
        else:
            test_args = [str(tests_dir / group) for group in selected if (tests_dir / group).exists()]
            if not test_args:
                raise SkipCase(f"no pjdfstests groups found for tier {tier}")
        log = self.result_dir / "pjdfstests.log"
        env = self.base_env()
        env["PATH"] = f"{bin_path.parent}:{tests_dir.parent}:{env.get('PATH', '')}"
        cmd = ["prove", "--recurse", "--verbose", *test_args]
        result = self.run_cmd("pjdfstests", cmd, cwd=work_dir, timeout=int(os.environ.get("PJDFSTEST_TIMEOUT_S", "1800")), env=env, ok_codes=(0, 1, 124))
        combined = result.stdout.read_text(encoding="utf-8", errors="replace") + "\n" + result.stderr.read_text(encoding="utf-8", errors="replace")
        log.write_text(combined, encoding="utf-8")
        report = self.parse_pjdfstests(combined, str(log), result.code)
        (self.result_dir / "pjdfstests.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.unmount(handle)
        if report["fail_regression_cases"] > 0:
            raise BlackboxError(f"pjdfstests regressions={report['fail_regression_cases']}; see {log}")
        return report

    def parse_pjdfstests(self, text: str, log_path: str, rc: int) -> dict[str, Any]:
        import re

        files_line = re.search(r"Files=(\d+),\s*Tests=(\d+),", text)
        total_files = int(files_line.group(1)) if files_line else 0
        total_cases = int(files_line.group(2)) if files_line else 0
        failed_file_re = re.compile(r"\S+/tests/(?P<rel>[^ ]+?\.t)\s+\(Wstat:\s*\d+\s+Tests:\s*(?P<tests>\d+)\s+Failed:\s*(?P<failed>\d+)\)")
        failed_files = []
        xfail_cases = 0
        fail_regression_cases = 0
        for match in failed_file_re.finditer(text):
            rel = match.group("rel")
            tests = int(match.group("tests"))
            failed = int(match.group("failed"))
            classification = "XFAIL_KNOWN" if self.pjdfstest_known_xfail(rel) else "FAIL_REGRESSION"
            if classification == "XFAIL_KNOWN":
                xfail_cases += failed
            else:
                fail_regression_cases += failed
            failed_files.append({"path": rel, "tests": tests, "failed": failed, "classification": classification})
        failed_cases = sum(item["failed"] for item in failed_files)
        if total_cases == 0:
            failed_cases = len(re.findall(r"^not ok\s+\d+", text, flags=re.MULTILINE))
            total_cases = failed_cases
        if rc != 0 and failed_cases == 0:
            failed_cases = 1
            total_cases = max(total_cases, 1)
            fail_regression_cases = max(fail_regression_cases, 1)
        passed_cases = max(total_cases - failed_cases, 0)
        denominator = max(total_cases - xfail_cases, 1)
        report = {
            "schema": "drive9-fuse-pjdfstests/v1",
            "rc": rc,
            "log": log_path,
            "total_files": total_files,
            "total_cases": total_cases,
            "passed_cases": passed_cases,
            "failed_cases": failed_cases,
            "xfail_known_cases": xfail_cases,
            "fail_regression_cases": fail_regression_cases,
            "raw_pass_rate": (passed_cases / total_cases) if total_cases else 0.0,
            "effective_pass_rate": ((passed_cases) / denominator) if denominator else 0.0,
            "failed_files": failed_files,
        }
        return report

    def pjdfstest_known_xfail(self, rel: str) -> bool:
        group = rel.split("/", 1)[0]
        groups = set(self.allowlist.get("known_xfail_groups", []))
        if platform.system() == "Darwin":
            groups.update(self.allowlist.get("darwin_known_xfail_groups", []))
        if group in groups:
            return True
        return any(fnmatch.fnmatch(rel, pattern) for pattern in self.allowlist.get("known_xfail_paths", []))

    # Performance.

    def run_perf_suite(self, tier: str) -> None:
        del tier
        self.run_case("perf", "micro", self.case_perf_micro)
        self.run_case("perf", "repos", self.case_perf_repos)

    def summarize_values(self, values: list[float], unit: str) -> dict[str, Any]:
        return {
            "unit": unit,
            "values": values,
            "mean": statistics.mean(values) if values else 0.0,
            "median": statistics.median(values) if values else 0.0,
            "min": min(values) if values else 0.0,
            "max": max(values) if values else 0.0,
            "stdev": statistics.stdev(values) if len(values) >= 2 else 0.0,
        }

    def perf_report_update(self, name: str, values: list[float], unit: str) -> None:
        path = self.result_dir / "perf-results.json"
        report = {"schema": "drive9-fuse-performance/v1", "timestamp": utc_ts(), "results": {}}
        if path.exists():
            report = json.loads(path.read_text(encoding="utf-8"))
        report.setdefault("results", {})[name] = self.summarize_values(values, unit)
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def timeit(self, fn: Callable[[], Any]) -> float:
        start = time.perf_counter()
        fn()
        return time.perf_counter() - start

    def case_perf_micro(self) -> dict[str, Any]:
        remote = self.remote_root("perf-micro")
        self.mkdir_remote(remote)
        handle = self.mount("perf_micro", remote, durability="interactive")
        root = handle.mountpoint
        defaults = self.perf_cfg["defaults"]
        runs = int(self.args.runs or defaults["runs"])
        metrics: dict[str, Any] = {}

        for mib in defaults["sequential_sizes_mib"]:
            size = int(mib) * 1024 * 1024
            payload = stable_bytes(min(size, 1024 * 1024), seed=int(mib))
            write_rates: list[float] = []
            read_rates: list[float] = []
            for run in range(runs):
                path = root / f"seq-{mib}m-{run}.bin"

                def write_body() -> None:
                    remaining = size
                    with path.open("wb") as f:
                        while remaining > 0:
                            chunk = payload[: min(len(payload), remaining)]
                            f.write(chunk)
                            remaining -= len(chunk)
                        f.flush()
                        os.fsync(f.fileno())

                write_seconds = self.timeit(write_body)
                write_rates.append((size / (1024 * 1024)) / write_seconds)

                def read_body() -> None:
                    with path.open("rb") as f:
                        while f.read(1024 * 1024):
                            pass

                read_seconds = self.timeit(read_body)
                read_rates.append((size / (1024 * 1024)) / read_seconds)
            self.perf_report_update(f"micro:sequential_write_{mib}m", write_rates, "MiB/s")
            self.perf_report_update(f"micro:sequential_read_{mib}m", read_rates, "MiB/s")
            metrics[f"sequential_write_{mib}m_mean_mib_s"] = statistics.mean(write_rates)
            metrics[f"sequential_read_{mib}m_mean_mib_s"] = statistics.mean(read_rates)

        count = int(defaults["small_file_count"])
        data = b"x" * int(defaults["small_file_bytes"])
        values: list[float] = []
        for run in range(runs):
            work = root / f"small-{run}"
            ensure_empty(work)

            def body() -> None:
                for idx in range(count):
                    p = work / f"f{idx:05d}.bin"
                    with p.open("wb") as f:
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())
                for path in work.iterdir():
                    _ = path.read_bytes()
                for path in work.iterdir():
                    path.unlink()

            seconds = self.timeit(body)
            values.append(count / seconds)
        self.perf_report_update("micro:small_file_churn", values, "files/s")

        meta_count = int(defaults["metadata_count"])
        values = []
        for run in range(runs):
            work = root / f"metadata-{run}"
            ensure_empty(work)

            def body() -> None:
                for idx in range(meta_count):
                    (work / f"d{idx:05d}").mkdir()
                for idx in range(meta_count):
                    (work / f"d{idx:05d}").rename(work / f"r{idx:05d}")
                for idx in range(meta_count):
                    (work / f"r{idx:05d}").stat()
                for idx in range(meta_count):
                    (work / f"r{idx:05d}").rmdir()

            seconds = self.timeit(body)
            values.append((meta_count * 4) / seconds)
        self.perf_report_update("micro:metadata_churn", values, "ops/s")

        if shutil.which("rg"):
            values = []
            for run in range(runs):
                work = root / f"grep-{run}"
                ensure_empty(work)
                for idx in range(200):
                    (work / f"f{idx:04d}.txt").write_text(f"needle line {idx}\n" * 20, encoding="utf-8")
                seconds = self.timeit(lambda: self.capture(["rg", "needle", str(work)], timeout=120))
                values.append(200 / seconds)
            self.perf_report_update("micro:rg_generated_tree", values, "files/s")

        self.unmount(handle)
        return metrics

    def case_perf_repos(self) -> dict[str, Any]:
        selected = [part.strip() for part in os.environ.get("BLACKBOX_FUSE_REPOS", "drive9,kimi-code").split(",") if part.strip()]
        repos = [repo for repo in self.perf_cfg["repos"] if repo["id"] in selected]
        if not repos:
            raise SkipCase("no repos selected")
        runs = int(self.args.runs or self.perf_cfg["defaults"]["runs"])
        timeout = int(self.perf_cfg["defaults"]["repo_timeout_seconds"])
        for repo in repos:
            for run_index in range(1, runs + 1):
                self.run_repo_perf(repo, run_index, timeout)
        return {"repos": [repo["id"] for repo in repos], "runs": runs}

    def run_repo_perf(self, repo: dict[str, Any], run_index: int, timeout: int) -> None:
        repo_id = repo["id"]
        env = self.base_env()
        env["GIT_TERMINAL_PROMPT"] = "0"
        native_root = self.tmp_dir / "perf-native" / repo_id / f"run-{run_index}"
        ensure_empty(native_root)
        native_target = native_root / "repo"
        seconds = self.timeit(lambda: self.capture(["git", "clone", "--no-local", repo["url"], str(native_target)], timeout=timeout, env=env))
        self.perf_append(f"repo:{repo_id}:native_git_clone", seconds, "seconds")

        remote = self.remote_root(f"perf-repo-{repo_id}-{run_index}")
        self.mkdir_remote(remote)
        handle = self.mount(f"perf_repo_{repo_id}_{run_index}", remote, profile="coding-agent", durability="interactive")
        try:
            normal_target = handle.mountpoint / "normal"
            seconds = self.timeit(lambda: self.capture(["git", "clone", "--no-local", repo["url"], str(normal_target)], timeout=timeout, env=env))
            self.perf_append(f"repo:{repo_id}:fuse_git_clone", seconds, "seconds")

            fast_target = handle.mountpoint / "fast"
            seconds = self.timeit(lambda: self.drive9_capture(["git", "clone", "--fast", repo["url"], str(fast_target)], timeout=timeout))
            self.perf_append(f"repo:{repo_id}:drive9_git_clone_fast", seconds, "seconds")

            blobless_target = handle.mountpoint / "blobless"
            seconds = self.timeit(lambda: self.drive9_capture(["git", "clone", "--fast", "--blobless", "--hydrate=sync", repo["url"], str(blobless_target)], timeout=timeout))
            self.perf_append(f"repo:{repo_id}:drive9_git_clone_fast_blobless", seconds, "seconds")

            if shutil.which("rg"):
                seconds = self.timeit(lambda: self.capture(["rg", "--files", str(blobless_target)], timeout=300, env=env))
                self.perf_append(f"repo:{repo_id}:rg_files", seconds, "seconds")
                seconds = self.timeit(lambda: self.capture(["rg", repo.get("rg_pattern", "TODO|FIXME"), str(blobless_target)], timeout=300, env=env))
                self.perf_append(f"repo:{repo_id}:rg_content", seconds, "seconds")

            self.capture(["git", "config", "user.name", "Drive9 Blackbox"], cwd=blobless_target, env=env)
            self.capture(["git", "config", "user.email", "blackbox@drive9.local"], cwd=blobless_target, env=env)
            seconds = self.timeit(lambda: self.repo_edit_commit(blobless_target, env))
            self.perf_append(f"repo:{repo_id}:edit_commit", seconds, "seconds")

            build_cmds = repo.get("build", [])
            if build_cmds:
                build_dir = blobless_target / repo.get("build_dir", ".")
                seconds = self.timeit(lambda: [self.capture(["bash", "-lc", cmd], cwd=build_dir, timeout=timeout, env=env) for cmd in build_cmds])
                self.perf_append(f"repo:{repo_id}:build", seconds, "seconds")
        finally:
            self.unmount(handle)

    def repo_edit_commit(self, repo: Path, env: dict[str, str]) -> None:
        candidates = [path for path in repo.rglob("*") if path.is_file() and ".git" not in path.parts and path.stat().st_size < 1024 * 1024]
        for path in candidates[:20]:
            with path.open("a", encoding="utf-8", errors="ignore") as handle:
                handle.write("\n# drive9 blackbox edit\n")
        (repo / "blackbox-new.txt").write_text("new file\n", encoding="utf-8")
        self.capture(["git", "add", "-A"], cwd=repo, env=env, timeout=300)
        self.capture(["git", "commit", "-m", "blackbox edit"], cwd=repo, env=env, timeout=300)

    def perf_append(self, name: str, value: float, unit: str) -> None:
        path = self.result_dir / "perf-results.json"
        report = {"schema": "drive9-fuse-performance/v1", "timestamp": utc_ts(), "results": {}}
        if path.exists():
            report = json.loads(path.read_text(encoding="utf-8"))
        existing = report.setdefault("results", {}).get(name, {"values": [], "unit": unit})
        values = list(existing.get("values", []))
        values.append(value)
        report["results"][name] = self.summarize_values(values, unit)
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class SkipCase(Exception):
    pass


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run self-contained Drive9 FUSE blackbox suites.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--preset", choices=["smoke", "standard", "daily"])
    group.add_argument("--suite", choices=["functional", "posix", "perf"])
    parser.add_argument("--tier", choices=["smoke", "standard", "daily"], help="Tier used with --suite. Defaults to daily.")
    parser.add_argument("--runs", type=int, default=0, help="Performance runs. Defaults to cases-perf.json.")
    parser.add_argument("--server-mode", choices=["auto", "existing", "local"], default=os.environ.get("BLACKBOX_FUSE_SERVER_MODE", "auto"))
    parser.add_argument("--drive9-cli", default=os.environ.get("BLACKBOX_FUSE_DRIVE9_CLI", ""))
    parser.add_argument("--out-dir", default=os.environ.get("BLACKBOX_FUSE_OUT_DIR", ""))
    parser.add_argument("--session", default=os.environ.get("BLACKBOX_FUSE_SESSION", ""))
    parser.add_argument("--strict-prereqs", action="store_true", default=os.environ.get("BLACKBOX_FUSE_STRICT", "0") == "1")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--keep-artifacts", action="store_true", default=os.environ.get("BLACKBOX_FUSE_KEEP_ARTIFACTS", "0") == "1")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    runner = Runner(args)
    try:
        return runner.run()
    finally:
        try:
            runner.write_daily_report()
        except Exception:
            pass
        runner.cleanup()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
