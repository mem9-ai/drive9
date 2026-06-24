from __future__ import annotations

import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, CommandResult, REPO_ROOT, ensure_empty, progress, utc_ts


@dataclass
class MountHandle:
    mountpoint: Path
    remote_root: str
    proc: Any
    log_dir: Path
    cache_dir: Path
    local_root: Path
    profile: str


@dataclass
class UnmountResult:
    attempted: bool
    exit_code: int | None
    mounted_after: bool
    forced: bool
    seconds: float


def pick_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return int(port)


def http_json(method: str, url: str, token: str = "", body: dict[str, Any] | None = None, timeout: int = 60) -> tuple[int, Any, str]:
    data = None
    headers: dict[str, str] = {}
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


def mysql_tcp_handshake_ready(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            header = sock.recv(4)
            if len(header) != 4:
                return False
            payload_len = header[0] | (header[1] << 8) | (header[2] << 16)
            if payload_len <= 0:
                return False
            payload = b""
            wanted = min(payload_len, 128)
            while len(payload) < wanted:
                chunk = sock.recv(wanted - len(payload))
                if not chunk:
                    break
                payload += chunk
            return bool(payload and payload[0] == 10)
    except OSError:
        return False


def log_tail(path: Path, *, max_chars: int = 1600) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()[-max_chars:]
    except OSError:
        return ""


class Drive9FuseTargetProvider:
    def __init__(self, args: Any, result_dir: Path, recorder: Any, *, suite: str, session: str) -> None:
        self.args = args
        self.suite = suite
        self.session = session
        self.result_dir = result_dir
        self.recorder = recorder
        self.logs_dir = result_dir / "logs"
        self.mount_logs_dir = result_dir / "mount-logs"
        self.tmp_dir = result_dir / "tmp"
        self.bin_dir = result_dir / "bin"
        self.env_home = self.tmp_dir / "home"
        self.cli = Path(args.drive9_cli).expanduser().resolve() if args.drive9_cli else self.bin_dir / "drive9"
        self.server_bin = self.bin_dir / "drive9-server-local"
        self.server_url = os.environ.get("DRIVE9_BASE", "")
        self.api_key = os.environ.get("DRIVE9_API_KEY", "")
        self.local_api_key = os.environ.get("DRIVE9_LOCAL_API_KEY", "local-dev-key")
        self.server_proc: subprocess.Popen[bytes] | None = None
        self.db_container = ""
        self.db_runtime = ""
        self.mounts: list[MountHandle] = []
        for path in (self.logs_dir, self.mount_logs_dir, self.tmp_dir, self.bin_dir, self.env_home):
            path.mkdir(parents=True, exist_ok=True)

    def base_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["HOME"] = str(self.env_home)
        if self.server_url:
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
        return env

    def run_cmd(
        self,
        name: str,
        cmd: list[str] | str,
        *,
        cwd: Path | None = None,
        timeout: int = 120,
        env: dict[str, str] | None = None,
        shell: bool = False,
        ok_codes: tuple[int, ...] = (0,),
    ) -> CommandResult:
        safe_name = name.replace("/", "-").replace(":", "-")
        log_dir = self.logs_dir / safe_name
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout = log_dir / "stdout.log"
        stderr = log_dir / "stderr.log"
        display = cmd if isinstance(cmd, str) else " ".join(cmd)
        start = time.monotonic()
        progress(f"command start: {safe_name} (timeout={timeout}s, logs={log_dir})")
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
        status = "done" if code in ok_codes else "failed"
        progress(f"command {status}: {safe_name} exit={code} in {seconds:.1f}s")
        return CommandResult(code=code, seconds=seconds, stdout=stdout, stderr=stderr, ok=code in ok_codes)

    def capture(self, cmd: list[str], *, cwd: Path | None = None, timeout: int = 120, env: dict[str, str] | None = None) -> str:
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
            progress(f"setup: using drive9 CLI {self.cli}")
            return
        progress("setup: building drive9 CLI")
        env = dict(os.environ)
        env["CGO_ENABLED"] = "0"
        result = self.run_cmd("build-cli", ["go", "build", "-o", str(self.cli), "./cmd/drive9"], timeout=600, env=env)
        if not result.ok:
            raise BlackboxError(f"build drive9 CLI failed; see {result.stderr}")
        progress(f"setup: drive9 CLI ready at {self.cli}")

    def build_server_local(self) -> None:
        progress("setup: building drive9-server-local")
        env = dict(os.environ)
        env["CGO_ENABLED"] = "0"
        result = self.run_cmd("build-server-local", ["go", "build", "-o", str(self.server_bin), "./cmd/drive9-server-local"], timeout=600, env=env)
        if not result.ok:
            raise BlackboxError(f"build drive9-server-local failed; see {result.stderr}")
        progress(f"setup: drive9-server-local ready at {self.server_bin}")

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
            progress("setup: using DRIVE9_LOCAL_DSN for local datastore")
            return dsn
        runtime = self.detect_runtime()
        self.db_runtime = runtime
        image = os.environ.get("DRIVE9_LOCAL_E2E_DB_IMAGE", "mysql:8.4")
        password = os.environ.get("DRIVE9_LOCAL_E2E_DB_PASSWORD", "drive9pass")
        db_name = os.environ.get("DRIVE9_LOCAL_E2E_DB_NAME", "drive9_local")
        self.db_container = f"drive9-blackbox-{int(time.time())}-{os.getpid()}"
        progress(f"setup: starting MySQL container {self.db_container} with {runtime} image={image}")
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
        progress(f"setup: waiting for MySQL container {self.db_container} readiness")
        next_wait_log = time.monotonic() + 10
        while time.monotonic() < deadline:
            port = ""
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
                    progress(f"setup: MySQL container ping ok on host port {port}; waiting for host TCP handshake")
                    host_ready_deadline = time.monotonic() + int(os.environ.get("DRIVE9_LOCAL_E2E_DB_HOST_READY_TIMEOUT_S", "60"))
                    stable = 0
                    next_host_wait_log = time.monotonic() + 10
                    while time.monotonic() < host_ready_deadline:
                        if mysql_tcp_handshake_ready("127.0.0.1", int(port)):
                            stable += 1
                            if stable >= 2:
                                time.sleep(float(os.environ.get("DRIVE9_LOCAL_E2E_DB_READY_GRACE_S", "1")))
                                progress(f"setup: MySQL ready at 127.0.0.1:{port}")
                                return f"root:{password}@tcp(127.0.0.1:{port})/{db_name}?parseTime=true"
                        else:
                            stable = 0
                        if time.monotonic() >= next_host_wait_log:
                            progress(f"setup: still waiting for MySQL host TCP handshake on 127.0.0.1:{port}")
                            next_host_wait_log = time.monotonic() + 10
                        time.sleep(0.5)
            if time.monotonic() >= next_wait_log:
                progress(f"setup: still waiting for MySQL container {self.db_container}")
                next_wait_log = time.monotonic() + 10
            time.sleep(1)
        raise BlackboxError("timed out waiting for MySQL container readiness")

    def start_server(self) -> None:
        mode = self.args.server_mode
        if mode == "auto":
            mode = "existing" if self.server_url else "local"
        if mode == "existing":
            if not self.server_url:
                raise BlackboxError("DRIVE9_BASE is required for --server-mode existing")
            progress(f"setup: using existing drive9 server {self.server_url}")
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
        progress(f"setup: starting drive9-server-local at {self.server_url}")
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
        progress(f"setup: waiting for drive9-server-local healthz at {self.server_url}/healthz")
        next_wait_log = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.server_proc.poll() is not None:
                tail = log_tail(log)
                detail = f"drive9-server-local exited early; see {log}"
                if tail:
                    detail += f"\nlast log lines:\n{tail}"
                raise BlackboxError(detail)
            try:
                code, parsed, _ = http_json("GET", f"{self.server_url}/healthz", timeout=5)
                if code == 200:
                    self.recorder.event({"type": "server", "mode": "local", "url": self.server_url, "health": parsed})
                    progress(f"setup: drive9-server-local healthy at {self.server_url}")
                    return
            except Exception:
                pass
            if time.monotonic() >= next_wait_log:
                progress(f"setup: still waiting for drive9-server-local healthz at {self.server_url}")
                next_wait_log = time.monotonic() + 10
            time.sleep(1)
        tail = log_tail(log)
        detail = f"timed out waiting for drive9-server-local; see {log}"
        if tail:
            detail += f"\nlast log lines:\n{tail}"
        raise BlackboxError(detail)

    def provision_existing_server(self) -> None:
        progress(f"setup: provisioning tenant on existing server {self.server_url}")
        code, parsed, raw = http_json("POST", f"{self.server_url}/v1/provision", timeout=60)
        if code not in (200, 202) or not isinstance(parsed, dict):
            raise BlackboxError(f"provision failed: code={code} body={raw}")
        key = str(parsed.get("api_key") or "")
        if not key:
            raise BlackboxError(f"provision response missing api_key: {raw}")
        self.api_key = key
        deadline = time.monotonic() + int(os.environ.get("POLL_TIMEOUT_S", "120"))
        progress("setup: waiting for provisioned tenant to become active")
        next_wait_log = time.monotonic() + 10
        while time.monotonic() < deadline:
            status_code, status_body, _ = http_json("GET", f"{self.server_url}/v1/status", token=self.api_key, timeout=20)
            if status_code == 200 and isinstance(status_body, dict) and status_body.get("status") == "active":
                progress("setup: provisioned tenant is active")
                return
            if time.monotonic() >= next_wait_log:
                progress("setup: still waiting for provisioned tenant")
                next_wait_log = time.monotonic() + 10
            time.sleep(float(os.environ.get("POLL_INTERVAL_S", "2")))
        raise BlackboxError("provisioned tenant did not become active")

    def drive9(self, name: str, args: list[str], *, timeout: int = 120, ok_codes: tuple[int, ...] = (0,)) -> CommandResult:
        return self.run_cmd(name, [str(self.cli), *args], timeout=timeout, env=self.base_env(), ok_codes=ok_codes)

    def drive9_capture(self, args: list[str], *, timeout: int = 120) -> str:
        return self.capture([str(self.cli), *args], timeout=timeout, env=self.base_env())

    def mkdir_remote(self, remote: str) -> None:
        name = "mkdir-" + remote.strip("/").replace("/", "-")
        result = self.drive9(name, ["fs", "mkdir", f":{remote}"], timeout=120)
        if not result.ok:
            raise BlackboxError(f"remote mkdir failed for {remote}; see {result.stderr}")

    def write_profile(self, name: str, body: str) -> Path:
        path = self.env_home / ".drive9" / "profiles" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body.strip() + "\n", encoding="utf-8")
        return path

    def remote_root(self, module_id: str, suffix: str = "") -> str:
        safe_id = module_id.replace(".", "-").replace("_", "-")
        parts = [f"blackbox-{self.suite}", self.session.replace("/", "-"), safe_id]
        if suffix:
            parts.append(suffix.strip("/"))
        return "/" + "/".join(part.strip("/") for part in parts if part)

    def mount(
        self,
        case: str,
        remote_root: str,
        *,
        read_only: bool = False,
        profile: str = "none",
        durability: str = "interactive",
        cache_key: str = "primary",
        extra: list[str] | None = None,
    ) -> MountHandle:
        mountpoint = self.tmp_dir / "mounts" / case / cache_key
        local_root = self.tmp_dir / "local-roots" / case / cache_key
        cache_dir = self.tmp_dir / "caches" / case / cache_key
        for path in (mountpoint, local_root, cache_dir):
            path.mkdir(parents=True, exist_ok=True)
        log_dir = self.mount_logs_dir / case / cache_key
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
            "--cache-dir",
            str(cache_dir),
        ]
        if profile and profile not in ("none", "interactive"):
            command.extend(["--local-root", str(local_root)])
        if read_only:
            command.append("--read-only")
        if extra:
            command.extend(extra)
        command.extend([f":{remote_root}", str(mountpoint)])
        env = self.base_env()
        out = (log_dir / "mount.out").open("ab")
        err = (log_dir / "mount.err").open("ab")
        out.write(f"\n# {utc_ts()} $ {' '.join(command)}\n".encode())
        out.flush()
        progress(f"mount start: {case}/{cache_key} remote={remote_root} mountpoint={mountpoint} profile={profile}")
        proc = subprocess.Popen(command, cwd=str(REPO_ROOT), env=env, stdout=out, stderr=err, start_new_session=True)
        deadline = time.monotonic() + int(os.environ.get("MOUNT_READY_TIMEOUT_S", "30"))
        next_wait_log = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                tail = log_tail(log_dir / "mount.err")
                detail = f"mount exited early for {case}; see {log_dir / 'mount.err'}"
                if tail:
                    detail += f"\nlast mount stderr:\n{tail}"
                raise BlackboxError(detail)
            if self.is_mounted(mountpoint):
                handle = MountHandle(mountpoint=mountpoint, remote_root=remote_root, proc=proc, log_dir=log_dir, cache_dir=cache_dir, local_root=local_root, profile=profile)
                self.mounts.append(handle)
                progress(f"mount ready: {case}/{cache_key} -> {mountpoint}")
                return handle
            if time.monotonic() >= next_wait_log:
                progress(f"mount waiting: {case}/{cache_key} -> {mountpoint}")
                next_wait_log = time.monotonic() + 10
            time.sleep(0.25)
        self.kill_process_group(proc)
        tail = log_tail(log_dir / "mount.err")
        detail = f"mount did not become ready for {case}; see {log_dir / 'mount.err'}"
        if tail:
            detail += f"\nlast mount stderr:\n{tail}"
        raise BlackboxError(detail)

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

    def unmount(
        self,
        handle: MountHandle,
        *,
        force: bool = False,
        pack_paths: list[str] | None = None,
        pack_archives: list[str] | None = None,
        no_auto_pack: bool = False,
    ) -> UnmountResult:
        started = time.monotonic()
        attempted = False
        exit_code: int | None = None
        forced = False
        if self.is_mounted(handle.mountpoint):
            attempted = True
            progress(f"unmount start: {handle.mountpoint}")
            args = ["umount", "--timeout", os.environ.get("FUSE_UMOUNT_TIMEOUT", "60s")]
            if no_auto_pack:
                args.append("--no-auto-pack")
            for archive in pack_archives or []:
                args.extend(["--pack", archive])
            for path in pack_paths or []:
                args.extend(["--pack-path", path])
            args.append(str(handle.mountpoint))
            result = self.drive9(f"umount-{handle.mountpoint.name}", args, timeout=120, ok_codes=(0, 1))
            exit_code = result.code
            if result.code != 0 and (pack_paths or pack_archives):
                raise BlackboxError(f"drive9 umount with pack arguments failed; see {result.stderr}")
        post_umount_wait_s = float(os.environ.get("FUSE_POST_UMOUNT_WAIT_S", "20"))
        deadline = time.monotonic() + max(0.0, post_umount_wait_s)
        next_wait_log = time.monotonic() + 10
        while time.monotonic() < deadline and self.is_mounted(handle.mountpoint):
            if time.monotonic() >= next_wait_log:
                progress(f"unmount waiting: {handle.mountpoint}")
                next_wait_log = time.monotonic() + 10
            time.sleep(0.25)
        if force and self.is_mounted(handle.mountpoint):
            forced = True
            progress(f"unmount force: {handle.mountpoint}")
            if platform.system() == "Linux":
                for cmd in (["fusermount3", "-uz", str(handle.mountpoint)], ["fusermount", "-uz", str(handle.mountpoint)], ["umount", "-l", str(handle.mountpoint)]):
                    if shutil.which(cmd[0]):
                        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                        if not self.is_mounted(handle.mountpoint):
                            break
            elif platform.system() == "Darwin":
                subprocess.run(["umount", "-f", str(handle.mountpoint)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        mounted_after = self.is_mounted(handle.mountpoint)
        self.kill_process_group(handle.proc)
        progress(f"unmount done: {handle.mountpoint}")
        return UnmountResult(attempted=attempted, exit_code=exit_code, mounted_after=mounted_after, forced=forced, seconds=time.monotonic() - started)

    def cleanup(self) -> None:
        progress("cleanup start")
        for handle in reversed(self.mounts):
            try:
                self.unmount(handle, force=True, no_auto_pack=True)
            except Exception:
                pass
        self.mounts.clear()
        if self.server_proc is not None:
            progress("cleanup: stopping drive9-server-local")
            self.kill_process_group(self.server_proc)
        if self.db_container and self.db_runtime and os.environ.get("DRIVE9_LOCAL_E2E_KEEP_DB", "0") != "1":
            progress(f"cleanup: removing MySQL container {self.db_container}")
            subprocess.run([self.db_runtime, "rm", "-f", self.db_container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        keep_all = getattr(self.args, "keep_all_artifacts", False)
        if not keep_all and not self.args.keep_artifacts and not self.recorder.has_failures():
            progress(f"cleanup: removing tmp artifacts {self.tmp_dir}")
            shutil.rmtree(self.tmp_dir, ignore_errors=True)
        progress("cleanup complete")

    def create_git_fixture(self, name: str = "git-fixture") -> Path:
        fixture = self.tmp_dir / name
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
        if hasattr(os, "symlink"):
            os.symlink("README.md", work / "link-to-readme")
        self.capture(["git", "add", "."], cwd=work)
        self.capture(["git", "commit", "-m", "fixture"], cwd=work)
        self.capture(["git", "clone", "--bare", str(work), str(bare)], cwd=fixture)
        return bare
