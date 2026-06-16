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
from pathlib import Path
from typing import Any

from .core import BlackboxError, CommandResult, MountHandle, REPO_ROOT, ensure_empty, utc_ts


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


class Drive9TargetProvider:
    def __init__(self, args: Any, result_dir: Path, recorder: Any) -> None:
        self.args = args
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
            "--perf-counters",
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
        proc = subprocess.Popen(command, cwd=str(REPO_ROOT), env=env, stdout=out, stderr=err, start_new_session=True)
        deadline = time.monotonic() + int(os.environ.get("MOUNT_READY_TIMEOUT_S", "30"))
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise BlackboxError(f"mount exited early for {case}; see {log_dir / 'mount.err'}")
            if self.is_mounted(mountpoint):
                handle = MountHandle(mountpoint=mountpoint, remote_root=remote_root, proc=proc, log_dir=log_dir, cache_dir=cache_dir, local_root=local_root, profile=profile)
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

    def unmount(
        self,
        handle: MountHandle,
        *,
        force: bool = False,
        pack_paths: list[str] | None = None,
        pack_archives: list[str] | None = None,
        no_auto_pack: bool = False,
    ) -> None:
        if self.is_mounted(handle.mountpoint):
            args = ["umount", "--timeout", os.environ.get("FUSE_UMOUNT_TIMEOUT", "60s")]
            if no_auto_pack:
                args.append("--no-auto-pack")
            for archive in pack_archives or []:
                args.extend(["--pack", archive])
            for path in pack_paths or []:
                args.extend(["--pack-path", path])
            args.append(str(handle.mountpoint))
            result = self.drive9(f"umount-{handle.mountpoint.name}", args, timeout=120, ok_codes=(0, 1))
            if result.code != 0 and (pack_paths or pack_archives):
                raise BlackboxError(f"drive9 umount with pack arguments failed; see {result.stderr}")
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
                self.unmount(handle, force=True, no_auto_pack=True)
            except Exception:
                pass
        self.mounts.clear()
        if self.server_proc is not None:
            self.kill_process_group(self.server_proc)
        if self.db_container and self.db_runtime and os.environ.get("DRIVE9_LOCAL_E2E_KEEP_DB", "0") != "1":
            subprocess.run([self.db_runtime, "rm", "-f", self.db_container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if not self.args.keep_artifacts and not self.recorder.has_failures():
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

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
