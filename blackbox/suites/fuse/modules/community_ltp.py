from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable, ModuleSkip
from .base import BaseModule


class CommunityLTPFS(BaseModule):
    id = "community.ltp.fs"
    category = "community.fs"
    description = "Run Linux Test Project filesystem tests with their work directory on Drive9 FUSE."
    labels = ("compatibility", "linux", "community")
    timeout = 7200

    def ensure_dependencies(self, ctx: Context) -> None:
        if ctx.capabilities.get("os") != "Linux":
            raise ModuleSkip("LTP filesystem tests are Linux-only", "platform skip")
        ctx.deps.ensure_ltp()

    def run(self, ctx: Context) -> dict[str, Any]:
        ltp = ctx.deps.ensure_ltp()
        runltp = shutil.which("runltp") or str(ltp / "runltp")
        if not Path(runltp).exists():
            raise DependencyUnavailable("runltp not found")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_ltp_fs", remote, durability="write-sync")
        try:
            work = handle.mountpoint / "ltp-work"
            work.mkdir()
            result = ctx.target.run_cmd("community-ltp-fs", [runltp, "-f", "fs", "-d", str(work)], timeout=int(os.environ.get("LTP_FS_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"LTP fs failed; see {result.stderr}")
            return {"ltp_root": str(ltp)}
        finally:
            ctx.target.unmount(handle)


class CommunityLTPSyscalls(CommunityLTPFS):
    id = "community.ltp.syscalls"
    category = "community.syscalls"
    description = "Run the filesystem-sensitive Linux Test Project syscall subset on Drive9 FUSE."
    labels = ("compatibility", "linux", "community")

    def run(self, ctx: Context) -> dict[str, Any]:
        ltp = ctx.deps.ensure_ltp()
        runltp = shutil.which("runltp") or str(ltp / "runltp")
        if not Path(runltp).exists():
            raise DependencyUnavailable("runltp not found")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_ltp_syscalls", remote, durability="write-sync")
        try:
            work = handle.mountpoint / "ltp-work"
            work.mkdir()
            result = ctx.target.run_cmd("community-ltp-syscalls", [runltp, "-f", "syscalls", "-d", str(work)], timeout=int(os.environ.get("LTP_SYSCALLS_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"LTP syscalls failed; see {result.stderr}")
            return {"ltp_root": str(ltp)}
        finally:
            ctx.target.unmount(handle)
