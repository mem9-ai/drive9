from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context, ModuleSkip
from harness.module_base import BaseModule


class CommunityLock(BaseModule):
    description = "Run POSIX advisory lock checks on Drive9 FUSE."
    labels = ("compatibility", "locking")
    timeout = 120

    def run(self, ctx: Context) -> dict[str, Any]:
        if not ctx.capabilities.get("features", {}).get("fcntl_lock"):
            raise ModuleSkip("fcntl locks are not supported on this platform", "platform skip")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_lock", remote)
        try:
            p = handle.mountpoint / "lock.txt"
            p.write_text("lock\n", encoding="utf-8")
            script = (
                "import fcntl, os, sys\n"
                "p = sys.argv[1]\n"
                "fd = os.open(p, os.O_RDWR)\n"
                "fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                "try:\n"
                "    fd2 = os.open(p, os.O_RDWR)\n"
                "    try:\n"
                "        fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                "        raise SystemExit('second exclusive lock unexpectedly succeeded')\n"
                "    except BlockingIOError:\n"
                "        pass\n"
                "finally:\n"
                "    fcntl.flock(fd, fcntl.LOCK_UN)\n"
                "    os.close(fd)\n"
            )
            result = ctx.target.run_cmd("community-lock", ["python3", "-c", script, str(p)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"lock check failed; see {result.stderr}")
            return {}
        finally:
            ctx.target.unmount(handle)
