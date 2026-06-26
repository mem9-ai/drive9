from __future__ import annotations

import hashlib
from typing import Any

from harness.core import BlackboxError, Context
from suites.drive9._base import Drive9WorkflowBase


class Drive9AutoPackUmountPath(Drive9WorkflowBase):
    id = "drive9.workflow.auto_pack_umount_path"
    description = "drive9 umount --pack-path persists selected coding-agent local-only paths."

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        archive_path = self.default_pack_archive(remote, "coding-agent")
        archive = f":{archive_path}"
        h1 = ctx.target.mount("drive9_auto_pack_umount_path", remote, profile="coding-agent", cache_key="first")
        try:
            (h1.mountpoint / "dist").mkdir()
            (h1.mountpoint / "dist" / "bundle.js").write_text("bundle\n", encoding="utf-8")
            (h1.mountpoint / ".git").mkdir()
            (h1.mountpoint / ".git" / "config").write_text("[core]\n", encoding="utf-8")
        finally:
            ctx.target.unmount(h1, pack_paths=["dist", ".git"])
        h2 = ctx.target.mount("drive9_auto_pack_umount_path", remote, profile="coding-agent", cache_key="second", extra=["--unpack", archive])
        try:
            if (h2.mountpoint / "dist" / "bundle.js").read_text(encoding="utf-8") != "bundle\n":
                raise BlackboxError("umount --pack-path dist did not restore")
            if (h2.mountpoint / ".git" / "config").read_text(encoding="utf-8") != "[core]\n":
                raise BlackboxError("umount --pack-path .git did not restore")
            return {"archive": archive}
        finally:
            ctx.target.unmount(h2, no_auto_pack=True)

    def default_pack_archive(self, remote_root: str, profile: str) -> str:
        normalized = "/" + remote_root.strip("/")
        digest = hashlib.sha256((profile + "\x00" + normalized).encode()).hexdigest()[:16]
        label = normalized.rstrip("/").rsplit("/", 1)[-1] or "root"
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in label)
        return f"/.drive9/packs/{safe}-{digest}.tar.gz"
