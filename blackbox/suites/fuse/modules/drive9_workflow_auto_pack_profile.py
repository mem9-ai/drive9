from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9AutoPackProfile(Drive9WorkflowBase):
    id = "drive9.workflow.auto_pack_profile"
    description = "Custom profile pack paths auto-pack on umount and auto-unpack on the next mount."

    def run(self, ctx: Context) -> dict[str, Any]:
        profile = "blackbox-pack"
        ctx.target.write_profile(
            profile,
            """
[local]
**/.git/**
**/dist/**
**/build/**
**/target/**

[remote]

[pack]
.git
dist
build
target
""",
        )
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        h1 = ctx.target.mount("drive9_auto_pack_profile", remote, profile=profile, cache_key="first")
        try:
            (h1.mountpoint / "dist").mkdir()
            (h1.mountpoint / "dist" / "app.js").write_text("console.log('pack')\n", encoding="utf-8")
            (h1.mountpoint / "build").mkdir()
            (h1.mountpoint / "build" / "out.txt").write_text("built\n", encoding="utf-8")
        finally:
            ctx.target.unmount(h1)
        h2 = ctx.target.mount("drive9_auto_pack_profile", remote, profile=profile, cache_key="second")
        try:
            if (h2.mountpoint / "dist" / "app.js").read_text(encoding="utf-8") != "console.log('pack')\n":
                raise BlackboxError("auto-unpacked dist file mismatch")
            if (h2.mountpoint / "build" / "out.txt").read_text(encoding="utf-8") != "built\n":
                raise BlackboxError("auto-unpacked build file mismatch")
            return {"profile": profile}
        finally:
            ctx.target.unmount(h2)
