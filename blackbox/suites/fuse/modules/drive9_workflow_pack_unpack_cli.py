from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9PackUnpackCLI(Drive9WorkflowBase):
    id = "drive9.workflow.pack_unpack_cli"
    description = "Explicit drive9 pack/unpack round trip for overlay content under a FUSE mount."

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        archive = f":{remote}/explicit-pack.tar.gz"
        h1 = ctx.target.mount("drive9_pack_unpack_cli", remote, profile="coding-agent", cache_key="pack")
        try:
            (h1.mountpoint / "dist").mkdir()
            (h1.mountpoint / "dist" / "asset.txt").write_text("asset\n", encoding="utf-8")
            result = ctx.target.drive9("drive9-pack-cli", ["pack", "--mount", str(h1.mountpoint), archive, "dist"], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 pack failed; see {result.stderr}")
        finally:
            ctx.target.unmount(h1, no_auto_pack=True)
        h2 = ctx.target.mount("drive9_pack_unpack_cli", remote, profile="coding-agent", cache_key="unpack", extra=["--unpack", archive])
        try:
            if (h2.mountpoint / "dist" / "asset.txt").read_text(encoding="utf-8") != "asset\n":
                raise BlackboxError("explicit unpack content mismatch")
            return {"archive": archive}
        finally:
            ctx.target.unmount(h2, no_auto_pack=True)
