from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9PortablePack(Drive9WorkflowBase):
    id = "drive9.workflow.portable_pack"
    description = "Built-in portable profile auto-packs the entire local overlay across remounts."

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        h1 = ctx.target.mount("drive9_portable_pack", remote, profile="portable", cache_key="first")
        try:
            (h1.mountpoint / "local.txt").write_text("portable\n", encoding="utf-8")
        finally:
            ctx.target.unmount(h1)
        h2 = ctx.target.mount("drive9_portable_pack", remote, profile="portable", cache_key="second")
        try:
            if (h2.mountpoint / "local.txt").read_text(encoding="utf-8") != "portable\n":
                raise BlackboxError("portable profile did not restore local overlay")
            return {}
        finally:
            ctx.target.unmount(h2)
