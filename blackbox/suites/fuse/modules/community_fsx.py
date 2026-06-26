from __future__ import annotations

import os
from typing import Any

from harness.core import BlackboxError, Context
from .base import BaseModule, module_config


class CommunityFSX(BaseModule):
    id = "community.fsx"
    category = "community.stress"
    description = "Run fsx randomized file operation stress on Drive9 FUSE."
    labels = ("stress", "community")
    timeout = 600

    def run(self, ctx: Context) -> dict[str, Any]:
        fsx = ctx.deps.ensure_fsx()
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_fsx", remote)
        try:
            ops = str(module_config(ctx, self.id).get("ops", 5000))
            args = [fsx, "-N", ops]
            if "fsx-linux" not in os.path.basename(fsx):
                args.append(str(handle.mountpoint / "fsx.bin"))
            result = ctx.target.run_cmd("community-fsx", args, cwd=handle.mountpoint, timeout=int(os.environ.get("FSX_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"fsx failed; see {result.stderr}")
            return {"ops": int(ops)}
        finally:
            ctx.target.unmount(handle)
