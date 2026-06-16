from __future__ import annotations

import os
from typing import Any

from ..core import BlackboxError, Context
from .base import BaseModule, module_config


class CommunityMdtest(BaseModule):
    id = "community.mdtest"
    category = "community.performance"
    description = "Run mdtest metadata create/stat/remove workload on Drive9 FUSE."
    labels = ("performance", "metadata", "community")
    timeout = 3600

    def run(self, ctx: Context) -> dict[str, Any]:
        mdtest = ctx.deps.ensure_mdtest()
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_mdtest", remote, durability="interactive")
        try:
            files = str(module_config(ctx, self.id).get("files", 1000))
            result = ctx.target.run_cmd("community-mdtest", [mdtest, "-d", str(handle.mountpoint / "mdtest"), "-n", files, "-u", "-L", "-F"], timeout=int(os.environ.get("MDTEST_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"mdtest failed; see {result.stderr}")
            return {"files": int(files), "stdout": str(result.stdout)}
        finally:
            ctx.target.unmount(handle)
