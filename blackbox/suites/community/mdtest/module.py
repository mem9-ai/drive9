from __future__ import annotations

import os
from typing import Any

from harness.core import BlackboxError, Context
from harness.module_base import BaseModule
from suites.community.mdtest.deps import ensure_mdtest


class CommunityMdtest(BaseModule):
    description = "Run mdtest metadata create/stat/remove workload on Drive9 FUSE."
    labels = ("performance", "metadata", "community")
    timeout = 1200

    def run(self, ctx: Context) -> dict[str, Any]:
        mdtest = ensure_mdtest(ctx)
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_mdtest", remote, profile="none")
        try:
            files = str(os.environ.get("MDTEST_FILES", "1000"))
            result = ctx.target.run_cmd("community-mdtest", [mdtest, "-d", str(handle.mountpoint / "mdtest"), "-n", files, "-u", "-L", "-F"], timeout=int(os.environ.get("MDTEST_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"mdtest failed; see {result.stderr}")
            return {"files": int(files), "stdout": str(result.stdout)}
        finally:
            ctx.target.unmount(handle)
