from __future__ import annotations

import os
import shutil
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable
from harness.module_base import BaseModule
from suites.community.vdbench.deps import ensure_vdbench


class CommunityVdbench(BaseModule):
    id = "community.vdbench"
    category = "community.performance"
    description = "Run vdbench file workload on Drive9 FUSE when vdbench is installed."
    labels = ("performance", "community", "manual-dependency")
    manual = True
    timeout = 1800

    def run(self, ctx: Context) -> dict[str, Any]:
        vdbench = ensure_vdbench(ctx)
        if not shutil.which("java"):
            raise DependencyUnavailable("vdbench requires java")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_vdbench", remote)
        try:
            artifact = ctx.artifact_dir(self.id)
            cfg_path = artifact / "vdbench.conf"
            cfg_path.write_text(
                "\n".join(
                    [
                        f"fsd=fsd1,anchor={handle.mountpoint / 'vdbench'},depth=2,width=4,files=32,size=4k",
                        "fwd=fwd1,fsd=fsd1,operation=create,xfersize=4k,fileio=random,fileselect=random",
                        "rd=rd1,fwd=fwd1,fwdrate=max,format=yes,elapsed=30,interval=5",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = ctx.target.run_cmd("community-vdbench", [vdbench, "-f", str(cfg_path), "-o", str(artifact / "out")], timeout=int(os.environ.get("VDBENCH_TIMEOUT_S", str(self.timeout))))
            if not result.ok:
                raise BlackboxError(f"vdbench failed; see {result.stderr}")
            return {"config": str(cfg_path)}
        finally:
            ctx.target.unmount(handle)
