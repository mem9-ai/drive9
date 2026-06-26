from __future__ import annotations

import shutil
from typing import Any

from harness.core import BlackboxError, Context
from harness.module_base import BaseModule


class PortedJuiceFSRmr(BaseModule):
    id = "ported.juicefs.rmr"
    category = "ported.juicefs.metadata"
    description = "JuiceFS-inspired recursive remove workload, rewritten for Drive9 FUSE."
    labels = ("metadata", "ported-juicefs")
    timeout = 900

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("ported_juicefs_rmr", remote, durability="interactive")
        try:
            root = handle.mountpoint / "tree"
            for i in range(20):
                d = root / f"d{i:03d}"
                d.mkdir(parents=True, exist_ok=True)
                for j in range(20):
                    (d / f"f{j:03d}.txt").write_text(f"{i}-{j}\n", encoding="utf-8")
            count = sum(1 for _ in root.rglob("*"))
            shutil.rmtree(root)
            if root.exists():
                raise BlackboxError("recursive remove left root behind")
            return {"entries_removed": count}
        finally:
            ctx.target.unmount(handle)
