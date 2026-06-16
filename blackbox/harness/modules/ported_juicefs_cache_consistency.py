from __future__ import annotations

import time
from typing import Any

from ..core import BlackboxError, Context
from .base import BaseModule


class PortedJuiceFSCacheConsistency(BaseModule):
    id = "ported.juicefs.cache_consistency"
    category = "ported.juicefs.cache"
    description = "JuiceFS-inspired two-mount cache visibility checks, rewritten for Drive9 FUSE."
    labels = ("compatibility", "cache", "ported-juicefs")
    timeout = 1200

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        h1 = ctx.target.mount("ported_juicefs_cache_consistency", remote, cache_key="writer")
        h2 = ctx.target.mount("ported_juicefs_cache_consistency", remote, cache_key="reader")
        try:
            (h1.mountpoint / "visible.txt").write_text("writer\n", encoding="utf-8")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                p = h2.mountpoint / "visible.txt"
                if p.exists() and p.read_text(encoding="utf-8") == "writer\n":
                    return {"visibility": "cross-mount"}
                time.sleep(0.25)
            raise BlackboxError("cross-mount write did not become visible")
        finally:
            ctx.target.unmount(h2)
            ctx.target.unmount(h1)
