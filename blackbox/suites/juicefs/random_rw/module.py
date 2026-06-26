from __future__ import annotations

import os
import random
from typing import Any

from harness.core import BlackboxError, Context, stable_bytes
from harness.module_base import BaseModule, module_config


class JuiceFSRandomRW(BaseModule):
    id = "juicefs.random_rw"
    category = "juicefs.io"
    description = "JuiceFS-inspired random write/read verification workload, rewritten for Drive9 FUSE."
    labels = ("io", "juicefs", "functional")
    timeout = 900

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        size = int(cfg.get("size_bytes", 4 * 1024 * 1024))
        ops = int(cfg.get("ops", 1024))
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("juicefs_random_rw", remote, durability="write-sync")
        try:
            path = handle.mountpoint / "random-rw.bin"
            model = bytearray(stable_bytes(size, seed=19))
            path.write_bytes(model)
            rng = random.Random(99)
            for idx in range(ops):
                offset = rng.randrange(0, size)
                length = min(rng.randrange(1, 4096), size - offset)
                data = stable_bytes(length, seed=idx)
                model[offset : offset + length] = data
                with path.open("r+b") as f:
                    f.seek(offset)
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
            if path.read_bytes() != bytes(model):
                raise BlackboxError("random read/write final content mismatch")
            return {"bytes": size, "ops": ops}
        finally:
            ctx.target.unmount(handle)
