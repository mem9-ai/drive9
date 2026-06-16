from __future__ import annotations

import threading
from typing import Any

from harness.core import BlackboxError, Context
from .base import BaseModule, module_config


class PortedJuiceFSRandomStress(BaseModule):
    id = "ported.juicefs.random_stress"
    category = "ported.juicefs.stress"
    description = "JuiceFS-inspired concurrent create/read/rename/remove stress, rewritten for Drive9 FUSE."
    labels = ("stress", "ported-juicefs")
    timeout = 1200

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        workers = int(cfg.get("workers", 4))
        files = int(cfg.get("files_per_worker", 64))
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("ported_juicefs_random_stress", remote, durability="interactive")
        errors: list[str] = []
        try:
            def worker(idx: int) -> None:
                try:
                    root = handle.mountpoint / f"worker-{idx}"
                    root.mkdir()
                    for n in range(files):
                        p = root / f"f{n:04d}.txt"
                        payload = f"worker={idx} file={n}\n"
                        p.write_text(payload, encoding="utf-8")
                        if p.read_text(encoding="utf-8") != payload:
                            raise BlackboxError(f"readback mismatch {p}")
                        p.rename(root / f"r{n:04d}.txt")
                except Exception as exc:
                    errors.append(str(exc))

            threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(workers)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            if errors:
                raise BlackboxError("; ".join(errors[:5]))
            count = sum(1 for _ in handle.mountpoint.rglob("*.txt"))
            return {"workers": workers, "files": count}
        finally:
            ctx.target.unmount(handle)
