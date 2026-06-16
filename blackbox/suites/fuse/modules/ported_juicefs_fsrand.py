from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, ensure_empty, sha256_file, stable_bytes
from .base import BaseModule, module_config


class PortedJuiceFSFsrand(BaseModule):
    id = "ported.juicefs.fsrand"
    category = "ported.juicefs.consistency"
    description = "JuiceFS-inspired deterministic random filesystem model test, rewritten for Drive9 FUSE."
    labels = ("compatibility", "stress", "ported-juicefs")
    timeout = 1200

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        ops = int(cfg.get("ops_by_preset", {}).get(ctx.selected_preset or "daily", 200))
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("ported_juicefs_fsrand", remote, durability="write-sync")
        oracle = ctx.tmp_dir / "oracle" / self.id
        ensure_empty(oracle)
        try:
            rng = random.Random(int(cfg.get("seed", 9)))
            for idx in range(ops):
                self.random_op(rng, oracle, handle.mountpoint, idx)
                if idx % 25 == 0:
                    self.compare_trees(oracle, handle.mountpoint)
            self.compare_trees(oracle, handle.mountpoint)
            return {"ops": ops}
        finally:
            ctx.target.unmount(handle)

    def random_op(self, rng: random.Random, oracle: Path, target: Path, idx: int) -> None:
        dirs = [Path(".")] + [p.relative_to(oracle) for p in oracle.rglob("*") if p.is_dir()]
        files = [p.relative_to(oracle) for p in oracle.rglob("*") if p.is_file()]
        op = rng.choice(["mkdir", "write", "append", "rename", "unlink", "truncate"])
        if op == "mkdir":
            rel = rng.choice(dirs) / f"d{idx:05d}"
            (oracle / rel).mkdir(exist_ok=True)
            (target / rel).mkdir(exist_ok=True)
        elif op == "write" or not files:
            rel = rng.choice(dirs) / f"f{idx:05d}.bin"
            data = stable_bytes(rng.randrange(1, 4096), seed=idx)
            (oracle / rel).write_bytes(data)
            (target / rel).write_bytes(data)
        elif op == "append":
            rel = rng.choice(files)
            data = stable_bytes(rng.randrange(1, 512), seed=idx)
            with (oracle / rel).open("ab") as f:
                f.write(data)
            with (target / rel).open("ab") as f:
                f.write(data)
        elif op == "rename":
            rel = rng.choice(files)
            new_rel = rel.parent / f"renamed-{idx:05d}.bin"
            (oracle / rel).rename(oracle / new_rel)
            (target / rel).rename(target / new_rel)
        elif op == "unlink":
            rel = rng.choice(files)
            (oracle / rel).unlink()
            (target / rel).unlink()
        elif op == "truncate":
            rel = rng.choice(files)
            size = rng.randrange(0, 2048)
            with (oracle / rel).open("r+b") as f:
                f.truncate(size)
            with (target / rel).open("r+b") as f:
                f.truncate(size)

    def compare_trees(self, oracle: Path, target: Path) -> None:
        oracle_paths = sorted(p.relative_to(oracle) for p in oracle.rglob("*"))
        target_paths = sorted(p.relative_to(target) for p in target.rglob("*"))
        if oracle_paths != target_paths:
            raise BlackboxError(f"tree mismatch oracle={oracle_paths[:20]} target={target_paths[:20]}")
        for rel in oracle_paths:
            op = oracle / rel
            tp = target / rel
            if op.is_file() and sha256_file(op) != sha256_file(tp):
                raise BlackboxError(f"content mismatch: {rel}")
