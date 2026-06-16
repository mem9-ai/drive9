from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, summarize
from .base import BaseModule, module_config


class CommunityFio(BaseModule):
    id = "community.fio"
    category = "community.performance"
    description = "Run fio sequential and random I/O workloads on Drive9 FUSE."
    labels = ("performance", "community")
    timeout = 3600

    def run(self, ctx: Context) -> dict[str, Any]:
        fio = ctx.deps.ensure_fio()
        size = module_config(ctx, self.id).get("size", "128m")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_fio", remote, durability="interactive")
        try:
            work = handle.mountpoint / "fio"
            work.mkdir()
            results: dict[str, Any] = {}
            for workload, rw in (("seq_write", "write"), ("seq_read", "read"), ("rand_rw", "randrw")):
                values: list[float] = []
                for run in range(ctx.runs):
                    output = ctx.artifact_dir(self.id) / f"{workload}-{run}.json"
                    args = [
                        fio,
                        f"--name={workload}",
                        f"--directory={work}",
                        f"--rw={rw}",
                        "--bs=1m" if rw != "randrw" else "--bs=4k",
                        f"--size={size}",
                        "--numjobs=1",
                        "--iodepth=1",
                        "--direct=0",
                        "--output-format=json",
                        f"--output={output}",
                    ]
                    result = ctx.target.run_cmd(f"community-fio-{workload}-{run}", args, timeout=int(os.environ.get("FIO_TIMEOUT_S", str(self.timeout))))
                    if not result.ok:
                        raise BlackboxError(f"fio {workload} failed; see {result.stderr}")
                    values.append(self.extract_bw_mib(output, rw))
                ctx.perf_values(f"community.fio.{workload}", values, "MiB/s")
                results[workload] = summarize(values, "MiB/s")
            return results
        finally:
            ctx.target.unmount(handle)

    def extract_bw_mib(self, output: Path, rw: str) -> float:
        data = json.loads(output.read_text(encoding="utf-8"))
        job = data.get("jobs", [{}])[0]
        if rw == "randrw":
            read_bw = float(job.get("read", {}).get("bw_bytes", 0))
            write_bw = float(job.get("write", {}).get("bw_bytes", 0))
            return (read_bw + write_bw) / (1024 * 1024)
        key = "read" if "read" in rw else "write"
        return float(job.get(key, {}).get("bw_bytes", 0)) / (1024 * 1024)
