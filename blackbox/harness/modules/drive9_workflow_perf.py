from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from ..core import Context, ensure_empty, env_flag, env_value, stable_bytes
from .base import module_config, timeit
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9WorkflowPerf(Drive9WorkflowBase):
    id = "drive9.workflow.perf"
    description = "Drive9 FUSE performance workloads: file I/O, SQLite, rg, git clone modes, build/edit."
    labels = ("drive9", "workflow", "performance")
    timeout = 7200

    def run(self, ctx: Context) -> dict[str, Any]:
        selected = [part.strip() for part in env_value("REPOS", "drive9,kimi-code", ctx.suite).split(",") if part.strip()]
        repos_cfg = ctx.config.get("repos", [])
        repos = [repo for repo in repos_cfg if repo.get("id") in selected]
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("drive9_workflow_perf", remote, profile="coding-agent", durability="interactive")
        try:
            self.micro(ctx, handle.mountpoint)
            for repo in repos:
                self.repo_perf(ctx, handle.mountpoint, repo)
            return {"repos": [repo.get("id") for repo in repos], "runs": ctx.runs}
        finally:
            ctx.target.unmount(handle)

    def micro(self, ctx: Context, root: Path) -> None:
        cfg = module_config(ctx, self.id).get("micro", {})
        for mib in cfg.get("sequential_sizes_mib", [1, 64]):
            size = int(mib) * 1024 * 1024
            payload = stable_bytes(min(size, 1024 * 1024), seed=int(mib))
            write_values: list[float] = []
            read_values: list[float] = []
            for run in range(ctx.runs):
                path = root / f"seq-{mib}m-{run}.bin"

                def write_body() -> None:
                    remaining = size
                    with path.open("wb") as f:
                        while remaining > 0:
                            chunk = payload[: min(len(payload), remaining)]
                            f.write(chunk)
                            remaining -= len(chunk)
                        f.flush()
                        os.fsync(f.fileno())

                seconds = timeit(write_body)
                write_values.append((size / (1024 * 1024)) / seconds)
                seconds = timeit(lambda: path.read_bytes())
                read_values.append((size / (1024 * 1024)) / seconds)
            ctx.perf_values(f"drive9.workflow.perf.seq_write_{mib}m", write_values, "MiB/s")
            ctx.perf_values(f"drive9.workflow.perf.seq_read_{mib}m", read_values, "MiB/s")

        rows = int(cfg.get("sqlite_rows", 1000))
        values = []
        for run in range(ctx.runs):
            db_path = root / f"sqlite-{run}.db"

            def sqlite_body() -> None:
                conn = sqlite3.connect(str(db_path))
                try:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
                    conn.executemany("INSERT INTO t(payload) VALUES (?)", [(f"row-{idx}",) for idx in range(rows)])
                    conn.commit()
                finally:
                    conn.close()

            values.append(timeit(sqlite_body))
        ctx.perf_values("drive9.workflow.perf.sqlite_wal_insert", values, "seconds")

        if shutil.which("rg"):
            values = []
            for run in range(ctx.runs):
                work = root / f"rg-{run}"
                ensure_empty(work)
                for idx in range(200):
                    (work / f"f{idx:04d}.txt").write_text(f"needle line {idx}\n" * 20, encoding="utf-8")
                values.append(timeit(lambda: ctx.target.capture(["rg", "needle", str(work)], timeout=300)))
            ctx.perf_values("drive9.workflow.perf.rg_generated_tree", values, "seconds")

    def repo_perf(self, ctx: Context, root: Path, repo: dict[str, Any]) -> None:
        repo_id = str(repo["id"])
        timeout = int(repo.get("timeout_seconds", 1800))
        env = ctx.target.base_env()
        native_values: list[float] = []
        fuse_values: list[float] = []
        fast_values: list[float] = []
        blobless_values: list[float] = []
        rg_values: list[float] = []
        edit_values: list[float] = []
        build_values: list[float] = []
        for run in range(ctx.runs):
            native_root = ctx.tmp_dir / "native-repos" / repo_id / f"run-{run}"
            ensure_empty(native_root)
            native_target = native_root / "repo"
            native_values.append(timeit(lambda: ctx.target.capture(["git", "clone", "--no-local", repo["url"], str(native_target)], timeout=timeout, env=env)))

            normal_target = root / f"{repo_id}-normal-{run}"
            fuse_values.append(timeit(lambda: ctx.target.capture(["git", "clone", "--no-local", repo["url"], str(normal_target)], timeout=timeout, env=env)))

            fast_target = root / f"{repo_id}-fast-{run}"
            fast_values.append(timeit(lambda: ctx.target.drive9_capture(["git", "clone", "--fast", repo["url"], str(fast_target)], timeout=timeout)))

            blobless_target = root / f"{repo_id}-blobless-{run}"
            blobless_values.append(timeit(lambda: ctx.target.drive9_capture(["git", "clone", "--fast", "--blobless", "--hydrate=sync", repo["url"], str(blobless_target)], timeout=timeout)))
            if shutil.which("rg"):
                rg_values.append(timeit(lambda: ctx.target.capture(["rg", repo.get("rg_pattern", "TODO|FIXME"), str(blobless_target)], timeout=300, env=env)))
            edit_values.append(timeit(lambda: self.edit_commit(ctx, blobless_target, env)))
            build_cmds = repo.get("build", [])
            if build_cmds and env_flag("ENABLE_REPO_BUILD", True, ctx.suite):
                build_dir = blobless_target / repo.get("build_dir", ".")
                build_values.append(timeit(lambda: [ctx.target.capture(["bash", "-lc", cmd], cwd=build_dir, timeout=timeout, env=env) for cmd in build_cmds]))

        ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.native_git_clone", native_values, "seconds")
        ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.fuse_git_clone", fuse_values, "seconds")
        ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.drive9_git_clone_fast", fast_values, "seconds")
        ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.drive9_git_clone_fast_blobless", blobless_values, "seconds")
        if rg_values:
            ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.rg", rg_values, "seconds")
        ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.edit_commit", edit_values, "seconds")
        if build_values:
            ctx.perf_values(f"drive9.workflow.perf.repo.{repo_id}.build", build_values, "seconds")

    def edit_commit(self, ctx: Context, repo: Path, env: dict[str, str]) -> None:
        ctx.target.capture(["git", "config", "user.name", "Drive9 Blackbox"], cwd=repo, env=env)
        ctx.target.capture(["git", "config", "user.email", "blackbox@drive9.local"], cwd=repo, env=env)
        candidates = [path for path in repo.rglob("*") if path.is_file() and ".git" not in path.parts and path.stat().st_size < 1024 * 1024]
        for path in candidates[:20]:
            with path.open("a", encoding="utf-8", errors="ignore") as handle:
                handle.write("\n# drive9 blackbox edit\n")
        (repo / "blackbox-new.txt").write_text("new file\n", encoding="utf-8")
        ctx.target.capture(["git", "add", "-A"], cwd=repo, env=env, timeout=300)
        ctx.target.capture(["git", "commit", "-m", "blackbox edit"], cwd=repo, env=env, timeout=300)
