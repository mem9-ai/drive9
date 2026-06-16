from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, ModuleSkip, ensure_empty, env_flag, env_value, write_json

from .base import module_config
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9LocalOverlayBuild(Drive9WorkflowBase):
    id = "drive9.workflow.local_overlay_build"
    description = "Compare native repo builds with Drive9 FUSE coding-agent local overlay builds."
    labels = ("drive9", "workflow", "performance", "local-overlay")
    timeout = 10800

    def ensure_dependencies(self, ctx: Context) -> None:
        super().ensure_dependencies(ctx)
        ctx.deps.require_tool("bash")

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        repos = self.selected_repos(ctx)
        if not repos:
            raise ModuleSkip("no repos selected for local overlay build")

        storages = [str(item) for item in cfg.get("storages", ["native", "fuse"])]
        unknown = sorted(set(storages) - {"native", "fuse"})
        if unknown:
            raise BlackboxError(f"unknown local overlay build storage(s): {', '.join(unknown)}")

        prewarm = env_flag("LOCAL_OVERLAY_PREWARM", bool(cfg.get("prewarm", True)), ctx.suite)
        verify_remote = env_flag("LOCAL_OVERLAY_VERIFY_REMOTE", bool(cfg.get("verify_remote_absence", True)), ctx.suite)
        env = self.base_repo_env(ctx)
        commits = {repo["id"]: self.resolve_commit(ctx, repo, env) for repo in repos}
        samples: list[dict[str, Any]] = []
        failures: list[str] = []

        artifact = ctx.artifact_dir(self.id)
        write_json(
            artifact / "manifest.json",
            {
                "repos": [repo["id"] for repo in repos],
                "repo_commits": commits,
                "storages": storages,
                "runs": ctx.runs,
                "prewarm": prewarm,
                "verify_remote_absence": verify_remote,
                "profile": str(cfg.get("profile", "coding-agent")),
                "allow_other": bool(cfg.get("allow_other", False)),
                "local_only_policy": (
                    "FUSE samples mount with coding-agent local overlay. Module/repo extra local-only "
                    "patterns route generated output probes such as bin/ or repo-specific temp trees to local root."
                ),
            },
        )

        if prewarm:
            for repo in repos:
                self.prewarm_repo(ctx, repo, commits[str(repo["id"])], env)

        for repo in repos:
            repo_id = str(repo["id"])
            for run_index in range(1, ctx.runs + 1):
                for storage in storages:
                    sample_env = self.sample_env(ctx, repo_id, storage, run_index, env)
                    if storage == "native":
                        checkout = ctx.tmp_dir / "local-overlay-build" / "native" / repo_id / f"run-{run_index}" / "repo"
                        ensure_empty(checkout.parent)
                        self.run_sample(ctx, repo, commits[repo_id], storage, run_index, checkout, sample_env, samples, failures)
                    else:
                        remote = ctx.target.remote_root(self.id, f"{repo_id}-run-{run_index}")
                        ctx.target.mkdir_remote(remote)
                        extra = self.mount_extra(ctx, repo)
                        handle = ctx.target.mount(
                            "drive9_local_overlay_build",
                            remote,
                            profile=str(cfg.get("profile", "coding-agent")),
                            durability=str(cfg.get("durability", "interactive")),
                            cache_key=f"{repo_id}-run-{run_index}",
                            extra=extra,
                        )
                        checkout = handle.mountpoint / "repo"
                        try:
                            self.run_sample(ctx, repo, commits[repo_id], storage, run_index, checkout, sample_env, samples, failures, handle=handle)
                        finally:
                            ctx.target.unmount(handle)
                        if verify_remote and self.phase_ok(samples, repo_id, "fuse", run_index, "checkout"):
                            self.verify_remote_absence(ctx, repo, remote, run_index, samples, failures)

        self.write_summary(ctx, samples, failures, commits)
        if failures:
            raise BlackboxError(f"local overlay build failures={len(failures)}; see {artifact / 'summary.md'}")
        return {"repos": [repo["id"] for repo in repos], "runs": ctx.runs, "storages": storages}

    def selected_repos(self, ctx: Context) -> list[dict[str, Any]]:
        selected = [part.strip() for part in env_value("REPOS", "drive9,kimi-code", ctx.suite).split(",") if part.strip()]
        repos = [repo for repo in ctx.config.get("repos", []) if repo.get("id") in selected]
        known = {str(repo.get("id")) for repo in ctx.config.get("repos", [])}
        unknown = sorted(set(selected) - known)
        if unknown:
            raise ModuleSkip(f"unknown BLACKBOX_REPOS value(s): {', '.join(unknown)}")
        return repos

    def base_repo_env(self, ctx: Context) -> dict[str, str]:
        env = ctx.target.base_env()
        cache_root = ctx.tmp_dir / "local-overlay-build" / "shared-cache"
        values = {
            "COREPACK_HOME": cache_root / "corepack",
            "BUN_INSTALL_CACHE_DIR": cache_root / "bun",
            "UV_CACHE_DIR": cache_root / "uv",
            "npm_config_cache": cache_root / "npm",
            "PNPM_STORE_DIR": cache_root / "pnpm-store",
            "npm_config_store_dir": cache_root / "pnpm-store",
            "GOMODCACHE": cache_root / "go" / "pkg" / "mod",
            "CARGO_HOME": cache_root / "cargo-home",
        }
        for key, path in values.items():
            path.mkdir(parents=True, exist_ok=True)
            env[key] = str(path)
        return env

    def sample_env(self, ctx: Context, repo_id: str, storage: str, run_index: int, base: dict[str, str]) -> dict[str, str]:
        env = dict(base)
        sample = ctx.tmp_dir / "local-overlay-build" / "sample-cache" / storage / repo_id / f"run-{run_index}"
        sample.mkdir(parents=True, exist_ok=True)
        values = {
            "GOCACHE": sample / "go-build-cache",
        }
        for key, path in values.items():
            path.mkdir(parents=True, exist_ok=True)
            env[key] = str(path)
        return env

    def resolve_commit(self, ctx: Context, repo: dict[str, Any], env: dict[str, str]) -> str:
        if repo.get("commit"):
            return str(repo["commit"])
        ref = str(repo.get("ref", "HEAD"))
        refs = [ref]
        if ref and ref != "HEAD" and not ref.startswith("refs/"):
            refs.append(f"refs/heads/{ref}")
            refs.append(f"refs/tags/{ref}")
        errors: list[str] = []
        for item in refs:
            try:
                out = ctx.target.capture(["git", "ls-remote", "--exit-code", str(repo["url"]), item], timeout=120, env=env)
            except Exception as exc:
                errors.append(str(exc))
                continue
            for line in out.splitlines():
                parts = line.split()
                if parts:
                    return parts[0]
        raise BlackboxError(f"could not resolve {repo.get('id')} ref {ref!r}: {'; '.join(errors)[-1000:]}")

    def local_overlay_config(self, repo: dict[str, Any]) -> dict[str, Any]:
        value = repo.get("local_overlay", {})
        return value if isinstance(value, dict) else {}

    def prewarm_repo(self, ctx: Context, repo: dict[str, Any], commit: str, env: dict[str, str]) -> None:
        overlay_cfg = self.local_overlay_config(repo)
        commands = list(overlay_cfg.get("prewarm", repo.get("prewarm", [])))
        if not commands:
            return
        repo_id = str(repo["id"])
        root = ctx.tmp_dir / "local-overlay-build" / "prewarm" / repo_id
        ensure_empty(root)
        checkout = root / "repo"
        clone = self.run_phase_commands(
            ctx,
            name=f"local-overlay-build-prewarm-{repo_id}-clone",
            commands=[["git", "clone", "--no-checkout", str(repo["url"]), str(checkout)]],
            cwd=root,
            timeout=self.clone_timeout(repo),
            env=env,
            shell=False,
        )
        if clone["status"] != "ok":
            raise BlackboxError(f"prewarm clone failed for {repo_id}; see {clone['stderr']}")
        checkout_phase = self.run_phase_commands(
            ctx,
            name=f"local-overlay-build-prewarm-{repo_id}-checkout",
            commands=[["git", "-C", str(checkout), "checkout", "--detach", commit]],
            cwd=root,
            timeout=self.clone_timeout(repo),
            env=env,
            shell=False,
        )
        if checkout_phase["status"] != "ok":
            raise BlackboxError(f"prewarm checkout failed for {repo_id}; see {checkout_phase['stderr']}")
        build_dir = checkout / str(overlay_cfg.get("build_dir", repo.get("build_dir", ".")))
        phase = self.run_phase_commands(
            ctx,
            name=f"local-overlay-build-prewarm-{repo_id}",
            commands=[str(command) for command in commands],
            cwd=build_dir,
            timeout=int(overlay_cfg.get("prewarm_timeout_seconds", self.prewarm_timeout(repo))),
            env=self.sample_env(ctx, repo_id, "prewarm", 0, env),
            shell=True,
        )
        ctx.recorder.event({"type": "local_overlay_build_prewarm", "module": self.id, "repo": repo_id, **phase})
        if phase["status"] != "ok":
            raise BlackboxError(f"prewarm failed for {repo_id}; see {phase['stderr']}")

    def run_sample(
        self,
        ctx: Context,
        repo: dict[str, Any],
        commit: str,
        storage: str,
        run_index: int,
        checkout: Path,
        env: dict[str, str],
        samples: list[dict[str, Any]],
        failures: list[str],
        *,
        handle: Any | None = None,
    ) -> None:
        repo_id = str(repo["id"])
        overlay_cfg = self.local_overlay_config(repo)
        checkout.parent.mkdir(parents=True, exist_ok=True)
        phases = [
            (
                "clone",
                [["git", "clone", "--no-checkout", str(repo["url"]), str(checkout)]],
                checkout.parent,
                self.clone_timeout(repo),
                False,
            ),
            (
                "checkout",
                [["git", "-C", str(checkout), "checkout", "--detach", commit]],
                checkout.parent,
                self.clone_timeout(repo),
                False,
            ),
        ]
        for phase_name, commands, cwd, timeout, shell in phases:
            event = self.phase_event(
                ctx,
                repo=repo,
                storage=storage,
                run_index=run_index,
                phase=phase_name,
                commit=commit,
                phase_result=self.run_phase_commands(
                    ctx,
                    name=f"local-overlay-build-{storage}-{repo_id}-run-{run_index}-{phase_name}",
                    commands=commands,
                    cwd=cwd,
                    timeout=timeout,
                    env=env,
                    shell=shell,
                ),
            )
            samples.append(event)
            ctx.recorder.event(event)
            if event["status"] != "ok":
                failures.append(f"{repo_id} {storage} run {run_index} {phase_name} failed; see {event['stderr']}")
                return
            if phase_name == "checkout" and handle is not None:
                self.write_overlay_probes(ctx, repo, run_index, checkout, handle, failures)

        build_cmds = [str(command) for command in overlay_cfg.get("build", repo.get("build", []))]
        if not build_cmds:
            return
        build_dir = checkout / str(overlay_cfg.get("build_dir", repo.get("build_dir", ".")))
        if not build_dir.exists():
            event = self.failed_phase_event(repo, storage, run_index, "build", commit, f"build_dir does not exist: {build_dir}")
            samples.append(event)
            ctx.recorder.event(event)
            failures.append(f"{repo_id} {storage} run {run_index} build_dir missing: {build_dir}")
            return
        event = self.phase_event(
            ctx,
            repo=repo,
            storage=storage,
            run_index=run_index,
            phase="build",
            commit=commit,
            phase_result=self.run_phase_commands(
                ctx,
                name=f"local-overlay-build-{storage}-{repo_id}-run-{run_index}-build",
                commands=build_cmds,
                cwd=build_dir,
                timeout=int(overlay_cfg.get("build_timeout_seconds", self.build_timeout(repo))),
                env=env,
                shell=True,
            ),
        )
        samples.append(event)
        ctx.recorder.event(event)
        if event["status"] != "ok":
            failures.append(f"{repo_id} {storage} run {run_index} build failed; see {event['stderr']}")

    def run_phase_commands(
        self,
        ctx: Context,
        *,
        name: str,
        commands: list[list[str] | str],
        cwd: Path,
        timeout: int,
        env: dict[str, str],
        shell: bool,
    ) -> dict[str, Any]:
        started = time.monotonic()
        started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        stdout = ""
        stderr = ""
        exit_code = 0
        for command in commands:
            result = ctx.target.run_cmd(name, command, cwd=cwd, timeout=timeout, env=env, shell=shell, ok_codes=(0,))
            stdout = str(result.stdout)
            stderr = str(result.stderr)
            exit_code = int(result.code)
            if not result.ok:
                break
        return {
            "status": "ok" if exit_code == 0 else "failed",
            "exit_code": exit_code,
            "duration_seconds": time.monotonic() - started,
            "started_at": started_at,
            "ended_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "cwd": str(cwd),
            "stdout": stdout,
            "stderr": stderr,
            "commands": [self.command_text(command) for command in commands],
        }

    def phase_event(
        self,
        ctx: Context,
        *,
        repo: dict[str, Any],
        storage: str,
        run_index: int,
        phase: str,
        commit: str,
        phase_result: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "type": "local_overlay_build_phase",
            "module": self.id,
            "session": ctx.session,
            "repo": str(repo["id"]),
            "storage": storage,
            "run": run_index,
            "phase": phase,
            "commit": commit,
            **phase_result,
        }

    def failed_phase_event(self, repo: dict[str, Any], storage: str, run_index: int, phase: str, commit: str, detail: str) -> dict[str, Any]:
        return {
            "type": "local_overlay_build_phase",
            "module": self.id,
            "repo": str(repo["id"]),
            "storage": storage,
            "run": run_index,
            "phase": phase,
            "commit": commit,
            "status": "failed",
            "exit_code": 127,
            "duration_seconds": 0.0,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ended_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "cwd": "",
            "stdout": "",
            "stderr": detail,
            "commands": [],
        }

    def write_overlay_probes(self, ctx: Context, repo: dict[str, Any], run_index: int, checkout: Path, handle: Any, failures: list[str]) -> None:
        repo_id = str(repo["id"])
        for rel in self.probe_paths(repo):
            mount_path = checkout / rel
            mount_path.parent.mkdir(parents=True, exist_ok=True)
            mount_path.write_text(f"drive9 blackbox local overlay probe {repo_id} run {run_index}\n", encoding="utf-8")
            local_path = self.local_overlay_path(handle, checkout, rel)
            event = {
                "type": "local_overlay_probe",
                "module": self.id,
                "repo": repo_id,
                "run": run_index,
                "stage": "post_checkout",
                "mount_path": str(mount_path),
                "local_path": str(local_path),
                "mount_exists": mount_path.exists(),
                "local_exists": local_path.exists(),
            }
            ctx.recorder.event(event)
            if not event["local_exists"]:
                failures.append(f"{repo_id} fuse run {run_index} local overlay probe did not land in local root: {rel}")

    def verify_remote_absence(
        self,
        ctx: Context,
        repo: dict[str, Any],
        remote: str,
        run_index: int,
        samples: list[dict[str, Any]],
        failures: list[str],
    ) -> None:
        repo_id = str(repo["id"])
        handle = ctx.target.mount(
            "drive9_local_overlay_build_remote_probe",
            remote,
            read_only=True,
            profile="none",
            durability="interactive",
            cache_key=f"{repo_id}-run-{run_index}",
        )
        try:
            repo_root = handle.mountpoint / "repo"
            visible = []
            for rel in self.probe_paths(repo):
                if (repo_root / rel).exists():
                    visible.append(rel)
            event = {
                "type": "local_overlay_remote_absence",
                "module": self.id,
                "repo": repo_id,
                "run": run_index,
                "remote_root": remote,
                "repo_root_visible": repo_root.exists(),
                "visible_probe_paths": visible,
                "status": "ok" if repo_root.exists() and not visible else "failed",
            }
            ctx.recorder.event(event)
            samples.append(
                {
                    "type": "local_overlay_build_phase",
                    "module": self.id,
                    "repo": repo_id,
                    "storage": "fuse",
                    "run": run_index,
                    "phase": "remote_absence",
                    "status": event["status"],
                    "exit_code": 0 if event["status"] == "ok" else 1,
                    "duration_seconds": 0.0,
                    "stdout": "",
                    "stderr": "" if event["status"] == "ok" else f"visible local-only probes: {visible}",
                    "commands": [],
                }
            )
            if event["status"] != "ok":
                failures.append(f"{repo_id} fuse run {run_index} local-only probes visible through profile=none mount: {visible}")
        finally:
            ctx.target.unmount(handle)

    def write_summary(self, ctx: Context, samples: list[dict[str, Any]], failures: list[str], commits: dict[str, str]) -> None:
        artifact = ctx.artifact_dir(self.id)
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for event in samples:
            groups.setdefault((str(event.get("repo", "")), str(event.get("storage", "")), str(event.get("phase", ""))), []).append(event)

        rows: list[dict[str, Any]] = []
        for key, events in sorted(groups.items()):
            repo, storage, phase = key
            values = [float(event["duration_seconds"]) for event in events if event.get("status") == "ok" and phase != "remote_absence"]
            row = {
                "repo": repo,
                "storage": storage,
                "phase": phase,
                "ok": sum(1 for event in events if event.get("status") == "ok"),
                "count": len(events),
                "mean": statistics.mean(values) if values else None,
                "median": statistics.median(values) if values else None,
                "min": min(values) if values else None,
                "max": max(values) if values else None,
            }
            rows.append(row)
            if values:
                ctx.perf_values(f"{self.id}.repo.{repo}.{storage}.{phase}", values, "seconds")

        by_key = {(row["repo"], row["storage"], row["phase"]): row for row in rows}
        ratios: list[dict[str, Any]] = []
        for repo in sorted({row["repo"] for row in rows}):
            for phase in ("clone", "checkout", "build"):
                native = by_key.get((repo, "native", phase))
                fuse = by_key.get((repo, "fuse", phase))
                if native and fuse and native.get("mean") and fuse.get("mean"):
                    ratio = float(fuse["mean"]) / float(native["mean"])
                    ratios.append({"repo": repo, "phase": phase, "fuse_native_ratio": ratio})
                    ctx.metric(f"{self.id}.repo.{repo}.{phase}.fuse_native_ratio", ratio, "x")

        write_json(
            artifact / "summary.json",
            {
                "commits": commits,
                "rows": rows,
                "ratios": ratios,
                "failures": failures,
            },
        )
        csv_lines = ["repo,storage,phase,ok,count,mean_s,median_s,min_s,max_s"]
        for row in rows:
            csv_lines.append(
                ",".join(
                    [
                        str(row["repo"]),
                        str(row["storage"]),
                        str(row["phase"]),
                        str(row["ok"]),
                        str(row["count"]),
                        self.optional_float(row["mean"]),
                        self.optional_float(row["median"]),
                        self.optional_float(row["min"]),
                        self.optional_float(row["max"]),
                    ]
                )
            )
        (artifact / "summary.csv").write_text("\n".join(csv_lines) + "\n", encoding="utf-8")
        lines = [
            "# Drive9 Local Overlay Build",
            "",
            "| repo | storage | phase | ok/count | mean s | median s | min s | max s |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
        for row in rows:
            lines.append(
                f"| {row['repo']} | {row['storage']} | {row['phase']} | {row['ok']}/{row['count']} | "
                f"{self.optional_float(row['mean'])} | {self.optional_float(row['median'])} | "
                f"{self.optional_float(row['min'])} | {self.optional_float(row['max'])} |"
            )
        lines.extend(["", "## FUSE / Native Ratios", ""])
        for ratio in ratios:
            lines.append(f"- `{ratio['repo']}` `{ratio['phase']}`: `{ratio['fuse_native_ratio']:.3f}x`")
        lines.extend(["", "## Failures", ""])
        if failures:
            lines.extend(f"- {failure}" for failure in failures)
        else:
            lines.append("- None")
        (artifact / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def mount_extra(self, ctx: Context, repo: dict[str, Any]) -> list[str]:
        cfg = module_config(ctx, self.id)
        extra: list[str] = []
        if bool(cfg.get("allow_other", False)):
            extra.append("--allow-other")
        for pattern in self.extra_local_only_patterns(ctx, repo):
            extra.extend(["--local-only", pattern])
        return extra

    def extra_local_only_patterns(self, ctx: Context, repo: dict[str, Any]) -> list[str]:
        cfg = module_config(ctx, self.id)
        overlay_cfg = self.local_overlay_config(repo)
        out: list[str] = []
        for value in cfg.get("additional_local_only_patterns", []):
            out.append(str(value))
        for value in overlay_cfg.get("local_only_patterns", []):
            out.append(str(value))
        return list(dict.fromkeys(item for item in out if item))

    def probe_paths(self, repo: dict[str, Any]) -> list[str]:
        overlay_cfg = self.local_overlay_config(repo)
        defaults = [
            ".git/drive9-blackbox-local-overlay-probe",
            ".cache/drive9-blackbox-local-overlay-probe",
            "tmp/drive9-blackbox-local-overlay-probe",
        ]
        paths = [str(value).strip().strip("/") for value in overlay_cfg.get("probe_paths", defaults)]
        return [value for value in dict.fromkeys(paths) if value]

    def local_overlay_path(self, handle: Any, checkout: Path, rel: str) -> Path:
        try:
            mount_rel = checkout.relative_to(handle.mountpoint)
        except ValueError:
            mount_rel = Path(checkout.name)
        return handle.local_root / "overlay" / mount_rel / rel

    def clone_timeout(self, repo: dict[str, Any]) -> int:
        overlay_cfg = self.local_overlay_config(repo)
        return int(overlay_cfg.get("clone_timeout_seconds", repo.get("timeout_seconds", 1800)))

    def build_timeout(self, repo: dict[str, Any]) -> int:
        overlay_cfg = self.local_overlay_config(repo)
        return int(overlay_cfg.get("build_timeout_seconds", repo.get("timeout_seconds", 1800)))

    def prewarm_timeout(self, repo: dict[str, Any]) -> int:
        overlay_cfg = self.local_overlay_config(repo)
        return int(overlay_cfg.get("prewarm_timeout_seconds", repo.get("timeout_seconds", 1800)))

    def command_text(self, command: list[str] | str) -> str:
        if isinstance(command, str):
            return command
        return " ".join(command)

    def optional_float(self, value: Any) -> str:
        if value is None:
            return ""
        return f"{float(value):.6f}"

    def phase_ok(self, samples: list[dict[str, Any]], repo_id: str, storage: str, run_index: int, phase: str) -> bool:
        return any(
            sample.get("repo") == repo_id
            and sample.get("storage") == storage
            and sample.get("run") == run_index
            and sample.get("phase") == phase
            and sample.get("status") == "ok"
            for sample in samples
        )
