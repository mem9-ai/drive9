from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, ModuleXFail, write_json
from harness.module_base import BaseModule

# Phase A (own write) + startup must finish before the probe signals ready;
# phase B (remote mutation observation) must finish within the probe timeout.
READY_TIMEOUT_S = 90
PROBE_TIMEOUT_S = 300


class Drive9NodeWatch(BaseModule):
    """Measure fs.watch / fs.watchFile visibility of remote mutations on a mount.

    Watch-based Node tooling (vite dev, nodemon, agent file watchers) is only
    as good as the mount's change-notification behavior. The probe starts
    watchers inside a mounted namespace, mutates target.txt through the mount
    itself (phase A: local events), then mutates it again through the drive9
    CLI — i.e. server-side, bypassing the mount (phase B: cross-channel
    events) — and records which channel observes the change and how quickly.

    Policy: content must eventually be observable through the mount (hard
    correctness floor). Remote fs.watch events are the ideal outcome; without
    them, a bounded polling fallback (fs.watchFile / raw stat poll) is an
    expected-known-limitation (XFAIL) rather than a failure, but unbounded
    staleness fails.
    """

    description = "Probe fs.watch/fs.watchFile visibility of CLI-side (remote) file changes on a Drive9 FUSE mount."
    labels = ("drive9", "functional", "watch")
    timeout = 900

    def ensure_dependencies(self, ctx: Context) -> None:
        ctx.deps.ensure_node_version()

    def run(self, ctx: Context) -> dict[str, Any]:
        node_bin = ctx.deps.ensure_node_version()
        probe_js = Path(__file__).resolve().parent / "watch_probe.js"
        artifact = ctx.artifact_dir(self.id)
        result_path = artifact / "watch-result.json"
        ready_path = artifact / "watch-ready"
        probe_log = artifact / "watch-probe.log"

        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        profile = os.environ.get("FUSE_PROFILE") or "none"
        handle = ctx.target.mount("drive9_node_watch", remote, profile=profile, extra=["--allow-other"])
        try:
            env = ctx.deps.node_env(node_bin)
            for key, value in ctx.target.base_env().items():
                env.setdefault(key, value)
            ready_path.unlink(missing_ok=True)
            result_path.unlink(missing_ok=True)
            with probe_log.open("wb") as log:
                proc = subprocess.Popen(
                    [node_bin, str(probe_js), str(handle.mountpoint), str(result_path), str(ready_path)],
                    cwd=str(artifact),
                    env=env,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
            try:
                self._wait_ready(ready_path, proc, probe_log)
                mutation = self._remote_mutation(ctx, handle, artifact)
                outcome = self._wait_probe(proc, result_path, probe_log)
            finally:
                if proc.poll() is None:
                    ctx.target.kill_process_group(proc)

            report = self._build_report(outcome, mutation)
            write_json(ctx.result_dir / "node_watch.json", report)
            for name, value in report["metrics"].items():
                ctx.metric(f"drive9.node_watch.{name}", float(value), "ms")
            self._apply_policy(report)
            return report
        finally:
            ctx.target.unmount(handle)

    def _wait_ready(self, ready_path: Path, proc: subprocess.Popen[bytes], probe_log: Path) -> None:
        deadline = time.monotonic() + READY_TIMEOUT_S
        while time.monotonic() < deadline:
            if ready_path.exists():
                return
            if proc.poll() is not None:
                raise BlackboxError(f"watch probe exited before ready (code {proc.returncode}); see {probe_log}")
            time.sleep(0.2)
        raise BlackboxError(f"watch probe did not signal readiness within {READY_TIMEOUT_S}s; see {probe_log}")

    def _remote_mutation(self, ctx: Context, handle: Any, artifact: Path) -> dict[str, Any]:
        """Mutate trigger.stamp + target.txt through the CLI (server-side)."""
        stamp = artifact / "trigger.stamp"
        stamp.write_text(f"{int(time.time() * 1000)}\n", encoding="utf-8")
        payload = artifact / "target-v2.txt"
        payload.write_text(f"v2-remote-{int(time.time() * 1000)}\n", encoding="utf-8")
        remote_root = handle.remote_root
        trigger_result = ctx.target.drive9("node-watch-trigger-cp", ["fs", "cp", str(stamp), f":{remote_root}/trigger.stamp"])
        if not trigger_result.ok:
            raise BlackboxError(f"failed to write trigger.stamp via CLI; see {trigger_result.stderr}")
        # Overwriting in place keeps the inode stable for the file watchers; if
        # the CLI refuses to overwrite, fall back to remove+create and record
        # the weaker mutation style.
        overwrite = ctx.target.drive9(
            "node-watch-target-cp",
            ["fs", "cp", str(payload), f":{remote_root}/target.txt"],
            ok_codes=(0,),
        )
        mutation_style = "overwrite"
        if not overwrite.ok:
            ctx.target.drive9("node-watch-target-rm", ["fs", "rm", f":{remote_root}/target.txt"])
            ctx.target.drive9("node-watch-target-cp-2", ["fs", "cp", str(payload), f":{remote_root}/target.txt"])
            mutation_style = "remove+create"
        return {"mutation_style": mutation_style, "trigger_epoch_ms": stamp.read_text(encoding="utf-8").strip()}

    def _wait_probe(self, proc: subprocess.Popen[bytes], result_path: Path, probe_log: Path) -> dict[str, Any]:
        try:
            code = proc.wait(timeout=PROBE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            ctx_err = f"; see {probe_log}"
            raise BlackboxError(f"watch probe exceeded {PROBE_TIMEOUT_S}s{ctx_err}") from None
        if not result_path.exists():
            raise BlackboxError(f"watch probe (code {code}) wrote no result; see {probe_log}")
        with result_path.open(encoding="utf-8") as handle:
            return json.load(handle)

    def _build_report(self, outcome: dict[str, Any], mutation: dict[str, Any]) -> dict[str, Any]:
        phase_a = outcome.get("phase_a", {})
        phase_b = outcome.get("phase_b", {})

        def latency_ms(channel_epoch_ms: Any) -> float | None:
            if channel_epoch_ms is None or phase_b.get("trigger_seen_epoch_ms") is None:
                return None
            delta = float(channel_epoch_ms) - float(phase_b["trigger_seen_epoch_ms"])
            return delta if delta >= 0 else None

        metrics = {
            "remote_file_watch_ms": latency_ms(phase_b.get("file_watch_epoch_ms")),
            "remote_dir_watch_ms": latency_ms(phase_b.get("dir_watch_epoch_ms")),
            "remote_watchfile_poll_ms": latency_ms(phase_b.get("watchfile_epoch_ms")),
            "remote_statpoll_ms": latency_ms(phase_b.get("statpoll_epoch_ms")),
        }
        remote_event_ms = metrics["remote_file_watch_ms"] if metrics["remote_file_watch_ms"] is not None else metrics["remote_dir_watch_ms"]
        poll_ms = metrics["remote_watchfile_poll_ms"] if metrics["remote_watchfile_poll_ms"] is not None else metrics["remote_statpoll_ms"]
        report: dict[str, Any] = {
            "schema": "drive9-blackbox-node-watch/v1",
            "mutation": mutation,
            "watcher_errors": outcome.get("watcher_errors", {}),
            "phase_a_local_events": phase_a,
            "phase_b_remote": phase_b,
            "metrics": {k: (v if v is not None else -1.0) for k, v in metrics.items()},
            "remote_event_observed": remote_event_ms is not None,
            "remote_event_ms": remote_event_ms,
            "poll_fallback_ms": poll_ms,
            "content_verified": bool(phase_b.get("content_verified")),
            "probe_exit_observed_change": bool(phase_b.get("content_verified")),
        }
        return report

    def _apply_policy(self, report: dict[str, Any]) -> None:
        if not report["content_verified"]:
            raise BlackboxError(
                "remote CLI mutation never became visible through the mount "
                "(neither watch events nor stat polling observed it within the probe window)"
            )
        if report["remote_event_observed"]:
            return
        local_events = bool(report["phase_a_local_events"].get("file_event") or report["phase_a_local_events"].get("dir_event"))
        reasons = ["fs.watch saw no event for the remote CLI mutation"]
        if not local_events:
            reasons.append("fs.watch also saw no event for same-mount writes (platform watcher backend may be absent on this mount)")
        if report["poll_fallback_ms"] is not None:
            reasons.append(f"polling fallback observed the change in {report['poll_fallback_ms']:.0f}ms")
            raise ModuleXFail("; ".join(reasons), classification="no remote fs.watch events; bounded polling fallback")
        raise BlackboxError("remote mutation visible only via untracked means; neither watch events nor polling latencies recorded")
