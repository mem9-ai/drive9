#!/usr/bin/env python3
"""Verify SQLite automatic WAL recycle resets generation before the first frame."""

import json
import os
from pathlib import Path
import re
import subprocess
import time


TRACE_FIELD = re.compile(r"([a-z_]+)=(\"(?:[^\"\\\\]|\\\\.)*\"|\S+)")


def write_result(path, *, product, harness, fixture, checks, cleanup, error=""):
    result = {
        "schema_version": 1,
        "product_outcome": product,
        "harness_outcome": harness,
        "fixture_prefix": fixture,
        "checks": checks,
        "cleanup": {"state": cleanup},
    }
    if error:
        result["error"] = error
    Path(path).write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")


def redact_tail(data, credential):
    text = data.decode("utf-8", errors="replace")[-1000:]
    text = text.replace(credential, "REDACTED")
    return re.sub(r"drive9_[A-Za-z0-9_-]+", "REDACTED", text)


def trace_events(work_root):
    fields = (
        "event", "path", "base_rev", "base_size", "pre_size", "new_size",
        "offset", "written", "snapshot_size", "tail_size", "revision", "size",
        "result", "dirty_seq",
    )
    events = []
    for log_path in sorted(work_root.rglob("mount-*.log")):
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            marker = "append-log trace "
            if marker not in line:
                continue
            values = {
                key: value.strip('"')
                for key, value in TRACE_FIELD.findall(line.split(marker, 1)[1])
            }
            if values.get("event"):
                events.append({key: values[key] for key in fields if key in values})
    return events


def generation_reset_sequences(events):
    sequences = []
    for header_index, header in enumerate(events):
        if header.get("event") != "non_tail_write":
            continue
        if header.get("offset") != "0" or header.get("written") != "32":
            continue
        if int(header.get("pre_size", "0")) <= 32:
            continue
        reset_index = next(
            (
                index for index in range(header_index + 1, len(events))
                if events[index].get("event") == "generation_reset_attempt"
                and events[index].get("snapshot_size") == "32"
            ),
            None,
        )
        if reset_index is None:
            continue
        result_index = next(
            (
                index for index in range(reset_index + 1, len(events))
                if events[index].get("event") == "generation_reset_result"
                and events[index].get("result") == "ok"
            ),
            None,
        )
        append_index = next(
            (
                index for index in range((result_index or reset_index) + 1, len(events))
                if events[index].get("event") == "append_attempt"
                and events[index].get("base_size") == "32"
            ),
            None,
        )
        if result_index is None or append_index is None:
            continue
        sequences.append({
            "header_write": header,
            "generation_reset": events[reset_index],
            "generation_reset_result": events[result_index],
            "first_frame_append": events[append_index],
        })
    return sequences


def workload_metrics(work_root):
    metrics = {}
    expected_paths = sorted(work_root.rglob("expected.json"))
    if expected_paths:
        expected = json.loads(expected_paths[0].read_text(encoding="utf-8"))
        for key in (
            "wal_size",
            "cumulative_wal_size",
            "early_commit_p95_ms",
            "late_commit_p95_ms",
            "commit_ms",
        ):
            if key in expected:
                metrics[key] = expected[key]
    perf_paths = sorted(work_root.rglob("perf-1/perf.jsonl"))
    if perf_paths:
        lines = [line for line in perf_paths[0].read_text(encoding="utf-8").splitlines() if line]
        if lines:
            sample = json.loads(lines[-1])
            write = sample.get("remote_ops", {}).get("write", {})
            counters = sample.get("counters", {})
            metrics["generation_reset_count"] = int(counters.get("append_log_generation_reset_count", 0))
            metrics["generation_reset_bytes"] = int(counters.get("append_log_generation_reset_bytes", 0))
            metrics["generation_reset_shadow_ready"] = int(counters.get("append_log_generation_reset_shadow_ready", 0))
            metrics["generation_reset_shadow_degraded"] = int(counters.get("append_log_generation_reset_shadow_degraded", 0))
            metrics["reset_request_p95_ms"] = int(write.get("p95_ns", 0)) / 1_000_000
            metrics["reset_request_max_ms"] = int(write.get("max_ns", 0)) / 1_000_000
    return metrics


def main():
    request = json.loads(Path(os.environ["DRIVE9_E2E_REQUEST"]).read_text(encoding="utf-8"))
    result_path = os.environ["DRIVE9_E2E_RESULT"]
    fixture = request.get("fixture_prefix", "")
    target = request.get("target_url", "").rstrip("/")
    credential = request.get("credential", "")
    case_root = Path(__file__).resolve().parent
    drive9 = Path("/usr/local/bin/drive9")
    workload = case_root / "bundle" / "fuse-s3-express-append-log.sh"
    if not fixture or not target or not credential or not drive9.is_file() or not workload.is_file():
        write_result(
            result_path,
            product="unknown",
            harness="error",
            fixture=fixture,
            checks=[{"name": "microvm_harness", "status": "error", "detail": "case bundle is incomplete"}],
            cleanup="complete",
            error="missing target configuration or prepared bundle files",
        )
        return

    work_root = Path(os.environ.get("TMPDIR", "/scratch/tmp")) / "issue-875-auto-reset-header-first"
    work_root.mkdir(parents=True, exist_ok=True)
    wrapper_home = work_root / "wrapper-home"
    wrapper_home.mkdir(parents=True, exist_ok=True)
    remote_root = f"/{fixture}-issue875-auto-reset-header-first"
    stage_path = work_root / "stage"
    env = os.environ.copy()
    env.update({
        "DRIVE9_BASE": target,
        "DRIVE9_API_KEY": credential,
        "DRIVE9_E2E_S3_EXPRESS_ENABLED": "1",
        "DRIVE9_CLI_BIN": str(drive9),
        "HOME": str(wrapper_home),
        "XDG_CONFIG_HOME": str(wrapper_home / "xdg"),
        "FUSE_MOUNT_ROOT": str(work_root),
        "FUSE_APPEND_LOG_REMOTE_ROOT": remote_root,
        "FUSE_APPEND_LOG_KEEP_ARTIFACTS": "1",
        "FUSE_APPEND_LOG_STAGE_FILE": str(stage_path),
        "FUSE_APPEND_LOG_DEBUG": "1",
        "FUSE_APPEND_LOG_TRACE_ONLY": "1",
        "FUSE_APPEND_LOG_RECORD_COMMIT_TIMES": "1",
        "FUSE_APPEND_LOG_TRANSACTIONS": "12",
        "FUSE_APPEND_LOG_WAL_AUTOCHECKPOINT": "5",
        "FUSE_APPEND_LOG_JOURNAL_SIZE_LIMIT": "-1",
        "FUSE_APPEND_LOG_FORCE_TRUNCATE_AFTER": "0",
    })
    outcome = "fail"
    harness = "ok"
    checks = []
    error = ""
    try:
        process = subprocess.run(
            [str(workload)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=600,
        )
        events = trace_events(work_root)
        sequences = generation_reset_sequences(events)
        metrics = workload_metrics(work_root)
        shadow_ready = metrics.get("generation_reset_shadow_ready", 0)
        shadow_degraded = metrics.get("generation_reset_shadow_degraded", 0)
        shadow_ready_ok = shadow_ready >= len(sequences) and shadow_degraded == 0
        detail = json.dumps({
            "generation_reset_sequences": sequences,
            "metrics": metrics,
        }, separators=(",", ":"))
        if process.returncode == 0 and sequences and shadow_ready_ok:
            outcome = "pass"
            checks.append({"name": "sqlite_auto_reset_generation_reset", "status": "pass", "detail": detail})
        elif process.returncode == 0:
            checks.append({
                "name": "sqlite_auto_reset_generation_reset",
                "status": "fail",
                "detail": "automatic reset did not perform 32-byte generation reset, fresh local shadow, and first-frame tail append; " + detail,
            })
        else:
            stage = stage_path.read_text(encoding="utf-8").strip() if stage_path.is_file() else "unknown"
            checks.append({
                "name": "sqlite_auto_reset_generation_reset",
                "status": "fail",
                "detail": f"stage={stage}; {detail}; {redact_tail(process.stdout, credential)}",
            })
    except subprocess.TimeoutExpired:
        checks.append({"name": "sqlite_auto_reset_generation_reset", "status": "fail", "detail": "workload exceeded 600 seconds"})
    except Exception as exc:
        outcome = "unknown"
        harness = "error"
        error = str(exc)
        checks.append({"name": "microvm_harness", "status": "error", "detail": "dynamic wrapper failed"})

    stage = stage_path.read_text(encoding="utf-8").strip() if stage_path.is_file() else ""
    cleanup = "complete"
    if stage not in {"", "preflight", "capability", "remote-root-create"}:
        cleanup_success = False
        for _ in range(3):
            cleanup_process = subprocess.run(
                [str(drive9), "fs", "rm", "-r", ":" + remote_root],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if cleanup_process.returncode == 0:
                cleanup_success = True
                break
            time.sleep(1)
        if not cleanup_success:
            cleanup = "incomplete"
    if cleanup == "complete":
        checks.append({"name": "fixture_cleanup", "status": "pass"})
    elif outcome != "pass":
        checks.append({"name": "fixture_cleanup", "status": "fail", "detail": "fixture cleanup did not complete"})
    write_result(result_path, product=outcome, harness=harness, fixture=fixture, checks=checks, cleanup=cleanup, error=error)


if __name__ == "__main__":
    main()
