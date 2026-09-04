#!/usr/bin/env python3
"""Trace SQLite WAL append/rewrite decisions through a supervised FUSE mount."""

import json
import os
from pathlib import Path
import re
import subprocess
import time


TRACE_FIELD = re.compile(r"([a-z_]+)=(\"(?:[^\"\\]|\\.)*\"|\S+)")


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


def trace_summary(work_root):
    events = []
    for log_path in sorted(work_root.rglob("mount-*.log")):
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            marker = "append-log trace "
            if marker not in line:
                continue
            values = {key: value.strip('"') for key, value in TRACE_FIELD.findall(line.split(marker, 1)[1])}
            event = values.get("event")
            if not event:
                continue
            events.append({
                key: values[key]
                for key in (
                    "event",
                    "base_rev",
                    "base_size",
                    "pre_size",
                    "new_size",
                    "offset",
                    "written",
                    "snapshot_size",
                    "tail_size",
                    "revision",
                    "size",
                    "result",
                    "dirty_seq",
                )
                if key in values
            })
    decisions = [event for event in events if event["event"] in {"append_attempt", "rewrite_attempt"}]
    rewrite_followups = []
    for index, event in enumerate(decisions):
        if event["event"] != "rewrite_attempt":
            continue
        next_event = decisions[index + 1] if index + 1 < len(decisions) else None
        rewrite_followups.append({"rewrite": event, "next_decision": next_event})
    return {
        "events": events,
        "append_attempts": sum(event["event"] == "append_attempt" for event in decisions),
        "rewrite_attempts": sum(event["event"] == "rewrite_attempt" for event in decisions),
        "rewrite_followups": rewrite_followups,
    }


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

    work_root = Path(os.environ.get("TMPDIR", "/scratch/tmp")) / "issue-875-append-log-trace"
    work_root.mkdir(parents=True, exist_ok=True)
    wrapper_home = work_root / "wrapper-home"
    wrapper_home.mkdir(parents=True, exist_ok=True)
    remote_root = f"/{fixture}-issue875-trace"
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
        "FUSE_APPEND_LOG_TRANSACTIONS": "50",
        "FUSE_APPEND_LOG_WAL_AUTOCHECKPOINT": "5",
        "FUSE_APPEND_LOG_JOURNAL_SIZE_LIMIT": "0",
        "FUSE_APPEND_LOG_FORCE_TRUNCATE_AFTER": "25",
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
        trace = trace_summary(work_root)
        detail = json.dumps(trace, separators=(",", ":"))
        if process.returncode == 0:
            outcome = "pass"
            checks.append({"name": "sqlite_wal_recycle_trace", "status": "pass", "detail": detail})
        else:
            stage = stage_path.read_text(encoding="utf-8").strip() if stage_path.is_file() else "unknown"
            checks.append({
                "name": "sqlite_wal_recycle_trace",
                "status": "fail",
                "detail": f"stage={stage}; trace={detail}; {redact_tail(process.stdout, credential)}",
            })
    except subprocess.TimeoutExpired:
        checks.append({"name": "sqlite_wal_recycle_trace", "status": "fail", "detail": "workload exceeded 600 seconds"})
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
