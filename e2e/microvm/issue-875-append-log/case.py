#!/usr/bin/env python3
"""Dynamic MicroVM wrapper for the canonical issue #875 FUSE workload."""

import json
import os
from pathlib import Path
import re
import subprocess
import time


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


def main():
    request_path = os.environ["DRIVE9_E2E_REQUEST"]
    result_path = os.environ["DRIVE9_E2E_RESULT"]
    request = json.loads(Path(request_path).read_text(encoding="utf-8"))
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

    work_root = Path(os.environ.get("TMPDIR", "/scratch/tmp")) / "issue-875-append-log"
    work_root.mkdir(parents=True, exist_ok=True)
    wrapper_home = work_root / "wrapper-home"
    wrapper_home.mkdir(parents=True, exist_ok=True)
    remote_root = f"/{fixture}-issue875"
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
    })
    started = time.monotonic()
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
        if process.returncode == 0:
            outcome = "pass"
            checks.append({"name": "issue_875_fuse_wal_workload", "status": "pass"})
            checks.append({
                "name": "issue_876_acknowledged_rows",
                "status": "pass",
                "detail": "same-mount and fresh-remount row count, fingerprint, and integrity verification passed",
            })
        else:
            stage = stage_path.read_text(encoding="utf-8").strip() if stage_path.is_file() else "unknown"
            detail = redact_tail(process.stdout, credential)
            checks.append({"name": "issue_875_fuse_wal_workload", "status": "fail", "detail": f"stage={stage}; {detail}"})
            if stage == "perf-validation":
                checks.append({
                    "name": "issue_876_acknowledged_rows",
                    "status": "pass",
                    "detail": "same-mount and fresh-remount row count, fingerprint, and integrity verification passed before the #875 perf gate",
                })
            elif stage in {"same-mount-reopen", "fresh-remount", "fresh-remount-verify"}:
                checks.append({
                    "name": "issue_876_acknowledged_rows",
                    "status": "fail",
                    "detail": f"SQLite acknowledged-row verification failed at stage={stage}",
                })
    except subprocess.TimeoutExpired:
        checks.append({"name": "issue_875_fuse_wal_workload", "status": "fail", "detail": "workload exceeded 600 seconds"})
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
                [str(drive9), "fs", "rm", "-r", remote_root],
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
