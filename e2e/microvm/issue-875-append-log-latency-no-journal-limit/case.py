#!/usr/bin/env python3
"""Contrast first and second SQLite checkpoint WAL read paths."""

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import statistics
import subprocess
import time


TRACE_FIELD = re.compile(r"([a-z_]+)=(\"(?:[^\"\\]|\\.)*\"|\S+)")
STRACE_LINE = re.compile(r"^(?P<timestamp>\d+\.\d+)\s+(?P<syscall>[a-zA-Z0-9_]+)\(.*<(?P<duration>\d+\.\d+)>$")
STRACE_FD_PATH = re.compile(r"<(?P<path>/[^>]+)>")
STRACE_PREAD = re.compile(r"^pread64\([^,]+,.*?,\s*(?P<length>\d+),\s*(?P<offset>\d+)\)")
FUSE_WAL_READ = re.compile(
    r"^(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+"
    r"dat9 debug: read path=(?P<path>\S+-wal) fh=(?P<fh>\d+) "
    r"ino=(?P<ino>\d+) off=(?P<offset>\d+) req=(?P<requested>\d+) "
    r"got=(?P<read>-?\d+) source=(?P<source>\S+) status=(?P<status>\d+) "
    r"dur=(?P<duration>\S+)$"
)
CHECKPOINT_COMMITS = {989, 990, 1981, 1982}


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


def percentile(values, fraction):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def summarize_commits(commits, *, wal_size, cumulative_wal_size, busy_timeout_ms):
    ranked = sorted(enumerate(commits, start=1), key=lambda item: item[1], reverse=True)
    return {
        "commit_count": len(commits),
        "wal_size": wal_size,
        "cumulative_wal_size": cumulative_wal_size,
        "sqlite_busy_timeout_ms": busy_timeout_ms,
        "commit_p50_ms": statistics.median(commits),
        "commit_p95_ms": percentile(commits, 0.95),
        "commit_p99_ms": percentile(commits, 0.99),
        "commit_max_ms": max(commits),
        "slowest_commits_ms": [
            {"commit": index, "ms": value}
            for index, value in ranked[:10]
        ],
    }


def run_local_sqlite_control(work_root):
    database = work_root / "local-control.db"
    conn = sqlite3.connect(database, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA journal_size_limit=-1")
    busy_timeout_ms = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    conn.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
    commits = []
    cumulative_wal_size = 0
    for index in range(1, 2001):
        started = time.perf_counter()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO entries(id, value) VALUES (?, ?)", (index, f"value-{index:04d}"))
        conn.execute("COMMIT")
        commits.append((time.perf_counter() - started) * 1000)
        cumulative_wal_size += os.path.getsize(str(database) + "-wal")
    rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
    check = conn.execute("PRAGMA integrity_check").fetchone()[0]
    wal_size = os.path.getsize(str(database) + "-wal")
    conn.close()
    if len(rows) != 2000 or check != "ok":
        raise RuntimeError(f"local SQLite validation count={len(rows)} integrity={check}")
    metrics = summarize_commits(
        commits,
        wal_size=wal_size,
        cumulative_wal_size=cumulative_wal_size,
        busy_timeout_ms=busy_timeout_ms,
    )
    metrics["fingerprint"] = hashlib.sha256(repr(rows).encode()).hexdigest()
    return metrics


def trace_summary(work_root):
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
                events.append(values)
    resets = [event for event in events if event.get("event") == "generation_reset_attempt"]
    first_frame_appends = [
        event for event in events
        if event.get("event") == "append_attempt" and event.get("base_size") == "32"
    ]
    return {
        "events": events,
        "generation_reset_attempts": len(resets),
        "generation_reset_snapshot_sizes": [event.get("snapshot_size") for event in resets],
        "first_frame_append_attempts": len(first_frame_appends),
    }


def trace_event_duration_ms(event):
    return int(event.get("duration_ns", 0)) / 1_000_000


def trace_event_lock_wait_ms(event):
    return int(event.get("lock_wait_ns", 0)) / 1_000_000


def trace_event_remote_commit_lock_wait_ms(event):
    return int(event.get("remote_commit_lock_wait_ns", 0)) / 1_000_000


def event_commit(event, timeline):
    timestamp = int(event.get("wall_unix_nano", 0))
    if timestamp <= 0:
        return None
    tolerance_ns = 5_000_000
    for commit in timeline:
        if commit["start_unix_nano"] - tolerance_ns <= timestamp <= commit["end_unix_nano"] + tolerance_ns:
            return commit["commit"]
    return None


def checkpoint_window_commit(timestamp_ns, timeline):
    if timestamp_ns <= 0:
        return None
    # Go's standard logger records seconds, while the workload records
    # nanoseconds. A timestamp at either edge of a long checkpoint belongs to
    # that transaction; the windows do not overlap in this single-writer case.
    tolerance_ns = 1_000_000_000
    for commit in timeline:
        if commit["commit"] not in CHECKPOINT_COMMITS:
            continue
        if commit["start_unix_nano"] - tolerance_ns <= timestamp_ns <= commit["end_unix_nano"] + tolerance_ns:
            return commit["commit"]
    return None


def checkpoint_correlation(timeline, events):
    event_types = {"append_attempt", "append_result", "generation_reset_attempt", "generation_reset_result", "rewrite_attempt", "rewrite_result", "main_db_read", "main_db_write", "main_db_fsync", "wal_fsync", "wal_shm_read"}
    by_commit = {}
    reset_events = []
    for event in events:
        if event.get("event") not in event_types:
            continue
        commit = event_commit(event, timeline)
        if commit is None:
            continue
        observation = {
            "event": event["event"],
            "duration_ms": trace_event_duration_ms(event),
            "lock_wait_ms": trace_event_lock_wait_ms(event),
            "remote_commit_lock_wait_ms": trace_event_remote_commit_lock_wait_ms(event),
        }
        by_commit.setdefault(commit, []).append(observation)
        if event.get("event") == "generation_reset_result":
            reset_events.append({"commit": commit, **observation})
    timeline_by_commit = {item["commit"]: item for item in timeline}
    slowest = sorted(timeline, key=lambda item: item["duration_ms"], reverse=True)[:20]
    slowest_observations = []
    for commit in slowest:
        slowest_observations.append({
            "commit": commit["commit"],
            "commit_ms": commit["duration_ms"],
            "wal_size_before": commit["wal_size_before"],
            "wal_size_after": commit["wal_size_after"],
            "wal_size_decreased": commit["wal_size_after"] < commit["wal_size_before"],
            "fuse_events": by_commit.get(commit["commit"], []),
        })
    return {
        "reset_events": reset_events,
        "slowest_commits": slowest_observations,
        "timeline_commit_count": len(timeline_by_commit),
    }


def strace_correlation(work_root, timeline):
    observations = []
    checkpoint_syscalls = []
    for log_path in sorted(work_root.glob("sqlite-strace.*")):
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = STRACE_LINE.match(line)
            if not match:
                continue
            duration_ms = float(match.group("duration")) * 1000
            event = {
                "wall_unix_nano": str(int(float(match.group("timestamp")) * 1_000_000_000)),
            }
            commit = event_commit(event, timeline)
            path_match = STRACE_FD_PATH.search(line)
            fd_path = path_match.group("path") if path_match else ""
            checkpoint_wal_syscall = (
                commit in CHECKPOINT_COMMITS
                and fd_path.endswith("append-log.db-wal")
                and match.group("syscall") in {
                    "pread64", "read", "mmap", "mmap2", "munmap", "msync", "madvise",
                }
            )
            if duration_ms < 1 and not checkpoint_wal_syscall:
                continue
            observations.append({
                "commit": commit,
                "syscall": match.group("syscall"),
                "duration_ms": duration_ms,
                "fd_path": fd_path,
            })
            if checkpoint_wal_syscall:
                pread = STRACE_PREAD.match(line.split(" ", 1)[1])
                checkpoint_syscalls.append({
                    "commit": commit,
                    "syscall": match.group("syscall"),
                    "duration_ms": duration_ms,
                    "fd_path": fd_path,
                    "pread_length": int(pread.group("length")) if pread else None,
                    "pread_offset": int(pread.group("offset")) if pread else None,
                    "raw": line,
                })
    slowest = sorted(observations, key=lambda item: item["duration_ms"], reverse=True)[:20]
    return {
        "slowest_syscalls": slowest,
        "checkpoint_syscalls": checkpoint_syscalls,
    }


def fuse_wal_read_correlation(work_root, timeline):
    reads = []
    for log_path in sorted(work_root.rglob("mount-*.log")):
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = FUSE_WAL_READ.match(line)
            if not match:
                continue
            timestamp_ns = int(
                time.mktime(time.strptime(match.group("timestamp"), "%Y/%m/%d %H:%M:%S"))
                * 1_000_000_000
            )
            commit = checkpoint_window_commit(timestamp_ns, timeline)
            if commit is None:
                continue
            reads.append({
                "commit": commit,
                "offset": int(match.group("offset")),
                "requested": int(match.group("requested")),
                "read": int(match.group("read")),
                "source": match.group("source"),
                "status": int(match.group("status")),
                "duration": match.group("duration"),
            })
    return reads


def checkpoint_shadow_rotation_errors(metrics, reads):
    errors = []
    reset_count = metrics.get("generation_reset_count", 0)
    if reset_count != 2:
        errors.append(f"generation_reset_count={reset_count}, want 2")
    shadow_ready = metrics.get("generation_reset_shadow_ready", 0)
    if shadow_ready != reset_count:
        errors.append(f"generation_reset_shadow_ready={shadow_ready}, want {reset_count}")
    shadow_degraded = metrics.get("generation_reset_shadow_degraded", 0)
    if shadow_degraded != 0:
        errors.append(f"generation_reset_shadow_degraded={shadow_degraded}, want 0")
    second_checkpoint_reads = [item for item in reads if item["commit"] == 1981]
    if not second_checkpoint_reads:
        errors.append("no WAL reads correlated to second checkpoint commit 1981")
    elif any(item["source"] != "shadow-spill" for item in second_checkpoint_reads):
        sources = sorted({item["source"] for item in second_checkpoint_reads})
        errors.append(f"second checkpoint WAL read sources={sources}, want shadow-spill only")
    return errors


def workload_metrics(work_root):
    expected_paths = sorted(work_root.rglob("expected.json"))
    if not expected_paths:
        return {}, []
    expected = json.loads(expected_paths[0].read_text(encoding="utf-8"))
    commits = expected.get("commit_ms", [])
    timeline = expected.get("commit_timeline", [])
    metrics = {
        "commit_count": len(commits),
        "wal_size": expected.get("wal_size"),
        "cumulative_wal_size": expected.get("cumulative_wal_size"),
        "early_commit_p95_ms": expected.get("early_commit_p95_ms"),
        "late_commit_p95_ms": expected.get("late_commit_p95_ms"),
        "sqlite_busy_timeout_ms": expected.get("sqlite_busy_timeout_ms"),
    }
    if commits:
        metrics.update({
            "commit_p50_ms": statistics.median(commits),
            "commit_p95_ms": percentile(commits, 0.95),
            "commit_p99_ms": percentile(commits, 0.99),
            "commit_max_ms": max(commits),
        })
    perf_paths = sorted(work_root.rglob("perf-1/perf.jsonl"))
    if perf_paths:
        lines = [line for line in perf_paths[0].read_text(encoding="utf-8").splitlines() if line]
        if lines:
            sample = json.loads(lines[-1])
            write = sample.get("remote_ops", {}).get("write", {})
            counters = sample.get("counters", {})
            metrics.update({
                "generation_reset_count": int(counters.get("append_log_generation_reset_count", 0)),
                "generation_reset_bytes": int(counters.get("append_log_generation_reset_bytes", 0)),
                "generation_reset_shadow_ready": int(counters.get("append_log_generation_reset_shadow_ready", 0)),
                "generation_reset_shadow_degraded": int(counters.get("append_log_generation_reset_shadow_degraded", 0)),
                "ordinary_full_rewrite_count": int(counters.get("append_log_full_rewrite_count", 0)) - int(counters.get("append_log_generation_reset_count", 0)),
                "conditional_put_p95_histogram_upper_ms": int(write.get("p95_ns", 0)) / 1_000_000,
                "conditional_put_max_ms": int(write.get("max_ns", 0)) / 1_000_000,
            })
    return metrics, timeline


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

    work_root = Path(os.environ.get("TMPDIR", "/scratch/tmp")) / "issue-875-append-log-checkpoint-read-source-contrast"
    work_root.mkdir(parents=True, exist_ok=True)
    wrapper_home = work_root / "wrapper-home"
    wrapper_home.mkdir(parents=True, exist_ok=True)
    remote_root = f"/{fixture}-issue875-checkpoint-read-source-contrast"
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
        "FUSE_APPEND_LOG_TRANSACTIONS": "2000",
        "FUSE_APPEND_LOG_WAL_AUTOCHECKPOINT": "1000",
        "FUSE_APPEND_LOG_JOURNAL_SIZE_LIMIT": "-1",
        "FUSE_APPEND_LOG_FORCE_TRUNCATE_AFTER": "0",
        "FUSE_APPEND_LOG_SQLITE_BUSY_TIMEOUT_MS": "0",
        "FUSE_APPEND_LOG_STRACE_PREFIX": str(work_root / "sqlite-strace"),
    })
    outcome = "fail"
    harness = "ok"
    checks = []
    error = ""
    try:
        local_control = run_local_sqlite_control(work_root)
        process = subprocess.run(
            [str(workload)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=600,
        )
        trace = trace_summary(work_root)
        metrics, timeline = workload_metrics(work_root)
        fuse_wal_reads = fuse_wal_read_correlation(work_root, timeline)
        shadow_errors = checkpoint_shadow_rotation_errors(metrics, fuse_wal_reads)
        detail = json.dumps({
            "local_sqlite_control": local_control,
            "fuse_busy_timeout_zero": {
                "trace": {key: value for key, value in trace.items() if key != "events"},
                "metrics": metrics,
                "checkpoint_correlation": checkpoint_correlation(timeline, trace["events"]),
                "strace_correlation": strace_correlation(work_root, timeline),
                "fuse_wal_reads": fuse_wal_reads,
                "checkpoint_shadow_rotation_errors": shadow_errors,
            },
        }, separators=(",", ":"))
        if process.returncode == 0 and not shadow_errors:
            outcome = "pass"
            checks.append({"name": "issue_875_checkpoint_read_source_contrast", "status": "pass", "detail": detail})
        elif process.returncode == 0:
            checks.append({
                "name": "issue_875_checkpoint_read_source_contrast",
                "status": "fail",
                "detail": "checkpoint shadow rotation assertion failed: " + "; ".join(shadow_errors) + "; " + detail,
            })
        else:
            stage = stage_path.read_text(encoding="utf-8").strip() if stage_path.is_file() else "unknown"
            checks.append({
                "name": "issue_875_checkpoint_read_source_contrast",
                "status": "fail",
                "detail": f"stage={stage}; {detail}; {redact_tail(process.stdout, credential)}",
            })
    except subprocess.TimeoutExpired:
        checks.append({"name": "issue_875_checkpoint_read_source_contrast", "status": "fail", "detail": "workload exceeded 600 seconds"})
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
