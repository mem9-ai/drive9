#!/usr/bin/env python3
"""Measure a growing SQLite WAL checkpoint against concurrent reads/writes.

The Drive9 target is paired with a same-Pod gVisor emptyDir control.  Each role
uses an independent process and SQLite connection.  Raw monotonic events are
written under /results while compact summaries are emitted to stdout.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import random
import shutil
import sqlite3
import statistics
import sys
import time
import traceback
from pathlib import Path
from typing import Any


RUN_ID = os.environ.get("RUN_ID", "sqlite-bigckpt-2fd0c88-dc8b8e52")
DRIVE9_DB = os.environ.get(
    "DRIVE9_DB",
    "/workspace/tmp/sqlite-bigckpt-2fd0c88-dc8b8e52/main.db",
)
LOCAL_DB = "/local/control.db"
RESULTS_ROOT = Path("/results")
SEED_ROWS = 64
PAYLOAD_BYTES = 1024 * 1024
PRE_CHECKPOINT_WRITES = 48
OVERLAP_PROBE_WRITES = 8
POST_CHECKPOINT_WRITES = 24
TOTAL_WRITES = (
    PRE_CHECKPOINT_WRITES + OVERLAP_PROBE_WRITES + POST_CHECKPOINT_WRITES
)
MIN_WAL_BEFORE_CHECKPOINT = 40 * 1024 * 1024
ROLE_READY_TIMEOUT_SECONDS = 60
ROLE_RUN_TIMEOUT_SECONDS = 900
SQLITE_BUSY_TIMEOUT_MS = 300_000
READER_PAUSE_SECONDS = 0.005
MONITOR_PAUSE_SECONDS = 0.1


def emit(marker: str, payload: dict[str, Any]) -> None:
    print(marker + " " + json.dumps(payload, sort_keys=True), flush=True)


def now_pair() -> tuple[int, int]:
    return time.monotonic_ns(), time.time_ns()


def error_detail(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "repr": repr(exc),
        "sqlite_errorcode": getattr(exc, "sqlite_errorcode", None),
        "sqlite_errorname": getattr(exc, "sqlite_errorname", None),
    }


def append_event(handle: Any, event: str, **fields: Any) -> None:
    record = {"event": event}
    record.update(fields)
    handle.write(json.dumps(record, sort_keys=True) + "\n")
    handle.flush()


def event_path(label: str, role: str) -> Path:
    return RESULTS_ROOT / f"{label}-{role}.jsonl"


def connect(path: str, *, query_only: bool = False) -> sqlite3.Connection:
    connection = sqlite3.connect(
        path,
        timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        isolation_level=None,
    )
    connection.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA cache_size=-4096")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    if query_only:
        connection.execute("PRAGMA query_only=ON")
    return connection


def file_state(path: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, candidate in (
        ("main", path),
        ("wal", path + "-wal"),
        ("shm", path + "-shm"),
        ("journal", path + "-journal"),
    ):
        try:
            stat = os.stat(candidate)
            result[label] = {
                "exists": True,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "inode": stat.st_ino,
            }
        except FileNotFoundError:
            result[label] = {"exists": False}
    return result


def assert_wal_settings(connection: sqlite3.Connection) -> dict[str, Any]:
    settings = {
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "synchronous": connection.execute("PRAGMA synchronous").fetchone()[0],
        "wal_autocheckpoint": connection.execute(
            "PRAGMA wal_autocheckpoint"
        ).fetchone()[0],
        "mmap_size": connection.execute("PRAGMA mmap_size").fetchone()[0],
        "page_size": connection.execute("PRAGMA page_size").fetchone()[0],
    }
    expected = {
        "journal_mode": "wal",
        "synchronous": 2,
        "wal_autocheckpoint": 0,
        "mmap_size": 0,
    }
    for key, value in expected.items():
        if settings[key] != value:
            raise AssertionError(f"{key}={settings[key]!r}, want {value!r}")
    return settings


def make_payload(sequence: int, row_id: int) -> tuple[bytes, str]:
    seed = (sequence << 32) ^ row_id ^ 0xD91A9C0FFEE
    generator = random.Random(seed)
    header = sequence.to_bytes(8, "big") + row_id.to_bytes(8, "big")
    payload = header + generator.randbytes(PAYLOAD_BYTES - len(header))
    return payload, hashlib.sha256(payload).hexdigest()


def reset_target(path: str) -> None:
    resolved = os.path.abspath(path)
    allowed_drive9 = f"/workspace/tmp/{RUN_ID}/main.db"
    if resolved not in (LOCAL_DB, allowed_drive9):
        raise RuntimeError(f"refusing to reset unexpected target {resolved}")
    parent = os.path.dirname(resolved)
    if resolved == LOCAL_DB:
        os.makedirs(parent, exist_ok=True)
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.unlink(resolved + suffix)
            except FileNotFoundError:
                pass
    else:
        shutil.rmtree(parent, ignore_errors=True)
        os.makedirs(parent, exist_ok=False)


def setup_target(label: str, path: str) -> tuple[sqlite3.Connection, dict[str, Any]]:
    reset_target(path)
    connection = connect(path)
    journal_mode = connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
    if journal_mode.lower() != "delete":
        raise AssertionError(f"setup journal_mode={journal_mode}")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA locking_mode=NORMAL")

    zero_digest = hashlib.sha256(bytes(PAYLOAD_BYTES)).hexdigest()
    started_mono, started_unix = now_pair()
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE blocks("
        "id INTEGER PRIMARY KEY, "
        "version INTEGER NOT NULL, "
        "digest TEXT NOT NULL, "
        "payload BLOB NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE commits("
        "seq INTEGER PRIMARY KEY, "
        "row_id INTEGER NOT NULL UNIQUE, "
        "digest TEXT NOT NULL)"
    )
    for row_id in range(1, SEED_ROWS + 1):
        connection.execute(
            "INSERT INTO blocks(id, version, digest, payload) "
            "VALUES (?, 0, ?, zeroblob(?))",
            (row_id, zero_digest, PAYLOAD_BYTES),
        )
    commit_started = time.monotonic_ns()
    connection.execute("COMMIT")
    commit_ended = time.monotonic_ns()
    ended_mono, ended_unix = now_pair()

    state_after_seed = file_state(path)
    main_size = state_after_seed["main"].get("size", 0)
    if main_size < 60 * 1024 * 1024:
        raise AssertionError(f"seed main.db only {main_size} bytes")

    wal_mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if wal_mode.lower() != "wal":
        raise AssertionError(f"cannot enable WAL: {wal_mode}")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    settings = assert_wal_settings(connection)
    setup = {
        "label": label,
        "path": path,
        "seed_rows": SEED_ROWS,
        "payload_bytes": PAYLOAD_BYTES,
        "started_mono_ns": started_mono,
        "ended_mono_ns": ended_mono,
        "started_unix_ns": started_unix,
        "ended_unix_ns": ended_unix,
        "elapsed_ms": (ended_mono - started_mono) / 1_000_000,
        "commit_ms": (commit_ended - commit_started) / 1_000_000,
        "state_after_seed": state_after_seed,
        "wal_settings": settings,
    }
    emit("SETUP_COMPLETE", setup)
    return connection, setup


def writer_role(
    label: str,
    path: str,
    ready: Any,
    start: Any,
    checkpoint_trigger: Any,
    checkpoint_started: Any,
    checkpoint_done: Any,
) -> None:
    log_path = event_path(label, "writer")
    connection: sqlite3.Connection | None = None
    try:
        with log_path.open("w", buffering=1) as log:
            connection = connect(path)
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA wal_autocheckpoint=0")
            settings = assert_wal_settings(connection)
            append_event(log, "role_ready", role="writer", settings=settings)
            ready.set()
            if not start.wait(ROLE_READY_TIMEOUT_SECONDS):
                raise TimeoutError("writer start event timeout")

            for sequence in range(1, TOTAL_WRITES + 1):
                row_id = SEED_ROWS + sequence
                payload_started = time.monotonic_ns()
                payload, digest = make_payload(sequence, row_id)
                payload_ended = time.monotonic_ns()

                tx_started, tx_unix_started = now_pair()
                begin_started = time.monotonic_ns()
                connection.execute("BEGIN IMMEDIATE")
                begin_ended = time.monotonic_ns()
                mutate_started = time.monotonic_ns()
                connection.execute(
                    "INSERT INTO blocks(id, version, digest, payload) "
                    "VALUES (?, ?, ?, ?)",
                    (row_id, sequence, digest, sqlite3.Binary(payload)),
                )
                connection.execute(
                    "INSERT INTO commits(seq, row_id, digest) VALUES (?, ?, ?)",
                    (sequence, row_id, digest),
                )
                mutate_ended = time.monotonic_ns()
                commit_started = time.monotonic_ns()
                connection.execute("COMMIT")
                commit_ended = time.monotonic_ns()
                tx_ended, tx_unix_ended = now_pair()
                state = file_state(path)
                append_event(
                    log,
                    "write",
                    sequence=sequence,
                    row_id=row_id,
                    payload_started_ns=payload_started,
                    payload_ended_ns=payload_ended,
                    tx_started_ns=tx_started,
                    tx_ended_ns=tx_ended,
                    tx_unix_started_ns=tx_unix_started,
                    tx_unix_ended_ns=tx_unix_ended,
                    begin_started_ns=begin_started,
                    begin_ended_ns=begin_ended,
                    mutate_started_ns=mutate_started,
                    mutate_ended_ns=mutate_ended,
                    commit_started_ns=commit_started,
                    commit_ended_ns=commit_ended,
                    payload_ms=(payload_ended - payload_started) / 1_000_000,
                    begin_ms=(begin_ended - begin_started) / 1_000_000,
                    mutate_ms=(mutate_ended - mutate_started) / 1_000_000,
                    commit_ms=(commit_ended - commit_started) / 1_000_000,
                    tx_ms=(tx_ended - tx_started) / 1_000_000,
                    state=state,
                )
                if sequence % 8 == 0:
                    emit(
                        "WRITE_PROGRESS",
                        {
                            "label": label,
                            "sequence": sequence,
                            "total": TOTAL_WRITES,
                            "last_tx_ms": (tx_ended - tx_started) / 1_000_000,
                            "wal_bytes": state["wal"].get("size", 0),
                        },
                    )

                if sequence == PRE_CHECKPOINT_WRITES:
                    wal_size = state["wal"].get("size", 0)
                    if wal_size < MIN_WAL_BEFORE_CHECKPOINT:
                        raise AssertionError(
                            f"WAL only {wal_size} bytes after {sequence} writes"
                        )
                    append_event(
                        log,
                        "checkpoint_trigger",
                        sequence=sequence,
                        mono_ns=tx_ended,
                        unix_ns=tx_unix_ended,
                        wal_bytes=wal_size,
                    )
                    checkpoint_trigger.set()
                    if not checkpoint_started.wait(30):
                        raise TimeoutError("checkpoint did not start")
                    time.sleep(0.02)

                if sequence == PRE_CHECKPOINT_WRITES + OVERLAP_PROBE_WRITES:
                    if not checkpoint_done.wait(ROLE_RUN_TIMEOUT_SECONDS):
                        raise TimeoutError("checkpoint did not finish before post phase")

            append_event(log, "writer_complete", writes=TOTAL_WRITES)
    except BaseException as exc:
        try:
            with log_path.open("a", buffering=1) as log:
                append_event(log, "role_error", role="writer", **error_detail(exc))
        finally:
            traceback.print_exc()
        raise
    finally:
        if connection is not None:
            connection.close()


def reader_role(
    label: str,
    path: str,
    ready: Any,
    start: Any,
    stop: Any,
) -> None:
    log_path = event_path(label, "reader")
    connection: sqlite3.Connection | None = None
    try:
        with log_path.open("w", buffering=1) as log:
            connection = connect(path, query_only=True)
            settings = assert_wal_settings(connection)
            append_event(log, "role_ready", role="reader", settings=settings)
            ready.set()
            if not start.wait(ROLE_READY_TIMEOUT_SECONDS):
                raise TimeoutError("reader start event timeout")
            observation = 0
            while not stop.is_set():
                observation += 1
                started, unix_started = now_pair()
                cursor = connection.execute(
                    "SELECT id, version, digest, payload "
                    "FROM blocks ORDER BY id DESC LIMIT 1"
                )
                row = cursor.fetchone()
                cursor.close()
                ended, unix_ended = now_pair()
                if row is None:
                    raise AssertionError("reader returned no row")
                row_id, version, expected_digest, payload = row
                verify_started = time.monotonic_ns()
                actual_digest = hashlib.sha256(payload).hexdigest()
                verify_ended = time.monotonic_ns()
                if actual_digest != expected_digest:
                    raise AssertionError(
                        f"digest mismatch row={row_id} version={version}"
                    )
                append_event(
                    log,
                    "read",
                    observation=observation,
                    row_id=row_id,
                    version=version,
                    bytes=len(payload),
                    started_ns=started,
                    ended_ns=ended,
                    unix_started_ns=unix_started,
                    unix_ended_ns=unix_ended,
                    read_ms=(ended - started) / 1_000_000,
                    verify_ms=(verify_ended - verify_started) / 1_000_000,
                )
                time.sleep(READER_PAUSE_SECONDS)
            append_event(log, "reader_complete", observations=observation)
    except BaseException as exc:
        try:
            with log_path.open("a", buffering=1) as log:
                append_event(log, "role_error", role="reader", **error_detail(exc))
        finally:
            traceback.print_exc()
        raise
    finally:
        if connection is not None:
            connection.close()


def checkpoint_role(
    label: str,
    path: str,
    ready: Any,
    checkpoint_trigger: Any,
    checkpoint_started: Any,
    checkpoint_done: Any,
) -> None:
    log_path = event_path(label, "checkpoint")
    connection: sqlite3.Connection | None = None
    try:
        with log_path.open("w", buffering=1) as log:
            connection = connect(path)
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA wal_autocheckpoint=0")
            settings = assert_wal_settings(connection)
            append_event(log, "role_ready", role="checkpoint", settings=settings)
            ready.set()
            if not checkpoint_trigger.wait(ROLE_RUN_TIMEOUT_SECONDS):
                raise TimeoutError("checkpoint trigger timeout")
            before = file_state(path)
            started, unix_started = now_pair()
            append_event(
                log,
                "checkpoint_start",
                started_ns=started,
                unix_started_ns=unix_started,
                before=before,
            )
            emit(
                "CHECKPOINT_START",
                {
                    "label": label,
                    "unix_ns": unix_started,
                    "wal_bytes": before["wal"].get("size", 0),
                    "main_bytes": before["main"].get("size", 0),
                },
            )
            checkpoint_started.set()
            result = connection.execute(
                "PRAGMA main.wal_checkpoint(TRUNCATE)"
            ).fetchone()
            ended, unix_ended = now_pair()
            after = file_state(path)
            append_event(
                log,
                "checkpoint",
                started_ns=started,
                ended_ns=ended,
                unix_started_ns=unix_started,
                unix_ended_ns=unix_ended,
                duration_ms=(ended - started) / 1_000_000,
                result=list(result) if result is not None else None,
                before=before,
                after=after,
            )
            emit(
                "CHECKPOINT_END",
                {
                    "label": label,
                    "unix_ns": unix_ended,
                    "duration_ms": (ended - started) / 1_000_000,
                    "result": list(result) if result is not None else None,
                    "state": after,
                },
            )
            if result is None or result[0] != 0:
                raise AssertionError(f"TRUNCATE checkpoint result={result}")
    except BaseException as exc:
        try:
            with log_path.open("a", buffering=1) as log:
                append_event(
                    log, "role_error", role="checkpoint", **error_detail(exc)
                )
        finally:
            traceback.print_exc()
        raise
    finally:
        checkpoint_done.set()
        if connection is not None:
            connection.close()


def monitor_role(
    label: str,
    path: str,
    ready: Any,
    start: Any,
    stop: Any,
) -> None:
    log_path = event_path(label, "monitor")
    try:
        with log_path.open("w", buffering=1) as log:
            append_event(log, "role_ready", role="monitor")
            ready.set()
            if not start.wait(ROLE_READY_TIMEOUT_SECONDS):
                raise TimeoutError("monitor start event timeout")
            sample = 0
            while not stop.is_set():
                mono_ns, unix_ns = now_pair()
                append_event(
                    log,
                    "file_sample",
                    sample=sample,
                    mono_ns=mono_ns,
                    unix_ns=unix_ns,
                    state=file_state(path),
                )
                sample += 1
                time.sleep(MONITOR_PAUSE_SECONDS)
            mono_ns, unix_ns = now_pair()
            append_event(
                log,
                "file_sample",
                sample=sample,
                mono_ns=mono_ns,
                unix_ns=unix_ns,
                state=file_state(path),
            )
    except BaseException as exc:
        try:
            with log_path.open("a", buffering=1) as log:
                append_event(log, "role_error", role="monitor", **error_detail(exc))
        finally:
            traceback.print_exc()
        raise


def read_events(label: str, role: str) -> list[dict[str, Any]]:
    records = []
    with event_path(label, role).open() as handle:
        for line in handle:
            records.append(json.loads(line))
    return records


def percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        raise ValueError("empty values")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def latency_summary(values: list[float]) -> dict[str, Any] | None:
    if not values:
        return None
    ordered = sorted(values)
    result: dict[str, Any] = {
        "count": len(ordered),
        "min_ms": ordered[0],
        "mean_ms": statistics.fmean(ordered),
        "p50_ms": percentile(ordered, 0.50),
        "max_ms": ordered[-1],
    }
    if len(ordered) >= 20:
        result["p95_ms"] = percentile(ordered, 0.95)
        result["p99_ms"] = percentile(ordered, 0.99)
    return result


def interval_phase(
    started_ns: int,
    ended_ns: int,
    checkpoint_started_ns: int,
    checkpoint_ended_ns: int,
) -> str:
    if ended_ns <= checkpoint_started_ns:
        return "before"
    if started_ns >= checkpoint_ended_ns:
        return "after"
    return "overlap"


def summarize_events(label: str) -> dict[str, Any]:
    writer_records = [
        item for item in read_events(label, "writer") if item["event"] == "write"
    ]
    reader_records = [
        item for item in read_events(label, "reader") if item["event"] == "read"
    ]
    checkpoint_records = [
        item
        for item in read_events(label, "checkpoint")
        if item["event"] == "checkpoint"
    ]
    monitor_records = [
        item
        for item in read_events(label, "monitor")
        if item["event"] == "file_sample"
    ]
    if len(checkpoint_records) != 1:
        raise AssertionError(f"{label}: checkpoint records={len(checkpoint_records)}")
    checkpoint = checkpoint_records[0]
    checkpoint_started_ns = checkpoint["started_ns"]
    checkpoint_ended_ns = checkpoint["ended_ns"]

    writes_by_phase: dict[str, list[dict[str, Any]]] = {
        "before": [],
        "overlap": [],
        "after": [],
    }
    for item in writer_records:
        phase = interval_phase(
            item["tx_started_ns"],
            item["tx_ended_ns"],
            checkpoint_started_ns,
            checkpoint_ended_ns,
        )
        writes_by_phase[phase].append(item)

    reads_by_phase: dict[str, list[dict[str, Any]]] = {
        "before": [],
        "overlap": [],
        "after": [],
    }
    for item in reader_records:
        phase = interval_phase(
            item["started_ns"],
            item["ended_ns"],
            checkpoint_started_ns,
            checkpoint_ended_ns,
        )
        reads_by_phase[phase].append(item)

    write_metrics: dict[str, Any] = {}
    for phase, records in writes_by_phase.items():
        write_metrics[phase] = {
            metric: latency_summary([item[metric] for item in records])
            for metric in ("begin_ms", "mutate_ms", "commit_ms", "tx_ms")
        }

    read_metrics = {
        phase: latency_summary([item["read_ms"] for item in records])
        for phase, records in reads_by_phase.items()
    }
    reader_ends = sorted(item["ended_ns"] for item in reader_records)
    max_reader_completion_gap_ms = (
        max(
            (right - left) / 1_000_000
            for left, right in zip(reader_ends, reader_ends[1:])
        )
        if len(reader_ends) >= 2
        else None
    )
    last_before = max(
        (value for value in reader_ends if value <= checkpoint_started_ns),
        default=None,
    )
    first_after = min(
        (value for value in reader_ends if value >= checkpoint_ended_ns),
        default=None,
    )
    checkpoint_reader_completion_gap_ms = (
        (first_after - last_before) / 1_000_000
        if last_before is not None and first_after is not None
        else None
    )

    overlap_writes = [
        {
            "sequence": item["sequence"],
            "begin_ms": item["begin_ms"],
            "mutate_ms": item["mutate_ms"],
            "commit_ms": item["commit_ms"],
            "tx_ms": item["tx_ms"],
            "started_offset_ms": (
                item["tx_started_ns"] - checkpoint_started_ns
            )
            / 1_000_000,
            "ended_offset_ms": (item["tx_ended_ns"] - checkpoint_started_ns)
            / 1_000_000,
        }
        for item in writes_by_phase["overlap"]
    ]
    slowest_overlap_reads = sorted(
        reads_by_phase["overlap"], key=lambda item: item["read_ms"], reverse=True
    )[:20]
    overlap_read_samples = [
        {
            "observation": item["observation"],
            "row_id": item["row_id"],
            "read_ms": item["read_ms"],
            "started_offset_ms": (item["started_ns"] - checkpoint_started_ns)
            / 1_000_000,
            "ended_offset_ms": (item["ended_ns"] - checkpoint_started_ns)
            / 1_000_000,
        }
        for item in slowest_overlap_reads
    ]

    samples_during = [
        item
        for item in monitor_records
        if checkpoint_started_ns <= item["mono_ns"] <= checkpoint_ended_ns
    ]
    monitor_summary = {
        "samples": len(monitor_records),
        "samples_during_checkpoint": len(samples_during),
        "max_wal_bytes_during_checkpoint": max(
            (
                item["state"]["wal"].get("size", 0)
                for item in samples_during
            ),
            default=None,
        ),
        "main_sizes_during_checkpoint": sorted(
            {
                item["state"]["main"].get("size", 0)
                for item in samples_during
            }
        ),
    }

    return {
        "checkpoint": checkpoint,
        "writer_events": len(writer_records),
        "reader_events": len(reader_records),
        "writes_by_phase": {
            phase: len(records) for phase, records in writes_by_phase.items()
        },
        "reads_by_phase": {
            phase: len(records) for phase, records in reads_by_phase.items()
        },
        "write_latency": write_metrics,
        "read_latency": read_metrics,
        "overlap_writes": overlap_writes,
        "slowest_overlap_reads": overlap_read_samples,
        "max_reader_completion_gap_ms": max_reader_completion_gap_ms,
        "checkpoint_reader_completion_gap_ms": checkpoint_reader_completion_gap_ms,
        "monitor": monitor_summary,
        "writer_equal_windows": {
            "before_last_24_tx_ms": latency_summary(
                [item["tx_ms"] for item in writes_by_phase["before"][-24:]]
            ),
            "after_first_24_tx_ms": latency_summary(
                [item["tx_ms"] for item in writes_by_phase["after"][:24]]
            ),
        },
        "reader_equal_windows": {
            "before_last_100_read_ms": latency_summary(
                [item["read_ms"] for item in reads_by_phase["before"][-100:]]
            ),
            "after_first_100_read_ms": latency_summary(
                [item["read_ms"] for item in reads_by_phase["after"][:100]]
            ),
        },
    }


def verify_database(connection: sqlite3.Connection) -> dict[str, Any]:
    row_count = connection.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
    commit_count = connection.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    max_id = connection.execute("SELECT MAX(id) FROM blocks").fetchone()[0]
    mismatch_count = connection.execute(
        "SELECT COUNT(*) FROM commits AS c "
        "JOIN blocks AS b ON b.id=c.row_id "
        "WHERE b.version != c.seq OR b.digest != c.digest"
    ).fetchone()[0]
    digest_mismatches = 0
    payload_bytes = 0
    cursor = connection.execute("SELECT digest, payload FROM blocks ORDER BY id")
    for expected_digest, payload in cursor:
        payload_bytes += len(payload)
        if hashlib.sha256(payload).hexdigest() != expected_digest:
            digest_mismatches += 1
    cursor.close()
    result = {
        "row_count": row_count,
        "expected_row_count": SEED_ROWS + TOTAL_WRITES,
        "commit_count": commit_count,
        "expected_commit_count": TOTAL_WRITES,
        "max_id": max_id,
        "version_digest_join_mismatches": mismatch_count,
        "payload_digest_mismatches": digest_mismatches,
        "payload_bytes_checked": payload_bytes,
        "integrity_check": connection.execute("PRAGMA integrity_check").fetchone()[0],
    }
    result["healthy"] = (
        result["row_count"] == result["expected_row_count"]
        and result["commit_count"] == result["expected_commit_count"]
        and result["max_id"] == result["expected_row_count"]
        and mismatch_count == 0
        and digest_mismatches == 0
        and result["integrity_check"] == "ok"
    )
    return result


def run_target(label: str, path: str) -> dict[str, Any]:
    emit("TARGET_START", {"label": label, "path": path})
    keeper, setup = setup_target(label, path)
    context = mp.get_context("spawn")
    start = context.Event()
    checkpoint_trigger = context.Event()
    checkpoint_started = context.Event()
    checkpoint_done = context.Event()
    stop = context.Event()
    ready = {role: context.Event() for role in ("writer", "reader", "checkpoint", "monitor")}
    processes = {
        "writer": context.Process(
            target=writer_role,
            name=f"{label}-writer",
            args=(
                label,
                path,
                ready["writer"],
                start,
                checkpoint_trigger,
                checkpoint_started,
                checkpoint_done,
            ),
        ),
        "reader": context.Process(
            target=reader_role,
            name=f"{label}-reader",
            args=(label, path, ready["reader"], start, stop),
        ),
        "checkpoint": context.Process(
            target=checkpoint_role,
            name=f"{label}-checkpoint",
            args=(
                label,
                path,
                ready["checkpoint"],
                checkpoint_trigger,
                checkpoint_started,
                checkpoint_done,
            ),
        ),
        "monitor": context.Process(
            target=monitor_role,
            name=f"{label}-monitor",
            args=(label, path, ready["monitor"], start, stop),
        ),
    }
    try:
        for process in processes.values():
            process.start()
        for role, event in ready.items():
            if not event.wait(ROLE_READY_TIMEOUT_SECONDS):
                raise TimeoutError(f"{label}: {role} did not become ready")
        measurement_started, measurement_unix_started = now_pair()
        start.set()

        deadline = time.monotonic() + ROLE_RUN_TIMEOUT_SECONDS
        for role in ("writer", "checkpoint"):
            remaining = max(0.0, deadline - time.monotonic())
            processes[role].join(remaining)
            if processes[role].is_alive():
                raise TimeoutError(f"{label}: {role} exceeded run timeout")
            if processes[role].exitcode != 0:
                raise RuntimeError(
                    f"{label}: {role} exit code {processes[role].exitcode}"
                )

        stop.set()
        for role in ("reader", "monitor"):
            processes[role].join(30)
            if processes[role].is_alive():
                raise TimeoutError(f"{label}: {role} did not stop")
            if processes[role].exitcode != 0:
                raise RuntimeError(
                    f"{label}: {role} exit code {processes[role].exitcode}"
                )
        measurement_ended, measurement_unix_ended = now_pair()

        event_summary = summarize_events(label)
        checkpoint = event_summary["checkpoint"]
        if event_summary["writer_events"] != TOTAL_WRITES:
            raise AssertionError(
                f"{label}: writer events={event_summary['writer_events']}"
            )
        if event_summary["reads_by_phase"]["before"] == 0:
            raise AssertionError(f"{label}: no reader observation before checkpoint")
        if event_summary["reads_by_phase"]["after"] == 0:
            raise AssertionError(f"{label}: no reader observation after checkpoint")
        if label == "drive9" and event_summary["reads_by_phase"]["overlap"] == 0:
            raise AssertionError("drive9: no reader observation overlapped checkpoint")
        if label == "drive9" and event_summary["writes_by_phase"]["overlap"] == 0:
            raise AssertionError("drive9: no writer transaction overlapped checkpoint")
        if event_summary["writes_by_phase"]["after"] < POST_CHECKPOINT_WRITES:
            raise AssertionError(
                f"{label}: post-checkpoint writes="
                f"{event_summary['writes_by_phase']['after']}, "
                f"want at least {POST_CHECKPOINT_WRITES}"
            )
        if checkpoint["before"]["wal"].get("size", 0) < MIN_WAL_BEFORE_CHECKPOINT:
            raise AssertionError("checkpoint WAL did not reach required size")

        final_checkpoint_started, final_checkpoint_unix_started = now_pair()
        final_checkpoint_result = keeper.execute(
            "PRAGMA main.wal_checkpoint(TRUNCATE)"
        ).fetchone()
        final_checkpoint_ended, final_checkpoint_unix_ended = now_pair()
        if final_checkpoint_result is None or final_checkpoint_result[0] != 0:
            raise AssertionError(
                f"{label}: final checkpoint result={final_checkpoint_result}"
            )
        verification = verify_database(keeper)
        if not verification["healthy"]:
            raise AssertionError(f"{label}: final verification={verification}")
        state_before_close = file_state(path)
        close_started, close_unix_started = now_pair()
        keeper.close()
        close_ended, close_unix_ended = now_pair()
        keeper = None
        state_after_close = file_state(path)

        summary = {
            "label": label,
            "path": path,
            "setup": setup,
            "measurement": {
                "started_mono_ns": measurement_started,
                "ended_mono_ns": measurement_ended,
                "started_unix_ns": measurement_unix_started,
                "ended_unix_ns": measurement_unix_ended,
                "elapsed_ms": (measurement_ended - measurement_started) / 1_000_000,
            },
            "events": event_summary,
            "final_checkpoint": {
                "started_mono_ns": final_checkpoint_started,
                "ended_mono_ns": final_checkpoint_ended,
                "started_unix_ns": final_checkpoint_unix_started,
                "ended_unix_ns": final_checkpoint_unix_ended,
                "duration_ms": (
                    final_checkpoint_ended - final_checkpoint_started
                )
                / 1_000_000,
                "result": list(final_checkpoint_result),
            },
            "verification_before_close": verification,
            "state_before_close": state_before_close,
            "close": {
                "started_mono_ns": close_started,
                "ended_mono_ns": close_ended,
                "started_unix_ns": close_unix_started,
                "ended_unix_ns": close_unix_ended,
                "duration_ms": (close_ended - close_started) / 1_000_000,
            },
            "state_after_close": state_after_close,
        }
        output_path = RESULTS_ROOT / f"{label}-summary.json"
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        emit("TARGET_SUMMARY", summary)
        return summary
    finally:
        stop.set()
        for process in processes.values():
            if process.is_alive():
                process.terminate()
                process.join(10)
        if keeper is not None:
            keeper.close()


def compact_comparison(
    control: dict[str, Any], drive9: dict[str, Any]
) -> dict[str, Any]:
    def values(summary: dict[str, Any]) -> dict[str, Any]:
        events = summary["events"]
        return {
            "measured_checkpoint_ms": events["checkpoint"]["duration_ms"],
            "checkpoint_main_before_bytes": events["checkpoint"]["before"]["main"].get(
                "size", 0
            ),
            "checkpoint_main_after_bytes": events["checkpoint"]["after"]["main"].get(
                "size", 0
            ),
            "checkpoint_wal_before_bytes": events["checkpoint"]["before"]["wal"].get(
                "size", 0
            ),
            "write_latency": events["write_latency"],
            "read_latency": events["read_latency"],
            "overlap_writes": events["overlap_writes"],
            "checkpoint_reader_completion_gap_ms": events[
                "checkpoint_reader_completion_gap_ms"
            ],
            "final_checkpoint_ms": summary["final_checkpoint"]["duration_ms"],
            "close_ms": summary["close"]["duration_ms"],
            "healthy": summary["verification_before_close"]["healthy"],
        }

    return {"emptydir": values(control), "drive9": values(drive9)}


def main() -> int:
    if not DRIVE9_DB.startswith(f"/workspace/tmp/{RUN_ID}/"):
        raise RuntimeError(f"unsafe DRIVE9_DB={DRIVE9_DB}")
    if not (1 <= SEED_ROWS <= 128 and 1 <= TOTAL_WRITES <= 128):
        raise RuntimeError("workload is outside hard safety bounds")
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    emit(
        "HARNESS_CONFIG",
        {
            "run_id": RUN_ID,
            "python": sys.version,
            "sqlite": sqlite3.sqlite_version,
            "platform": dict(
                zip(
                    ("sysname", "nodename", "release", "version", "machine"),
                    os.uname(),
                )
            ),
            "seed_rows": SEED_ROWS,
            "payload_bytes": PAYLOAD_BYTES,
            "pre_checkpoint_writes": PRE_CHECKPOINT_WRITES,
            "overlap_probe_writes": OVERLAP_PROBE_WRITES,
            "post_checkpoint_writes": POST_CHECKPOINT_WRITES,
            "total_writes": TOTAL_WRITES,
            "minimum_wal_before_checkpoint": MIN_WAL_BEFORE_CHECKPOINT,
            "checkpoint_mode": "TRUNCATE",
            "wal_autocheckpoint": 0,
            "drive9_db": DRIVE9_DB,
            "local_db": LOCAL_DB,
        },
    )
    control = run_target("emptydir", LOCAL_DB)
    drive9 = run_target("drive9", DRIVE9_DB)
    comparison = compact_comparison(control, drive9)
    (RESULTS_ROOT / "comparison.json").write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n"
    )
    emit("FINAL_COMPARISON", comparison)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
