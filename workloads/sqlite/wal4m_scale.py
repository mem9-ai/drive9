#!/usr/bin/env python3
"""Build and measure fixed-size SQLite WAL checkpoints.

The measured checkpoint is deliberately ``wal_checkpoint(TRUNCATE)`` so the
results remain comparable with the earlier large-WAL reproduction.  It is a
strong checkpoint, not SQLite's default PASSIVE auto-checkpoint.

Only Python's standard library is used.  Every worker owns its SQLite
connection and all timestamps use the same monotonic clock.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import multiprocessing as mp
import os
import queue
import sqlite3
import statistics
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Iterable


PAGE_SIZE = 4096
WAL_HEADER_BYTES = 32
WAL_FRAME_HEADER_BYTES = 24
WAL_FRAME_BYTES = PAGE_SIZE + WAL_FRAME_HEADER_BYTES
WAL_TARGET_FRAMES = 1000
WAL_MAX_CACHE_FRAMES = (4 * 1024 * 1024 - WAL_HEADER_BYTES) // WAL_FRAME_BYTES
WAL_TARGET_BYTES = WAL_HEADER_BYTES + WAL_TARGET_FRAMES * WAL_FRAME_BYTES
WAL_MAX_CACHE_BYTES = WAL_HEADER_BYTES + WAL_MAX_CACHE_FRAMES * WAL_FRAME_BYTES
PAYLOAD_BYTES = 32 * 1024
PRE_MIXED_COMMITS = 64
FIXED_UPDATE_SLOTS = 16
POST_MIXED_WRITES = 40
TXLOG_SLOTS = 512
READER_INTERVAL_SECONDS = 0.05
WRITER_INTERVAL_SECONDS = 0.1
OVERLAP_WRITER_DELAY_SECONDS = 0.020
MONITOR_INTERVAL_SECONDS = 0.1
ZERO_PAYLOAD = bytes(PAYLOAD_BYTES)
ZERO_DIGEST = hashlib.sha256(ZERO_PAYLOAD).hexdigest()


class TransactionFailure(RuntimeError):
    def __init__(self, record: dict[str, Any], original: BaseException):
        super().__init__(str(original))
        self.record = record
        self.original = original


def json_dump(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def append_jsonl(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8", buffering=1) as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    result: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                result.append(json.loads(line))
    return result


def error_record(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "sqlite_errorcode": getattr(exc, "sqlite_errorcode", None),
        "sqlite_errorname": getattr(exc, "sqlite_errorname", None),
        "traceback": traceback.format_exc(),
    }


def file_state(db_path: str) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for label, suffix in (
        ("main", ""),
        ("wal", "-wal"),
        ("shm", "-shm"),
        ("journal", "-journal"),
    ):
        try:
            stat = os.stat(db_path + suffix)
            state[label] = {
                "exists": True,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "inode": stat.st_ino,
            }
        except FileNotFoundError:
            state[label] = {"exists": False}
    return state


def wal_frames(db_path: str) -> tuple[int, int]:
    """Derive frame count from a fresh WAL's stat size without opening it.

    Opening the WAL from a non-SQLite fd would perturb the Drive9 sidecar
    cache/Release path being measured.  The case separately proves a 4 KiB
    database page size and aborts unless it starts without an old WAL, so the
    fresh WAL byte length is sufficient here.
    """
    wal_path = db_path + "-wal"
    size = os.stat(wal_path).st_size
    if size < WAL_HEADER_BYTES:
        raise AssertionError(f"WAL is only {size} bytes")
    payload_bytes = size - WAL_HEADER_BYTES
    if payload_bytes % WAL_FRAME_BYTES:
        raise AssertionError(
            f"WAL size {size} is not header + whole {WAL_FRAME_BYTES}-byte frames"
        )
    return size, payload_bytes // WAL_FRAME_BYTES


def connect(
    db_path: str,
    busy_timeout_ms: int,
    *,
    query_only: bool = False,
    immutable: bool = False,
) -> sqlite3.Connection:
    if immutable:
        uri = "file:" + Path(db_path).absolute().as_posix() + "?mode=ro&immutable=1"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=busy_timeout_ms / 1000,
            isolation_level=None,
        )
    else:
        connection = sqlite3.connect(
            db_path,
            timeout=busy_timeout_ms / 1000,
            isolation_level=None,
        )
    connection.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA cache_size=-4096")
    if not immutable:
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
    if query_only:
        connection.execute("PRAGMA query_only=ON")
    return connection


def assert_runtime(connection: sqlite3.Connection) -> dict[str, Any]:
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
        "page_size": PAGE_SIZE,
    }
    for key, wanted in expected.items():
        if settings[key] != wanted:
            raise AssertionError(f"{key}={settings[key]!r}, want {wanted!r}")
    return settings


def payload_for(sequence: int, row_id: int, operation: str) -> tuple[bytes, str]:
    seed = hashlib.sha256(f"drive9:{sequence}:{row_id}:{operation}".encode()).digest()
    payload = (seed * math.ceil(PAYLOAD_BYTES / len(seed)))[:PAYLOAD_BYTES]
    return payload, hashlib.sha256(payload).hexdigest()


def update_row_for(update_index: int, seed_rows: int) -> int:
    candidates = seed_rows // 2
    if candidates < FIXED_UPDATE_SLOTS:
        raise AssertionError(
            f"base has {candidates} even rows, need {FIXED_UPDATE_SLOTS}"
        )
    # Every scale touches the same number of existing rows.  The slots are
    # spread proportionally through the seed extent, then repeated in the same
    # order.  This prevents the 2 GiB case from having more unique dirty rows
    # merely because it contains more candidate rows.
    slot = update_index % FIXED_UPDATE_SLOTS
    candidate_index = round(slot * (candidates - 1) / (FIXED_UPDATE_SLOTS - 1))
    return 2 * (candidate_index + 1)


def stable_hot_rows(seed_rows: int) -> list[int]:
    last_odd = seed_rows if seed_rows % 2 else seed_rows - 1
    middle_odd = ((seed_rows // 2) // 2) * 2 + 1
    return sorted({1, middle_odd, last_odd})


def timed_mixed_transaction(
    connection: sqlite3.Connection,
    *,
    sequence: int,
    operation: str,
    seed_rows: int,
    update_index: int,
    insert_index: int,
) -> dict[str, Any]:
    if operation == "U":
        row_id = update_row_for(update_index, seed_rows)
    elif operation == "I":
        row_id = seed_rows + insert_index + 1
    else:
        raise ValueError(f"unknown operation {operation!r}")
    payload, digest = payload_for(sequence, row_id, operation)
    started_ns = time.monotonic_ns()
    unix_started_ns = time.time_ns()
    begin_started_ns = time.monotonic_ns()
    try:
        connection.execute("BEGIN IMMEDIATE")
    except BaseException as exc:
        ended_ns = time.monotonic_ns()
        raise TransactionFailure(
            {
                "event": "mixed_failure",
                "sequence": sequence,
                "operation": operation,
                "row_id": row_id,
                "phase": "begin",
                "started_ns": started_ns,
                "ended_ns": ended_ns,
                "begin_wait_lower_bound_ms": (ended_ns - begin_started_ns)
                / 1_000_000,
                "error": error_record(exc),
            },
            exc,
        ) from exc
    begin_ended_ns = time.monotonic_ns()
    mutate_started_ns = time.monotonic_ns()
    try:
        if operation == "U":
            cursor = connection.execute(
                "UPDATE blocks SET version=?, digest=?, payload=? "
                "WHERE id=? AND kind='seed'",
                (sequence, digest, sqlite3.Binary(payload), row_id),
            )
            if cursor.rowcount != 1:
                raise AssertionError(f"seed row {row_id} was not updated")
        else:
            connection.execute(
                "INSERT INTO blocks(id, kind, version, digest, payload) "
                "VALUES (?, 'growth', ?, ?, ?)",
                (row_id, sequence, digest, sqlite3.Binary(payload)),
            )
        cursor = connection.execute(
            "UPDATE txlog SET applied=1, op=?, row_id=?, version=?, digest=? "
            "WHERE seq=? AND applied=0",
            (operation, row_id, sequence, digest, sequence),
        )
        if cursor.rowcount != 1:
            raise AssertionError(f"txlog slot {sequence} unavailable")
        connection.execute(
            "UPDATE state SET last_seq=?, last_row_id=?, last_op=? WHERE id=1",
            (sequence, row_id, operation),
        )
        mutate_ended_ns = time.monotonic_ns()
        commit_started_ns = time.monotonic_ns()
        connection.execute("COMMIT")
        commit_ended_ns = time.monotonic_ns()
    except BaseException as exc:
        failed_ns = time.monotonic_ns()
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise TransactionFailure(
            {
                "event": "mixed_failure",
                "sequence": sequence,
                "operation": operation,
                "row_id": row_id,
                "phase": "mutate_or_commit",
                "started_ns": started_ns,
                "ended_ns": failed_ns,
                "begin_ms": (begin_ended_ns - begin_started_ns) / 1_000_000,
                "elapsed_ms": (failed_ns - started_ns) / 1_000_000,
                "error": error_record(exc),
            },
            exc,
        ) from exc
    ended_ns = time.monotonic_ns()
    return {
        "event": "mixed_commit",
        "sequence": sequence,
        "operation": operation,
        "row_id": row_id,
        "digest": digest,
        "started_ns": started_ns,
        "ended_ns": ended_ns,
        "unix_started_ns": unix_started_ns,
        "unix_ended_ns": time.time_ns(),
        "begin_ms": (begin_ended_ns - begin_started_ns) / 1_000_000,
        "mutate_ms": (mutate_ended_ns - mutate_started_ns) / 1_000_000,
        "commit_ms": (commit_ended_ns - commit_started_ns) / 1_000_000,
        "tx_ms": (ended_ns - started_ns) / 1_000_000,
    }


def timed_filler_transaction(
    connection: sqlite3.Connection, filler_sequence: int
) -> dict[str, Any]:
    started_ns = time.monotonic_ns()
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "UPDATE state SET filler_seq=? WHERE id=1", (filler_sequence,)
    )
    commit_started_ns = time.monotonic_ns()
    connection.execute("COMMIT")
    ended_ns = time.monotonic_ns()
    return {
        "event": "filler_commit",
        "filler_sequence": filler_sequence,
        "started_ns": started_ns,
        "ended_ns": ended_ns,
        "commit_ms": (ended_ns - commit_started_ns) / 1_000_000,
        "tx_ms": (ended_ns - started_ns) / 1_000_000,
    }


def next_operation(sequence: int) -> str:
    return "U" if sequence % 2 else "I"


def count_operations(records: Iterable[dict[str, Any]]) -> tuple[int, int]:
    update_count = 0
    insert_count = 0
    for record in records:
        if record["operation"] == "U":
            update_count += 1
        elif record["operation"] == "I":
            insert_count += 1
    return update_count, insert_count


def reader_role(
    db_path: str,
    seed_rows: int,
    busy_timeout_ms: int,
    event_path: str,
    ready: Any,
    start: Any,
    stop: Any,
    status_queue: Any,
) -> None:
    connection: sqlite3.Connection | None = None
    output = Path(event_path)
    try:
        connection = connect(db_path, busy_timeout_ms, query_only=True)
        stable_rows = stable_hot_rows(seed_rows)
        if not stable_rows:
            raise AssertionError("no stable seed rows")
        # Warm the exact fixed reader set before signaling readiness.  These
        # reads are intentionally excluded from before/during/after latency.
        warm_rows = stable_rows + [
            int(connection.execute("SELECT last_row_id FROM state WHERE id=1").fetchone()[0])
        ]
        for row_id in warm_rows:
            cursor = connection.execute(
                "SELECT digest, payload FROM blocks WHERE id=?", (row_id,)
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None or hashlib.sha256(row[1]).hexdigest() != row[0]:
                raise AssertionError(f"reader prewarm failed for row {row_id}")
        ready.set()
        if not start.wait(30):
            raise TimeoutError("reader start timeout")
        iteration = 0
        while not stop.is_set():
            category = "stable_main" if iteration % 2 == 0 else "recent_commit"
            started_ns = time.monotonic_ns()
            unix_started_ns = time.time_ns()
            append_jsonl(
                output,
                {
                    "event": "read_start",
                    "iteration": iteration,
                    "category": category,
                    "started_ns": started_ns,
                    "unix_started_ns": unix_started_ns,
                },
            )
            if category == "stable_main":
                row_id = stable_rows[(iteration // 2) % len(stable_rows)]
            else:
                cursor = connection.execute(
                    "SELECT last_row_id FROM state WHERE id=1"
                )
                row_id = int(cursor.fetchone()[0])
                cursor.close()
            fetch_started_ns = time.monotonic_ns()
            cursor = connection.execute(
                "SELECT version, digest, payload FROM blocks WHERE id=?", (row_id,)
            )
            row = cursor.fetchone()
            cursor.close()
            fetch_ended_ns = time.monotonic_ns()
            if row is None:
                raise AssertionError(f"reader row {row_id} missing")
            version, digest, payload = row
            hash_started_ns = time.monotonic_ns()
            actual_digest = hashlib.sha256(payload).hexdigest()
            hash_ended_ns = time.monotonic_ns()
            if actual_digest != digest:
                raise AssertionError(
                    f"reader digest mismatch for row {row_id}: {actual_digest} != {digest}"
                )
            ended_ns = time.monotonic_ns()
            append_jsonl(
                output,
                {
                    "event": "read",
                    "iteration": iteration,
                    "category": category,
                    "row_id": row_id,
                    "version": version,
                    "started_ns": started_ns,
                    "ended_ns": ended_ns,
                    "unix_started_ns": unix_started_ns,
                    "unix_ended_ns": time.time_ns(),
                    "fetch_ms": (fetch_ended_ns - fetch_started_ns) / 1_000_000,
                    "hash_ms": (hash_ended_ns - hash_started_ns) / 1_000_000,
                    "total_ms": (ended_ns - started_ns) / 1_000_000,
                },
            )
            iteration += 1
            remaining = READER_INTERVAL_SECONDS - (time.monotonic_ns() - started_ns) / 1e9
            if remaining > 0:
                stop.wait(remaining)
        status_queue.put({"ok": True, "iterations": iteration})
    except BaseException as exc:
        status_queue.put({"ok": False, "error": error_record(exc)})
    finally:
        if connection is not None:
            connection.close()


def checkpoint_role(
    db_path: str,
    busy_timeout_ms: int,
    ready: Any,
    gate: Any,
    invoked: Any,
    start_queue: Any,
    pragma_started_value: Any,
    result_queue: Any,
) -> None:
    connection: sqlite3.Connection | None = None
    try:
        connection = connect(db_path, busy_timeout_ms)
        settings = assert_runtime(connection)
        ready.set()
        if not gate.wait(30):
            raise TimeoutError("checkpoint gate timeout")
        start_state = file_state(db_path)
        armed_ns = time.monotonic_ns()
        armed_unix_ns = time.time_ns()
        start_queue.put(
            {
                "armed_ns": armed_ns,
                "armed_unix_ns": armed_unix_ns,
                "start_state": start_state,
                "settings": settings,
            }
        )
        started_ns = time.monotonic_ns()
        unix_started_ns = time.time_ns()
        pragma_started_value.value = started_ns
        invoked.set()
        row = connection.execute("PRAGMA main.wal_checkpoint(TRUNCATE)").fetchone()
        ended_ns = time.monotonic_ns()
        result_queue.put(
            {
                "ok": True,
                "started_ns": started_ns,
                "ended_ns": ended_ns,
                "unix_started_ns": unix_started_ns,
                "unix_ended_ns": time.time_ns(),
                "duration_ms": (ended_ns - started_ns) / 1_000_000,
                "result": list(row),
                "end_state": file_state(db_path),
            }
        )
    except BaseException as exc:
        result_queue.put({"ok": False, "error": error_record(exc)})
    finally:
        if connection is not None:
            connection.close()


def overlap_writer_role(
    db_path: str,
    seed_rows: int,
    busy_timeout_ms: int,
    sequence: int,
    update_index: int,
    insert_index: int,
    ready: Any,
    invoked: Any,
    start_queue: Any,
    result_queue: Any,
) -> None:
    connection: sqlite3.Connection | None = None
    try:
        connection = connect(db_path, busy_timeout_ms)
        assert_runtime(connection)
        ready.set()
        if not invoked.wait(30):
            raise TimeoutError("checkpoint invocation timeout")
        time.sleep(OVERLAP_WRITER_DELAY_SECONDS)
        attempt_started_ns = time.monotonic_ns()
        start_queue.put(
            {
                "attempt_started_ns": attempt_started_ns,
                "attempt_unix_ns": time.time_ns(),
            }
        )
        try:
            record = timed_mixed_transaction(
                connection,
                sequence=sequence,
                operation=next_operation(sequence),
                seed_rows=seed_rows,
                update_index=update_index,
                insert_index=insert_index,
            )
        except TransactionFailure as exc:
            result_queue.put({"ok": False, "record": exc.record})
            return
        try:
            wal_size, frames = wal_frames(db_path)
            record["wal_bytes_after"] = wal_size
            record["wal_frames_after"] = frames
        except (FileNotFoundError, AssertionError) as exc:
            record["wal_observation_after"] = str(exc)
        result_queue.put({"ok": True, "record": record})
    except BaseException as exc:
        result_queue.put({"ok": False, "error": error_record(exc)})
    finally:
        if connection is not None:
            connection.close()


def run_quiet_checkpoint(
    context: Any,
    db_path: str,
    busy_timeout_ms: int,
    timeout_seconds: float,
    label: str,
) -> dict[str, Any]:
    ready = context.Event()
    gate = context.Event()
    invoked = context.Event()
    start_queue = context.Queue()
    result_queue = context.Queue()
    pragma_started = context.Value("q", 0)
    process = context.Process(
        target=checkpoint_role,
        args=(
            db_path,
            busy_timeout_ms,
            ready,
            gate,
            invoked,
            start_queue,
            pragma_started,
            result_queue,
        ),
        name=f"quiet-checkpointer-{label}",
    )
    process.start()
    if not ready.wait(30):
        process.terminate()
        process.join(10)
        return {"ok": False, "error": {"message": "quiet checkpoint not ready"}}
    gate.set()
    try:
        armed = start_queue.get(timeout=30)
    except queue.Empty:
        process.terminate()
        process.join(10)
        return {"ok": False, "error": {"message": "quiet checkpoint did not arm"}}
    start_wait_deadline = time.monotonic() + 5
    while pragma_started.value == 0 and time.monotonic() < start_wait_deadline:
        time.sleep(0.001)
    started_ns = int(pragma_started.value or armed["armed_ns"])
    deadline_ns = started_ns + int(timeout_seconds * 1e9)
    checkpoint_result: dict[str, Any] | None = None
    timeout_observed_ns: int | None = None
    while checkpoint_result is None:
        try:
            checkpoint_result = result_queue.get_nowait()
            break
        except queue.Empty:
            pass
        if time.monotonic_ns() >= deadline_ns:
            try:
                checkpoint_result = result_queue.get_nowait()
            except queue.Empty:
                timeout_observed_ns = time.monotonic_ns()
                process.terminate()
                process.join(10)
                if process.is_alive():
                    process.kill()
                    process.join(10)
                return {
                    "ok": False,
                    "timed_out": True,
                    "duration_lower_bound_ms": (
                        timeout_observed_ns - started_ns
                    )
                    / 1_000_000,
                    "end_state": file_state(db_path),
                }
            break
        if not process.is_alive():
            try:
                checkpoint_result = result_queue.get(timeout=2)
            except queue.Empty:
                return {
                    "ok": False,
                    "error": {"message": "quiet checkpoint exited without result"},
                }
            break
        time.sleep(MONITOR_INTERVAL_SECONDS)
    assert checkpoint_result is not None
    close_wait_started_ns = time.monotonic_ns()
    process.join(10)
    checkpoint_result["worker_close_wait_ms"] = (
        time.monotonic_ns() - close_wait_started_ns
    ) / 1_000_000
    checkpoint_result["worker_close_hung"] = process.is_alive()
    if process.is_alive():
        process.terminate()
        process.join(10)
    return checkpoint_result


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize_values(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min": min(values),
        "mean": statistics.fmean(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


def summarize_transactions(records: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for operation in ("U", "I"):
        selected = [record for record in records if record["operation"] == operation]
        result[operation] = {
            field: summarize_values([float(record[field]) for record in selected])
            for field in ("begin_ms", "mutate_ms", "commit_ms", "tx_ms")
        }
    return result


def summarize_reads(
    records: list[dict[str, Any]],
    baseline_started_ns: int,
    checkpoint_started_ns: int,
    checkpoint_ended_ns: int,
    post_ended_ns: int,
) -> dict[str, Any]:
    windows: dict[str, list[dict[str, Any]]] = {
        "before": [],
        "overlap": [],
        "after": [],
    }
    for record in records:
        if record.get("event") != "read":
            continue
        started_ns = int(record["started_ns"])
        ended_ns = int(record["ended_ns"])
        if started_ns < checkpoint_ended_ns and ended_ns > checkpoint_started_ns:
            windows["overlap"].append(record)
        elif ended_ns <= checkpoint_started_ns and started_ns >= baseline_started_ns:
            windows["before"].append(record)
        elif started_ns >= checkpoint_ended_ns and ended_ns <= post_ended_ns:
            windows["after"].append(record)
    result: dict[str, Any] = {}
    for window, selected in windows.items():
        result[window] = {}
        for category in ("stable_main", "recent_commit"):
            category_records = [
                record for record in selected if record["category"] == category
            ]
            result[window][category] = {
                field: summarize_values(
                    [float(record[field]) for record in category_records]
                )
                for field in ("fetch_ms", "hash_ms", "total_ms")
            }
    return result


def unfinished_read(records: list[dict[str, Any]], observed_ns: int) -> dict[str, Any] | None:
    starts = {
        int(record["iteration"]): record
        for record in records
        if record.get("event") == "read_start"
    }
    for record in records:
        if record.get("event") == "read":
            starts.pop(int(record["iteration"]), None)
    if not starts:
        return None
    latest = starts[max(starts)]
    return {
        "iteration": latest["iteration"],
        "category": latest["category"],
        "started_ns": latest["started_ns"],
        "observed_lower_bound_ms": (observed_ns - int(latest["started_ns"]))
        / 1_000_000,
    }


def verify_changed_set(
    connection: sqlite3.Connection,
    acks: list[dict[str, Any]],
    seed_rows: int,
    *,
    expected_filler_seq: int,
    sample_limit: int = 0,
    quick_check: bool = False,
) -> dict[str, Any]:
    latest_by_row: dict[int, dict[str, Any]] = {}
    insert_count = 0
    for ack in acks:
        latest_by_row[int(ack["row_id"])] = ack
        if ack["operation"] == "I":
            insert_count += 1
    expected_sequences = [int(ack["sequence"]) for ack in acks]
    actual_sequences = [
        int(row[0])
        for row in connection.execute(
            "SELECT seq FROM txlog WHERE applied=1 ORDER BY seq"
        ).fetchall()
    ]
    selected_items = sorted(latest_by_row.items())
    if sample_limit == 1 and selected_items:
        selected_items = [selected_items[-1]]
    elif sample_limit and len(selected_items) > sample_limit:
        indexes = {
            round(index * (len(selected_items) - 1) / (sample_limit - 1))
            for index in range(sample_limit)
        }
        selected_items = [selected_items[index] for index in sorted(indexes)]
    mismatches: list[dict[str, Any]] = []
    for row_id, expected in selected_items:
        cursor = connection.execute(
            "SELECT version, digest, payload FROM blocks WHERE id=?", (row_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            mismatches.append({"row_id": row_id, "error": "missing"})
            continue
        version, digest, payload = row
        actual_digest = hashlib.sha256(payload).hexdigest()
        if (
            int(version) != int(expected["sequence"])
            or digest != expected["digest"]
            or actual_digest != expected["digest"]
        ):
            mismatches.append(
                {
                    "row_id": row_id,
                    "expected_sequence": expected["sequence"],
                    "actual_version": version,
                    "expected_digest": expected["digest"],
                    "stored_digest": digest,
                    "actual_digest": actual_digest,
                }
            )
    stable_candidates = stable_hot_rows(seed_rows)
    stable_mismatches: list[dict[str, Any]] = []
    for row_id in stable_candidates:
        cursor = connection.execute(
            "SELECT version, digest, payload FROM blocks WHERE id=?", (row_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            stable_mismatches.append({"row_id": row_id, "error": "missing"})
            continue
        version, digest, payload = row
        actual_digest = hashlib.sha256(payload).hexdigest()
        if version != 0 or digest != ZERO_DIGEST or actual_digest != ZERO_DIGEST:
            stable_mismatches.append(
                {
                    "row_id": row_id,
                    "version": version,
                    "stored_digest": digest,
                    "actual_digest": actual_digest,
                }
            )
    block_count = int(connection.execute("SELECT count(*) FROM blocks").fetchone()[0])
    state = connection.execute(
        "SELECT last_seq, filler_seq, last_row_id, last_op FROM state WHERE id=1"
    ).fetchone()
    expected_last = acks[-1] if acks else None
    state_exact = (
        state[0] == (expected_last["sequence"] if expected_last else 0)
        and state[1] == expected_filler_seq
        and state[2] == (expected_last["row_id"] if expected_last else 1)
        and state[3] == (expected_last["operation"] if expected_last else "seed")
    )
    quick_result = None
    quick_ms = None
    if quick_check:
        quick_started_ns = time.monotonic_ns()
        quick_result = connection.execute("PRAGMA quick_check(1)").fetchone()[0]
        quick_ms = (time.monotonic_ns() - quick_started_ns) / 1_000_000
    result = {
        "expected_ack_count": len(acks),
        "actual_applied_count": len(actual_sequences),
        "sequence_exact": actual_sequences == expected_sequences,
        "expected_block_count": seed_rows + insert_count,
        "actual_block_count": block_count,
        "changed_rows_total": len(latest_by_row),
        "changed_rows_checked": len(selected_items),
        "changed_mismatches": mismatches,
        "stable_rows_checked": stable_candidates,
        "stable_mismatches": stable_mismatches,
        "state": {
            "last_seq": state[0],
            "filler_seq": state[1],
            "last_row_id": state[2],
            "last_op": state[3],
        },
        "expected_filler_seq": expected_filler_seq,
        "state_exact": state_exact,
        "quick_check": quick_result,
        "quick_check_ms": quick_ms,
    }
    result["ok"] = (
        result["sequence_exact"]
        and block_count == result["expected_block_count"]
        and not mismatches
        and not stable_mismatches
        and state_exact
        and (quick_result in (None, "ok"))
    )
    return result


def prepare_base(db_path: Path, target_bytes: int, output: Path) -> int:
    if db_path.exists() or any(Path(str(db_path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
        raise FileExistsError(f"refusing to replace existing database {db_path}")
    if output.exists():
        raise FileExistsError(f"refusing to replace existing output {output}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        connection.execute(f"PRAGMA page_size={PAGE_SIZE}")
        connection.execute("PRAGMA auto_vacuum=NONE")
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA locking_mode=EXCLUSIVE")
        connection.executescript(
            "BEGIN;"
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE blocks("
            "id INTEGER PRIMARY KEY, kind TEXT NOT NULL, version INTEGER NOT NULL, "
            "digest TEXT NOT NULL, payload BLOB NOT NULL);"
            "CREATE TABLE txlog("
            "seq INTEGER PRIMARY KEY, applied INTEGER NOT NULL, op TEXT, "
            "row_id INTEGER, version INTEGER, digest TEXT);"
            "CREATE TABLE state("
            "id INTEGER PRIMARY KEY CHECK(id=1), last_seq INTEGER NOT NULL, "
            "filler_seq INTEGER NOT NULL, last_row_id INTEGER NOT NULL, "
            "last_op TEXT NOT NULL);"
            "INSERT INTO state VALUES(1, 0, 0, 1, 'seed');"
            "COMMIT;"
        )
        connection.execute("BEGIN")
        connection.executemany(
            "INSERT INTO txlog(seq, applied) VALUES (?, 0)",
            ((sequence,) for sequence in range(1, TXLOG_SLOTS + 1)),
        )
        connection.execute("COMMIT")
        seed_rows = 0
        while True:
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            current_bytes = page_count * PAGE_SIZE
            if current_bytes >= target_bytes and seed_rows >= 2 and seed_rows % 2 == 0:
                break
            remaining = max(PAYLOAD_BYTES, target_bytes - current_bytes)
            batch = min(256, max(1, math.ceil(remaining / (PAYLOAD_BYTES + 128))))
            if seed_rows + batch < 2:
                batch = 2 - seed_rows
            connection.execute("BEGIN")
            for _ in range(batch):
                seed_rows += 1
                connection.execute(
                    "INSERT INTO blocks(id, kind, version, digest, payload) "
                    "VALUES (?, 'seed', 0, ?, zeroblob(?))",
                    (seed_rows, ZERO_DIGEST, PAYLOAD_BYTES),
                )
            connection.execute("COMMIT")
        connection.execute("BEGIN")
        connection.executemany(
            "INSERT INTO meta(key, value) VALUES (?, ?)",
            (
                ("target_bytes", str(target_bytes)),
                ("seed_rows", str(seed_rows)),
                ("payload_bytes", str(PAYLOAD_BYTES)),
                ("page_size", str(PAGE_SIZE)),
            ),
        )
        connection.execute("COMMIT")
        journal_mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise AssertionError(f"journal_mode={journal_mode!r}, want wal")
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if tuple(checkpoint) != (0, 0, 0):
            raise AssertionError(f"template checkpoint returned {checkpoint!r}")
        quick_check = connection.execute("PRAGMA quick_check(1)").fetchone()[0]
        if quick_check != "ok":
            raise AssertionError(f"template quick_check={quick_check!r}")
    finally:
        connection.close()
    state = file_state(str(db_path))
    for label in ("wal", "shm", "journal"):
        if state[label]["exists"]:
            raise AssertionError(f"clean template still has {label}: {state[label]}")
    read_only = connect(str(db_path), 30_000, query_only=True, immutable=True)
    try:
        actual_page_size = int(read_only.execute("PRAGMA page_size").fetchone()[0])
        actual_seed_rows = int(
            read_only.execute("SELECT count(*) FROM blocks WHERE kind='seed'").fetchone()[0]
        )
    finally:
        read_only.close()
    mode_check = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        actual_mode = str(mode_check.execute("PRAGMA journal_mode").fetchone()[0])
    finally:
        mode_check.close()
    post_check_state = file_state(str(db_path))
    for label in ("wal", "shm", "journal"):
        if post_check_state[label]["exists"]:
            raise AssertionError(
                f"journal-mode verification left {label}: {post_check_state[label]}"
            )
    result = {
        "schema_version": 1,
        "db_path": str(db_path),
        "target_bytes": target_bytes,
        "actual_bytes": state["main"]["size"],
        "actual_page_count": state["main"]["size"] // PAGE_SIZE,
        "page_size": actual_page_size,
        "persisted_journal_mode": actual_mode,
        "seed_rows": actual_seed_rows,
        "payload_bytes": PAYLOAD_BYTES,
        "sidecars_absent": True,
        "sqlite_version": sqlite3.sqlite_version,
        "quick_check": "ok",
    }
    json_dump(output, result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


def run_case(args: argparse.Namespace) -> int:
    db_path = str(Path(args.db).absolute())
    output_dir = Path(args.output_dir).absolute()
    if output_dir.exists():
        raise FileExistsError(f"refusing to replace output directory {output_dir}")
    output_dir.mkdir(parents=True)
    result_path = output_dir / "result.json"
    preclose_result_path = output_dir / "preclose-result.json"
    ack_path = output_dir / "acks.jsonl"
    writer_path = output_dir / "writer.jsonl"
    reader_path = output_dir / "reader.jsonl"
    monitor_path = output_dir / "monitor.jsonl"
    for path in (ack_path, writer_path, reader_path, monitor_path):
        path.touch(exist_ok=False)

    initial_state = file_state(db_path)
    if not initial_state["main"]["exists"]:
        raise FileNotFoundError(db_path)
    if initial_state["wal"]["exists"] or initial_state["shm"]["exists"]:
        raise AssertionError(f"case must start without sidecars: {initial_state}")
    keeper = connect(db_path, args.busy_timeout_ms)
    context = mp.get_context("spawn")
    reader_stop = context.Event()
    reader_start = context.Event()
    reader_ready = context.Event()
    reader_status_queue = context.Queue()
    reader_process: mp.Process | None = None
    checkpoint_process: mp.Process | None = None
    writer_process: mp.Process | None = None
    all_acks: list[dict[str, Any]] = []
    pre_records: list[dict[str, Any]] = []
    post_records: list[dict[str, Any]] = []
    filler_records: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "schema_version": 1,
        "label": args.label,
        "db_path": db_path,
        "initial_state": initial_state,
        "sqlite_version": sqlite3.sqlite_version,
        "checkpoint_mode": "TRUNCATE",
        "checkpoint_semantics": "strong; not the default PASSIVE auto-checkpoint",
        "wal_target_frames": WAL_TARGET_FRAMES,
        "wal_target_bytes": WAL_TARGET_BYTES,
        "drive9_whole_file_cache_max_bytes": 4 * 1024 * 1024,
        "max_frames_below_cache_cutoff": WAL_MAX_CACHE_FRAMES,
        "status": "running",
    }
    exit_code = 1
    try:
        settings = assert_runtime(keeper)
        seed_rows = int(
            keeper.execute("SELECT value FROM meta WHERE key='seed_rows'").fetchone()[0]
        )
        result["settings"] = settings
        result["seed_rows"] = seed_rows
        reader_process = context.Process(
            target=reader_role,
            args=(
                db_path,
                seed_rows,
                args.busy_timeout_ms,
                str(reader_path),
                reader_ready,
                reader_start,
                reader_stop,
                reader_status_queue,
            ),
            name=f"reader-{args.label}",
        )
        reader_process.start()
        if not reader_ready.wait(30):
            raise TimeoutError("reader did not become ready")
        reader_start.set()
        result["reader_started_before_wal_build"] = True
        sequence = 1
        update_index = 0
        insert_index = 0
        previous_frames = 0
        for _ in range(PRE_MIXED_COMMITS):
            operation = next_operation(sequence)
            record = timed_mixed_transaction(
                keeper,
                sequence=sequence,
                operation=operation,
                seed_rows=seed_rows,
                update_index=update_index,
                insert_index=insert_index,
            )
            if operation == "U":
                update_index += 1
            else:
                insert_index += 1
            wal_size, frames = wal_frames(db_path)
            record["wal_bytes_after"] = wal_size
            record["wal_frames_after"] = frames
            record["wal_frame_delta"] = frames - previous_frames
            append_jsonl(writer_path, record)
            append_jsonl(ack_path, record)
            pre_records.append(record)
            all_acks.append(record)
            previous_frames = frames
            sequence += 1
            remaining = WRITER_INTERVAL_SECONDS - record["tx_ms"] / 1000
            if remaining > 0:
                time.sleep(remaining)
            if frames >= WAL_TARGET_FRAMES:
                raise AssertionError(
                    f"{PRE_MIXED_COMMITS} fixed mixed commits reached {frames} frames; "
                    f"need room below target {WAL_TARGET_FRAMES}"
                )
        filler_sequence = 0
        while previous_frames < WAL_TARGET_FRAMES:
            filler_sequence += 1
            record = timed_filler_transaction(keeper, filler_sequence)
            wal_size, frames = wal_frames(db_path)
            record["wal_bytes_after"] = wal_size
            record["wal_frames_after"] = frames
            record["wal_frame_delta"] = frames - previous_frames
            if record["wal_frame_delta"] != 1:
                raise AssertionError(
                    f"filler added {record['wal_frame_delta']} frames, want exactly 1"
                )
            append_jsonl(writer_path, record)
            filler_records.append(record)
            previous_frames = frames
        wal_size, frames = wal_frames(db_path)
        if (wal_size, frames) != (WAL_TARGET_BYTES, WAL_TARGET_FRAMES):
            raise AssertionError(
                f"WAL target mismatch: bytes={wal_size}, frames={frames}"
            )
        pre_update_count, pre_insert_count = count_operations(pre_records)
        mixed_frames = sum(int(record["wal_frame_delta"]) for record in pre_records)
        operation_frame_totals = {
            operation: sum(
                int(record["wal_frame_delta"])
                for record in pre_records
                if record["operation"] == operation
            )
            for operation in ("U", "I")
        }
        for operation, frame_total in operation_frame_totals.items():
            if frame_total < mixed_frames * 0.40:
                raise AssertionError(
                    f"operation {operation} contributed only {frame_total}/{mixed_frames} frames"
                )
        checkpoint_input_state = file_state(db_path)
        logical_page_count = int(keeper.execute("PRAGMA page_count").fetchone()[0])
        result["pre_checkpoint"] = {
            "mixed_commits": len(pre_records),
            "update_commits": pre_update_count,
            "insert_commits": pre_insert_count,
            "filler_commits": len(filler_records),
            "mixed_frames": mixed_frames,
            "operation_frame_totals": operation_frame_totals,
            "wal_bytes": wal_size,
            "wal_frames": frames,
            "main_bytes": checkpoint_input_state["main"]["size"],
            "logical_page_count": logical_page_count,
            "writer_latency_ms": summarize_transactions(pre_records),
        }
        print(
            json.dumps(
                {
                    "event": "wal_target_ready",
                    "label": args.label,
                    "wal_bytes": wal_size,
                    "wal_frames": frames,
                    "mixed_commits": len(pre_records),
                },
                sort_keys=True,
            ),
            flush=True,
        )

        baseline_started_ns = time.monotonic_ns()
        time.sleep(args.baseline_seconds)

        checkpoint_ready = context.Event()
        checkpoint_gate = context.Event()
        checkpoint_invoked = context.Event()
        checkpoint_start_queue = context.Queue()
        checkpoint_result_queue = context.Queue()
        checkpoint_pragma_started = context.Value("q", 0)
        writer_ready = context.Event()
        overlap_start_queue = context.Queue()
        overlap_result_queue = context.Queue()
        checkpoint_process = context.Process(
            target=checkpoint_role,
            args=(
                db_path,
                args.busy_timeout_ms,
                checkpoint_ready,
                checkpoint_gate,
                checkpoint_invoked,
                checkpoint_start_queue,
                checkpoint_pragma_started,
                checkpoint_result_queue,
            ),
            name=f"checkpointer-{args.label}",
        )
        writer_process = context.Process(
            target=overlap_writer_role,
            args=(
                db_path,
                seed_rows,
                args.busy_timeout_ms,
                sequence,
                update_index,
                insert_index,
                writer_ready,
                checkpoint_invoked,
                overlap_start_queue,
                overlap_result_queue,
            ),
            name=f"overlap-writer-{args.label}",
        )
        checkpoint_process.start()
        writer_process.start()
        if not checkpoint_ready.wait(30) or not writer_ready.wait(30):
            raise TimeoutError("checkpoint/writer roles did not become ready")
        checkpoint_gate.set()
        checkpoint_start = checkpoint_start_queue.get(timeout=30)
        result["checkpoint_arm"] = checkpoint_start
        start_wait_deadline = time.monotonic() + 5
        while checkpoint_pragma_started.value == 0:
            if time.monotonic() >= start_wait_deadline:
                raise TimeoutError("checkpoint did not enter PRAGMA")
            time.sleep(0.001)
        checkpoint_started_ns = int(checkpoint_pragma_started.value)
        try:
            overlap_start = overlap_start_queue.get(timeout=5)
        except queue.Empty:
            overlap_start = {"error": "writer did not report BEGIN attempt start"}
        result["overlap_writer_attempt"] = overlap_start
        print(
            json.dumps(
                {
                    "event": "checkpoint_started",
                    "label": args.label,
                    "wal_bytes": wal_size,
                    "wal_frames": frames,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        checkpoint_deadline_ns = checkpoint_started_ns + int(
            args.checkpoint_timeout_seconds * 1e9
        )
        timed_out = False
        timeout_observed_ns: int | None = None
        checkpoint_result: dict[str, Any] | None = None
        while checkpoint_result is None:
            try:
                checkpoint_result = checkpoint_result_queue.get_nowait()
                break
            except queue.Empty:
                pass
            sample = {
                "event": "file_state",
                "monotonic_ns": time.monotonic_ns(),
                "unix_ns": time.time_ns(),
                "state": file_state(db_path),
            }
            append_jsonl(monitor_path, sample)
            if time.monotonic_ns() >= checkpoint_deadline_ns:
                try:
                    checkpoint_result = checkpoint_result_queue.get_nowait()
                except queue.Empty:
                    timed_out = True
                    timeout_observed_ns = time.monotonic_ns()
                    checkpoint_process.terminate()
                    checkpoint_process.join(10)
                    if checkpoint_process.is_alive():
                        checkpoint_process.kill()
                        checkpoint_process.join(10)
                break
            if not checkpoint_process.is_alive():
                try:
                    checkpoint_result = checkpoint_result_queue.get(timeout=2)
                except queue.Empty:
                    checkpoint_result = {
                        "ok": False,
                        "error": {"message": "checkpoint worker exited without result"},
                    }
                break
            time.sleep(MONITOR_INTERVAL_SECONDS)
        if timed_out:
            assert timeout_observed_ns is not None
            checkpoint_ended_ns = timeout_observed_ns
            checkpoint_result = {
                "ok": False,
                "timed_out": True,
                "duration_lower_bound_ms": (
                    checkpoint_ended_ns - checkpoint_started_ns
                )
                / 1_000_000,
                "end_state": file_state(db_path),
            }
        else:
            assert checkpoint_result is not None
            checkpoint_ended_ns = int(
                checkpoint_result.get("ended_ns", time.monotonic_ns())
            )
            close_wait_started_ns = time.monotonic_ns()
            checkpoint_process.join(10)
            checkpoint_result["worker_close_wait_ms"] = (
                time.monotonic_ns() - close_wait_started_ns
            ) / 1_000_000
            checkpoint_result["worker_close_hung"] = checkpoint_process.is_alive()
            if checkpoint_process.is_alive():
                checkpoint_process.terminate()
                checkpoint_process.join(10)
        result["primary_checkpoint"] = checkpoint_result

        try:
            overlap_result = overlap_result_queue.get(timeout=30)
        except queue.Empty:
            overlap_result = {
                "ok": False,
                "error": {"message": "no overlap writer result"},
            }
            attempt_started_ns = overlap_start.get("attempt_started_ns")
            if attempt_started_ns is not None:
                overlap_result["begin_wait_lower_bound_ms"] = (
                    time.monotonic_ns() - int(attempt_started_ns)
                ) / 1_000_000
        writer_close_wait_started_ns = time.monotonic_ns()
        writer_process.join(10)
        overlap_result["worker_close_wait_ms"] = (
            time.monotonic_ns() - writer_close_wait_started_ns
        ) / 1_000_000
        overlap_result["worker_close_hung"] = writer_process.is_alive()
        if writer_process.is_alive():
            overlap_result["worker_killed_after_checkpoint"] = True
            writer_process.terminate()
            writer_process.join(10)
        result["overlap_writer"] = overlap_result
        if overlap_result.get("ok"):
            overlap_record = overlap_result["record"]
            append_jsonl(writer_path, overlap_record)
            append_jsonl(ack_path, overlap_record)
            all_acks.append(overlap_record)
            if overlap_record["operation"] == "U":
                update_index += 1
            else:
                insert_index += 1
            sequence += 1
            overlap_frames = overlap_record.get("wal_frames_after")
            if (
                not timed_out
                and overlap_frames is not None
                and int(overlap_frames) > WAL_MAX_CACHE_FRAMES
            ):
                raise AssertionError(
                    f"overlap pushed WAL past <=4 MiB cutoff: {overlap_frames} frames"
                )
        elif overlap_result.get("record"):
            append_jsonl(writer_path, overlap_result["record"])

        primary_success = (
            checkpoint_result.get("ok")
            and checkpoint_result.get("result") == [0, 0, 0]
            and not timed_out
        )
        overlap_success = bool(overlap_result.get("ok"))
        if primary_success and overlap_success:
            for _ in range(POST_MIXED_WRITES):
                operation = next_operation(sequence)
                record = timed_mixed_transaction(
                    keeper,
                    sequence=sequence,
                    operation=operation,
                    seed_rows=seed_rows,
                    update_index=update_index,
                    insert_index=insert_index,
                )
                if operation == "U":
                    update_index += 1
                else:
                    insert_index += 1
                append_jsonl(writer_path, record)
                append_jsonl(ack_path, record)
                post_records.append(record)
                all_acks.append(record)
                sequence += 1
                remaining = WRITER_INTERVAL_SECONDS - record["tx_ms"] / 1000
                if remaining > 0:
                    time.sleep(remaining)
            time.sleep(args.post_seconds)
        post_ended_ns = time.monotonic_ns()
        reader_stop.set()
        try:
            reader_status = reader_status_queue.get(timeout=30)
        except queue.Empty:
            reader_status = {
                "ok": False,
                "error": {"message": "no reader status"},
            }
        reader_close_wait_started_ns = time.monotonic_ns()
        reader_process.join(10)
        reader_status["worker_close_wait_ms"] = (
            time.monotonic_ns() - reader_close_wait_started_ns
        ) / 1_000_000
        reader_status["worker_close_hung"] = reader_process.is_alive()
        if reader_process.is_alive():
            reader_process.terminate()
            reader_process.join(10)
        result["reader_status"] = reader_status
        read_records = read_jsonl(reader_path)
        result["reader_unfinished"] = unfinished_read(
            read_records, time.monotonic_ns()
        )

        if primary_success and overlap_success:
            result["final_checkpoint"] = run_quiet_checkpoint(
                context,
                db_path,
                args.busy_timeout_ms,
                args.final_checkpoint_timeout_seconds,
                args.label,
            )
            final_success = (
                result["final_checkpoint"].get("ok")
                and result["final_checkpoint"].get("result") == [0, 0, 0]
            )
            if final_success:
                live_verification = verify_changed_set(
                    keeper,
                    all_acks,
                    seed_rows,
                    expected_filler_seq=len(filler_records),
                    sample_limit=0,
                    quick_check=False,
                )
            else:
                live_verification = {
                    "ok": False,
                    "skipped": "final checkpoint did not complete successfully",
                }
            result["live_verification"] = live_verification
            result["post_checkpoint"] = {
                "mixed_commits": len(post_records),
                "writer_latency_ms": summarize_transactions(post_records),
                "main_bytes": file_state(db_path)["main"]["size"],
            }
            result["reader_latency_ms"] = summarize_reads(
                read_records,
                baseline_started_ns,
                checkpoint_started_ns,
                checkpoint_ended_ns,
                post_ended_ns,
            )
            reader_window_counts = {
                window: sum(
                    result["reader_latency_ms"][window][category]["fetch_ms"][
                        "count"
                    ]
                    for category in ("stable_main", "recent_commit")
                )
                for window in ("before", "overlap", "after")
            }
            result["reader_window_counts"] = reader_window_counts
            reader_gate_ok = (
                reader_status.get("ok")
                and reader_window_counts["before"] > 0
                and reader_window_counts["after"] > 0
            )
            result["status"] = (
                "ok"
                if live_verification["ok"]
                and final_success
                and reader_gate_ok
                else "correctness_failed"
            )
            exit_code = 0 if result["status"] == "ok" else 1
        else:
            if primary_success:
                result["post_checkpoint"] = {
                    "skipped": "overlap writer failed after primary checkpoint"
                }
                result["reader_latency_ms"] = summarize_reads(
                    read_records,
                    baseline_started_ns,
                    checkpoint_started_ns,
                    checkpoint_ended_ns,
                    post_ended_ns,
                )
                try:
                    result["pre_ack_verification_after_writer_failure"] = (
                        verify_changed_set(
                            keeper,
                            all_acks,
                            seed_rows,
                            expected_filler_seq=len(filler_records),
                            sample_limit=0,
                            quick_check=False,
                        )
                    )
                except BaseException as exc:
                    result["pre_ack_verification_after_writer_failure"] = {
                        "ok": False,
                        "error": error_record(exc),
                    }
                result["status"] = "application_write_failed_after_checkpoint"
                exit_code = 3
            else:
                result["status"] = (
                    "checkpoint_timeout" if timed_out else "checkpoint_failed"
                )
                exit_code = 2
    except BaseException as exc:
        result["status"] = "harness_error"
        result["error"] = error_record(exc)
        exit_code = 1
    finally:
        reader_stop.set()
        for process in (reader_process, writer_process, checkpoint_process):
            if process is not None and process.is_alive():
                process.terminate()
                process.join(10)
                if process.is_alive():
                    process.kill()
                    process.join(5)
        try:
            result["final_file_state_before_keeper_close"] = file_state(db_path)
        except BaseException as exc:
            result["final_file_state_error"] = str(exc)
        result["preclose_evidence_unix_ns"] = time.time_ns()
        json_dump(preclose_result_path, result)
        if result["status"] != "ok":
            result["ended_unix_ns"] = time.time_ns()
            result["keeper_close_skipped_after_failure"] = True
            json_dump(result_path, result)
            print(
                json.dumps(
                    {
                        "event": "case_partial_complete",
                        "label": args.label,
                        "status": result["status"],
                        "result_path": str(result_path),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(exit_code)
        keeper_close_started_ns = time.monotonic_ns()
        keeper.close()
        result["keeper_close_ms"] = (
            time.monotonic_ns() - keeper_close_started_ns
        ) / 1_000_000
        result["ended_unix_ns"] = time.time_ns()
        json_dump(result_path, result)
        print(
            json.dumps(
                {
                    "event": "case_complete",
                    "label": args.label,
                    "status": result["status"],
                    "result_path": str(result_path),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    return exit_code


def verify_command(args: argparse.Namespace) -> int:
    db_path = str(Path(args.db).absolute())
    acks = read_jsonl(Path(args.acks))
    connection = connect(
        db_path,
        args.busy_timeout_ms,
        query_only=True,
        immutable=args.immutable,
    )
    try:
        result = verify_changed_set(
            connection,
            acks,
            args.seed_rows,
            expected_filler_seq=args.expected_filler_seq,
            sample_limit=args.sample_limit,
            quick_check=args.quick_check,
        )
        result["db_path"] = db_path
        result["immutable"] = args.immutable
        result["file_state"] = file_state(db_path)
    finally:
        connection.close()
    if args.output:
        json_dump(Path(args.output), result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["ok"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare", help="create a clean WAL-mode template")
    prepare.add_argument("--db", required=True)
    prepare.add_argument("--target-bytes", required=True, type=int)
    prepare.add_argument("--output", required=True)
    run = subparsers.add_parser("run", help="run one checkpoint/concurrency case")
    run.add_argument("--db", required=True)
    run.add_argument("--label", required=True)
    run.add_argument("--output-dir", required=True)
    run.add_argument("--baseline-seconds", type=float, default=2.0)
    run.add_argument("--post-seconds", type=float, default=2.0)
    run.add_argument("--checkpoint-timeout-seconds", type=float, default=180.0)
    run.add_argument("--final-checkpoint-timeout-seconds", type=float, default=180.0)
    run.add_argument("--busy-timeout-ms", type=int, default=210_000)
    verify = subparsers.add_parser("verify", help="verify acknowledged changed rows")
    verify.add_argument("--db", required=True)
    verify.add_argument("--acks", required=True)
    verify.add_argument("--seed-rows", required=True, type=int)
    verify.add_argument("--expected-filler-seq", required=True, type=int)
    verify.add_argument("--sample-limit", type=int, default=0)
    verify.add_argument("--quick-check", action="store_true")
    verify.add_argument("--immutable", action="store_true")
    verify.add_argument("--busy-timeout-ms", type=int, default=30_000)
    verify.add_argument("--output")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "prepare":
        return prepare_base(Path(args.db).absolute(), args.target_bytes, Path(args.output).absolute())
    if args.command == "run":
        return run_case(args)
    if args.command == "verify":
        return verify_command(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    sys.exit(main())
