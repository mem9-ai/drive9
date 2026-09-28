#!/usr/bin/env python3
"""Run the bounded SQLite WAL workload used to distinguish PR #901.

The connection remains open after validation so an external runner can capture
the authoritative main/WAL objects before Linux close(2) reaches FUSE Release.
All measurements and control files live outside the tested mount.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def sqlite_error(error: BaseException) -> dict[str, Any]:
    return {
        "type": type(error).__name__,
        "message": str(error),
        "sqlite_errorcode": getattr(error, "sqlite_errorcode", None),
        "sqlite_errorname": getattr(error, "sqlite_errorname", None),
    }


def file_state(database: Path) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for label, path in {
        "main": database,
        "wal": Path(str(database) + "-wal"),
        "shm": Path(str(database) + "-shm"),
        "journal": Path(str(database) + "-journal"),
    }.items():
        try:
            stat = path.stat()
        except FileNotFoundError:
            state[label] = {"exists": False, "size": None}
        else:
            state[label] = {
                "exists": True,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
    return state


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + f".part.{os.getpid()}")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def monitor_files(
    database: Path,
    destination: Path,
    stop: threading.Event,
) -> None:
    with destination.open("x", encoding="utf-8") as handle:
        while not stop.wait(0.1):
            sample = {
                "monotonic_ns": time.monotonic_ns(),
                "utc": utc_now(),
                "files": file_state(database),
            }
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
            handle.flush()


def execute_timed(
    connection: sqlite3.Connection,
    sql: str,
    parameters: tuple[Any, ...] = (),
) -> tuple[sqlite3.Cursor, float]:
    started = time.monotonic()
    cursor = connection.execute(sql, parameters)
    return cursor, (time.monotonic() - started) * 1000.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=2560)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--timeline", type=Path, required=True)
    parser.add_argument("--ready", type=Path, required=True)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--hold-seconds", type=float, default=90.0)
    args = parser.parse_args()

    if args.rows <= 0 or args.hold_seconds <= 0:
        parser.error("rows and hold-seconds must be positive")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    args.result.parent.mkdir(parents=True, exist_ok=True)
    for path in (
        args.db,
        Path(str(args.db) + "-wal"),
        Path(str(args.db) + "-shm"),
        Path(str(args.db) + "-journal"),
    ):
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    result: dict[str, Any] = {
        "schema_version": 1,
        "started_utc": utc_now(),
        "database": str(args.db),
        "rows_requested": args.rows,
        "python": os.sys.version,
        "sqlite": sqlite3.sqlite_version,
        "operations": {},
        "errors": [],
    }
    connection: sqlite3.Connection | None = None
    monitor_stop = threading.Event()
    monitor = threading.Thread(
        target=monitor_files,
        args=(args.db, args.timeline, monitor_stop),
        daemon=True,
    )
    monitor.start()

    try:
        connection = sqlite3.connect(
            args.db,
            timeout=300.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA page_size=4096")
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        journal_mode = connection.execute(
            "PRAGMA journal_mode=WAL"
        ).fetchone()[0]
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA wal_autocheckpoint=1000")
        connection.execute("PRAGMA mmap_size=0")
        result["pragmas"] = {
            "page_size": page_size,
            "journal_mode": journal_mode,
            "synchronous": connection.execute(
                "PRAGMA synchronous"
            ).fetchone()[0],
            "wal_autocheckpoint": connection.execute(
                "PRAGMA wal_autocheckpoint"
            ).fetchone()[0],
        }

        _, vacuum_ms = execute_timed(connection, "VACUUM")
        result["operations"]["vacuum_ms"] = vacuum_ms
        execute_timed(connection, "BEGIN")
        execute_timed(
            connection,
            "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB)",
        )
        _, insert_ms = execute_timed(
            connection,
            "WITH RECURSIVE c(x) AS (VALUES(1) "
            "UNION ALL SELECT x+1 FROM c WHERE x < ?) "
            "INSERT INTO kv(k,v) SELECT x, randomblob(4096) FROM c",
            (args.rows,),
        )
        result["operations"]["insert_ms"] = insert_ms

        commit_started = time.monotonic()
        try:
            connection.execute("COMMIT")
        except sqlite3.Error as error:
            result["operations"]["commit"] = {
                "ok": False,
                "duration_ms": (time.monotonic() - commit_started) * 1000.0,
                "error": sqlite_error(error),
            }
            result["errors"].append({"phase": "commit", **sqlite_error(error)})
        else:
            result["operations"]["commit"] = {
                "ok": True,
                "duration_ms": (time.monotonic() - commit_started) * 1000.0,
            }

        result["after_commit_files"] = file_state(args.db)
        for name, sql in (
            ("same_connection_count", "SELECT count(*) FROM kv"),
            ("quick_check", "PRAGMA quick_check"),
            ("integrity_check", "PRAGMA integrity_check"),
        ):
            started = time.monotonic()
            try:
                row = connection.execute(sql).fetchone()
            except sqlite3.Error as error:
                operation = {
                    "ok": False,
                    "duration_ms": (time.monotonic() - started) * 1000.0,
                    "error": sqlite_error(error),
                }
                result["errors"].append({"phase": name, **sqlite_error(error)})
            else:
                operation = {
                    "ok": True,
                    "duration_ms": (time.monotonic() - started) * 1000.0,
                    "value": row[0] if row else None,
                }
            result["operations"][name] = operation
    except BaseException as error:
        result["errors"].append({"phase": "setup_or_insert", **sqlite_error(error)})
    finally:
        monitor_stop.set()
        monitor.join(timeout=2.0)
        result["before_close_files"] = file_state(args.db)
        result["ready_utc"] = utc_now()
        atomic_json(args.result, result)
        args.ready.touch(exist_ok=False)

        deadline = time.monotonic() + args.hold_seconds
        while not args.release.exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        result["released_by_runner"] = args.release.exists()
        if connection is not None:
            close_started = time.monotonic()
            try:
                connection.close()
            except sqlite3.Error as error:
                result["operations"]["close"] = {
                    "ok": False,
                    "duration_ms": (time.monotonic() - close_started) * 1000.0,
                    "error": sqlite_error(error),
                }
                result["errors"].append(
                    {"phase": "close", **sqlite_error(error)}
                )
            else:
                result["operations"]["close"] = {
                    "ok": True,
                    "duration_ms": (time.monotonic() - close_started) * 1000.0,
                }
        result["after_close_files"] = file_state(args.db)
        result["finished_utc"] = utc_now()
        atomic_json(args.result, result)

    count = result["operations"].get("same_connection_count", {})
    quick = result["operations"].get("quick_check", {})
    integrity = result["operations"].get("integrity_check", {})
    return 0 if (
        result["operations"].get("commit", {}).get("ok") is True
        and count.get("ok") is True
        and count.get("value") == args.rows
        and quick.get("value") == "ok"
        and integrity.get("value") == "ok"
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
