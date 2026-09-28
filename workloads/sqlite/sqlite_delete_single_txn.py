#!/usr/bin/env python3
"""Create one bounded SQLite database in a single DELETE-journal transaction."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path


def timed(connection: sqlite3.Connection, sql: str, parameters: tuple = ()) -> float:
    started = time.monotonic()
    connection.execute(sql, parameters)
    return (time.monotonic() - started) * 1000


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=2560)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("rows must be positive")

    args.db.parent.mkdir(parents=True, exist_ok=True)
    result: dict = {
        "rows_requested": args.rows,
        "sqlite_version": sqlite3.sqlite_version,
        "timings_ms": {},
    }
    connection = sqlite3.connect(args.db, timeout=120.0, isolation_level=None)
    try:
        connection.execute("PRAGMA page_size=4096")
        result["journal_mode"] = connection.execute(
            "PRAGMA journal_mode=DELETE"
        ).fetchone()[0]
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA cache_size=-2048")
        result["timings_ms"]["vacuum"] = timed(connection, "VACUUM")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB)")
        result["timings_ms"]["insert"] = timed(
            connection,
            "WITH RECURSIVE c(x) AS (VALUES(1) "
            "UNION ALL SELECT x+1 FROM c WHERE x < ?) "
            "INSERT INTO kv(k,v) SELECT x, randomblob(4096) FROM c",
            (args.rows,),
        )
        result["timings_ms"]["commit"] = timed(connection, "COMMIT")
    finally:
        started = time.monotonic()
        connection.close()
        result["timings_ms"]["close"] = (time.monotonic() - started) * 1000

    check = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, timeout=120.0)
    try:
        result["count"] = check.execute("SELECT count(*) FROM kv").fetchone()[0]
        result["integrity_check"] = check.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]
    finally:
        check.close()
    result["main_size"] = args.db.stat().st_size
    args.result.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if (
        result["journal_mode"] == "delete"
        and result["count"] == args.rows
        and result["integrity_check"] == "ok"
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
