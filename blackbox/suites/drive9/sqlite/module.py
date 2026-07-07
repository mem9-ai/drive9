from __future__ import annotations

import sqlite3
from typing import Any

from harness.core import BlackboxError, Context
from suites.drive9._base import Drive9WorkflowBase

# Deterministic workload parameters.
KV_ROWS = 128
COUNTER_COMMIT_ROWS = 5
COUNTER_ROLLBACK_ROWS = 5
UPDATED_PREFIX_COUNT = 10  # rows with key < 'k010' are bulk-updated.
DELETE_KEY = "k127"
K000_VAL_BUMP = 1000  # value added to val for updated rows.
INDEX_NAME = "idx_kv_val"


class Drive9SqliteBlackbox(Drive9WorkflowBase):
    description = (
        "Drive9 FUSE SQLite durability: create a WAL-mode SQLite DB on a "
        "FUSE mount, run a deterministic SQL workload, unmount, remount to a "
        "different directory, and verify the database is consistent and complete."
    )
    timeout = 1200

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)

        # ---- Phase 1: mount, create the SQLite DB, run the SQL workload. ----
        h1 = ctx.target.mount("drive9_sqlite", remote, profile="coding-agent", cache_key="first")
        try:
            expected = self._create_and_populate(ctx, h1.mountpoint / "test.db")
        finally:
            ctx.target.unmount(h1, no_auto_pack=True)

        # ---- Phase 2: remount to a *different* directory and verify. ----
        h2 = ctx.target.mount("drive9_sqlite", remote, profile="coding-agent", cache_key="second")
        try:
            self._verify(ctx, h2.mountpoint / "test.db", expected)
        finally:
            ctx.target.unmount(h2, no_auto_pack=True)

        return {
            "rows_kv": expected["rows_kv"],
            "rows_counter": expected["rows_counter"],
            "wal": expected["wal"],
            "integrity": expected["integrity"],
        }

    # ------------------------------------------------------------------
    # Phase 1
    # ------------------------------------------------------------------

    def _create_and_populate(self, ctx: Context, db_path: Any) -> dict[str, Any]:
        """Create test.db on the FUSE mount, enable WAL, run the SQL workload,
        and return the expected state captured before unmount."""
        # isolation_level=None puts the connection in autocommit mode so we
        # can issue explicit BEGIN/COMMIT/ROLLBACK for the transaction tests
        # without colliding with Python sqlite3's implicit transactions.
        conn = sqlite3.connect(str(db_path), isolation_level=None)
        try:
            # Enable WAL and confirm it sticks.
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise BlackboxError(f"failed to enable WAL mode, got {mode!r}")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Schema.
            conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, val INTEGER, note TEXT)")
            conn.execute("CREATE TABLE counter(id INTEGER PRIMARY KEY, n INTEGER)")

            # Deterministic kv population.
            conn.executemany(
                "INSERT INTO kv(key, val, note) VALUES (?, ?, ?)",
                [
                    (f"k{idx:03d}", idx, f"row-{idx:03d}")
                    for idx in range(KV_ROWS)
                ],
            )

            # Secondary index.
            conn.execute(f"CREATE INDEX {INDEX_NAME} ON kv(val)")

            # Bulk update a bounded prefix; record the affected count.
            cur = conn.execute(
                "UPDATE kv SET val = val + ? WHERE key < ?",
                (K000_VAL_BUMP, f"k{UPDATED_PREFIX_COUNT:03d}"),
            )
            updated = cur.rowcount
            if updated != UPDATED_PREFIX_COUNT:
                raise BlackboxError(
                    f"UPDATE affected {updated} rows, expected {UPDATED_PREFIX_COUNT}"
                )

            # Delete a single known row.
            cur = conn.execute("DELETE FROM kv WHERE key = ?", (DELETE_KEY,))
            deleted = cur.rowcount
            if deleted != 1:
                raise BlackboxError(f"DELETE affected {deleted} rows, expected 1")

            # A committed transaction.
            conn.execute("BEGIN")
            conn.executemany(
                "INSERT INTO counter(n) VALUES (?)",
                [(idx,) for idx in range(COUNTER_COMMIT_ROWS)],
            )
            conn.execute("COMMIT")

            # A rolled-back transaction: rows must NOT persist.
            conn.execute("BEGIN")
            conn.executemany(
                "INSERT INTO counter(n) VALUES (?)",
                [(1000 + idx) for idx in range(COUNTER_ROLLBACK_ROWS)],
            )
            conn.execute("ROLLBACK")

            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise BlackboxError(f"integrity_check before unmount: {integrity!r}")

            expected = self._capture_state(conn)
            expected["wal"] = mode
            expected["integrity"] = integrity
            expected["updated"] = updated
            expected["deleted"] = deleted
            # Force a WAL checkpoint so the main DB file is up to date before
            # the mount goes away. TRUNCATE also reclaims the WAL file.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            return expected
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Phase 2
    # ------------------------------------------------------------------

    def _verify(self, ctx: Context, db_path: Any, expected: dict[str, Any]) -> None:
        """Re-open the DB after remount and assert the state matches phase 1."""
        conn = sqlite3.connect(str(db_path), isolation_level=None)
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if mode != "wal":
                raise BlackboxError(
                    f"journal_mode after remount is {mode!r}, expected 'wal'"
                )

            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise BlackboxError(f"integrity_check after remount: {integrity!r}")

            actual = self._capture_state(conn)

            for field in (
                "rows_kv",
                "rows_counter",
                "sum_val",
                "k000_val",
                "k127_present",
                "min_key",
                "max_key",
            ):
                if actual[field] != expected[field]:
                    raise BlackboxError(
                        f"{field} mismatch after remount: "
                        f"got {actual[field]!r}, expected {expected[field]!r}"
                    )

            # The rolled-back counter rows must be absent.
            rolled_back = conn.execute(
                "SELECT COUNT(*) FROM counter WHERE n >= 1000"
            ).fetchone()[0]
            if rolled_back != 0:
                raise BlackboxError(
                    f"rolled-back counter rows leaked: {rolled_back} found"
                )

            # The secondary index must still be used by the planner.
            plan = conn.execute(
                "EXPLAIN QUERY PLAN SELECT val FROM kv WHERE val = ?",
                (expected["k000_val"],),
            ).fetchall()
            plan_text = " ".join(" ".join(str(c) for c in row) for row in plan)
            if INDEX_NAME not in plan_text:
                raise BlackboxError(
                    f"index {INDEX_NAME} not used by planner after remount; "
                    f"plan={plan_text!r}"
                )
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _capture_state(self, conn: sqlite3.Connection) -> dict[str, Any]:
        rows_kv = conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        rows_counter = conn.execute("SELECT COUNT(*) FROM counter").fetchone()[0]
        sum_val = conn.execute("SELECT COALESCE(SUM(val), 0) FROM kv").fetchone()[0]
        k000_val = conn.execute(
            "SELECT val FROM kv WHERE key = 'k000'"
        ).fetchone()[0]
        k127_present = conn.execute(
            "SELECT COUNT(*) FROM kv WHERE key = ?", (DELETE_KEY,)
        ).fetchone()[0] == 1
        min_key = conn.execute("SELECT MIN(key) FROM kv").fetchone()[0]
        max_key = conn.execute("SELECT MAX(key) FROM kv").fetchone()[0]
        return {
            "rows_kv": rows_kv,
            "rows_counter": rows_counter,
            "sum_val": sum_val,
            "k000_val": k000_val,
            "k127_present": k127_present,
            "min_key": min_key,
            "max_key": max_key,
        }