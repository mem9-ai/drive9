from __future__ import annotations

import ctypes
import os
import sqlite3
import sys
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

# SQLite mmap_size used to exercise mmap of the *main* DB file over a
# FUSE mount. The drive9 FUSE layer opens *.db with FOPEN_DIRECT_IO, so
# unless the kernel negotiated CAP_DIRECT_IO_ALLOW_MMAP, an mmap of the
# main DB returns ENODEV. SQLite itself gracefully falls back to pread on
# ENODEV, so this PRAGMA alone does not surface the failure — we add a
# direct mmap probe (see _mmap_probe) to detect the gap deterministically.
SQLITE_MMAP_SIZE = 4 * 1024 * 1024  # 4 MiB — comfortably above one page.


class Drive9SqliteBlackbox(Drive9WorkflowBase):
    description = (
        "Drive9 FUSE SQLite durability: create a WAL-mode SQLite DB on a "
        "FUSE mount with a non-zero mmap_size, run a deterministic SQL "
        "workload, unmount, remount to a different directory, and verify "
        "the database is consistent, complete, and its main DB file is "
        "mmap-able (exercises FOPEN_DIRECT_IO + CAP_DIRECT_IO_ALLOW_MMAP)."
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
            "mmap_ok": expected["mmap_ok"],
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

            # Request mmap for the main DB file. SQLite honors this by
            # mmap-ing the DB with MAP_SHARED. On a drive9 FUSE mount the
            # main DB is opened FOPEN_DIRECT_IO, so this mmap either
            # succeeds (kernel negotiated CAP_DIRECT_IO_ALLOW_MMAP) or
            # fails with ENODEV (SQLite falls back to pread). We assert
            # the pragma is accepted and probe mmap directly in phase 2.
            mmap_size_row = conn.execute(f"PRAGMA mmap_size={SQLITE_MMAP_SIZE}").fetchone()
            if mmap_size_row[0] != SQLITE_MMAP_SIZE:
                raise BlackboxError(f"mmap_size not accepted: got {mmap_size_row!r}")

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
                [(1000 + idx,) for idx in range(COUNTER_ROLLBACK_ROWS)],
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
            expected["mmap_size"] = mmap_size_row[0]
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
        """Re-open the DB after remount and assert the state matches phase 1,
        plus verify the main DB file is mmap-able over the FUSE mount."""
        # First, close any open SQLite connection before probing mmap so the
        # kernel page cache / mmap does not race with an active handle.
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

        # Direct mmap probe of the main DB file. The main DB is opened with
        # FOPEN_DIRECT_IO by drive9, so an mmap(MAP_SHARED) either succeeds
        # (CAP_DIRECT_IO_ALLOW_MMAP negotiated) or fails with ENODEV. This is
        # the deterministic check that the fix is in effect; SQLite's own
        # graceful ENODEV→pread fallback hides the failure otherwise.
        mmap_ok = self._mmap_probe(db_path)
        if not mmap_ok:
            raise BlackboxError(
                "mmap of main DB file over FUSE mount failed (ENODEV). "
                "The FUSE mount did not negotiate CAP_DIRECT_IO_ALLOW_MMAP, "
                "so FOPEN_DIRECT_IO files cannot be mmap-ed. See the "
                "go-fuse MountOptions.EnableDirectIoMmap / drive9 "
                "newGoFuseMountOptions fix."
            )
        expected["mmap_ok"] = mmap_ok

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

    def _mmap_probe(self, db_path: Any) -> bool:
        """Attempt a read-only MAP_SHARED mmap of the main DB file.

        Returns True if the mmap succeeds, False if the kernel rejects it
        with ENODEV (the FOPEN_DIRECT_IO + no CAP_DIRECT_IO_ALLOW_MMAP
        failure mode). Any other error is re-raised so we do not mask
        unrelated failures.
        """
        path = os.fsencode(os.fspath(db_path))
        size = os.path.getsize(os.fspath(db_path))
        if size == 0:
            return False
        libc = _libc()
        # O_RDONLY = 0 on POSIX.
        fd = libc.open(path, 0)
        if fd < 0:
            err = ctypes.get_errno()
            raise BlackboxError(f"open({path!r}) failed: errno={err}")
        try:
            # PROT_READ = 1, MAP_SHARED = 1.
            addr = libc.mmap(None, size, 1, 1, fd, 0)
            if addr is None or addr == ctypes.c_void_p(-1).value:
                err = ctypes.get_errno()
                # ENODEV (19 on Linux, 6 on macOS) is the target failure.
                if err in (19, 6):
                    return False
                raise BlackboxError(f"mmap({db_path!r}) failed: errno={err}")
            # Read the SQLite header magic to confirm the mapping is usable.
            # A valid SQLite DB file starts with "SQLite format 3\0".
            try:
                buf = (ctypes.c_char * 16).from_address(addr)
                if buf.value[:15] != b"SQLite format 3":
                    raise BlackboxError(
                        f"mmap succeeded but header magic mismatch: {bytes(buf.value)!r}"
                    )
            finally:
                libc.munmap(addr, size)
            return True
        finally:
            libc.close(fd)


def _libc() -> ctypes.CDLL:
    """Return libc with errno propagation enabled."""
    if sys.platform == "darwin":
        lib = ctypes.CDLL("libc.dylib", use_errno=True)
    else:
        lib = ctypes.CDLL("libc.so.6", use_errno=True)
    # prototypes so the calls behave across platforms.
    lib.open.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.open.restype = ctypes.c_int
    lib.close.argtypes = [ctypes.c_int]
    lib.close.restype = ctypes.c_int
    lib.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_longlong]
    lib.mmap.restype = ctypes.c_void_p
    lib.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.munmap.restype = ctypes.c_int
    return lib