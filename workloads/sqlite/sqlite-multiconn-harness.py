#!/usr/bin/env python3
"""Drive9 validation harness: port of official SQLite multi-connection cases.

Sources (sqlite/sqlite trunk, test/):
  wal.test           wal-10.*  (multi-connection snapshot / locking)
  walthread.test     walthread-1 (10 R/W threads + checkpointer thread)
                     walthread-4 (1 reader + 1 writer, frame-count checkpoint)
  waloverwrite.test  1.*       (WAL recycle + db/wal recovery)
  wal5.test          1.*, 2.*  (blocking checkpoint, PRAGMA variant; attached dbs)
  walro.test         1.1.*, 1.2.* (read-only connection with readonly_shm)

Methodology: run each case twice - once against an ext4 control path and once
against the Drive9 FUSE mount - and compare transcripts. Deterministic cases
must match exactly; stress cases compare invariants only. This keeps SQLite
version differences out of the signal.

Not ported (needs C API / VFS fault injection unavailable in Python sqlite3):
  wal-10.28-30 (held sqlite3_prepare/step statement)
  walrestart.test, pendingrace.test, snapshot*.test, walcrash*.test

Usage:
  python3 sqlite-multiconn-harness.py run --db PATH --case NAME \
      --label L --output-dir DIR [--seconds N]
  python3 sqlite-multiconn-harness.py compare --control A.json --mount B.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import shutil
import sqlite3
import sys
import threading
import time
from pathlib import Path

STRESS_CASES = {"walthread1", "walthread4"}


def md5_hex(value) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if not isinstance(value, bytes):
        value = bytes(value)
    return hashlib.md5(value).hexdigest()


def connect(path: str, timeout: float = 5.0) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=timeout, isolation_level=None)
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def rows(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.execute(sql, params)
    return [tuple(r) for r in cur.fetchall()]


def scalar(conn: sqlite3.Connection, sql: str):
    out = rows(conn, sql)
    return out[0][0] if out and out[0] else None


def page_size(conn: sqlite3.Connection) -> int:
    return int(scalar(conn, "PRAGMA page_size"))


def db_pages(conn: sqlite3.Connection) -> int:
    return int(scalar(conn, "PRAGMA page_count"))


def wal_pages(db_path: str, pgsz: int) -> int:
    wal = db_path + "-wal"
    if not os.path.exists(wal):
        return 0
    size = os.path.getsize(wal)
    if size < 32:
        return 0
    return (size - 32) // (pgsz + 24)


def checkpoint(conn: sqlite3.Connection, mode: str = ""):
    sql = "PRAGMA wal_checkpoint"
    if mode:
        sql += "(" + mode + ")"
    out = rows(conn, sql)
    return tuple(out[0]) if out and out[0] else None


def integrity_ok(conn: sqlite3.Connection) -> bool:
    out = rows(conn, "PRAGMA integrity_check")
    return bool(out) and out[0] and out[0][0] == "ok"


def try_exec(conn: sqlite3.Connection, sql: str):
    """Return ("ok", rows) or ("err", message)."""
    try:
        return "ok", rows(conn, sql)
    except sqlite3.OperationalError as exc:
        return "err", str(exc)


class _Md5SumAgg:
    """Port of SQLite's md5sum() aggregate (not built into system sqlite3)."""

    def __init__(self):
        self.digest = hashlib.md5()

    def step(self, value) -> None:
        if value is None:
            return
        if isinstance(value, (bytes, bytearray, memoryview)):
            self.digest.update(bytes(value))
        else:
            self.digest.update(str(value).encode("utf-8"))

    def finalize(self) -> str:
        return self.digest.hexdigest()


def register_md5sum(conn: sqlite3.Connection) -> None:
    conn.create_aggregate("md5sum", 1, _Md5SumAgg)


def emit(event: dict) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def result_path(output_dir: str, label: str) -> str:
    return os.path.join(output_dir, label + ".result.json")


# --------------------------------------------------------------------------
# wal-10.* : three connections, snapshot isolation and locking.
# --------------------------------------------------------------------------

def case_wal10(db_path: str) -> dict:
    steps = []

    def chk(name, got):
        steps.append({"step": name, "value": got})

    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)

    db = connect(db_path, timeout=0.0)
    db2 = connect(db_path, timeout=5.0)
    db3 = connect(db_path, timeout=5.0)

    jm = scalar(db, "PRAGMA journal_mode = wal")
    db.execute("PRAGMA auto_vacuum = 0")
    db.execute("CREATE TABLE t1(a, b)")
    db.execute("INSERT INTO t1 VALUES(1, 2)")
    chk("setup", {"journal_mode": jm, "rows": rows(db, "SELECT * FROM t1")})

    db.execute("BEGIN")
    db.execute("INSERT INTO t1 VALUES(3, 4)")
    chk("reader.sees.prewrite.snapshot", rows(db2, "SELECT * FROM t1"))
    db.execute("COMMIT")
    chk("reader.sees.committed", rows(db2, "SELECT * FROM t1"))

    db2.execute("BEGIN")
    chk("db2.read.txn", rows(db2, "SELECT * FROM t1"))
    db.execute("INSERT INTO t1 VALUES(5, 6)")
    chk("db2.snapshot.unchanged", rows(db2, "SELECT * FROM t1"))
    chk("db3.sees.newest", rows(db3, "SELECT * FROM t1"))
    db2.execute("COMMIT")

    db2.execute("BEGIN")
    db2.execute("INSERT INTO t1 VALUES(7, 8)")
    status, msg = try_exec(db, "INSERT INTO t1 VALUES(9, 10)")
    chk("writer.locked", {"status": status, "is_locked": "locked" in msg})

    db.execute("BEGIN")
    chk("db.old.snapshot", rows(db, "SELECT * FROM t1"))
    db2.execute("COMMIT")
    status, msg = try_exec(db, "INSERT INTO t1 VALUES(9, 10)")
    chk("writer.locked.on.old.snapshot", {"status": status, "is_locked": "locked" in msg})
    db.execute("COMMIT")
    db.execute("BEGIN")
    db.execute("INSERT INTO t1 VALUES(9, 10)")
    db.execute("COMMIT")
    chk("db.writes.latest", rows(db, "SELECT * FROM t1"))

    db2.execute("BEGIN")
    chk("db2.read.1", rows(db2, "SELECT * FROM t1"))
    chk("ckpt.reader.nonblocking", checkpoint(db))
    db.execute("INSERT INTO t1 VALUES(11, 12)")
    chk("db2.snapshot.2", rows(db2, "SELECT * FROM t1"))
    chk("ckpt.writer.nonblocking", checkpoint(db))
    db2.execute("COMMIT")
    db2.execute("BEGIN")
    chk("db2.read.3", rows(db2, "SELECT * FROM t1"))
    chk("ckpt.a", checkpoint(db))
    chk("ckpt.b", checkpoint(db))
    db3.execute("BEGIN")
    chk("db3.read", rows(db3, "SELECT * FROM t1"))
    status, _ = try_exec(db, "INSERT INTO t1 VALUES(13, 14)")
    chk("db.write.while.db3.reads", status)
    chk("db.rows.after.write", rows(db, "SELECT * FROM t1"))
    db3.execute("COMMIT")
    db2.execute("COMMIT")
    chk("db.rows.final1", rows(db, "SELECT * FROM t1"))
    chk("ckpt.full1", checkpoint(db))
    db2.execute("BEGIN")
    chk("db2.read.4", rows(db2, "SELECT * FROM t1"))
    chk("ckpt.full2", checkpoint(db))
    status, _ = try_exec(db, "INSERT INTO t1 VALUES(15, 16)")
    chk("db.write.ok", status)
    status, _ = try_exec(db3, "INSERT INTO t1 VALUES(17, 18)")
    chk("db3.write.ok", status)

    # wal-10.31-34: reader that fails to upgrade must release its write locks.
    db2.execute("COMMIT")
    db.execute("BEGIN")
    chk("db.read.5", rows(db, "SELECT * FROM t1"))
    status, _ = try_exec(db2, "INSERT INTO t1 VALUES(21, 22)")
    chk("db2.write.while.db.reads", status)
    status, msg = try_exec(db, "INSERT INTO t1 VALUES(23, 24)")
    chk("db.upgrade.locked", {"status": status, "is_locked": "locked" in msg})
    status, _ = try_exec(db2, "INSERT INTO t1 VALUES(23, 24)")
    chk("db2.write.after.upgrade.fail", status)
    chk("db.snapshot.6", rows(db, "SELECT * FROM t1"))
    db.execute("COMMIT")
    chk("db.rows.final2", rows(db, "SELECT * FROM t1"))

    # wal-10.35-37: a busy checkpointer releases all locks.
    db.execute("DELETE FROM t1")
    db.execute("INSERT INTO t1 VALUES('a', 'b')")
    db.execute("INSERT INTO t1 VALUES('c', 'd')")
    db2.execute("BEGIN")
    chk("db2.read.7", rows(db2, "SELECT * FROM t1"))
    chk("ckpt.busy.releases", checkpoint(db))
    status, _ = try_exec(db3, "INSERT INTO t1 VALUES('e', 'f')")
    chk("db3.write.8", status)
    chk("db2.snapshot.8", rows(db2, "SELECT * FROM t1"))
    db2.execute("COMMIT")
    chk("ckpt.final", checkpoint(db))
    chk("integrity", integrity_ok(db))

    db.close()
    db2.close()
    db3.close()
    return {"case": "wal10", "transcript": steps}


# --------------------------------------------------------------------------
# walthread-1: 10 R/W workers + one checkpointer worker, threads and processes.
# --------------------------------------------------------------------------

def _walthread1_init(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)
    conn = connect(db_path)
    register_md5sum(conn)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("CREATE TABLE t1(x PRIMARY KEY)")
    conn.execute("INSERT INTO t1 VALUES(?)", (os.urandom(100),))
    conn.execute("INSERT INTO t1 VALUES(?)", (os.urandom(100),))
    conn.execute("INSERT INTO t1 SELECT md5sum(x) FROM t1")
    conn.close()


def _walthread1_worker(db_path: str, stop, errors: list, counters: list) -> None:
    conn = connect(db_path, timeout=60.0)
    register_md5sum(conn)
    conn.execute("PRAGMA wal_autocheckpoint = 0")
    n_read = n_write = 0
    try:
        while not stop.is_set():
            conn.execute("BEGIN")
            m1 = scalar(conn, "SELECT md5sum(x) FROM t1 WHERE rowid != (SELECT max(rowid) FROM t1)")
            last = scalar(conn, "SELECT x FROM t1 WHERE rowid = (SELECT max(rowid) FROM t1)")
            m2 = scalar(conn, "SELECT md5sum(x) FROM t1 WHERE rowid != (SELECT max(rowid) FROM t1)")
            ik = integrity_ok(conn)
            conn.execute("COMMIT")
            if not (m1 == last == m2):
                errors.append("snapshot mismatch")
                return
            if not ik:
                errors.append("integrity check failed")
                return
            n_read += 1
            conn.execute("BEGIN")
            conn.execute("INSERT INTO t1 VALUES(?)", (os.urandom(101),))
            conn.execute("INSERT INTO t1 VALUES(?)", (os.urandom(101),))
            conn.execute("INSERT INTO t1 SELECT md5sum(x) FROM t1")
            conn.execute("COMMIT")
            n_write += 1
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc) and "busy" not in str(exc):
            errors.append(str(exc))
    finally:
        counters.extend([n_read, n_write])
        conn.close()


def _walthread1_checkpointer(db_path: str, stop, counters: list) -> None:
    conn = connect(db_path, timeout=60.0)
    n = 0
    try:
        while not stop.is_set():
            conn.execute("PRAGMA wal_checkpoint")
            time.sleep(0.0005)
            n += 1
    except sqlite3.OperationalError:
        pass
    finally:
        counters.append(n)
        conn.close()


def _walthread1_run(db_path: str, seconds: float, use_processes: bool) -> dict:
    errors: list = []
    read_total = write_total = ckpt_total = 0
    workers = []
    manager = multiprocessing.Manager()
    shared_errors = manager.list() if use_processes else []
    for _ in range(10):
        if use_processes:
            counters = manager.list()
            stop = multiprocessing.Event()
            p = multiprocessing.Process(
                target=_walthread1_worker, args=(db_path, stop, shared_errors, counters)
            )
        else:
            stop = threading.Event()
            counters = []
            p = threading.Thread(
                target=_walthread1_worker, args=(db_path, stop, errors, counters)
            )
        workers.append((p, stop, counters))
    ckpt_stop = threading.Event()
    ckpt_counters = []
    ckpt_thread = threading.Thread(
        target=_walthread1_checkpointer, args=(db_path, ckpt_stop, ckpt_counters)
    )
    for p, _, _ in workers:
        p.start()
    ckpt_thread.start()
    time.sleep(seconds)
    for _, stop, _ in workers:
        stop.set()
    ckpt_stop.set()
    for p, _, _ in workers:
        p.join(timeout=seconds + 60)
    ckpt_thread.join(timeout=60)
    for _, _, counters in workers:
        if counters is not None and len(counters) >= 2:
            read_total += int(counters[0])
            write_total += int(counters[1])
    ckpt_total = ckpt_counters[0] if ckpt_counters else 0
    if use_processes:
        errors.extend(list(shared_errors))
        manager.shutdown()
    final = connect(db_path)
    register_md5sum(final)
    ok = integrity_ok(final)
    final.close()
    mode = "processes" if use_processes else "threads"
    return {
        "mode": mode,
        "reads": read_total,
        "writes": write_total,
        "checkpoints": ckpt_total,
        "errors": sorted(set(errors)),
        "integrity_ok": ok,
    }


def case_walthread1(db_path: str, seconds: float) -> dict:
    _walthread1_init(db_path)
    return {
        "case": "walthread1",
        "observations": [
            _walthread1_run(db_path, seconds, False),
            _walthread1_run(db_path, seconds, True),
        ],
    }


# --------------------------------------------------------------------------
# walthread-4: one pure reader + one writer that checkpoints on frame count.
# --------------------------------------------------------------------------

def case_walthread4(db_path: str, seconds: float) -> dict:
    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)
    conn = connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)")
    conn.close()
    pgsz = page_size(connect(db_path))
    stop = threading.Event()
    reader_errs: list = []
    writer_errs: list = []
    busy_count = [0]
    ckpt_count = [0]

    def reader():
        c = connect(db_path, timeout=0.0)
        try:
            while not stop.is_set():
                try:
                    if not integrity_ok(c):
                        reader_errs.append("integrity check failed")
                        return
                except sqlite3.OperationalError as exc:
                    busy_count[0] += 1
        finally:
            c.close()

    def writer():
        c = connect(db_path, timeout=0.0)
        row = 1
        try:
            while not stop.is_set():
                try:
                    c.execute("REPLACE INTO t1 VALUES(?, ?)", (row, os.urandom(300)))
                    row = 1 if row == 10 else row + 1
                    if wal_pages(db_path, pgsz) > 15:
                        c.execute("PRAGMA wal_checkpoint")
                        ckpt_count[0] += 1
                except sqlite3.OperationalError as exc:
                    busy_count[0] += 1
        finally:
            c.close()

    rt = threading.Thread(target=reader)
    wt = threading.Thread(target=writer)
    rt.start()
    wt.start()
    time.sleep(seconds)
    stop.set()
    rt.join(timeout=60)
    wt.join(timeout=60)
    final = connect(db_path)
    ok = integrity_ok(final)
    final.close()
    return {
        "case": "walthread4",
        "observations": {
            "integrity_ok": ok,
            "checkpoints": ckpt_count[0],
            "reader_busy": busy_count[0],
            "errors": sorted(set(reader_errs + writer_errs)),
        },
    }


# --------------------------------------------------------------------------
# waloverwrite 1.*: WAL page recycle + db/wal recovery (page_size 1024).
# --------------------------------------------------------------------------

def _waloverwrite_once(db_path: str, tn: int) -> dict:
    steps = []

    def chk(name, got):
        steps.append({"step": name, "value": got})

    os.path.exists(db_path) and os.unlink(db_path)
    for suffix in ("-wal", "-shm", "-journal"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)

    db = connect(db_path)
    db.execute("PRAGMA page_size = 1024")
    db.execute("CREATE TABLE t1(x, y)")
    db.execute("CREATE TABLE t2(x, y)")
    db.execute("CREATE INDEX i1y ON t1(y)")
    for i in range(1, 21):
        db.execute("INSERT INTO t1 VALUES(?, ?)", (i, os.urandom(800)))
    db.close()

    db = connect(db_path)
    pg0 = db_pages(db)
    chk("page_count.range", pg0)
    db.execute("PRAGMA journal_mode = wal")
    db.execute("PRAGMA cache_size = 5")
    if tn == 2:
        db.execute("UPDATE t1 SET y = ? WHERE x=4", (os.urandom(799),))
    db.execute("BEGIN")
    for _ in range(5):
        for x in rows(db, "SELECT x FROM t1"):
            db.execute("UPDATE t1 SET y = ? WHERE x=?", (os.urandom(799), x[0]))
    db.execute("COMMIT")
    frames1 = wal_pages(db_path, 1024)
    chk("frames.round1.range", frames1)
    chk("integrity.1", integrity_ok(db))

    db2_path = db_path + ".copy"
    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db2_path + suffix
        os.path.exists(p) and os.unlink(p)
    shutil.copyfile(db_path, db2_path)
    db2 = connect(db2_path)
    chk("recover.db.only.sum", scalar(db2, "SELECT sum(length(y)) FROM t1"))
    db2.close()
    shutil.copyfile(db_path, db2_path)
    shutil.copyfile(db_path + "-wal", db2_path + "-wal")
    db2 = connect(db2_path)
    chk("recover.db.wal.sum", scalar(db2, "SELECT sum(length(y)) FROM t1"))
    chk("recover.integrity", integrity_ok(db2))
    db2.close()

    db.execute("PRAGMA wal_checkpoint")
    db.execute("BEGIN")
    for x in rows(db, "SELECT x FROM t1"):
        db.execute("UPDATE t1 SET y = ? WHERE x=?", (os.urandom(798), x[0]))
    for i in range(1, 21):
        db.execute("INSERT INTO t2 VALUES(?, ?)", (i, os.urandom(800)))
    db.execute("SAVEPOINT abc")
    for _ in range(5):
        for x in rows(db, "SELECT x FROM t1"):
            db.execute("UPDATE t1 SET y = ? WHERE x=?", (os.urandom(797), x[0]))
    db.execute("ROLLBACK TO abc")
    db.execute("COMMIT")
    frames2 = wal_pages(db_path, 1024)
    chk("frames.round2.range", frames2)

    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db2_path + suffix
        os.path.exists(p) and os.unlink(p)
    shutil.copyfile(db_path, db2_path)
    db2 = connect(db2_path)
    chk("recover2.db.only.sum", scalar(db2, "SELECT sum(length(y)) FROM t1"))
    db2.close()
    shutil.copyfile(db_path + "-wal", db2_path + "-wal")
    db2 = connect(db2_path)
    chk("recover2.db.wal.sum", scalar(db2, "SELECT sum(length(y)) FROM t1"))
    chk("recover2.integrity", integrity_ok(db2))
    db2.close()
    db.close()
    return {"tn": tn, "transcript": steps}


def case_waloverwrite(db_path: str) -> dict:
    return {
        "case": "waloverwrite",
        "transcript": [
            _waloverwrite_once(db_path + ".tn1", 1),
            _waloverwrite_once(db_path + ".tn2", 2),
        ],
    }


# --------------------------------------------------------------------------
# wal5 (PRAGMA variant): blocking checkpoint, single and attached databases.
# --------------------------------------------------------------------------

def case_wal5(db_path: str) -> dict:
    steps = []

    def chk(name, got):
        steps.append({"step": name, "value": got})

    db = connect(db_path, timeout=0.05)
    db2 = connect(db_path, timeout=0.05)
    db3 = connect(db_path, timeout=0.05)
    db.execute("PRAGMA page_size = 1024")
    db.execute("PRAGMA auto_vacuum = 0")
    db.execute("CREATE TABLE t1(x, y)")
    db.execute("PRAGMA journal_mode = WAL")
    for i in (1, 2, 3):
        db.execute("INSERT INTO t1 VALUES(?, zeroblob(1200))", (i,))
    chk("setup.pages", [db_pages(db), wal_pages(db_path, 1024)])

    db2.execute("BEGIN")
    chk("db2.read", rows(db2, "SELECT x FROM t1"))
    chk("ckpt.passive", checkpoint(db))
    db.execute("INSERT INTO t1 VALUES(4, zeroblob(1200))")
    chk("write.while.reader", [db_pages(db), wal_pages(db_path, 1024)])

    # Blocking checkpoint: reader commits mid-checkpoint from another thread.
    result_holder = {}

    def do_restart():
        # Busy-wait generously: the reader commits at a fixed 0.2s delay and
        # the outcome must not depend on mount latency.
        c = connect(db_path, timeout=10.0)
        result_holder["ckpt"] = checkpoint(c, "RESTART")
        c.close()

    t = threading.Thread(target=do_restart)
    t.start()
    time.sleep(0.2)
    db2.execute("COMMIT")
    t.join(timeout=30)
    chk("ckpt.restart", result_holder.get("ckpt"))
    chk("pages.after.restart", [db_pages(db), wal_pages(db_path, 1024)])
    db.execute("INSERT INTO t1 VALUES(5, zeroblob(1200))")
    chk("pages.after.write2", [db_pages(db), wal_pages(db_path, 1024)])
    chk("integrity", integrity_ok(db))

    # Attached-database checkpoints.
    for c in (db, db2, db3):
        c.close()
    for suffix in ("", "-wal", "-shm", "-journal", ".aux", ".aux-wal", ".aux-shm"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)
    db = connect(db_path, timeout=0.05)
    db2 = connect(db_path, timeout=0.05)
    db3 = connect(db_path, timeout=0.05)
    aux_path = db_path + ".aux"
    for c in (db, db2, db3):
        c.execute("ATTACH ? AS aux", (aux_path,))
    db.execute("PRAGMA aux.auto_vacuum = 0")
    db.execute("PRAGMA main.auto_vacuum = 0")
    db.execute("PRAGMA main.page_size = 1024")
    db.execute("PRAGMA main.journal_mode = WAL")
    db.execute("PRAGMA aux.page_size = 1024")
    db.execute("PRAGMA aux.journal_mode = WAL")
    db.execute("CREATE TABLE t1(a, b)")
    db.execute("INSERT INTO t1 VALUES(1, 2)")
    db.execute("CREATE TABLE aux.t2(a, b)")
    db.execute("INSERT INTO t2 VALUES(1, 2)")
    aux_pages = lambda: int(scalar(db, "PRAGMA aux.page_count"))
    chk("attached.pages.before", [
        db_pages(db), wal_pages(db_path, 1024), aux_pages(), wal_pages(aux_path, 1024)
    ])
    chk("attached.ckpt", checkpoint(db))
    chk("attached.pages.after", [
        db_pages(db), wal_pages(db_path, 1024), aux_pages(), wal_pages(aux_path, 1024)
    ])
    db.execute("INSERT INTO t2 VALUES(3, 4)")
    db2.execute("BEGIN")
    chk("attached.db2.read", rows(db2, "SELECT * FROM t1"))
    chk("attached.ckpt.restart.reader", checkpoint(db, "RESTART"))
    chk("attached.pages.final", [
        db_pages(db), wal_pages(db_path, 1024), aux_pages(), wal_pages(aux_path, 1024)
    ])
    chk("attached.integrity.main", integrity_ok(db))

    for c in (db, db2, db3):
        c.close()
    return {"case": "wal5", "transcript": steps}


# --------------------------------------------------------------------------
# walro 1.1.* / 1.2.*: read-only connection with readonly_shm=1.
# --------------------------------------------------------------------------

def case_walro(db_path: str) -> dict:
    steps = []

    def chk(name, got):
        steps.append({"step": name, "value": got})

    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db_path + suffix
        os.path.exists(p) and os.unlink(p)

    db2 = connect(db_path)
    db2.execute("PRAGMA auto_vacuum = 0")
    db2.execute("PRAGMA journal_mode = WAL")
    db2.execute("CREATE TABLE t1(x, y)")
    db2.execute("INSERT INTO t1 VALUES('a', 'b')")
    # A second RW connection opened after WAL mode forces the wal-index into
    # -shm, which readonly_shm=1 needs to read un-checkpointed frames.
    db3 = connect(db_path)
    db3.execute("SELECT * FROM t1")
    chk("shm.exists", os.path.exists(db_path + "-shm"))

    ro_uri = "file:" + db_path + "?readonly_shm=1"
    db = sqlite3.connect(ro_uri, uri=True, isolation_level=None)
    chk("ro.read.1", rows(db, "SELECT * FROM t1"))
    db2.execute("INSERT INTO t1 VALUES('c', 'd')")
    chk("ro.read.2", rows(db, "SELECT * FROM t1"))
    status, msg = try_exec(db, "INSERT INTO t1 VALUES('e', 'f')")
    chk("ro.write.denied", {"status": status, "readonly": "readonly" in msg})
    status, msg = try_exec(db, "PRAGMA wal_checkpoint")
    chk("ro.checkpoint.denied", {"status": status, "readonly": "readonly" in msg})
    db2.execute("INSERT INTO t1 VALUES('e', 'f')")
    chk("ro.read.3", rows(db, "SELECT * FROM t1"))
    db2.execute("INSERT INTO t1 VALUES('g', 'h')")
    db2.execute("PRAGMA wal_checkpoint")
    chk("ro.read.4", rows(db, "SELECT * FROM t1"))
    db2.execute("INSERT INTO t1 VALUES('i', 'j')")
    db2.close()
    db.close()
    chk("sidecars.exist", [os.path.exists(db_path + "-wal"), os.path.exists(db_path + "-shm")])

    db = sqlite3.connect(ro_uri, uri=True, isolation_level=None)
    chk("reopen.ro.read", rows(db, "SELECT * FROM t1"))
    db2 = connect(db_path)
    chk("rw.read", rows(db2, "SELECT * FROM t1"))
    db2.execute("PRAGMA wal_checkpoint")
    db2.execute("INSERT INTO t1 VALUES('k', 'l')")
    chk("ro.read.after.ckpt", rows(db, "SELECT * FROM t1"))
    db.close()
    db2.close()
    db3.close()
    return {"case": "walro", "transcript": steps}


CASES = {
    "wal10": case_wal10,
    "walthread1": case_walthread1,
    "walthread4": case_walthread4,
    "waloverwrite": case_waloverwrite,
    "wal5": case_wal5,
    "walro": case_walro,
}


def run_one(case: str, db_path: str, seconds: float) -> dict:
    fn = CASES[case]
    if case in ("walthread1", "walthread4"):
        return fn(db_path, seconds)
    return fn(db_path)


def do_run(args) -> int:
    os.makedirs(args.output_dir, exist_ok=True)
    emit({"event": "case_started", "case": args.case, "label": args.label})
    started = time.monotonic()
    try:
        result = run_one(args.case, args.db, args.seconds)
        result["label"] = args.label
        result["db"] = args.db
        result["duration_s"] = round(time.monotonic() - started, 3)
        result["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - harness must capture everything
        result = {
            "case": args.case,
            "label": args.label,
            "db": args.db,
            "duration_s": round(time.monotonic() - started, 3),
            "status": "error",
            "error": repr(exc),
        }
    out = result_path(args.output_dir, args.label)
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
        fh.write("\n")
    emit({"event": "case_complete", "case": args.case, "label": args.label,
          "status": result["status"], "result_path": out})
    return 0 if result["status"] == "ok" else 1


def load_json(path: str) -> dict:
    with open(path) as fh:
        return json.load(fh)


def do_compare(args) -> int:
    control = load_json(args.control)
    mount = load_json(args.mount)
    if control["case"] != mount["case"]:
        print("case mismatch: %s vs %s" % (control["case"], mount["case"]))
        return 1
    case = control["case"]
    if case in STRESS_CASES:
        c = control["observations"]
        m = mount["observations"]
        c_entries = c if isinstance(c, list) else [c]
        m_entries = m if isinstance(m, list) else [m]
        diffs = []
        for i, (ce, me) in enumerate(zip(c_entries, m_entries)):
            if ce.get("integrity_ok") != me.get("integrity_ok"):
                diffs.append("mode%d.integrity_ok" % i)
            if ce.get("errors"):
                diffs.append("mode%d.control_errors" % i)
            if me.get("errors"):
                diffs.append("mode%d.mount_errors" % i)
            if (ce.get("checkpoints", 0) or 0) <= 0 or (me.get("checkpoints", 0) or 0) <= 0:
                diffs.append("mode%d.checkpoints" % i)
        verdict = not diffs
        note = "stress invariants compared (counts are timing-dependent)"
    else:
        diffs = []
        def flatten(prefix, node, acc):
            if isinstance(node, dict):
                for k, v in node.items():
                    flatten(prefix + [str(k)], v, acc)
            else:
                acc.append((prefix, node))
        c_flat, m_flat = [], []
        flatten([], control["transcript"], c_flat)
        flatten([], mount["transcript"], m_flat)
        cmap = {tuple(p): v for p, v in c_flat}
        mmap = {tuple(p): v for p, v in m_flat}
        diffs = [p for p in sorted(set(cmap) | set(mmap)) if cmap.get(p) != mmap.get(p)]
        verdict = not diffs
        note = "deterministic transcript compared exactly"
    report = {
        "case": case,
        "verdict": "pass" if verdict else "fail",
        "diffs": [list(p) for p in diffs],
        "note": note,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if verdict else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run")
    run_p.add_argument("--db", required=True)
    run_p.add_argument("--case", required=True, choices=sorted(CASES))
    run_p.add_argument("--label", required=True)
    run_p.add_argument("--output-dir", required=True)
    run_p.add_argument("--seconds", type=float, default=5.0)
    run_p.set_defaults(func=do_run)

    cmp_p = sub.add_parser("compare")
    cmp_p.add_argument("--control", required=True)
    cmp_p.add_argument("--mount", required=True)
    cmp_p.set_defaults(func=do_compare)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
