#!/bin/sh
# drive9/fuse/sqlite-wal-remount: SQLite rollback + WAL + 1000 sequential synchronous=FULL
# commits + churn + concurrent WAL readers/writer + fresh remount & remote snapshot integrity.
set -eu

COMMITS="${FUSE_SQLITE_COMMITS:-1000}"
ROWS="${FUSE_SQLITE_ROWS:-64}"
HELPER="$RUN_DIR/sqlite_suite.py"
export COMMITS ROWS HELPER

cat > "$HELPER" <<'PYEOF'
import hashlib
import json
import os
import sqlite3
import sys
import threading
import time

MOUNT = os.environ["MOUNT"]
RUN_DIR = os.environ["RUN_DIR"]
COMMITS = int(os.environ.get("COMMITS", "1000"))
ROWS = int(os.environ.get("ROWS", "64"))
STATE = os.path.join(RUN_DIR, "sqlite_expected.json")


def db_fingerprint(db_path, table):
    con = sqlite3.connect(db_path, timeout=30.0)
    try:
        ic = con.execute("PRAGMA integrity_check").fetchone()[0]
        assert ic == "ok", f"{db_path} integrity_check={ic}"
        h = hashlib.sha256()
        cnt = 0
        for row in con.execute(f"SELECT * FROM {table} ORDER BY id"):
            h.update(repr(row).encode("utf-8") + b"\n")
            cnt += 1
        return cnt, h.hexdigest()
    finally:
        con.close()


mode = sys.argv[1]
work = os.path.join(MOUNT, "sqlite-suite")

if mode == "write-all":
    os.makedirs(work, exist_ok=True)
    expected = {}

    # 1. Rollback-journal CRUD + index + delete + update + VACUUM
    rb_db = os.path.join(work, "rollback.db")
    con = sqlite3.connect(rb_db, timeout=30.0)
    con.execute("PRAGMA journal_mode=DELETE")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, k TEXT UNIQUE, body BLOB)")
    con.execute("CREATE INDEX idx_items_k ON items(k)")
    with con:
        for i in range(ROWS):
            con.execute("INSERT INTO items(id, k, body) VALUES(?, ?, ?)", (i, f"key-{i:04d}", os.urandom(512)))
    with con:
        con.execute("DELETE FROM items WHERE id % 5 = 0")
        con.execute("UPDATE items SET k = k || '-upd' WHERE id % 3 = 0")
    con.execute("VACUUM")
    con.close()
    expected["rollback.db"] = {"table": "items", "fp": db_fingerprint(rb_db, "items")}

    # 2. Sequential WAL commit sequence (1000 independent synchronous=FULL transactions)
    seq_db = os.path.join(work, "wal_commits.db")
    con = sqlite3.connect(seq_db, timeout=30.0, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA wal_autocheckpoint=100")
    con.execute("CREATE TABLE seq_log(id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, digest TEXT NOT NULL)")
    for s in range(1, COMMITS + 1):
        d = hashlib.sha256(f"commit-{s}".encode()).hexdigest()
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT INTO seq_log(id, seq, digest) VALUES(?, ?, ?)", (s, s, d))
        con.execute("COMMIT")
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    cnt, fp = db_fingerprint(seq_db, "seq_log")
    assert cnt == COMMITS, (cnt, COMMITS)
    expected["wal_commits.db"] = {"table": "seq_log", "fp": (cnt, fp)}

    # 3. Concurrent WAL single-writer + multi-reader + churn DB rewrites
    conc_db = os.path.join(work, "wal_concurrent.db")
    con = sqlite3.connect(conc_db, timeout=30.0)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    con.close()

    stop = threading.Event()
    errs = []

    def reader_worker():
        while not stop.is_set():
            try:
                rc = sqlite3.connect(conc_db, timeout=30.0)
                rc.execute("SELECT COUNT(*) FROM events").fetchone()
                rc.close()
            except Exception as e:
                errs.append(repr(e))
            time.sleep(0.01)

    readers = [threading.Thread(target=reader_worker, daemon=True) for _ in range(3)]
    for t in readers:
        t.start()
    wcon = sqlite3.connect(conc_db, timeout=30.0)
    for i in range(1, 41):
        with wcon:
            wcon.execute("INSERT INTO events(id, payload) VALUES(?, ?)", (i, f"ev-{i:03d}-" + ("x" * 128)))
    wcon.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    wcon.close()
    stop.set()
    for t in readers:
        t.join(5.0)
    assert not errs, f"concurrent WAL reader errors: {errs}"
    expected["wal_concurrent.db"] = {"table": "events", "fp": db_fingerprint(conc_db, "events")}

    with open(STATE, "w") as f:
        json.dump(expected, f)

elif mode == "verify-all":
    with open(STATE) as f:
        expected = json.load(f)
    for name, info in expected.items():
        cnt, fp = db_fingerprint(os.path.join(work, name), info["table"])
        want_cnt, want_fp = info["fp"]
        assert (cnt, fp) == (want_cnt, want_fp), f"{name}: got {(cnt, fp)} != {(want_cnt, want_fp)}"

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-sqlite-write-sequence' --claim 'sqlite-rollback-wal-and-1000-commits' -- python3 "$HELPER" write-all
check shell --req 'drive9-sqlite-same-mount-reopen' --claim 'sqlite-same-mount-reopen-integrity' -- python3 "$HELPER" verify-all

sample before_remount
drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-sqlite-drain'

remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-sqlite-fresh-remount-verify' --claim 'sqlite-fresh-remount-integrity' -- python3 "$HELPER" verify-all
sample remote_snap --as remote
check manifest-contains --this @remote_snap --against @before_remount \
    --paths 'sqlite-suite/rollback.db,sqlite-suite/wal_commits.db,sqlite-suite/wal_concurrent.db' \
    --req 'drive9-sqlite-remote-parity'
