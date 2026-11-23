#!/bin/sh
# drive9/fuse/s3-express-wal: `[prod-ext]` S3 Express append-log SQLite WAL FUSE gate.
# Run with `--no-mount` against a hosted server with `DRIVE9_E2E_S3_EXPRESS_ENABLED=1`
# and `storage_capabilities.append_log_v1=true`. Exits 2 (`unsupported`) otherwise.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

TRANSACTIONS="${FUSE_APPEND_LOG_TRANSACTIONS:-1000}"
MAX_REMOTE_RATIO="${FUSE_APPEND_LOG_MAX_REMOTE_WRITE_RATIO:-10}"
MAX_LAT_RATIO="${FUSE_APPEND_LOG_MAX_LATE_TO_EARLY_P95_RATIO:-4}"
REMOTE_ROOT="/s3-express-wal-$$-$(date +%s)"
MP="$RUN_DIR/s3x-mnt"
EXP_JSON="$RUN_DIR/expected.json"
export TRANSACTIONS MAX_REMOTE_RATIO MAX_LAT_RATIO REMOTE_ROOT MP EXP_JSON

cleanup() {
    "$CLI" umount --timeout 45s "$MP" >/dev/null 2>&1 || true
    if [ "$(uname -s)" = "Linux" ]; then
        fusermount3 -uz "$MP" >/dev/null 2>&1 || fusermount -uz "$MP" >/dev/null 2>&1 || true
    else
        umount -f "$MP" >/dev/null 2>&1 || diskutil unmount force "$MP" >/dev/null 2>&1 || true
    fi
    if [ "${S3X_CREATED:-0}" = "1" ]; then
        "$CLI" fs rm -r "$REMOTE_ROOT" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

for d in /Library/Filesystems/macfuse.fs/Contents/Resources /usr/local/bin /opt/homebrew/bin; do
    if [ -x "$d/mount_macfuse" ] || [ -x "$d/mount_fusefs" ]; then
        PATH="$d:$PATH"; export PATH; break
    fi
done

require --req 'drive9-s3x-wal-capability' --claim 's3-express-append-log-fuse-supported' -- sh -c '
    set -eu
    [ "${DRIVE9_E2E_S3_EXPRESS_ENABLED:-1}" != "0" ] || exit 2
    if [ "$(uname -s)" = "Linux" ]; then
        [ -e /dev/fuse ] && { command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1; } || exit 2
    elif [ "$(uname -s)" = "Darwin" ]; then
        command -v mount_macfuse >/dev/null 2>&1 || command -v mount_fusefs >/dev/null 2>&1 || exit 2
    else
        exit 2
    fi
    python3 - "$SERVER" "$API_KEY" <<'\''PY'\''
import json, sys, urllib.request
req = urllib.request.Request(sys.argv[1].rstrip("/") + "/v1/status", headers={"Authorization": "Bearer " + sys.argv[2]})
st = json.load(urllib.request.urlopen(req, timeout=20))
sys.exit(0 if st.get("storage_capabilities", {}).get("append_log_v1") is True else 2)
PY
'

"$CLI" fs mkdir "$REMOTE_ROOT" >/dev/null
S3X_CREATED=1

check shell --req 'drive9-s3x-wal-commits-and-remount' --claim 's3-express-sqlite-wal-commits-remount-and-perf-ratios' -- sh -c '
    set -eu
    mkdir -p "$MP" "$RUN_DIR/perf-1" "$RUN_DIR/perf-2"
    "$CLI" mount --mode=fuse --foreground --profile none --durability fsync \
        --append-log "**/append-log.db-wal" \
        --perf-dir "$RUN_DIR/perf-1" \
        ":$REMOTE_ROOT" "$MP" >"$RUN_DIR/mount-1.log" 2>&1 &
    mpid=$!
    deadline=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        "$CLI" mount health "$MP" >/dev/null 2>&1 && break
        sleep 1
    done
    "$CLI" mount health "$MP" >/dev/null 2>&1

    python3 - "$MP/append-log.db" "$EXP_JSON" "$TRANSACTIONS" "$SERVER" "$API_KEY" "$REMOTE_ROOT" <<'\''PY'\''
import hashlib, json, os, sqlite3, sys, time, urllib.request
db, exp_path, txns, srv, key, rroot = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4].rstrip("/"), sys.argv[5], sys.argv[6]
conn = sqlite3.connect(db, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=FULL")
conn.execute("PRAGMA wal_autocheckpoint=1000")
conn.execute("PRAGMA journal_size_limit=0")
conn.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
cum_wal = 0
max_wal = 0
times = []
for i in range(1, txns + 1):
    t0 = time.perf_counter()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO entries(id, value) VALUES (?, ?)", (i, f"value-{i:04d}"))
    conn.execute("COMMIT")
    times.append(time.perf_counter() - t0)
    wsz = os.path.getsize(db + "-wal")
    cum_wal += wsz
    if wsz > max_wal:
        max_wal = wsz
rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
fp = hashlib.sha256(repr(rows).encode()).hexdigest()
wal_sz = os.path.getsize(db + "-wal")
assert wal_sz > 0 and conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
req = urllib.request.Request(f"{srv}/v1/fs{rroot}/append-log.db-wal", method="HEAD", headers={"Authorization": f"Bearer {key}"})
with urllib.request.urlopen(req, timeout=20) as resp:
    hdrs = {k.lower(): v for k, v in resp.headers.items()}
assert hdrs.get("x-dat9-content-layout") == "append_log", hdrs
assert int(hdrs.get("x-dat9-revision", "0")) >= txns, hdrs
assert int(hdrs.get("content-length", "-1")) == wal_sz, (hdrs, wal_sz)
conn.close()
p95 = lambda xs: sorted(xs)[max(0, min(len(xs) - 1, int(len(xs) * 0.95 + 0.999999) - 1))] * 1000.0
json.dump({"count": len(rows), "fingerprint": fp, "wal_size": wal_sz, "max_wal_size": max_wal, "cumulative_wal_size": cum_wal,
           "early_commit_p95_ms": p95(times[:100]), "late_commit_p95_ms": p95(times[-100:])}, open(exp_path, "w"))
PY

    "$CLI" umount --timeout 60s "$MP"
    wait "$mpid" || true

    "$CLI" mount --mode=fuse --foreground --profile none --durability fsync \
        --append-log "**/append-log.db-wal" \
        --perf-dir "$RUN_DIR/perf-2" \
        ":$REMOTE_ROOT" "$MP" >"$RUN_DIR/mount-2.log" 2>&1 &
    mpid2=$!
    deadline=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        "$CLI" mount health "$MP" >/dev/null 2>&1 && break
        sleep 1
    done
    "$CLI" mount health "$MP" >/dev/null 2>&1

    python3 - "$MP/append-log.db" "$EXP_JSON" "$RUN_DIR/perf-1/perf.jsonl" "$MAX_REMOTE_RATIO" "$MAX_LAT_RATIO" <<'\''PY'\''
import hashlib, json, sqlite3, sys
db, exp_path, perf_path, max_rr, max_lr = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), float(sys.argv[5])
want = json.load(open(exp_path))
conn = sqlite3.connect(db)
rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
assert len(rows) == want["count"]
assert hashlib.sha256(repr(rows).encode()).hexdigest() == want["fingerprint"]
assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
conn.close()

lines = [l for l in open(perf_path).read().splitlines() if l]
sample = json.loads(lines[-1])
append = sample.get("remote_ops", {}).get("append_log", {})
write = sample.get("remote_ops", {}).get("write", {})
counters = sample.get("counters", {})
ac, ab = int(append.get("count", 0)), int(append.get("bytes", 0))
wb = int(write.get("bytes", 0))
frb = int(counters.get("append_log_full_rewrite_bytes", 0))
frc = int(counters.get("append_log_full_rewrite_count", 0))
grc = int(counters.get("append_log_generation_reset_count", 0))
wal_bound = max(want["wal_size"], want.get("max_wal_size", want["wal_size"]))
assert wb < want["cumulative_wal_size"], (wb, want)
if ac > 0:
    assert ab >= 1
    assert (ab + frb) <= wal_bound * max_rr, (ab, frb, wal_bound, max_rr)
    assert (ab + frb) < want["cumulative_wal_size"]
    assert grc <= frc and (frc - grc) <= 1
assert want["late_commit_p95_ms"] <= max(want["early_commit_p95_ms"], 5.0) * max_lr, want
PY
    "$CLI" umount --timeout 60s "$MP"
    wait "$mpid2" || true
'
