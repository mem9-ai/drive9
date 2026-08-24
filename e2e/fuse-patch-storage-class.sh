#!/usr/bin/env bash
# e2e/fuse-patch-storage-class.sh — PATCH-vs-storage-class mismatch
# (server-side patch_unsupported_target) reproduction and fix verification.
#
# Construction (no DB surgery): the server is first started with a HIGH
# DRIVE9_INLINE_THRESHOLD so a 200KB seed file is stored inline (db9). The
# server is then restarted against the same database with a LOW threshold, so
# a freshly mounted client caches a threshold below the file size while the
# object remains db9-stored. A partial overwrite at the same size then makes
# the client's size heuristic select PATCH, which the server rejects with
# 400 "file is not S3-stored".
#
# Scenarios (SCENARIO env, default "fixed"):
#   repro    — pre-fix expectation: the mounted write/fsync fails with EINVAL,
#              remote content is never committed, and every flush attempt logs
#              patch_unsupported_target (use with main-built binaries).
#   fallback — fixed client + OLD server (no X-Dat9-Storage-Type header):
#              exactly one PATCH attempt, then a full-upload fallback commits
#              the data and heals the object to S3.
#   fixed    — fixed client + fixed server: the stat header seeds the storage
#              class, so PATCH is never attempted and the write succeeds.
#
# Binaries default to the repo builds (bin/drive9-server, bin/drive9);
# override with DRIVE9_SERVER_BIN / DRIVE9_CLI_BIN for cross-version runs.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SCENARIO="${SCENARIO:-fixed}"
SERVER_BIN="${DRIVE9_SERVER_BIN:-$ROOT_DIR/bin/drive9-server}"
CLI_BIN="${DRIVE9_CLI_BIN:-$ROOT_DIR/bin/drive9}"

SEED_THRESHOLD="${SEED_THRESHOLD:-262144}"   # 256 KiB: seed file lands inline
PATCH_THRESHOLD="${PATCH_THRESHOLD:-51200}"  # 50 KiB: below seed size
SEED_SIZE="${SEED_SIZE:-204800}"             # 200 KiB
WRITE_OFFSET="${WRITE_OFFSET:-65536}"
WRITE_BYTES="${WRITE_BYTES:-4096}"

DEFAULT_LOCAL_DSN="root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
LOCAL_API_KEY="${DRIVE9_LOCAL_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-90}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-1}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"

PASS=0
FAIL=0
SERVER_PID=""
MOUNT_POINT=""
TMP_DIR="$(mktemp -d)"

check() { # check <label> <expected> <actual>
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "PASS: $label"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $label (expected=$expected actual=$actual)"
    FAIL=$((FAIL + 1))
  fi
}

cleanup() {
  local rc=$?
  if [ -n "$MOUNT_POINT" ]; then
    env HOME="$E2E_HOME" DRIVE9_SERVER="${PUBLIC_URL:-}" DRIVE9_API_KEY="$LOCAL_API_KEY" \
      "$CLI_BIN" umount "$MOUNT_POINT" >/dev/null 2>&1 || umount "$MOUNT_POINT" >/dev/null 2>&1 || true
  fi
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ]; then
    rm -rf "$TMP_DIR"
  else
    echo "Preserving artifacts at $TMP_DIR" >&2
  fi
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
  exit "$rc"
}
trap cleanup EXIT

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }; }

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

ensure_tidb() {
  : "${DRIVE9_LOCAL_DSN:=$DEFAULT_LOCAL_DSN}"
  export DRIVE9_LOCAL_DSN
  local host port db deadline
  host="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(1) if m else "")' "$DRIVE9_LOCAL_DSN")"
  port="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(2) if m else "")' "$DRIVE9_LOCAL_DSN")"
  db="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(3) if m else "drive9_local")' "$DRIVE9_LOCAL_DSN")"
  if [ -z "$host" ] || [ -z "$port" ]; then
    echo "cannot parse host/port from DRIVE9_LOCAL_DSN=$DRIVE9_LOCAL_DSN" >&2
    exit 1
  fi
  echo "Using TiDB at $host:$port (database $db)"
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2)
s.settimeout(3)
data = s.recv(64)
s.close()
sys.exit(0 if len(data) > 4 else 1)
PY
    then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out waiting for TiDB at $host:$port (start playground or set DRIVE9_LOCAL_DSN)" >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
  if command -v mysql >/dev/null 2>&1; then
    mysql --protocol=tcp -h "$host" -P "$port" -u root -e "CREATE DATABASE IF NOT EXISTS \`$db\`;" >/dev/null
  elif command -v mycli >/dev/null 2>&1; then
    mycli --host "$host" --port "$port" -u root -e "CREATE DATABASE IF NOT EXISTS \`$db\`;" >/dev/null
  else
    echo "mysql or mycli is required to CREATE DATABASE \`$db\`" >&2
    exit 1
  fi
}

wait_server() {
  local deadline
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    if [ "$(curl -sS -o /dev/null -w '%{http_code}' "$PUBLIC_URL/healthz" 2>/dev/null || true)" = "200" ]; then
      return
    fi
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "drive9-server exited early; log tail:" >&2
      tail -50 "$SERVER_LOG" >&2 || true
      exit 1
    fi
    [ "$(date +%s)" -ge "$deadline" ] && { echo "timed out waiting for healthz" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
}

provision_owner_key() {
  local base="$1" body code key status deadline
  body="$(mktemp)"
  code="$(curl -sS -o "$body" -w "%{http_code}" -X POST "$base/v1/provision" || true)"
  if [ "$code" != "202" ] && [ "$code" != "200" ]; then
    echo "POST $base/v1/provision failed: HTTP ${code:-000}" >&2
    cat "$body" >&2 || true
    rm -f "$body"
    exit 1
  fi
  key="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("api_key") or "")' "$body")"
  rm -f "$body"
  if [ -z "$key" ]; then
    echo "provision response missing api_key" >&2
    exit 1
  fi
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    status="$(curl -sS -H "Authorization: Bearer ${key}" "$base/v1/status" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status") or "")' 2>/dev/null || true)"
    if [ "$status" = "active" ]; then
      printf '%s\n' "$key"
      return
    fi
    [ "$(date +%s)" -ge "$deadline" ] && { echo "tenant not active (status=${status:-unknown})" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
}

start_server() { # start_server <threshold>
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  env \
    DRIVE9_LISTEN_ADDR="$LISTEN_ADDR" \
    DRIVE9_PUBLIC_URL="$PUBLIC_URL" \
    DRIVE9_TENANT_PROVIDER="${DRIVE9_TENANT_PROVIDER:-local}" \
    DRIVE9_LOCAL_DSN="$DRIVE9_LOCAL_DSN" \
    DRIVE9_META_DSN="${DRIVE9_META_DSN:-$DRIVE9_LOCAL_DSN}" \
    DRIVE9_LOCAL_MYSQL_DSN="${DRIVE9_LOCAL_MYSQL_DSN:-$DRIVE9_LOCAL_DSN}" \
    DRIVE9_LOCAL_EMBEDDING_MODE="${DRIVE9_LOCAL_EMBEDDING_MODE:-app}" \
    DRIVE9_S3_DIR="$TMP_DIR/s3" \
    DRIVE9_INLINE_THRESHOLD="$1" \
    DRIVE9_LOG_LEVEL=warn \
    "$SERVER_BIN" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  wait_server
}

is_mounted() {
  # macOS reports the physical path (/private/var/...) while mktemp returns
  # the /var/... symlink form; normalize both sides before comparing.
  local mp="$1" phys
  phys="$(cd "$(dirname "$mp")" 2>/dev/null && pwd -P)/$(basename "$mp")"
  mount | awk -v mp="$mp" -v phys="$phys" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==phys)) found=1} END{exit !found}'
}

wait_mount() {
  local deadline
  deadline=$(($(date +%s) + MOUNT_READY_TIMEOUT_S))
  while :; do
    if is_mounted "$MOUNT_POINT" && ls "$MOUNT_POINT" >/dev/null 2>&1; then
      return
    fi
    [ "$(date +%s)" -ge "$deadline" ] && { echo "mount not ready at $MOUNT_POINT" >&2; exit 1; }
    sleep 1
  done
}

sha256_of() { shasum -a 256 "$1" | awk '{print $1}'; }

need_cmd curl
need_cmd python3
need_cmd go
need_cmd shasum

# CI convenience: build the default binaries when absent. Explicitly overridden
# binary paths must already exist.
if [ -z "${DRIVE9_SERVER_BIN:-}" ] && [ ! -x "$SERVER_BIN" ]; then
  echo "Building drive9-server"
  make build-server
fi
if [ -z "${DRIVE9_CLI_BIN:-}" ] && [ ! -x "$CLI_BIN" ]; then
  echo "Building drive9 CLI"
  make build-cli
fi
need_cmd "$SERVER_BIN"
need_cmd "$CLI_BIN"

E2E_HOME="$TMP_DIR/home"
mkdir -p "$E2E_HOME"

ensure_tidb

LISTEN_ADDR="127.0.0.1:$(pick_port)"
PUBLIC_URL="http://${LISTEN_ADDR}"
SERVER_LOG="$TMP_DIR/server.log"
: >"$SERVER_LOG"

REMOTE_PATH="/patch-e2e/seed.bin"
SEED_FILE="$TMP_DIR/seed.bin"
EXPECTED_FILE="$TMP_DIR/expected.bin"

# Deterministic 200KB seed content.
python3 - "$SEED_FILE" "$SEED_SIZE" <<'PY'
import sys
path, size = sys.argv[1], int(sys.argv[2])
block = bytes(range(256)) * 1024
with open(path, "wb") as f:
    remaining = size
    while remaining > 0:
        chunk = block[: min(len(block), remaining)]
        f.write(chunk)
        remaining -= len(chunk)
PY

echo "=== phase 1: seed 200KB file with inline threshold $SEED_THRESHOLD (db9-stored)"
start_server "$SEED_THRESHOLD"
if [ -z "${DRIVE9_API_KEY:-}" ]; then
  LOCAL_API_KEY="$(provision_owner_key "$PUBLIC_URL")"
else
  LOCAL_API_KEY="$DRIVE9_API_KEY"
fi
code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT \
  -H "Authorization: Bearer $LOCAL_API_KEY" \
  --data-binary "@$SEED_FILE" \
  "$PUBLIC_URL/v1/fs$REMOTE_PATH")
check "seed PUT accepted" "200" "$code"

seed_head=$(curl -sS -I -H "Authorization: Bearer $LOCAL_API_KEY" "$PUBLIC_URL/v1/fs$REMOTE_PATH")
seed_storage=$(echo "$seed_head" | tr -d '\r' | grep -i '^x-dat9-storage-type:' | awk '{print $2}')
if [ -n "$seed_storage" ]; then
  check "seed file storage type advertised as db9" "db9" "$seed_storage"
else
  echo "INFO: server does not advertise X-Dat9-Storage-Type (old server binary)"
fi

echo "=== phase 2: restart server with inline threshold $PATCH_THRESHOLD"
start_server "$PATCH_THRESHOLD"
PATCH_LOG_MARK=$(wc -l <"$SERVER_LOG" | tr -d ' ')

echo "=== phase 3: mount with $(basename "$CLI_BIN") (durability=write-sync)"
MOUNT_POINT="$TMP_DIR/mnt"
mkdir -p "$MOUNT_POINT"
env HOME="$E2E_HOME" DRIVE9_SERVER="$PUBLIC_URL" DRIVE9_API_KEY="$LOCAL_API_KEY" \
  "$CLI_BIN" mount --mode=fuse --durability write-sync "$MOUNT_POINT" >"$TMP_DIR/mount.log" 2>&1 &
wait_mount

echo "=== phase 4: partial overwrite (${WRITE_BYTES}B @ ${WRITE_OFFSET}) + fsync"
set +e
write_err=$(python3 - "$MOUNT_POINT$REMOTE_PATH" "$WRITE_OFFSET" "$WRITE_BYTES" 2>&1 <<'PY'
import os, sys
path, offset, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
payload = bytes((i * 7) % 256 for i in range(count))
try:
    fd = os.open(path, os.O_WRONLY)
    try:
        os.pwrite(fd, payload, offset)
        os.fsync(fd)
    finally:
        os.close(fd)
except OSError as e:
    print(f"errno={e.errno} ({e.strerror})")
    sys.exit(1)
PY
)
write_rc=$?
set -e

patch_count=$(tail -n +"$((PATCH_LOG_MARK + 1))" "$SERVER_LOG" | grep -c 'patch_unsupported_target' || true)

case "$SCENARIO" in
  repro)
    check "mounted write fails pre-fix" "1" "$write_rc"
    echo "INFO: write error: ${write_err:-none}"
    if [ "$patch_count" -ge 1 ]; then
      echo "PASS: server logged patch_unsupported_target ($patch_count time(s))"
      PASS=$((PASS + 1))
    else
      echo "FAIL: expected patch_unsupported_target in server log"
      FAIL=$((FAIL + 1))
    fi
    # The failed write must never have committed: remote content is the seed.
    # GET follows redirects: S3-stored objects 302 to the (mock) S3 URL.
  curl -sSL -H "Authorization: Bearer $LOCAL_API_KEY" "$PUBLIC_URL/v1/fs$REMOTE_PATH" -o "$TMP_DIR/remote.bin"
    check "remote content unchanged (seed sha256)" "$(sha256_of "$SEED_FILE")" "$(sha256_of "$TMP_DIR/remote.bin")"
    ;;
  fallback)
    check "mounted write succeeds via fallback" "0" "$write_rc"
    echo "INFO: write error (want none): ${write_err:-none}"
    check "exactly one PATCH attempt before fallback" "1" "$patch_count"
    ;;
  fixed)
    check "mounted write succeeds" "0" "$write_rc"
    echo "INFO: write error (want none): ${write_err:-none}"
    check "no PATCH attempt (storage class seeded from header)" "0" "$patch_count"
    ;;
  *)
    echo "unknown SCENARIO: $SCENARIO" >&2
    exit 1
    ;;
esac

if [ "$SCENARIO" != "repro" ]; then
  # Expected content = seed with the overwritten block replaced.
  python3 - "$SEED_FILE" "$EXPECTED_FILE" "$WRITE_OFFSET" "$WRITE_BYTES" <<'PY'
import sys
src, dst, offset, count = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
data = bytearray(open(src, "rb").read())
data[offset:offset + count] = bytes((i * 7) % 256 for i in range(count))
open(dst, "wb").write(data)
PY
  # GET follows redirects: S3-stored objects 302 to the (mock) S3 URL.
  curl -sSL -H "Authorization: Bearer $LOCAL_API_KEY" "$PUBLIC_URL/v1/fs$REMOTE_PATH" -o "$TMP_DIR/remote.bin"
  check "remote content matches expected post-write bytes" "$(sha256_of "$EXPECTED_FILE")" "$(sha256_of "$TMP_DIR/remote.bin")"

  # The fallback uploaded 200KB >= the active threshold, healing the object to
  # S3 when the server advertises storage type.
  head_after=$(curl -sS -I -H "Authorization: Bearer $LOCAL_API_KEY" "$PUBLIC_URL/v1/fs$REMOTE_PATH")
  storage_after=$(echo "$head_after" | tr -d '\r' | grep -i '^x-dat9-storage-type:' | awk '{print $2}')
  if [ -n "$storage_after" ]; then
    check "post-write storage type healed to s3" "s3" "$storage_after"
  fi
fi

echo "=== done: $PASS passed, $FAIL failed (scenario=$SCENARIO)"
