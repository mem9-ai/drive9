#!/usr/bin/env bash
# End-to-end smoke test for shared-schema (fs_id) mode against real engines:
#   - MySQL container      → control-plane meta DB
#   - TiDB container       → shared-schema DB (validates CLUSTERED DDL) AND a
#                            second database simulating a pre-existing
#                            standalone tenant DB (coexistence)
#
# Covers: provision onto shared pool (instant active), CRUD, cross-tenant
# isolation, SSE events, multipart upload, fork rejection, shared delete +
# purge, server restart persistence, and standalone↔shared coexistence.
#
# Override endpoints to skip containers:
#   META_DSN=... SHARED_DSN=... STANDALONE_DSN=... bash e2e/shared-mode-smoke-test.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_RUNTIME="${DRIVE9_SHARED_E2E_DB_RUNTIME:-}"
MYSQL_IMAGE="${DRIVE9_SHARED_E2E_MYSQL_IMAGE:-mysql:8.4}"
TIDB_IMAGE="${DRIVE9_SHARED_E2E_TIDB_IMAGE:-pingcap/tidb:v8.5.6}"
DB_PASSWORD="${DRIVE9_SHARED_E2E_DB_PASSWORD:-drive9pass}"
META_DB="${DRIVE9_SHARED_E2E_META_DB:-drive9_meta}"
SHARED_DB="${DRIVE9_SHARED_E2E_SHARED_DB:-drive9_shared}"
STANDALONE_DB="${DRIVE9_SHARED_E2E_STANDALONE_DB:-drive9_standalone}"
META_DSN="${META_DSN:-}"
SHARED_DSN="${SHARED_DSN:-}"
STANDALONE_DSN="${STANDALONE_DSN:-}"
KEEP_DB="${KEEP_DB:-0}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-180}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"

# Remember which endpoints were supplied externally: the container startup
# paths assign these variables too, and only externally supplied endpoints
# should bypass container execs.
META_DSN_EXTERNAL=0; [ -n "$META_DSN" ] && META_DSN_EXTERNAL=1
SHARED_DSN_EXTERNAL=0; [ -n "$SHARED_DSN" ] && SHARED_DSN_EXTERNAL=1

MYSQL_CONTAINER=""
TIDB_CONTAINER=""
NETWORK=""
SERVER_PID=""
TMP_DIR="$(mktemp -d)"
PASS_COUNT=0
FAIL_COUNT=0

log() { echo "[shared-e2e] $*"; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); log "PASS: $1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); log "FAIL: $1" >&2; }
check_eq() { if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (want=$2 got=$3)"; fi }

cleanup() {
  local rc=$?
  [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1 && {
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  }
  if [ "$KEEP_DB" != "1" ]; then
    [ -n "$MYSQL_CONTAINER" ] && "$DB_RUNTIME" rm -f "$MYSQL_CONTAINER" >/dev/null 2>&1 || true
    [ -n "$TIDB_CONTAINER" ] && "$DB_RUNTIME" rm -f "$TIDB_CONTAINER" >/dev/null 2>&1 || true
    [ -n "$NETWORK" ] && "$DB_RUNTIME" network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
  [ "$rc" -eq 0 ] && rm -rf "$TMP_DIR" || log "failed; artifacts kept at $TMP_DIR"
  log "passed=$PASS_COUNT failed=$FAIL_COUNT"
  # Fold the trapped exit code into the final status: a hard abort under
  # set -e (build failure, container startup, ...) must not report success
  # just because no check_eq ran yet.
  [ "$rc" -eq 0 ] && [ "$FAIL_COUNT" -eq 0 ]
  exit $?
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

detect_runtime() {
  if [ -n "$DB_RUNTIME" ]; then return; fi
  if command -v docker >/dev/null 2>&1; then DB_RUNTIME="docker"; return; fi
  if command -v podman >/dev/null 2>&1; then DB_RUNTIME="podman"; return; fi
  echo "docker or podman is required when DSNs are not set" >&2
  exit 1
}

# run_mysql_dsn runs the mysql client against a DSN of the form
# user:pass@tcp(host:port)/dbname?params. Used instead of container execs
# when an endpoint is supplied externally.
run_mysql_dsn() {
  python3 - "$1" "$2" <<'PY'
import re, subprocess, sys
dsn, sql = sys.argv[1], sys.argv[2]
m = re.match(r'([^:]+):([^@]*)@tcp\(([^:]+):(\d+)\)/([^?]+)', dsn)
if not m:
    sys.exit(f"cannot parse DSN for mysql client: {dsn!r}")
user, pw, host, port, _db = m.groups()
cmd = ["mysql", "-N", "-h", host, "-P", port, "-u", user, f"-p{pw}", "-e", sql]
sys.exit(subprocess.run(cmd).returncode)
PY
}

meta_exec() {
  if [ "$META_DSN_EXTERNAL" = "1" ]; then
    run_mysql_dsn "$META_DSN" "$1"
  else
    "$DB_RUNTIME" exec "$MYSQL_CONTAINER" mysql -uroot -p"$DB_PASSWORD" -N -e "$1"
  fi
}
tidb_exec() {
  if [ "$SHARED_DSN_EXTERNAL" = "1" ]; then
    run_mysql_dsn "$SHARED_DSN" "$1" 2>/dev/null
  else
    "$DB_RUNTIME" exec "$MYSQL_CONTAINER" mysql -h tidb-e2e -P 4000 -uroot -N -e "$1" 2>/dev/null
  fi
}

start_mysql_container() {
  MYSQL_CONTAINER="drive9-shared-e2e-mysql-$(date +%s)-$$"
  log "starting MySQL container $MYSQL_CONTAINER ($MYSQL_IMAGE)"
  "$DB_RUNTIME" run -d --name "$MYSQL_CONTAINER" --network "$NETWORK" \
    -e MYSQL_ROOT_PASSWORD="$DB_PASSWORD" -p 127.0.0.1::3306 "$MYSQL_IMAGE" >/dev/null
  local deadline port
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    port=$("$DB_RUNTIME" port "$MYSQL_CONTAINER" 3306/tcp 2>/dev/null | awk -F: 'END{print $NF}')
    [ -n "$port" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for mysql port" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  while :; do
    meta_exec "SELECT 1" >/dev/null 2>&1 && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for MySQL auth readiness" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  meta_exec "CREATE DATABASE IF NOT EXISTS $META_DB;"
  META_DSN="root:${DB_PASSWORD}@tcp(127.0.0.1:${port})/${META_DB}?parseTime=true"
  log "meta db: $META_DB (mysql 127.0.0.1:$port)"
}

start_tidb_container() {
  TIDB_CONTAINER="tidb-e2e"
  log "starting TiDB container $TIDB_CONTAINER ($TIDB_IMAGE) — this takes ~1 min"
  "$DB_RUNTIME" rm -f "$TIDB_CONTAINER" >/dev/null 2>&1 || true
  "$DB_RUNTIME" run -d --name "$TIDB_CONTAINER" --network "$NETWORK" --network-alias tidb-e2e \
    -p 127.0.0.1::4000 "$TIDB_IMAGE" >/dev/null
  local deadline port
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    port=$("$DB_RUNTIME" port "$TIDB_CONTAINER" 4000/tcp 2>/dev/null | awk -F: 'END{print $NF}')
    [ -n "$port" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for tidb port" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  while :; do
    tidb_exec "SELECT 1" >/dev/null 2>&1 && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for TiDB readiness" >&2; "$DB_RUNTIME" logs "$TIDB_CONTAINER" 2>&1 | tail -20 >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  tidb_exec "CREATE DATABASE IF NOT EXISTS $SHARED_DB; CREATE DATABASE IF NOT EXISTS $STANDALONE_DB;"
  SHARED_DSN="root:@tcp(127.0.0.1:${port})/${SHARED_DB}?parseTime=true"
  STANDALONE_DSN="root:@tcp(127.0.0.1:${port})/${STANDALONE_DB}?parseTime=true"
  TIDB_PORT="$port"
  log "tidb up: shared=$SHARED_DB standalone=$STANDALONE_DB (127.0.0.1:$port)"
}

http() { # method path [api_key] [body]
  local method="$1" path="$2" key="${3:-}" body="${4:-}"
  local args=(-sS -o "$TMP_DIR/resp.json" -w "%{http_code}" -X "$method")
  [ -n "$key" ] && args+=(-H "Authorization: Bearer $key")
  [ -n "$body" ] && args+=(-H "Content-Type: application/octet-stream" --data-binary "$body")
  local code
  code=$(curl "${args[@]}" "http://127.0.0.1:${SERVER_PORT}${path}" 2>/dev/null) || code="000"
  printf '%s|%s' "$code" "$(cat "$TMP_DIR/resp.json" 2>/dev/null || true)"
}
field() { printf '%s' "$2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null || true; }

start_server() {
  env \
    DRIVE9_LISTEN_ADDR="127.0.0.1:$SERVER_PORT" \
    DRIVE9_PUBLIC_URL="http://127.0.0.1:$SERVER_PORT" \
    DRIVE9_META_DSN="$META_DSN" \
    DRIVE9_SHARED_POOL_DSN="$SHARED_DSN" \
    DRIVE9_SHARED_POOL_ORG="*" \
    DRIVE9_TOKEN_SIGNING_KEY="$TOKEN_KEY" \
    DRIVE9_MASTER_KEY="$MASTER_KEY" \
    DRIVE9_TENANT_PROVIDER=tidb_zero \
    DRIVE9_ZERO_API_URL="http://127.0.0.1:1/unused" \
    DRIVE9_DISABLE_AUTO_EMBEDDING=true \
    DRIVE9_LEADER_DISABLED=1 \
    DRIVE9_S3_DIR="$TMP_DIR/s3" \
    "$TMP_DIR/drive9-server" >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  local deadline
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    curl -sf "http://127.0.0.1:$SERVER_PORT/healthz" >/dev/null 2>&1 && break
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died; log:" >&2; tail -30 "$SERVER_LOG" >&2; exit 1; }
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for /healthz" >&2; tail -30 "$SERVER_LOG" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  log "server is up on :$SERVER_PORT"
}

stop_server() {
  [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1 && {
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  }
  SERVER_PID=""
}

need_cmd curl; need_cmd python3; need_cmd jq; need_cmd go
if [ "$META_DSN_EXTERNAL" = "1" ] || [ "$SHARED_DSN_EXTERNAL" = "1" ]; then
  # External endpoints are probed with the local mysql client instead of
  # container execs.
  need_cmd mysql
fi

if [ -z "$META_DSN" ] || [ -z "$SHARED_DSN" ] || [ -z "$STANDALONE_DSN" ]; then
  # A container runtime is only needed when one of the DSNs is not supplied
  # externally; with all three set, no container is started at all.
  detect_runtime
  NETWORK="drive9-shared-e2e-net-$$"
  "$DB_RUNTIME" network create "$NETWORK" >/dev/null
fi
[ -z "$META_DSN" ] && start_mysql_container
if [ -z "$SHARED_DSN" ] || [ -z "$STANDALONE_DSN" ]; then
  start_tidb_container
fi

SERVER_PORT="$(pick_port)"
SERVER_LOG="$TMP_DIR/server.log"
TOKEN_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"
MASTER_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"

log "building drive9-server + harness"
go build -o "$TMP_DIR/drive9-server" ./cmd/drive9-server
go build -o "$TMP_DIR/standalone-tenant" ./e2e/harness/standalone-tenant

start_server

# ============ 1. Provision two shared tenants (instant active) ============
resp=$(http POST /v1/provision); code="${resp%%|*}"; body="${resp#*|}"
check_eq "provision A returns 202" "202" "$code"
A_KEY="$(field api_key "$body")"; A_TENANT="$(field tenant_id "$body")"
check_eq "provision A status is active (shared path is instant)" "active" "$(field status "$body")"

resp=$(http POST /v1/provision); code="${resp%%|*}"; body="${resp#*|}"
check_eq "provision B returns 202" "202" "$code"
B_KEY="$(field api_key "$body")"; B_TENANT="$(field tenant_id "$body")"
check_eq "provision B status is active" "active" "$(field status "$body")"

# ============ 2. CRUD + isolation on shared tenants ============
resp=$(http PUT /v1/fs/a.txt "$A_KEY" "hello from A")
check_eq "A write /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
check_eq "A read /a.txt content" "hello from A" "${resp#*|}"
resp=$(http POST "/v1/fs/docs?mkdir" "$A_KEY")
check_eq "A mkdir /docs returns 200" "200" "${resp%%|*}"
resp=$(http PUT /v1/fs/docs/note.txt "$A_KEY" "nested note")
check_eq "A write /docs/note.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET "/v1/fs/docs/?list" "$A_KEY")
case "${resp#*|}" in *note.txt*) pass "A list /docs contains note.txt" ;; *) fail "A list /docs: ${resp#*|}" ;; esac

resp=$(http PUT /v1/fs/a.txt "$B_KEY" "hello from B")
check_eq "B write same path /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B reads its own /a.txt content" "hello from B" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
check_eq "A still reads its own /a.txt content" "hello from A" "${resp#*|}"
resp=$(http GET /v1/fs/docs/note.txt "$B_KEY")
check_eq "B cannot read A's /docs/note.txt" "404" "${resp%%|*}"

# ============ 3. Raw SQL on TiDB: fs_id isolation + CLUSTERED ============
if [ -n "$TIDB_CONTAINER" ]; then
  A_FS_ID=$(meta_exec "SELECT fs_id FROM $META_DB.fs_registry WHERE tenant_id='$A_TENANT';")
  B_FS_ID=$(meta_exec "SELECT fs_id FROM $META_DB.fs_registry WHERE tenant_id='$B_TENANT';")
  [ -n "$A_FS_ID" ] && [ -n "$B_FS_ID" ] && [ "$A_FS_ID" != "$B_FS_ID" ] \
    && pass "fs_registry assigns distinct fs_ids ($A_FS_ID vs $B_FS_ID)" \
    || fail "fs_registry fs_id assignment (A=$A_FS_ID B=$B_FS_ID)"
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$A_FS_ID;")
  [ "${N:-0}" -ge 2 ] && pass "shared file_nodes has A's rows ($N)" || fail "shared file_nodes A rows=$N"
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$B_FS_ID;")
  [ "${N:-0}" -ge 1 ] && pass "shared file_nodes has B's rows ($N)" || fail "shared file_nodes B rows=$N"
  CLUSTERED=$(tidb_exec "SELECT TIDB_PK_TYPE FROM information_schema.tables WHERE table_schema='$SHARED_DB' AND table_name='file_nodes';")
  check_eq "shared file_nodes is CLUSTERED on TiDB" "CLUSTERED" "${CLUSTERED:-}"
  N=$(tidb_exec "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$SHARED_DB' AND table_name='llm_usage';")
  check_eq "shared DB has no llm_usage table" "0" "${N:-?}"
fi

# ============ 4. SSE events on shared tenant ============
SSE_OUT="$TMP_DIR/sse.log"
curl -sS -N --max-time 12 -H "Authorization: Bearer $B_KEY" \
  "http://127.0.0.1:$SERVER_PORT/v1/events?since=0" >"$SSE_OUT" 2>/dev/null &
SSE_PID=$!
sleep 2
resp=$(http PUT /v1/fs/sse-target.txt "$B_KEY" "sse payload")
check_eq "B write for SSE returns 200" "200" "${resp%%|*}"
sleep 4
kill "$SSE_PID" >/dev/null 2>&1 || true
wait "$SSE_PID" 2>/dev/null || true
if grep -q "initial_sync\|server_restart" "$SSE_OUT" && grep -q "sse-target.txt" "$SSE_OUT"; then
  pass "SSE stream delivered reset + file event for shared tenant"
else
  fail "SSE stream missing events: $(head -c 400 "$SSE_OUT")"
fi

# ============ 5. Multipart upload (v2) on shared tenant ============
BIG_FILE="$TMP_DIR/big.bin"
python3 -c "import os;open('$BIG_FILE','wb').write(os.urandom(10*1024*1024))"
CHECKSUMS=$(python3 - "$BIG_FILE" <<'PY'
import base64, struct, sys
_TABLE = []
for i in range(256):
    c = i
    for _ in range(8):
        c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
    _TABLE.append(c)
def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF
part_size = 8 * 1024 * 1024
out = []
with open(sys.argv[1], "rb") as f:
    while True:
        chunk = f.read(part_size)
        if not chunk:
            break
        out.append(base64.b64encode(struct.pack(">I", crc32c(chunk))).decode())
print(",".join(out))
PY
)
INIT_PAYLOAD="$TMP_DIR/init.json"
jq -n --arg path "/big.bin" --argjson total_size 10485760 --arg checksums "$CHECKSUMS" \
  '{path:$path,total_size:$total_size,part_checksums:($checksums|split(","))}' > "$INIT_PAYLOAD"
code=$(curl -sS -o "$TMP_DIR/plan.json" -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $B_KEY" -H "Content-Type: application/json" \
  --data-binary "@$INIT_PAYLOAD" "http://127.0.0.1:$SERVER_PORT/v1/uploads/initiate")
check_eq "multipart initiate returns 202" "202" "$code"
UPLOAD_ID=$(jq -r '.upload_id // empty' "$TMP_DIR/plan.json")
python3 - "$TMP_DIR/plan.json" "$BIG_FILE" <<'PY'
import json, sys, urllib.request
plan_path, file_path = sys.argv[1], sys.argv[2]
plan = json.load(open(plan_path))
with open(file_path, "rb") as data_file:
    for idx, p in enumerate(plan.get("parts", []), 1):
        size = int(p["size"])
        data = data_file.read(size)
        if len(data) != size:
            raise SystemExit(f"short read part {idx}")
        req = urllib.request.Request(p["url"], data=data, method="PUT")
        req.add_header("Content-Length", str(size))
        for hk, hv in (p.get("headers") or {}).items():
            req.add_header(hk, hv)
        if p.get("checksum_crc32c"):
            req.add_header("x-amz-checksum-crc32c", p["checksum_crc32c"])
        with urllib.request.urlopen(req, timeout=120) as resp:
            if getattr(resp, "status", 200) >= 300:
                raise SystemExit(f"part {idx} HTTP {resp.status}")
PY
check_eq "multipart parts uploaded" "0" "$?"
resp=$(http POST "/v1/uploads/$UPLOAD_ID/complete" "$B_KEY")
check_eq "multipart complete returns 200" "200" "${resp%%|*}"
check_eq "multipart complete status ok" "ok" "$(field status "${resp#*|}")"
# Large files are served via a redirect to the object store; follow it.
BIG_URL=$(curl -sS -o /dev/null -w "%{redirect_url}" -H "Authorization: Bearer $B_KEY" "http://127.0.0.1:$SERVER_PORT/v1/fs/big.bin")
if [ -n "$BIG_URL" ]; then
  BIG_SIZE=$(curl -sS "$BIG_URL" | wc -c | tr -d ' ')
  check_eq "multipart file served via redirect, size matches" "10485760" "$BIG_SIZE"
else
  resp=$(http GET /v1/fs/big.bin "$B_KEY")
  check_eq "multipart file readable inline" "200" "${resp%%|*}"
fi

# ============ 6. Fork rejected on shared tenant ============
resp=$(http POST /v1/fork "$B_KEY")
check_eq "fork on shared tenant is rejected (409)" "409" "${resp%%|*}"

# ============ 7. Standalone tenant coexistence ============
S_TENANT="standalone-$(python3 -c 'import uuid;print(uuid.uuid4())')"
S_KEY=$("$TMP_DIR/standalone-tenant" \
  -meta-dsn "$META_DSN" -master-key "$MASTER_KEY" -token-secret "$TOKEN_KEY" \
  -tenant-id "$S_TENANT" -db-host 127.0.0.1 -db-port "${TIDB_PORT:-4000}" \
  -db-user root -db-password "" -db-name "$STANDALONE_DB" -db-tls=false \
  -provider tidb_zero -cluster-id standalone-e2e -skip-ensure 2>"$TMP_DIR/harness.log")
[ -n "$S_KEY" ] && pass "standalone tenant registered via harness" || { cat "$TMP_DIR/harness.log" >&2; fail "harness returned no key"; }

resp=$(http PUT /v1/fs/a.txt "$S_KEY" "hello from standalone")
check_eq "standalone write /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$S_KEY")
check_eq "standalone reads its own content" "hello from standalone" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "shared B unaffected by standalone writes" "hello from B" "${resp#*|}"
resp=$(http GET /v1/fs/docs/note.txt "$S_KEY")
check_eq "standalone cannot read shared A's nested file" "404" "${resp%%|*}"
if [ -n "$TIDB_CONTAINER" ]; then
  N=$(tidb_exec "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='$STANDALONE_DB' AND table_name='file_nodes' AND column_name='fs_id';")
  check_eq "standalone file_nodes has NO fs_id column" "0" "${N:-?}"
  N=$(tidb_exec "SELECT COUNT(*) FROM $STANDALONE_DB.file_nodes WHERE path='/a.txt';")
  check_eq "standalone DB holds its own file" "1" "${N:-?}"
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE path='/a.txt' AND fs_id NOT IN ($A_FS_ID,$B_FS_ID);")
  check_eq "no foreign rows leaked into shared DB" "0" "${N:-?}"
fi

# ============ 8. Server restart persistence ============
log "restarting server to verify persistence"
stop_server
start_server
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B reads /a.txt after server restart" "hello from B" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$S_KEY")
check_eq "standalone reads /a.txt after restart" "hello from standalone" "${resp#*|}"

# ============ 9. Delete tenant A: purge; others intact ============
resp=$(http DELETE /v1/tenant "$A_KEY")
check_eq "DELETE /v1/tenant A returns 202" "202" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
[ "${resp%%|*}" = "401" ] || [ "${resp%%|*}" = "403" ] && pass "A key rejected after delete (got ${resp%%|*})" || fail "A key after delete (got ${resp%%|*})"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B unaffected after A delete" "hello from B" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$S_KEY")
check_eq "standalone unaffected after A delete" "hello from standalone" "${resp#*|}"
if [ -n "$TIDB_CONTAINER" ]; then
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$A_FS_ID;")
  check_eq "A's file_nodes purged from shared DB" "0" "${N:-?}"
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.inodes WHERE fs_id=$A_FS_ID;")
  check_eq "A's inodes purged from shared DB" "0" "${N:-?}"
  N=$(tidb_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$B_FS_ID;")
  [ "${N:-0}" -ge 1 ] && pass "B's rows still in shared DB" || fail "B's rows missing after A purge"
fi

log "all shared-mode e2e checks done"
