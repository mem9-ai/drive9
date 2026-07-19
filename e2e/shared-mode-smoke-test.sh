#!/usr/bin/env bash
# End-to-end smoke test for shared-schema (fs_id) mode:
#   1 meta DB + 1 shared DB, two tenants provisioned onto the shared DB,
#   CRUD + cross-tenant isolation + delete/purge verification.
#
# Defaults start a throwaway MySQL container. To run against a TiDB endpoint
# instead, set META_DSN / SHARED_DSN (and TLS=true in the DSN if needed):
#   META_DSN='root:pass@tcp(host:4000)/meta?parseTime=true' \
#   SHARED_DSN='root:pass@tcp(host:4000)/shared?parseTime=true' \
#     bash e2e/shared-mode-smoke-test.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_RUNTIME="${DRIVE9_SHARED_E2E_DB_RUNTIME:-}"
DB_IMAGE="${DRIVE9_SHARED_E2E_DB_IMAGE:-mysql:8.4}"
DB_PASSWORD="${DRIVE9_SHARED_E2E_DB_PASSWORD:-drive9pass}"
META_DB="${DRIVE9_SHARED_E2E_META_DB:-drive9_meta}"
SHARED_DB="${DRIVE9_SHARED_E2E_SHARED_DB:-drive9_shared}"
META_DSN="${META_DSN:-}"
SHARED_DSN="${SHARED_DSN:-}"
KEEP_DB="${KEEP_DB:-0}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-90}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-1}"
LISTEN_ADDR="${LISTEN_ADDR:-}"

DB_CONTAINER=""
SERVER_PID=""
TMP_DIR="$(mktemp -d)"
PASS_COUNT=0
FAIL_COUNT=0

log() { echo "[shared-e2e] $*"; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); log "PASS: $1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); log "FAIL: $1" >&2; }
check_eq() { # name want got
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (want=$2 got=$3)"; fi
}

cleanup() {
  local rc=$?
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$DB_CONTAINER" ] && [ "$KEEP_DB" != "1" ]; then
    "$DB_RUNTIME" rm -f "$DB_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -eq 0 ]; then
    rm -rf "$TMP_DIR"
  else
    log "failed; artifacts kept at $TMP_DIR"
  fi
  log "passed=$PASS_COUNT failed=$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
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
  echo "docker or podman is required when META_DSN/SHARED_DSN are not set" >&2
  exit 1
}

start_mysql_container() {
  detect_runtime
  DB_CONTAINER="drive9-shared-e2e-$(date +%s)-$$"
  log "starting $DB_RUNTIME container $DB_CONTAINER from $DB_IMAGE"
  "$DB_RUNTIME" run -d \
    --name "$DB_CONTAINER" \
    -e MYSQL_ROOT_PASSWORD="$DB_PASSWORD" \
    -p 127.0.0.1::3306 \
    "$DB_IMAGE" >/dev/null

  local deadline port
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    port=$("$DB_RUNTIME" port "$DB_CONTAINER" 3306/tcp 2>/dev/null | awk -F: 'END{print $NF}')
    [ -n "$port" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for port mapping" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  while :; do
    # mysqladmin ping succeeds before the entrypoint finishes setting the
    # root password, so use a real authenticated query as the readiness gate.
    "$DB_RUNTIME" exec "$DB_CONTAINER" mysql -uroot -p"$DB_PASSWORD" -N -e "SELECT 1" >/dev/null 2>&1 && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for MySQL readiness" >&2; exit 1; }
    sleep "$POLL_INTERVAL_S"
  done
  DB_PORT_HOST="$port"
  mysql_exec() { "$DB_RUNTIME" exec "$DB_CONTAINER" mysql -uroot -p"$DB_PASSWORD" -N -e "$1"; }
  mysql_exec "CREATE DATABASE IF NOT EXISTS $META_DB; CREATE DATABASE IF NOT EXISTS $SHARED_DB;"
  META_DSN="root:${DB_PASSWORD}@tcp(127.0.0.1:${port})/${META_DB}?parseTime=true"
  SHARED_DSN="root:${DB_PASSWORD}@tcp(127.0.0.1:${port})/${SHARED_DB}?parseTime=true"
  log "meta db: $META_DB, shared db: $SHARED_DB (127.0.0.1:$port)"
}

http() { # method path [api_key] [body] -> "code|body"
  local method="$1" path="$2" key="${3:-}" body="${4:-}"
  local args=(-sS -o "$TMP_DIR/resp.json" -w "%{http_code}" -X "$method")
  [ -n "$key" ] && args+=(-H "Authorization: Bearer $key")
  [ -n "$body" ] && args+=(-H "Content-Type: application/octet-stream" --data-binary "$body")
  local code
  code=$(curl "${args[@]}" "http://127.0.0.1:${SERVER_PORT}${path}" 2>/dev/null) || code="000"
  printf '%s|%s' "$code" "$(cat "$TMP_DIR/resp.json" 2>/dev/null || true)"
}

field() { printf '%s' "$2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null || true; }

need_cmd curl
need_cmd python3
need_cmd jq

if [ -z "$META_DSN" ] || [ -z "$SHARED_DSN" ]; then
  start_mysql_container
fi

SERVER_PORT="${LISTEN_ADDR##*:}"
[ -n "$LISTEN_ADDR" ] || SERVER_PORT="$(pick_port)"
SERVER_LOG="$TMP_DIR/server.log"

log "building drive9-server"
go build -o "$TMP_DIR/drive9-server" ./cmd/drive9-server

log "starting drive9-server on :$SERVER_PORT (shared pool mode)"
env \
  DRIVE9_LISTEN_ADDR="127.0.0.1:$SERVER_PORT" \
  DRIVE9_PUBLIC_URL="http://127.0.0.1:$SERVER_PORT" \
  DRIVE9_META_DSN="$META_DSN" \
  DRIVE9_SHARED_POOL_DSN="$SHARED_DSN" \
  DRIVE9_SHARED_POOL_ORG="*" \
  DRIVE9_TOKEN_SIGNING_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(32))')" \
  DRIVE9_MASTER_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(32))')" \
  DRIVE9_TENANT_PROVIDER=tidb_zero \
  DRIVE9_ZERO_API_URL="http://127.0.0.1:1/unused" \
  DRIVE9_DISABLE_AUTO_EMBEDDING=true \
  DRIVE9_LEADER_DISABLED=1 \
  DRIVE9_S3_DIR="$TMP_DIR/s3" \
  "$TMP_DIR/drive9-server" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

deadline=$(($(date +%s) + POLL_TIMEOUT_S))
while :; do
  if curl -sf "http://127.0.0.1:$SERVER_PORT/healthz" >/dev/null 2>&1; then break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died; log:" >&2; tail -30 "$SERVER_LOG" >&2; exit 1; }
  [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for /healthz" >&2; tail -30 "$SERVER_LOG" >&2; exit 1; }
  sleep "$POLL_INTERVAL_S"
done
log "server is up"

# --- 1. Provision two tenants; both should land on the shared DB and be active immediately
resp=$(http POST /v1/provision)
code="${resp%%|*}"; body="${resp#*|}"
check_eq "provision A returns 202" "202" "$code"
A_KEY="$(field api_key "$body")"; A_TENANT="$(field tenant_id "$body")"
check_eq "provision A status is active (shared path is instant)" "active" "$(field status "$body")"

resp=$(http POST /v1/provision)
code="${resp%%|*}"; body="${resp#*|}"
check_eq "provision B returns 202" "202" "$code"
B_KEY="$(field api_key "$body")"; B_TENANT="$(field tenant_id "$body")"
check_eq "provision B status is active" "active" "$(field status "$body")"

# --- 2. Basic CRUD on A
resp=$(http PUT /v1/fs/a.txt "$A_KEY" "hello from A")
check_eq "A write /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
check_eq "A read /a.txt returns 200" "200" "${resp%%|*}"
check_eq "A read /a.txt content" "hello from A" "${resp#*|}"
resp=$(http POST "/v1/fs/docs?mkdir" "$A_KEY")
check_eq "A mkdir /docs returns 200" "200" "${resp%%|*}"
resp=$(http PUT /v1/fs/docs/note.txt "$A_KEY" "nested note")
check_eq "A write /docs/note.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET "/v1/fs/docs/?list" "$A_KEY")
case "${resp#*|}" in *note.txt*) pass "A list /docs contains note.txt" ;; *) fail "A list /docs missing note.txt: ${resp#*|}" ;; esac

# --- 3. Cross-tenant isolation: same path under B holds different content
resp=$(http PUT /v1/fs/a.txt "$B_KEY" "hello from B")
check_eq "B write same path /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B reads its own /a.txt content" "hello from B" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
check_eq "A still reads its own /a.txt content" "hello from A" "${resp#*|}"
resp=$(http GET /v1/fs/docs/note.txt "$B_KEY")
check_eq "B cannot read A's /docs/note.txt" "404" "${resp%%|*}"

# --- 4. Raw SQL: both tenants' rows share the tables with distinct fs_ids
if [ -n "$DB_CONTAINER" ]; then
  A_FS_ID=$(mysql_exec "SELECT fs_id FROM $META_DB.fs_registry WHERE tenant_id='$A_TENANT';")
  B_FS_ID=$(mysql_exec "SELECT fs_id FROM $META_DB.fs_registry WHERE tenant_id='$B_TENANT';")
  [ -n "$A_FS_ID" ] && [ -n "$B_FS_ID" ] && [ "$A_FS_ID" != "$B_FS_ID" ] \
    && pass "fs_registry assigns distinct fs_ids ($A_FS_ID vs $B_FS_ID)" \
    || fail "fs_registry fs_id assignment (A=$A_FS_ID B=$B_FS_ID)"
  N=$(mysql_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$A_FS_ID;")
  [ "${N:-0}" -ge 2 ] && pass "shared file_nodes has A's rows ($N)" || fail "shared file_nodes A rows=$N"
  N=$(mysql_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$B_FS_ID;")
  [ "${N:-0}" -ge 1 ] && pass "shared file_nodes has B's rows ($N)" || fail "shared file_nodes B rows=$N"
fi

# --- 5. Delete tenant A: purges its shared rows; B unaffected
resp=$(http DELETE /v1/tenant "$A_KEY")
check_eq "DELETE /v1/tenant A returns 202" "202" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
# Delete revokes the API key, so auth fails at the key-status check (401)
# before ever reaching the tenant-status gate (403) — accept either.
[ "${resp%%|*}" = "401" ] || [ "${resp%%|*}" = "403" ] && pass "A key rejected after delete (got ${resp%%|*})" || fail "A key rejected after delete (want 401|403, got ${resp%%|*})"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B unaffected after A delete" "hello from B" "${resp#*|}"
if [ -n "$DB_CONTAINER" ]; then
  N=$(mysql_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$A_FS_ID;")
  check_eq "A's file_nodes purged from shared DB" "0" "${N:-?}"
  N=$(mysql_exec "SELECT COUNT(*) FROM $SHARED_DB.inodes WHERE fs_id=$A_FS_ID;")
  check_eq "A's inodes purged from shared DB" "0" "${N:-?}"
  N=$(mysql_exec "SELECT COUNT(*) FROM $SHARED_DB.file_nodes WHERE fs_id=$B_FS_ID;")
  [ "${N:-0}" -ge 1 ] && pass "B's rows still in shared DB" || fail "B's rows missing after A purge"
fi

log "all shared-mode e2e checks done"
