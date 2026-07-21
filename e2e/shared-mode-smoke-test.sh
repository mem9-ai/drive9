#!/usr/bin/env bash
# End-to-end smoke test for shared-schema (fs_id) mode against real engines:
#   - MySQL container      → control-plane meta DB
#   - TiDB container       → shared-schema DB (validates CLUSTERED DDL) AND a
#                            second database simulating a pre-existing
#                            standalone tenant DB (coexistence)
#
# Covers: provision onto shared pool (instant active), CRUD, cross-tenant
# isolation (read/list/grep/find/copy/hardlink/rename/delete, concurrent
# same-path writes, SSE non-leak, multipart session ownership), SSE events,
# multipart upload, fork rejection, shared delete + purge, server restart
# persistence, standalone↔shared coexistence, then the full
# api-smoke-test.sh + cli-smoke-test.sh suites against the same shared
# server (each suite provisions its own fresh tenant; fork/sql/semantic skipped).
#
# Override endpoints to skip containers:
#   META_DSN=... SHARED_DSN=... STANDALONE_DSN=... bash e2e/shared-mode-smoke-test.sh
#
# Knobs:
#   RUN_SHARED_API_CLI_SMOKE=0  skip the nested api/cli suites (default 1)

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
# After the shared-mode-specific checks, re-run the full API + CLI smoke suites
# against this shared server. Each suite provisions its own tenant (DRIVE9_API_KEY
# is cleared). Default on; set RUN_SHARED_API_CLI_SMOKE=0 to skip.
RUN_SHARED_API_CLI_SMOKE="${RUN_SHARED_API_CLI_SMOKE:-1}"

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
# http_hdr METHOD PATH API_KEY HEADER_NAME HEADER_VALUE [HEADER_NAME HEADER_VALUE ...]
# Used for copy/hardlink/rename which carry the source path in a header.
http_hdr() {
  local method="$1" path="$2" key="$3"
  shift 3
  local args=(-sS -o "$TMP_DIR/resp.json" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $key")
  while [ "$#" -ge 2 ]; do
    args+=(-H "$1: $2")
    shift 2
  done
  local code
  code=$(curl "${args[@]}" "http://127.0.0.1:${SERVER_PORT}${path}" 2>/dev/null) || code="000"
  printf '%s|%s' "$code" "$(cat "$TMP_DIR/resp.json" 2>/dev/null || true)"
}
field() { printf '%s' "$2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null || true; }
# list_has_name BODY NAME → "true"/"false" for list responses {entries:[{name:...}]}
list_has_name() {
  printf '%s' "$1" | python3 -c '
import json, sys
name = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("false"); raise SystemExit
entries = data.get("entries") if isinstance(data, dict) else None
if not isinstance(entries, list):
    print("false"); raise SystemExit
print("true" if any(isinstance(e, dict) and e.get("name") == name for e in entries) else "false")
' "$2" 2>/dev/null || echo "false"
}
# search_has_path BODY PATH → "true"/"false" for grep/find arrays [{path:...}].
# Accepts paths with or without a leading slash (API responses vary by suite).
search_has_path() {
  printf '%s' "$1" | python3 -c '
import json, sys
want = sys.argv[1].lstrip("/")
try:
    data = json.load(sys.stdin)
except Exception:
    print("false"); raise SystemExit
if not isinstance(data, list):
    print("false"); raise SystemExit
def norm(p):
    return (p or "").lstrip("/")
print("true" if any(isinstance(e, dict) and norm(e.get("path")) == want for e in data) else "false")
' "$2" 2>/dev/null || echo "false"
}

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

# ============ 2b. Cross-tenant isolation: list / grep / find / mutations ============
# A-only marker used for search isolation (unique string B must never see).
ISOLATION_MARKER="shared-isolation-marker-A-only"
resp=$(http PUT /v1/fs/docs/secret-a.txt "$A_KEY" "$ISOLATION_MARKER")
check_eq "A write isolation marker returns 200" "200" "${resp%%|*}"

# Root list: each tenant only sees its own tree (B must not see A's /docs).
resp=$(http GET "/v1/fs/?list" "$A_KEY")
check_eq "A root list returns 200" "200" "${resp%%|*}"
check_eq "A root list includes a.txt" "true" "$(list_has_name "${resp#*|}" "a.txt")"
check_eq "A root list includes docs/" "true" "$(list_has_name "${resp#*|}" "docs")"
resp=$(http GET "/v1/fs/?list" "$B_KEY")
check_eq "B root list returns 200" "200" "${resp%%|*}"
check_eq "B root list includes a.txt" "true" "$(list_has_name "${resp#*|}" "a.txt")"
check_eq "B root list does NOT include A's docs/" "false" "$(list_has_name "${resp#*|}" "docs")"

# B listing a directory that only exists under A must not leak A's children.
# Product behavior: missing dir may be 404 or 200+empty entries; either is fine
# as long as A's note.txt / secret-a.txt never appear.
resp=$(http GET "/v1/fs/docs/?list" "$B_KEY")
B_DOCS_CODE="${resp%%|*}"
B_DOCS_BODY="${resp#*|}"
case "$B_DOCS_CODE" in
  404) pass "B list of A's /docs returns 404" ;;
  200)
    pass "B list of A's /docs returns 200 (empty-or-local only)"
    check_eq "B list /docs does NOT include note.txt" "false" "$(list_has_name "$B_DOCS_BODY" "note.txt")"
    check_eq "B list /docs does NOT include secret-a.txt" "false" "$(list_has_name "$B_DOCS_BODY" "secret-a.txt")"
    entry_n=$(printf '%s' "$B_DOCS_BODY" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin); e=d.get("entries") if isinstance(d,dict) else None
  print(len(e) if isinstance(e,list) else -1)
except Exception:
  print(-1)')
    check_eq "B list /docs entry count is 0" "0" "$entry_n"
    ;;
  *) fail "B list of A's /docs unexpected status $B_DOCS_CODE body=$B_DOCS_BODY" ;;
esac

# Grep: A finds its marker; B's grep for the same string is empty.
resp=$(http GET "/v1/fs/?grep=shared-isolation-marker-A-only&limit=20" "$A_KEY")
check_eq "A grep isolation marker returns 200" "200" "${resp%%|*}"
check_eq "A grep finds /docs/secret-a.txt" "true" "$(search_has_path "${resp#*|}" "/docs/secret-a.txt")"
resp=$(http GET "/v1/fs/?grep=shared-isolation-marker-A-only&limit=20" "$B_KEY")
check_eq "B grep isolation marker returns 200" "200" "${resp%%|*}"
check_eq "B grep does NOT find A's secret" "false" "$(search_has_path "${resp#*|}" "/docs/secret-a.txt")"
# Empty array is also fine; any hit with A's path is a leak.
case "${resp#*|}" in *secret-a*|*isolation-marker*) fail "B grep body leaked A content: ${resp#*|}" ;; *) pass "B grep body has no A marker/path" ;; esac

# Find by name: note.txt only exists under A.
resp=$(http GET "/v1/fs/?find=&name=note.txt" "$A_KEY")
check_eq "A find note.txt returns 200" "200" "${resp%%|*}"
check_eq "A find returns /docs/note.txt" "true" "$(search_has_path "${resp#*|}" "/docs/note.txt")"
resp=$(http GET "/v1/fs/?find=&name=note.txt" "$B_KEY")
check_eq "B find note.txt returns 200" "200" "${resp%%|*}"
check_eq "B find does NOT return A's note.txt" "false" "$(search_has_path "${resp#*|}" "/docs/note.txt")"

# Mutations must not reach the other tenant's namespace (source missing → 404).
resp=$(http_hdr POST "/v1/fs/stolen-copy.txt?copy" "$B_KEY" "X-Dat9-Copy-Source" "/docs/note.txt")
check_eq "B copy from A's /docs/note.txt returns 404" "404" "${resp%%|*}"
resp=$(http_hdr POST "/v1/fs/stolen-link.txt?hardlink=1" "$B_KEY" "X-Dat9-Hardlink-Source" "/docs/note.txt")
check_eq "B hardlink from A's /docs/note.txt returns 404" "404" "${resp%%|*}"
resp=$(http_hdr POST "/v1/fs/stolen-renamed.txt?rename" "$B_KEY" "X-Dat9-Rename-Source" "/docs/note.txt")
check_eq "B rename of A's /docs/note.txt returns 404" "404" "${resp%%|*}"
resp=$(http DELETE /v1/fs/docs/note.txt "$B_KEY")
check_eq "B delete of A's /docs/note.txt returns 404" "404" "${resp%%|*}"
resp=$(http GET /v1/fs/docs/note.txt "$A_KEY")
check_eq "A still has /docs/note.txt after B mutation attempts" "nested note" "${resp#*|}"

# Delete own path must not affect the peer's same-path file.
resp=$(http DELETE /v1/fs/a.txt "$B_KEY")
check_eq "B delete own /a.txt returns 200" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/a.txt "$A_KEY")
check_eq "A /a.txt intact after B deletes same path" "hello from A" "${resp#*|}"
resp=$(http GET /v1/fs/a.txt "$B_KEY")
check_eq "B /a.txt gone after self-delete" "404" "${resp%%|*}"
# Restore B's /a.txt for later shared-mode steps (restart / standalone checks).
resp=$(http PUT /v1/fs/a.txt "$B_KEY" "hello from B")
check_eq "B restore /a.txt returns 200" "200" "${resp%%|*}"

# Concurrent same-path writes: each tenant still reads only its own bytes.
# Use dedicated curl (not http()) so concurrent jobs do not share resp.json.
curl -sS -o /dev/null -X PUT \
  -H "Authorization: Bearer $A_KEY" -H "Content-Type: application/octet-stream" \
  --data-binary "race-from-A" "http://127.0.0.1:${SERVER_PORT}/v1/fs/race.txt" &
PID_A=$!
curl -sS -o /dev/null -X PUT \
  -H "Authorization: Bearer $B_KEY" -H "Content-Type: application/octet-stream" \
  --data-binary "race-from-B" "http://127.0.0.1:${SERVER_PORT}/v1/fs/race.txt" &
PID_B=$!
wait "$PID_A" "$PID_B" || true
resp=$(http GET /v1/fs/race.txt "$A_KEY")
check_eq "A reads own race.txt after concurrent write" "race-from-A" "${resp#*|}"
resp=$(http GET /v1/fs/race.txt "$B_KEY")
check_eq "B reads own race.txt after concurrent write" "race-from-B" "${resp#*|}"

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

# ============ 4. SSE events on shared tenant (+ cross-tenant non-leak) ============
SSE_OUT="$TMP_DIR/sse.log"
SSE_A_OUT="$TMP_DIR/sse-a.log"
curl -sS -N --max-time 12 -H "Authorization: Bearer $B_KEY" \
  "http://127.0.0.1:$SERVER_PORT/v1/events?since=0" >"$SSE_OUT" 2>/dev/null &
SSE_PID=$!
curl -sS -N --max-time 12 -H "Authorization: Bearer $A_KEY" \
  "http://127.0.0.1:$SERVER_PORT/v1/events?since=0" >"$SSE_A_OUT" 2>/dev/null &
SSE_A_PID=$!
sleep 2
resp=$(http PUT /v1/fs/sse-target.txt "$B_KEY" "sse payload")
check_eq "B write for SSE returns 200" "200" "${resp%%|*}"
sleep 4
kill "$SSE_PID" >/dev/null 2>&1 || true
kill "$SSE_A_PID" >/dev/null 2>&1 || true
wait "$SSE_PID" 2>/dev/null || true
wait "$SSE_A_PID" 2>/dev/null || true
if grep -q "initial_sync\|server_restart" "$SSE_OUT" && grep -q "sse-target.txt" "$SSE_OUT"; then
  pass "SSE stream delivered reset + file event for shared tenant"
else
  fail "SSE stream missing events: $(head -c 400 "$SSE_OUT")"
fi
if grep -q "initial_sync\|server_restart" "$SSE_A_OUT"; then
  pass "A SSE stream delivered reset event"
else
  fail "A SSE stream missing reset: $(head -c 400 "$SSE_A_OUT")"
fi
if grep -q "sse-target.txt" "$SSE_A_OUT"; then
  fail "A SSE stream leaked B's sse-target.txt event"
else
  pass "A SSE stream does not contain B's sse-target.txt"
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
# Peer tenant must not see the completed multipart object.
resp=$(http GET /v1/fs/big.bin "$A_KEY")
check_eq "A cannot read B's multipart /big.bin" "404" "${resp%%|*}"
# Upload session ownership: A cannot complete B's in-flight upload_id.
jq -n --arg path "/iso-upload.bin" --argjson total_size 10485760 --arg checksums "$CHECKSUMS" \
  '{path:$path,total_size:$total_size,part_checksums:($checksums|split(","))}' > "$TMP_DIR/init-iso.json"
code=$(curl -sS -o "$TMP_DIR/plan-iso.json" -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $B_KEY" -H "Content-Type: application/json" \
  --data-binary "@$TMP_DIR/init-iso.json" "http://127.0.0.1:$SERVER_PORT/v1/uploads/initiate")
check_eq "B isolation multipart initiate returns 202" "202" "$code"
ISO_UPLOAD_ID=$(jq -r '.upload_id // empty' "$TMP_DIR/plan-iso.json")
[ -n "$ISO_UPLOAD_ID" ] && pass "B isolation upload_id issued" || fail "B isolation upload_id empty"
resp=$(http POST "/v1/uploads/$ISO_UPLOAD_ID/complete" "$A_KEY")
# Wrong-tenant complete must not succeed (404 not-found or 403 forbidden).
case "${resp%%|*}" in
  404|403) pass "A cannot complete B's upload session (got ${resp%%|*})" ;;
  *) fail "A complete of B's upload want 404/403 got ${resp%%|*} body=${resp#*|}" ;;
esac
# B can still complete its own session after parts are uploaded.
python3 - "$TMP_DIR/plan-iso.json" "$BIG_FILE" <<'PY'
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
check_eq "B isolation multipart parts uploaded" "0" "$?"
resp=$(http POST "/v1/uploads/$ISO_UPLOAD_ID/complete" "$B_KEY")
check_eq "B completes own isolation upload" "200" "${resp%%|*}"
resp=$(http GET /v1/fs/iso-upload.bin "$A_KEY")
check_eq "A cannot read B's iso-upload.bin" "404" "${resp%%|*}"

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

# ============ 10. Full API + CLI smoke (fresh tenants each) ============
# Reuse the live shared-mode server for the broad regression suites. Each
# suite provisions its own tenant; we intentionally do not hand them A/B/S
# keys. Semantic checks are off because this server boots with
# DRIVE9_DISABLE_AUTO_EMBEDDING=true. Fork is unsupported on shared tenants
# (409), so RUN_CLI_FORK_CHECKS=0.
run_full_smoke() {
  local name="$1"
  local script="$2"
  log "=== [$name] $script (fresh provision against shared server) ==="
  set +e
  # -u DRIVE9_API_KEY forces fresh provision even if the outer env has a key.
  # Semantic is off (no auto-embedding). Fork is off (shared tenants 409).
  # SQL is off (ExecSQL is unsupported on shared-schema stores).
  env -u DRIVE9_API_KEY \
    DRIVE9_BASE="http://127.0.0.1:$SERVER_PORT" \
    RUN_SEMANTIC_CHECKS=0 \
    RUN_CLI_SEMANTIC_CHECKS=0 \
    RUN_CLI_FORK_CHECKS=0 \
    RUN_SQL_CHECKS=0 \
    bash "$script"
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    pass "$name smoke against shared server"
  else
    fail "$name smoke against shared server (rc=$rc)"
  fi
}

if [ "$RUN_SHARED_API_CLI_SMOKE" = "1" ]; then
  run_full_smoke "api" "e2e/api-smoke-test.sh"
  run_full_smoke "cli" "e2e/cli-smoke-test.sh"
else
  log "skipping nested api/cli smoke (RUN_SHARED_API_CLI_SMOKE=$RUN_SHARED_API_CLI_SMOKE)"
fi

log "all shared-mode e2e checks done"
