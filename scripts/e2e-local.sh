#!/usr/bin/env bash
#
# e2e-local.sh — one-shot local e2e for drive9.
#
# Starts TiDB if 127.0.0.1:4000 is down (docker/podman, pingcap/tidb unistore),
# starts drive9-server with DRIVE9_TENANT_PROVIDER=local, then runs
# e2e/smoke-all.sh. Suites POST /v1/provision themselves (no local-dev-key).
#
# Usage:
#   make e2e-local
#   bash scripts/e2e-local.sh
#   bash scripts/e2e-local.sh --keep-server
#   bash scripts/e2e-local.sh --no-build
#   RUN_API_ONLY=1 bash scripts/e2e-local.sh
#   RUN_FUSE_SMOKE=0 bash scripts/e2e-local.sh
#   DRIVE9_SERVER_BIN=/path/to/drive9-server bash scripts/e2e-local.sh --no-build
#
# Defaults:
#   RUN_API_ONLY=0              full local-e2e.yml PR set (api/cli + pack + FUSE)
#   RUN_FUSE_SMOKE=1            FUSE suites on (needs macFUSE on macOS; mount uses --mode=fuse)
#   RUN_SEMANTIC_CHECKS=0       api embedding/recall (set 1 to enable)
#   RUN_CLI_SEMANTIC_CHECKS=0   cli embedding/recall (set 1 to enable)
#   RUN_SQL_CHECKS=0            api POST /v1/sql (set 1 to enable)
#   RUN_CLI_FORK_CHECKS=0       cli tenant fork (set 1 to enable)
#   RUN_TOKENS_SMOKE=0          /v1/tokens management smoke (set 1 to enable)
#   RUN_SSE_SMOKE=0             /v1/events retention smoke (set 1 to enable)
#
# Compatible with macOS bash 3.2.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LISTEN_ADDR="${DRIVE9_LISTEN_ADDR:-127.0.0.1:9009}"
DEFAULT_DSN="root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
DB_IMAGE="${DRIVE9_LOCAL_E2E_DB_IMAGE:-pingcap/tidb:v8.5.6}"
DB_CONTAINER_NAME="${DRIVE9_LOCAL_E2E_DB_CONTAINER:-drive9-e2e-tidb}"
DB_NAME="${DRIVE9_LOCAL_E2E_DB_NAME:-drive9_local}"
SERVER_BIN="${DRIVE9_SERVER_BIN:-$ROOT/bin/drive9-server}"
SMOKE_SCRIPT="${DRIVE9_LOCAL_E2E_SMOKE_SCRIPT:-e2e/smoke-all.sh}"
KEEP_SERVER=0
KEEP_DB=0
DO_BUILD=1
STARTED_DB=0
SERVER_PID=""
S3_DIR=""
WORK_DIR="$(mktemp -d -t drive9-e2e-local-XXXXXX)"
cleanup_done=0
KEEP_WORK_DIR=0

while [ $# -gt 0 ]; do
  case "$1" in
    --keep-server) KEEP_SERVER=1; shift ;;
    --keep-db)     KEEP_DB=1; shift ;;
    --no-build)    DO_BUILD=0; shift ;;
    -h|--help)
      sed -n '2,/^set -euo pipefail$/p' "$0" | sed '/^set -euo pipefail$/d; s/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

cleanup() {
  if [ "$cleanup_done" = 1 ]; then return; fi
  cleanup_done=1
  echo ""
  echo "=== teardown ==="
  if [ "$KEEP_SERVER" != 1 ] && [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "stopping server (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  elif [ "$KEEP_SERVER" = 1 ] && [ -n "$SERVER_PID" ]; then
    echo "leaving server running (pid $SERVER_PID)"
  fi
  if [ "$STARTED_DB" = 1 ] && [ "$KEEP_DB" != 1 ] && [ -n "${DB_RUNTIME:-}" ]; then
    echo "removing $DB_CONTAINER_NAME"
    "$DB_RUNTIME" rm -f "$DB_CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
  if [ "$KEEP_SERVER" = 1 ] || [ "$KEEP_WORK_DIR" = 1 ]; then
    echo "preserving $WORK_DIR (server.log)"
    return
  fi
  rm -rf "$WORK_DIR" || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

log() { printf '\n>>> %s\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

need() {
  if ! have "$1"; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

tcp_open() {
  local host="$1" port="$2"
  python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1)
s.close()
PY
}

mysql_handshake() {
  local host="$1" port="$2"
  python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2)
s.settimeout(3)
data = s.recv(64)
s.close()
sys.exit(0 if len(data) > 4 else 1)
PY
}

wait_tcp() {
  local host="$1" port="$2" tries="${3:-90}"
  local i=0
  while [ "$i" -lt "$tries" ]; do
    if mysql_handshake "$host" "$port"; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_http() {
  local url="$1" tries="${2:-60}"
  local i=0
  while [ "$i" -lt "$tries" ]; do
    if curl -sf -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

dsn_host() {
  python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/", sys.argv[1]); print(m.group(1) if m else "")' "$1"
}

dsn_port() {
  python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/", sys.argv[1]); print(m.group(2) if m else "")' "$1"
}

pick_runtime() {
  if [ -n "${DRIVE9_LOCAL_E2E_DB_RUNTIME:-}" ]; then
    if ! have "$DRIVE9_LOCAL_E2E_DB_RUNTIME"; then
      echo "DRIVE9_LOCAL_E2E_DB_RUNTIME=$DRIVE9_LOCAL_E2E_DB_RUNTIME is not available" >&2
      exit 1
    fi
    DB_RUNTIME="$DRIVE9_LOCAL_E2E_DB_RUNTIME"
    return
  fi
  if have docker; then
    DB_RUNTIME=docker
    return
  fi
  if have podman; then
    DB_RUNTIME=podman
    return
  fi
  echo "need docker or podman to start TiDB (or start TiDB on 127.0.0.1:4000 first)" >&2
  exit 1
}

container_running() {
  [ -n "$("$DB_RUNTIME" ps -q -f "name=^${DB_CONTAINER_NAME}$" 2>/dev/null || true)" ]
}

create_database() {
  local host="$1" port="$2" db="$3"
  if have mysql; then
    mysql --protocol=tcp -h "$host" -P "$port" -u root \
      -e "CREATE DATABASE IF NOT EXISTS \`$db\`;"
    return
  fi
  pick_runtime
  if container_running; then
    "$DB_RUNTIME" run --rm --network "container:${DB_CONTAINER_NAME}" mysql:8.0.36 \
      mysql --protocol=tcp -h 127.0.0.1 -P 4000 -u root \
      -e "CREATE DATABASE IF NOT EXISTS \`$db\`;"
    return
  fi
  "$DB_RUNTIME" run --rm mysql:8.0.36 \
    mysql --protocol=tcp -h host.docker.internal -P "$port" -u root \
    -e "CREATE DATABASE IF NOT EXISTS \`$db\`;"
}

ensure_tidb() {
  : "${DRIVE9_LOCAL_DSN:=$DEFAULT_DSN}"
  export DRIVE9_LOCAL_DSN
  local host port
  host="$(dsn_host "$DRIVE9_LOCAL_DSN")"
  port="$(dsn_port "$DRIVE9_LOCAL_DSN")"
  if [ -z "$host" ] || [ -z "$port" ]; then
    echo "cannot parse host/port from DRIVE9_LOCAL_DSN=$DRIVE9_LOCAL_DSN" >&2
    exit 1
  fi

  if mysql_handshake "$host" "$port"; then
    log "reusing TiDB at $host:$port"
  else
    pick_runtime
    if container_running; then
      log "waiting for existing $DB_CONTAINER_NAME"
    else
      log "starting $DB_IMAGE as $DB_CONTAINER_NAME"
      "$DB_RUNTIME" rm -f "$DB_CONTAINER_NAME" >/dev/null 2>&1 || true
      "$DB_RUNTIME" run -d --name "$DB_CONTAINER_NAME" -p "${port}:4000" \
        "$DB_IMAGE" --store=unistore --path=/tmp/tidb >/dev/null
      STARTED_DB=1
    fi
    if ! wait_tcp "$host" "$port" 90; then
      echo "TiDB did not become ready on $host:$port" >&2
      if [ "$STARTED_DB" = 1 ]; then
        "$DB_RUNTIME" logs "$DB_CONTAINER_NAME" 2>&1 | tail -40 >&2 || true
      fi
      exit 1
    fi
  fi
  create_database "$host" "$port" "$DB_NAME"
  echo "TiDB ready (database $DB_NAME)"
}

build_server() {
  if [ -n "${DRIVE9_SERVER_BIN:-}" ]; then
    if [ ! -x "$SERVER_BIN" ]; then
      echo "DRIVE9_SERVER_BIN is not executable: $SERVER_BIN" >&2
      exit 1
    fi
    : "${DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API:=0}"
    export DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API
    log "using DRIVE9_SERVER_BIN=$SERVER_BIN"
    return
  fi
  : "${DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API:=1}"
  export DRIVE9_E2E_ALLOW_LEGACY_TOKEN_API
  if [ "$DO_BUILD" -eq 0 ] && [ -x "$SERVER_BIN" ]; then
    log "reusing existing $SERVER_BIN (--no-build)"
    return
  fi
  log "building drive9-server"
  make build-server
  SERVER_BIN="$ROOT/bin/drive9-server"
}

start_server() {
  local listen_host listen_port
  listen_host="${LISTEN_ADDR%:*}"
  listen_port="${LISTEN_ADDR##*:}"
  if tcp_open "$listen_host" "$listen_port"; then
    echo "$LISTEN_ADDR is already in use; stop that process or set DRIVE9_LISTEN_ADDR" >&2
    exit 1
  fi

  log "starting drive9-server on $LISTEN_ADDR"
  S3_DIR="$WORK_DIR/s3"
  mkdir -p "$S3_DIR"

  export DRIVE9_LISTEN_ADDR="$LISTEN_ADDR"
  export DRIVE9_PUBLIC_URL="${DRIVE9_PUBLIC_URL:-http://$LISTEN_ADDR}"
  export DRIVE9_TENANT_PROVIDER="${DRIVE9_TENANT_PROVIDER:-local}"
  export DRIVE9_LOCAL_DSN="${DRIVE9_LOCAL_DSN:-$DEFAULT_DSN}"
  export DRIVE9_META_DSN="${DRIVE9_META_DSN:-$DRIVE9_LOCAL_DSN}"
  export DRIVE9_LOCAL_MYSQL_DSN="${DRIVE9_LOCAL_MYSQL_DSN:-$DRIVE9_LOCAL_DSN}"
  export DRIVE9_S3_DIR="${DRIVE9_S3_DIR:-$S3_DIR}"

  "$SERVER_BIN" >"$WORK_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  echo "server pid: $SERVER_PID  (logs: $WORK_DIR/server.log)"

  if ! wait_http "http://$LISTEN_ADDR/healthz" 60; then
    echo "drive9-server did not become healthy" >&2
    tail -50 "$WORK_DIR/server.log" >&2 || true
    exit 1
  fi
  echo "server healthy at http://$LISTEN_ADDR/healthz"
}

apply_smoke_defaults() {
  export DRIVE9_BASE="http://$LISTEN_ADDR"
  unset DRIVE9_API_KEY
  : "${RUN_SEMANTIC_CHECKS:=0}"
  : "${RUN_CLI_SEMANTIC_CHECKS:=0}"
  : "${RUN_API_ONLY:=0}"
  : "${RUN_FUSE_SMOKE:=1}"
  export RUN_SEMANTIC_CHECKS RUN_CLI_SEMANTIC_CHECKS RUN_API_ONLY RUN_FUSE_SMOKE
}

need curl
need python3
need jq

ensure_tidb
build_server
start_server
apply_smoke_defaults

log "running $SMOKE_SCRIPT (DRIVE9_BASE=$DRIVE9_BASE RUN_API_ONLY=$RUN_API_ONLY RUN_FUSE_SMOKE=$RUN_FUSE_SMOKE)"
SMOKE_RC=0
bash "$SMOKE_SCRIPT" || SMOKE_RC=$?
if [ "$SMOKE_RC" -ne 0 ]; then
  KEEP_WORK_DIR=1
  echo "smoke failed; preserving $WORK_DIR (server.log)" >&2
fi
exit "$SMOKE_RC"
