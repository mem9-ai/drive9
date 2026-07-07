#!/usr/bin/env bash
# Start a local drive9-server-local instance and run the smoke suite against it.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT_DIR/e2e/tmp-helper.sh"
drive9_e2e_init_tmpdir
cd "$ROOT_DIR"

DB_RUNTIME="${DRIVE9_LOCAL_E2E_DB_RUNTIME:-}"
DB_IMAGE="${DRIVE9_LOCAL_E2E_DB_IMAGE:-mysql:8.4}"
DB_NAME="${DRIVE9_LOCAL_E2E_DB_NAME:-drive9_local}"
DB_PASSWORD="${DRIVE9_LOCAL_E2E_DB_PASSWORD:-drive9pass}"
KEEP_DB="${DRIVE9_LOCAL_E2E_KEEP_DB:-0}"
EMBEDDING_MODE="${DRIVE9_LOCAL_EMBEDDING_MODE:-none}"
LOCAL_INIT_SCHEMA="${DRIVE9_LOCAL_INIT_SCHEMA:-true}"
LOCAL_API_KEY="${DRIVE9_LOCAL_API_KEY:-local-dev-key}"
S3_DIR="${DRIVE9_S3_DIR:-}"
LISTEN_ADDR="${DRIVE9_LISTEN_ADDR:-}"
PUBLIC_URL="${DRIVE9_PUBLIC_URL:-}"
SERVER_LOG="${DRIVE9_LOCAL_E2E_SERVER_LOG:-}"
RUN_SMOKE_SCRIPT="${DRIVE9_LOCAL_E2E_SMOKE_SCRIPT:-e2e/smoke-all.sh}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-90}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-1}"

DB_CONTAINER=""
SERVER_PID=""
TMP_DIR="$(mktemp -d)"
E2E_HOME="$TMP_DIR/home"

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
    echo "Local smoke failed; preserving artifacts at $TMP_DIR" >&2
  fi
  exit "$rc"
}
trap cleanup EXIT

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

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
  if [ -n "$DB_RUNTIME" ]; then
    command -v "$DB_RUNTIME" >/dev/null 2>&1 || {
      echo "DRIVE9_LOCAL_E2E_DB_RUNTIME=$DB_RUNTIME is not available" >&2
      exit 1
    }
    return
  fi
  if command -v docker >/dev/null 2>&1; then
    DB_RUNTIME="docker"
    return
  fi
  if command -v podman >/dev/null 2>&1; then
    DB_RUNTIME="podman"
    return
  fi
  echo "docker or podman is required when DRIVE9_LOCAL_DSN is not set" >&2
  exit 1
}

start_mysql_container() {
  detect_runtime
  DB_CONTAINER="drive9-local-e2e-$(date +%s)-$$"
  echo "Starting $DB_RUNTIME container $DB_CONTAINER from $DB_IMAGE"
  "$DB_RUNTIME" run -d \
    --name "$DB_CONTAINER" \
    -e MYSQL_ROOT_PASSWORD="$DB_PASSWORD" \
    -e MYSQL_DATABASE="$DB_NAME" \
    -p 127.0.0.1::3306 \
    "$DB_IMAGE" >/dev/null

  local deadline port
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  port=""
  while :; do
    port=$("$DB_RUNTIME" port "$DB_CONTAINER" 3306/tcp 2>/dev/null | awk -F: 'END{print $NF}')
    if [ -n "$port" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out waiting for container port mapping" >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_S"
  done

  while :; do
    if "$DB_RUNTIME" exec "$DB_CONTAINER" mysqladmin ping -uroot -p"$DB_PASSWORD" --silent >/dev/null 2>&1; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out waiting for MySQL readiness" >&2
      "$DB_RUNTIME" logs "$DB_CONTAINER" >&2 || true
      exit 1
    fi
    sleep "$POLL_INTERVAL_S"
  done

  DRIVE9_LOCAL_DSN="root:${DB_PASSWORD}@tcp(127.0.0.1:${port})/${DB_NAME}?parseTime=true"
  export DRIVE9_LOCAL_DSN
}

wait_server() {
  local deadline code
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    code=$(curl -sS -o /dev/null -w "%{http_code}" "$PUBLIC_URL/healthz" 2>/dev/null || true)
    if [ "$code" = "200" ]; then
      return
    fi
    if [ -n "$SERVER_PID" ] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "drive9-server-local exited before becoming healthy" >&2
      if [ -f "$SERVER_LOG" ]; then
        tail -200 "$SERVER_LOG" >&2 || true
      fi
      exit 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out waiting for drive9-server-local healthz" >&2
      if [ -f "$SERVER_LOG" ]; then
        tail -200 "$SERVER_LOG" >&2 || true
      fi
      exit 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
}

need_cmd curl
need_cmd jq
need_cmd python3
need_cmd go
mkdir -p "$E2E_HOME"

if [ -z "${DRIVE9_LOCAL_DSN:-}" ]; then
  start_mysql_container
else
  echo "Using existing DRIVE9_LOCAL_DSN"
fi

if [ -z "$LISTEN_ADDR" ]; then
  LISTEN_ADDR="127.0.0.1:$(pick_port)"
fi
if [ -z "$PUBLIC_URL" ]; then
  PUBLIC_URL="http://${LISTEN_ADDR}"
fi
if [ -z "$S3_DIR" ]; then
  S3_DIR="$TMP_DIR/s3"
fi
if [ -z "$SERVER_LOG" ]; then
  SERVER_LOG="$(drive9_e2e_tmp_path "drive9-server-local-e2e-$(date +%s)-$$.log")"
fi

case "$EMBEDDING_MODE" in
  none|skip|disabled|off)
    unset DRIVE9_EMBED_API_BASE DRIVE9_EMBED_API_KEY DRIVE9_EMBED_MODEL DRIVE9_EMBED_DIMENSIONS
    unset DRIVE9_QUERY_EMBED_API_BASE DRIVE9_QUERY_EMBED_API_KEY DRIVE9_QUERY_EMBED_MODEL DRIVE9_QUERY_EMBED_DIMENSIONS
    ;;
esac

echo "Building drive9-server-local"
make build-server-local

echo "Starting drive9-server-local at $PUBLIC_URL"
env \
  DRIVE9_LISTEN_ADDR="$LISTEN_ADDR" \
  DRIVE9_PUBLIC_URL="$PUBLIC_URL" \
  DRIVE9_LOCAL_DSN="$DRIVE9_LOCAL_DSN" \
  DRIVE9_LOCAL_INIT_SCHEMA="$LOCAL_INIT_SCHEMA" \
  DRIVE9_LOCAL_EMBEDDING_MODE="$EMBEDDING_MODE" \
  DRIVE9_LOCAL_API_KEY="$LOCAL_API_KEY" \
  DRIVE9_S3_DIR="$S3_DIR" \
  DRIVE9_LOG_LEVEL="${DRIVE9_LOG_LEVEL:-warn}" \
  ./bin/drive9-server-local >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

wait_server
echo "drive9-server-local is healthy"

echo "Running $RUN_SMOKE_SCRIPT"
env -u DRIVE9_VAULT_TOKEN \
  DRIVE9_BASE="$PUBLIC_URL" \
  DRIVE9_API_KEY="$LOCAL_API_KEY" \
  HOME="$E2E_HOME" \
  RUN_SEMANTIC_CHECKS="${RUN_SEMANTIC_CHECKS:-0}" \
  RUN_CLI_SEMANTIC_CHECKS="${RUN_CLI_SEMANTIC_CHECKS:-0}" \
  RUN_UPLOAD_LIMIT_BOUNDARY="${RUN_UPLOAD_LIMIT_BOUNDARY:-0}" \
  RUN_CLI_UPLOAD_LIMIT_BOUNDARY="${RUN_CLI_UPLOAD_LIMIT_BOUNDARY:-0}" \
  RUN_FUSE_SMOKE="${RUN_FUSE_SMOKE:-0}" \
  POLL_TIMEOUT_S="$POLL_TIMEOUT_S" \
  POLL_INTERVAL_S="$POLL_INTERVAL_S" \
  LARGE_FILE_MB="${LARGE_FILE_MB:-8}" \
  CLI_LARGE_FILE_MB="${CLI_LARGE_FILE_MB:-8}" \
  CLI_SOURCE="${CLI_SOURCE:-build}" \
  bash "$RUN_SMOKE_SCRIPT"

echo "Local smoke completed. Server log: $SERVER_LOG"
