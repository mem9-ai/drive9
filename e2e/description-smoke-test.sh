#!/usr/bin/env bash
# description-smoke-test: file description feature smoke (self-contained stack).
#
# Starts TiDB + stub embedder (default) or Ollama, builds binaries, runs
# drive9-server (provider=local), then checks description storage/embed/overwrite behavior.
# Everything is cleaned up on exit.
#
# Prerequisites: Docker, Go, jq, mycli or mysql client.
#
# Env:
#   USE_STUB_EMBEDDER=1 (default) | USE_LOCAL_OLLAMA=1 | neither → Docker Ollama
#   OLLAMA_MODEL, EMBED_DIMS (API key is provisioned after healthz)
#   POLL_TIMEOUT_S, POLL_INTERVAL_S
#
# Usage:
#   bash e2e/description-smoke-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TIDB_IMAGE="${DESCRIPTION_TIDB_IMAGE:-pingcap/tidb:v8.5.6}"
OLLAMA_IMAGE="${DESCRIPTION_OLLAMA_IMAGE:-ollama/ollama:latest}"
OLLAMA_MODEL="${OLLAMA_MODEL:-mxbai-embed-large}"
EMBED_DIMS="${EMBED_DIMS:-1024}"
TIDB_PORT="${DESCRIPTION_TIDB_PORT:-4400}"
STUB_PORT="${DESCRIPTION_STUB_PORT:-11435}"
SERVER_PORT="${DESCRIPTION_SERVER_PORT:-9009}"
SERVER_PID=""
STUB_PID=""
COMPOSE_PROJECT="drive9-desc-smoke"

USE_LOCAL_OLLAMA="${USE_LOCAL_OLLAMA:-0}"
USE_STUB_EMBEDDER="${USE_STUB_EMBEDDER:-1}"
OLLAMA_HOST="${OLLAMA_HOST:-127.0.0.1}"
OLLAMA_PORT="${OLLAMA_PORT:-11434}"
OLLAMA_API_BASE="http://${OLLAMA_HOST}:${OLLAMA_PORT}"
STUB_API_BASE="http://127.0.0.1:${STUB_PORT}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

PASS=0
FAIL=0
TOTAL=0
SKIP=0

info() { echo "  -> $*"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL + 1))
  fi
}

skip_check() {
  TOTAL=$((TOTAL + 1))
  SKIP=$((SKIP + 1))
  echo "SKIP $*"
}

cleanup() {
  local exit_code=$?
  info "cleaning up..."
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  docker rm -f "${COMPOSE_PROJECT}-tidb" 2>/dev/null || true
  if [ -n "$STUB_PID" ] && kill -0 "$STUB_PID" 2>/dev/null; then
    kill "$STUB_PID" 2>/dev/null || true
    wait "$STUB_PID" 2>/dev/null || true
  fi
  if [ "$USE_LOCAL_OLLAMA" != "1" ]; then
    docker rm -f "${COMPOSE_PROJECT}-ollama" 2>/dev/null || true
  fi
  if [ "$exit_code" -ne 0 ]; then
    echo "failed (rc=$exit_code); server log: /tmp/drive9-server-desc-smoke.log" >&2
  fi
}
trap cleanup EXIT

wait_for_tcp() {
  local host="$1" port="$2" label="$3" max_wait="${4:-60}"
  local waited=0
  info "waiting for $label at $host:$port..."
  while ! nc -z "$host" "$port" 2>/dev/null; do
    if [ "$waited" -ge "$max_wait" ]; then
      echo "timeout: $label not ready within ${max_wait}s" >&2
      exit 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
  info "$label ready (waited ${waited}s)"
}

wait_for_http() {
  local url="$1" label="$2" max_wait="${3:-60}"
  local waited=0
  info "waiting for $label at $url..."
  while ! curl -sf "$url" >/dev/null 2>&1; do
    if [ "$waited" -ge "$max_wait" ]; then
      echo "timeout: $label not ready within ${max_wait}s" >&2
      exit 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
  info "$label ready (waited ${waited}s)"
}

echo "=== drive9 description-smoke-test ==="
echo "USE_STUB_EMBEDDER=$USE_STUB_EMBEDDER USE_LOCAL_OLLAMA=$USE_LOCAL_OLLAMA"

info "building drive9 binaries..."
cd "$PROJECT_ROOT"
make build-cli build-server

info "starting TiDB container..."
docker run -d \
  --name "${COMPOSE_PROJECT}-tidb" \
  -p "${TIDB_PORT}:4000" \
  -e TZ=UTC \
  "${TIDB_IMAGE}" --store=unistore --path=/tmp/tidb >/dev/null

wait_for_tcp 127.0.0.1 "$TIDB_PORT" "TiDB" 60
sleep 2

info "creating database..."
if command -v mycli >/dev/null 2>&1; then
  mycli --host 127.0.0.1 --port "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;" 2>/dev/null
elif command -v mysql >/dev/null 2>&1; then
  mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;" 2>/dev/null
else
  echo "neither mycli nor mysql client found" >&2
  exit 1
fi

if [ "$USE_STUB_EMBEDDER" = "1" ]; then
  info "starting stub embedder on port ${STUB_PORT}..."
  go run "${SCRIPT_DIR}/tools/stub-embedder" &
  STUB_PID=$!
  wait_for_http "${STUB_API_BASE}/v1/embeddings" "stub embedder" 10
elif [ "$USE_LOCAL_OLLAMA" = "1" ]; then
  info "using local Ollama at ${OLLAMA_API_BASE}..."
  if ! curl -sf "${OLLAMA_API_BASE}" >/dev/null 2>&1; then
    echo "local Ollama not running at ${OLLAMA_API_BASE}" >&2
    exit 1
  fi
  if ! curl -sf "${OLLAMA_API_BASE}/api/tags" | grep -q "\"${OLLAMA_MODEL}\""; then
    info "pulling model $OLLAMA_MODEL..."
    ollama pull "$OLLAMA_MODEL"
  fi
  wait_for_http "${OLLAMA_API_BASE}" "local Ollama" 10
else
  info "starting Ollama container..."
  docker run -d \
    --name "${COMPOSE_PROJECT}-ollama" \
    -p "${OLLAMA_PORT}:11434" \
    -v "${COMPOSE_PROJECT}-ollama:/root/.ollama" \
    "${OLLAMA_IMAGE}" >/dev/null
  wait_for_tcp 127.0.0.1 "$OLLAMA_PORT" "Ollama" 60
  info "pulling Ollama model: $OLLAMA_MODEL..."
  docker exec "${COMPOSE_PROJECT}-ollama" ollama pull "$OLLAMA_MODEL"
  wait_for_http "http://127.0.0.1:${OLLAMA_PORT}" "Ollama API" 30
fi

info "starting drive9-server (provider=local)..."
export DRIVE9_TENANT_PROVIDER="${DRIVE9_TENANT_PROVIDER:-local}"
export DRIVE9_LOCAL_DSN="${DRIVE9_LOCAL_DSN:-root@tcp(127.0.0.1:${TIDB_PORT})/drive9_local?parseTime=true}"
export DRIVE9_META_DSN="${DRIVE9_META_DSN:-$DRIVE9_LOCAL_DSN}"
export DRIVE9_LOCAL_MYSQL_DSN="${DRIVE9_LOCAL_MYSQL_DSN:-$DRIVE9_LOCAL_DSN}"
export DRIVE9_LOCAL_EMBEDDING_MODE=app

if [ "$USE_STUB_EMBEDDER" = "1" ]; then
  export DRIVE9_EMBED_API_BASE="${STUB_API_BASE}/v1"
  export DRIVE9_EMBED_API_KEY=stub
  export DRIVE9_EMBED_MODEL="stub-model"
  export DRIVE9_EMBED_DIMENSIONS=1024
  export DRIVE9_QUERY_EMBED_API_BASE="${STUB_API_BASE}/v1"
  export DRIVE9_QUERY_EMBED_API_KEY=stub
  export DRIVE9_QUERY_EMBED_MODEL="stub-model"
  export DRIVE9_QUERY_EMBED_DIMENSIONS=1024
else
  export DRIVE9_EMBED_API_BASE="${OLLAMA_API_BASE}/v1"
  export DRIVE9_EMBED_API_KEY=ollama
  export DRIVE9_EMBED_MODEL="$OLLAMA_MODEL"
  export DRIVE9_EMBED_DIMENSIONS="$EMBED_DIMS"
  export DRIVE9_QUERY_EMBED_API_BASE="${OLLAMA_API_BASE}/v1"
  export DRIVE9_QUERY_EMBED_API_KEY=ollama
  export DRIVE9_QUERY_EMBED_MODEL="$OLLAMA_MODEL"
  export DRIVE9_QUERY_EMBED_DIMENSIONS="$EMBED_DIMS"
fi

export DRIVE9_SEMANTIC_WORKERS=1
export DRIVE9_SEMANTIC_POLL_INTERVAL_MS=200

"${PROJECT_ROOT}/bin/drive9-server" >/tmp/drive9-server-desc-smoke.log 2>&1 &
SERVER_PID=$!

waited=0
while ! curl -sf "http://127.0.0.1:${SERVER_PORT}/healthz" >/dev/null 2>&1; do
  if [ "$waited" -ge "$POLL_TIMEOUT_S" ]; then
    echo "timeout: drive9-server not ready within ${POLL_TIMEOUT_S}s" >&2
    exit 1
  fi
  sleep "$POLL_INTERVAL_S"
  waited=$((waited + POLL_INTERVAL_S))
done
info "drive9-server ready (waited ${waited}s)"

BASE="${DRIVE9_BASE:-http://127.0.0.1:${SERVER_PORT}}"
PROVISION_JSON="$(curl -sS -X POST "${BASE}/v1/provision")"
API_KEY="$(printf '%s' "$PROVISION_JSON" | jq -r '.api_key // empty')"
TENANT_ID="$(printf '%s' "$PROVISION_JSON" | jq -r '.tenant_id // empty')"
if [ -z "$API_KEY" ] || [ -z "$TENANT_ID" ]; then
  echo "provision failed: $PROVISION_JSON" >&2
  exit 1
fi
TENANT_DB="drive9_$(printf '%s' "$TENANT_ID" | tr -d '-' | tr '[:upper:]' '[:lower:]')"
waited=0
while :; do
  state="$(curl -sS -H "Authorization: Bearer ${API_KEY}" "${BASE}/v1/status" | jq -r '.status // empty')"
  if [ "$state" = "active" ]; then
    break
  fi
  if [ "$waited" -ge "$POLL_TIMEOUT_S" ]; then
    echo "timeout: tenant not active (status=$state)" >&2
    exit 1
  fi
  sleep "$POLL_INTERVAL_S"
  waited=$((waited + POLL_INTERVAL_S))
done

CLI="${PROJECT_ROOT}/bin/drive9"
DB_DSN="root@tcp(127.0.0.1:${TIDB_PORT})/${TENANT_DB}"

if command -v mycli >/dev/null 2>&1; then
  MYSQL_CLIENT="mycli --dsn ${DB_DSN}"
else
  MYSQL_CLIENT="mysql -h 127.0.0.1 -P ${TIDB_PORT} -u root -D ${TENANT_DB}"
fi

sql_scalar() {
  if command -v mycli >/dev/null 2>&1; then
    $MYSQL_CLIENT -e "$1" --csv 2>/dev/null | tail -1 | tr -d '\r' | sed 's/^"//;s/"$//'
  else
    $MYSQL_CLIENT -N -B -e "$1" 2>/dev/null | tail -1 | awk '{$1=$1};1'
  fi
}

wait_for_task() {
  local resource_id="$1"
  local max_wait="${2:-60}"
  local waited=0
  while true; do
    local status
    status=$(sql_scalar "SELECT status FROM semantic_tasks WHERE resource_id = '${resource_id}' ORDER BY created_at DESC LIMIT 1;")
    if [ "$status" = "succeeded" ] || [ "$status" = "completed" ]; then
      return 0
    fi
    if [ "$status" = "dead_lettered" ]; then
      local err
      err=$(sql_scalar "SELECT last_error FROM semantic_tasks WHERE resource_id = '${resource_id}' ORDER BY created_at DESC LIMIT 1;")
      echo "FAIL embed task dead_lettered: $err" >&2
      return 1
    fi
    if [ "$waited" -ge "$max_wait" ]; then
      echo "FAIL embed task timeout after ${max_wait}s" >&2
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

echo ""
echo "[0] cleanup previous artifacts"
$MYSQL_CLIENT -e "DELETE FROM semantic_tasks WHERE task_type = 'embed';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM semantic WHERE inode_id IN (SELECT inode_id FROM file_nodes WHERE path LIKE '/smoke-%');" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM file_nodes WHERE path LIKE '/smoke-%';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM uploads WHERE target_path LIKE '/smoke-%';" 2>/dev/null || true

echo "[1] small file upload with description"
$CLI ctx add e2e "$BASE" "$API_KEY" 2>/dev/null || true
$CLI ctx e2e 2>/dev/null || true
$CLI fs cp --description "quarterly financial report Q1 2026" /etc/hosts :/smoke-small.txt

DESC=$(sql_scalar "SELECT s.description FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description stored" "$DESC" "quarterly financial report Q1 2026"

FILE_ID=$(sql_scalar "SELECT fn.file_id FROM file_nodes fn WHERE fn.path = '/smoke-small.txt';")
if wait_for_task "$FILE_ID" 60; then
  HAS_DESC_EMB=$(sql_scalar "SELECT s.description_embedding IS NOT NULL FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id WHERE fn.path = '/smoke-small.txt';")
  check_eq "description_embedding generated" "$HAS_DESC_EMB" "1"
  REV_MATCH=$(sql_scalar "SELECT s.description_embedding_revision = i.revision FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id JOIN inodes i ON i.inode_id = fn.inode_id WHERE fn.path = '/smoke-small.txt';")
  check_eq "description_embedding_revision matches revision" "$REV_MATCH" "1"
else
  check_eq "description_embedding generated" "failed" "1"
fi

echo "[2] large file multipart upload with description"
dd if=/dev/urandom of=/tmp/smoke-large.bin bs=1M count=5 2>/dev/null
$CLI fs cp --description "5MB random blob for backup" /tmp/smoke-large.bin :/smoke-large.bin

DESC2=$(sql_scalar "SELECT s.description FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id WHERE fn.path = '/smoke-large.bin';")
check_eq "large file description stored" "$DESC2" "5MB random blob for backup"

echo "[3] overwrite without description preserves old value"
cat /etc/hosts | $CLI fs cp - :/smoke-small.txt
FILE_ID=$(sql_scalar "SELECT fn.file_id FROM file_nodes fn WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 60 || true
DESC3=$(sql_scalar "SELECT s.description FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description preserved after overwrite without desc" "$DESC3" "quarterly financial report Q1 2026"

echo "[4] overwrite with new description replaces old value"
$CLI fs cp --description "updated description after review" /etc/hosts :/smoke-small.txt
FILE_ID=$(sql_scalar "SELECT fn.file_id FROM file_nodes fn WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 60 || true
DESC4=$(sql_scalar "SELECT s.description FROM semantic s JOIN file_nodes fn ON fn.inode_id = s.inode_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description updated after overwrite with new desc" "$DESC4" "updated description after review"

echo "[5] grep API on local TiDB (known platform limit)"
RESP=$(curl -sf "${BASE}/v1/fs/?grep=financial+report" -H "Authorization: Bearer ${API_KEY}" || true)
if [ -z "$RESP" ] || [ "$RESP" = "null" ] || [ "$RESP" = "[]" ]; then
  skip_check "grep vector recall empty on local TiDB (no vec_embed_cosine_distance)"
else
  info "grep returned results: $RESP"
  check_eq "grep returned non-empty" "1" "1"
fi

echo ""
echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
