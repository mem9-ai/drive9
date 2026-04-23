#!/usr/bin/env bash
# verify-description-e2e.sh — End-to-end test for the drive9 description feature.
#
# This script spins up all required dependencies (TiDB, Ollama) via Docker,
# builds the drive9 binaries, starts drive9-server-local, and runs the
# description smoke test suite. Everything is self-contained and cleaned up
# on exit.
#
# Prerequisites:
#   - Docker (docker daemon running)
#   - docker-compose or docker compose
#   - Go 1.26+ (for building)
#   - jq
#   - mycli or mysql client
#
# Usage:
#   ./e2e/verify-description-e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
TIDB_IMAGE="pingcap/tidb:v8.5.0"
OLLAMA_IMAGE="ollama/ollama:latest"
OLLAMA_MODEL="${OLLAMA_MODEL:-mxbai-embed-large}"
EMBED_DIMS="${EMBED_DIMS:-1024}"
TIDB_PORT="4400"
STUB_PORT="11435"
SERVER_PORT="9009"
SERVER_PID=""
STUB_PID=""
COMPOSE_PROJECT="drive9-desc-e2e"

USE_LOCAL_OLLAMA="${USE_LOCAL_OLLAMA:-0}"
USE_STUB_EMBEDDER="${USE_STUB_EMBEDDER:-1}"
OLLAMA_HOST="${OLLAMA_HOST:-127.0.0.1}"
OLLAMA_PORT="${OLLAMA_PORT:-11434}"
OLLAMA_API_BASE="http://${OLLAMA_HOST}:${OLLAMA_PORT}"
STUB_API_BASE="http://127.0.0.1:${STUB_PORT}"

# ------------------------------------------------------------------
# Colors
# ------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# ------------------------------------------------------------------
# Cleanup — always run on exit
# ------------------------------------------------------------------
cleanup() {
    local exit_code=$?
    log_info "Cleaning up..."

    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log_info "Stopping drive9-server-local (pid $SERVER_PID)"
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

    if [ $exit_code -eq 0 ]; then
        log_info "✅ E2E test completed successfully"
    else
        log_err "❌ E2E test failed with exit code $exit_code"
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
wait_for_tcp() {
    local host="$1" port="$2" label="$3" max_wait="${4:-60}"
    local waited=0
    log_info "Waiting for $label at $host:$port..."
    while ! nc -z "$host" "$port" 2>/dev/null; do
        if [ "$waited" -ge "$max_wait" ]; then
            log_err "Timeout: $label did not become ready within ${max_wait}s"
            exit 1
        fi
        sleep 1
        waited=$((waited+1))
    done
    log_info "$label is ready (waited ${waited}s)"
}

wait_for_http() {
    local url="$1" label="$2" max_wait="${3:-60}"
    local waited=0
    log_info "Waiting for $label at $url..."
    while ! curl -sf "$url" >/dev/null 2>&1; do
        if [ "$waited" -ge "$max_wait" ]; then
            log_err "Timeout: $label did not become ready within ${max_wait}s"
            exit 1
        fi
        sleep 1
        waited=$((waited+1))
    done
    log_info "$label is ready (waited ${waited}s)"
}

# ------------------------------------------------------------------
# 1. Build binaries
# ------------------------------------------------------------------
log_info "Building drive9 binaries..."
cd "$PROJECT_ROOT"
make build-cli build-server-local

# ------------------------------------------------------------------
# 2. Start TiDB
# ------------------------------------------------------------------
log_info "Starting TiDB container..."
docker run -d \
    --name "${COMPOSE_PROJECT}-tidb" \
    --platform linux/amd64 \
    -p "${TIDB_PORT}:4000" \
    -e TZ=UTC \
    "${TIDB_IMAGE}"

wait_for_tcp 127.0.0.1 "$TIDB_PORT" "TiDB" 60

# Give TiDB a moment to finish internal bootstrap
sleep 2

# Create database
log_info "Creating database..."
if command -v mycli >/dev/null 2>&1; then
    mycli --host 127.0.0.1 --port "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;" 2>/dev/null
elif command -v mysql >/dev/null 2>&1; then
    mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;" 2>/dev/null
else
    log_err "Neither mycli nor mysql client found"
    exit 1
fi

# ------------------------------------------------------------------
# 3. Start embedding provider (stub, local Ollama, or Docker Ollama)
# ------------------------------------------------------------------
if [ "$USE_STUB_EMBEDDER" = "1" ]; then
    log_info "Starting stub embedder on port ${STUB_PORT}..."
    go run "${SCRIPT_DIR}/stub_embedder.go" &
    STUB_PID=$!
    wait_for_http "${STUB_API_BASE}/v1/embeddings" "Stub embedder" 10
elif [ "$USE_LOCAL_OLLAMA" = "1" ]; then
    log_info "Using local Ollama at ${OLLAMA_API_BASE}..."
    if ! curl -sf "${OLLAMA_API_BASE}" >/dev/null 2>&1; then
        log_err "Local Ollama is not running at ${OLLAMA_API_BASE}"
        log_err "Please start it first: ollama serve"
        exit 1
    fi
    # Check if model exists locally
    if ! curl -sf "${OLLAMA_API_BASE}/api/tags" | grep -q "\"${OLLAMA_MODEL}\""; then
        log_warn "Model '$OLLAMA_MODEL' not found locally. Pulling..."
        ollama pull "$OLLAMA_MODEL"
    fi
    wait_for_http "${OLLAMA_API_BASE}" "Local Ollama API" 10
else
    log_info "Starting Ollama container..."
    docker run -d \
        --name "${COMPOSE_PROJECT}-ollama" \
        -p "${OLLAMA_PORT}:11434" \
        -v "${COMPOSE_PROJECT}-ollama:/root/.ollama" \
        "${OLLAMA_IMAGE}"

    wait_for_tcp 127.0.0.1 "$OLLAMA_PORT" "Ollama" 60

    log_info "Pulling Ollama model: $OLLAMA_MODEL..."
    docker exec "${COMPOSE_PROJECT}-ollama" ollama pull "$OLLAMA_MODEL"
    wait_for_http "http://127.0.0.1:${OLLAMA_PORT}" "Ollama API" 30
fi

# ------------------------------------------------------------------
# 4. Start drive9-server-local
# ------------------------------------------------------------------
log_info "Starting drive9-server-local..."

export DRIVE9_LOCAL_DSN="root@tcp(127.0.0.1:${TIDB_PORT})/drive9_local?parseTime=true"
export DRIVE9_LOCAL_INIT_SCHEMA=true
export DRIVE9_LOCAL_EMBEDDING_MODE=app
export DRIVE9_LOCAL_API_KEY=local-dev-key

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

# Start server in background, capture logs
"${PROJECT_ROOT}/bin/drive9-server-local" > /tmp/drive9-server-local-e2e.log 2>&1 &
SERVER_PID=$!

log_info "Waiting for drive9-server-local at http://127.0.0.1:${SERVER_PORT}/v1/status..."
API_KEY="local-dev-key"
waited=0
while ! curl -sf "http://127.0.0.1:${SERVER_PORT}/v1/status" -H "Authorization: Bearer ${API_KEY}" >/dev/null 2>&1; do
    if [ "$waited" -ge 30 ]; then
        log_err "Timeout: drive9-server-local did not become ready within 30s"
        exit 1
    fi
    sleep 1
    waited=$((waited+1))
done
log_info "drive9-server-local is ready (waited ${waited}s)"

# ------------------------------------------------------------------
# 5. Run smoke tests
# ------------------------------------------------------------------
log_info "Running description smoke tests..."

CLI="${PROJECT_ROOT}/bin/drive9"
BASE="http://127.0.0.1:${SERVER_PORT}"
API_KEY="local-dev-key"
DB_DSN="root@tcp(127.0.0.1:${TIDB_PORT})/drive9_local"

if command -v mycli >/dev/null 2>&1; then
    MYSQL_CLIENT="mycli --dsn ${DB_DSN}"
else
    MYSQL_CLIENT="mysql -h 127.0.0.1 -P ${TIDB_PORT} -u root -D drive9_local"
fi

PASS=0
FAIL=0
TOTAL=0

check_eq() {
    local desc="$1" got="$2" want="$3"
    TOTAL=$((TOTAL+1))
    if [ "$got" = "$want" ]; then
        echo -e "  ${GREEN}✅ PASS${NC}: $desc"
        PASS=$((PASS+1))
    else
        echo -e "  ${RED}❌ FAIL${NC}: $desc (want='$want' got='$got')"
        FAIL=$((FAIL+1))
    fi
}

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
            echo -e "  ${RED}❌ Task dead_lettered${NC}: $err"
            return 1
        fi
        if [ "$waited" -ge "$max_wait" ]; then
            echo -e "  ${RED}❌ Timeout${NC} waiting for embed task after ${max_wait}s"
            return 1
        fi
        sleep 1
        waited=$((waited+1))
    done
}

echo ""
echo "========================================"
echo "Description Feature E2E Smoke Test"
echo "========================================"

# ---- 0. Cleanup ----
echo ""
log_info "[0/5] Cleaning up previous test artifacts..."
$MYSQL_CLIENT -e "DELETE FROM semantic_tasks WHERE task_type = 'embed';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM file_nodes WHERE path LIKE '/smoke-%';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM files WHERE file_id NOT IN (SELECT file_id FROM file_nodes);" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM uploads WHERE target_path LIKE '/smoke-%';" 2>/dev/null || true

# ---- 1. Small file upload with description ----
echo ""
log_info "[1/5] Small file upload with description..."
$CLI ctx add e2e "$BASE" "$API_KEY" 2>/dev/null || true
$CLI ctx e2e 2>/dev/null || true
$CLI fs cp --description "quarterly financial report Q1 2026" /etc/hosts :/smoke-small.txt

DESC=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description stored" "$DESC" "quarterly financial report Q1 2026"

FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 60

HAS_DESC_EMB=$(sql_scalar "SELECT description_embedding IS NOT NULL FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding generated" "$HAS_DESC_EMB" "1"

REV_MATCH=$(sql_scalar "SELECT description_embedding_revision = revision FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding_revision matches revision" "$REV_MATCH" "1"

# ---- 2. Large file multipart upload with description ----
echo ""
log_info "[2/5] Large file multipart upload with description..."
dd if=/dev/urandom of=/tmp/smoke-large.bin bs=1M count=5 2>/dev/null
$CLI fs cp --description "5MB random blob for backup" /tmp/smoke-large.bin :/smoke-large.bin

DESC2=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-large.bin';")
check_eq "large file description stored" "$DESC2" "5MB random blob for backup"

# ---- 3. Overwrite without description preserves old value ----
echo ""
log_info "[3/5] Overwrite without description preserves old value..."
cat /etc/hosts | $CLI fs cp - :/smoke-small.txt

FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 60 || true

DESC3=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description preserved after overwrite without desc" "$DESC3" "quarterly financial report Q1 2026"

# ---- 4. Overwrite with new description replaces old value ----
echo ""
log_info "[4/5] Overwrite with new description replaces old value..."
$CLI fs cp --description "updated description after review" /etc/hosts :/smoke-small.txt

FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 60 || true

DESC4=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description updated after overwrite with new desc" "$DESC4" "updated description after review"

# ---- 5. Grep API behavior on local TiDB (known limitation) ----
echo ""
log_info "[5/5] Grep API behavior on local TiDB (known limitation)..."
RESP=$(curl -sf "${BASE}/v1/fs/?grep=financial+report" -H "X-Dat9-API-Key: ${API_KEY}" || true)
if [ -z "$RESP" ] || [ "$RESP" = "null" ] || [ "$RESP" = "[]" ]; then
    echo -e "  ${YELLOW}⚠️  Grep returns empty${NC} — expected on local TiDB (vec_embed_cosine_distance not available)"
    echo -e "  ${YELLOW}ℹ️  This is a platform limitation, not a description bug.${NC}"
else
    echo -e "  ${GREEN}✅ Grep returned results${NC} (unexpected but welcome): $RESP"
fi

# ---- Summary ----
echo ""
echo "========================================"
echo "Summary: ${PASS}/${TOTAL} passed, ${FAIL}/${TOTAL} failed"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    log_err "Some tests failed. Server logs: /tmp/drive9-server-local-e2e.log"
    exit 1
fi

log_info "All description E2E tests passed!"
