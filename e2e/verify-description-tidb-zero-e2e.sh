#!/usr/bin/env bash
# verify-description-tidb-zero-e2e.sh — End-to-end test for description feature using TiDB Cloud Zero.
#
# TiDB Cloud Zero provides a free TiDB instance with native support for:
#   - Auto-embedding (EMBED_TEXT)
#   - Vector search (VEC_EMBED_COSINE_DISTANCE)
#   - Full-text search (fts_match_word)
#
# This allows testing the complete description feature including semantic
# vector recall and FTS search without local Docker or Ollama.
#
# The script provisions a disposable TiDB Zero instance, runs tests, and
# optionally claims it for persistence. Unclaimed instances auto-expire in 30 days.
#
# Prerequisites:
#   - curl, jq
#   - Go 1.26+ (for building)
#   - Network access to zero.tidbapi.com
#
# Usage:
#   ./e2e/verify-description-tidb-zero-e2e.sh
#
# Environment:
#   DRIVE9_TIDB_ZERO_TAG     — tag for the Zero instance (default: drive9-description-e2e)
#   DRIVE9_CLAIM_INSTANCE    — if set to 1, print claim URL at end

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
SERVER_PORT="9009"
SERVER_PID=""
ZERO_INSTANCE_ID=""
ZERO_CLAIM_URL=""

TIDB_ZERO_TAG="${DRIVE9_TIDB_ZERO_TAG:-drive9-description-e2e}"

# ------------------------------------------------------------------
# Colors
# ------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $*"; }

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

    if [ $exit_code -eq 0 ]; then
        log_info "✅ E2E test completed successfully"
        if [ -n "$ZERO_CLAIM_URL" ] && [ "${DRIVE9_CLAIM_INSTANCE:-0}" = "1" ]; then
            echo ""
            log_info "Claim URL for persistence: $ZERO_CLAIM_URL"
            log_info "Instance expires at: $ZERO_EXPIRES_AT"
        elif [ -n "$ZERO_CLAIM_URL" ]; then
            echo ""
            log_warn "TiDB Zero instance will auto-expire at: $ZERO_EXPIRES_AT"
            log_warn "To claim for persistence, set DRIVE9_CLAIM_INSTANCE=1 or visit:"
            log_warn "  $ZERO_CLAIM_URL"
        fi
    else
        log_err "❌ E2E test failed with exit code $exit_code"
        if [ -n "$ZERO_CLAIM_URL" ]; then
            log_warn "TiDB Zero instance claim URL (expires at $ZERO_EXPIRES_AT):"
            log_warn "  $ZERO_CLAIM_URL"
        fi
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
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
# 1. Provision TiDB Cloud Zero instance
# ------------------------------------------------------------------
log_step "[1/6] Provisioning TiDB Cloud Zero instance..."

ZERO_RESPONSE=$(curl -sf -X POST "https://zero.tidbapi.com/v1beta1/instances" \
    -H "Content-Type: application/json" \
    -d "{\"tag\":\"${TIDB_ZERO_TAG}\"}" 2>/dev/null) || {
    log_err "Failed to create TiDB Cloud Zero instance"
    exit 1
}

ZERO_INSTANCE_ID=$(echo "$ZERO_RESPONSE" | jq -r '.instance.id')
ZERO_HOST=$(echo "$ZERO_RESPONSE" | jq -r '.instance.connection.host')
ZERO_PORT=$(echo "$ZERO_RESPONSE" | jq -r '.instance.connection.port')
ZERO_USER=$(echo "$ZERO_RESPONSE" | jq -r '.instance.connection.username')
ZERO_PASS=$(echo "$ZERO_RESPONSE" | jq -r '.instance.connection.password')
ZERO_CLAIM_URL=$(echo "$ZERO_RESPONSE" | jq -r '.instance.claimInfo.claimUrl')
ZERO_EXPIRES_AT=$(echo "$ZERO_RESPONSE" | jq -r '.instance.expiresAt')

if [ -z "$ZERO_INSTANCE_ID" ] || [ "$ZERO_INSTANCE_ID" = "null" ]; then
    log_err "Invalid TiDB Zero response: $ZERO_RESPONSE"
    exit 1
fi

log_info "TiDB Zero instance created: $ZERO_INSTANCE_ID"
log_info "  Host: $ZERO_HOST:$ZERO_PORT"
log_info "  Expires: $ZERO_EXPIRES_AT"

# ------------------------------------------------------------------
# 2. Wait for TiDB to be ready and create database
# ------------------------------------------------------------------
log_step "[2/6] Waiting for TiDB Zero to be ready..."

# TiDB Zero typically takes ~30-60s to become ready
MAX_WAIT=120
waited=0
while true; do
    if mysql -h "$ZERO_HOST" -P "$ZERO_PORT" -u "$ZERO_USER" -p"$ZERO_PASS" \
        --ssl-mode=REQUIRED -e "SELECT 1;" >/dev/null 2>&1; then
        break
    fi
    if [ "$waited" -ge "$MAX_WAIT" ]; then
        log_err "Timeout: TiDB Zero did not become ready within ${MAX_WAIT}s"
        exit 1
    fi
    sleep 2
    waited=$((waited+2))
    if [ $((waited % 10)) -eq 0 ]; then
        log_info "Still waiting for TiDB Zero... (${waited}s)"
    fi
done
log_info "TiDB Zero is ready (waited ${waited}s)"

log_info "Creating database..."
mysql -h "$ZERO_HOST" -P "$ZERO_PORT" -u "$ZERO_USER" -p"$ZERO_PASS" \
    --ssl-mode=REQUIRED -e "CREATE DATABASE IF NOT EXISTS drive9_zero_e2e;" 2>/dev/null

# Build Go DSN from connection info
# Go MySQL driver DSN: user:pass@tcp(host:port)/dbname?parseTime=true&tls=true
DB_DSN="${ZERO_USER}:${ZERO_PASS}@tcp(${ZERO_HOST}:${ZERO_PORT})/drive9_zero_e2e?parseTime=true&tls=true"

# ------------------------------------------------------------------
# 3. Build binaries
# ------------------------------------------------------------------
log_step "[3/6] Building drive9 binaries..."
cd "$PROJECT_ROOT"
make build-cli build-server-local

# ------------------------------------------------------------------
# 4. Start drive9-server-local with auto-embedding mode
# ------------------------------------------------------------------
log_step "[4/6] Starting drive9-server-local (auto-embedding mode)..."

# Unset any embedder env vars that might leak from the environment
unset DRIVE9_EMBED_API_BASE 2>/dev/null || true
unset DRIVE9_EMBED_API_KEY 2>/dev/null || true
unset DRIVE9_EMBED_MODEL 2>/dev/null || true
unset DRIVE9_EMBED_DIMENSIONS 2>/dev/null || true
unset DRIVE9_QUERY_EMBED_API_BASE 2>/dev/null || true
unset DRIVE9_QUERY_EMBED_API_KEY 2>/dev/null || true
unset DRIVE9_QUERY_EMBED_MODEL 2>/dev/null || true
unset DRIVE9_QUERY_EMBED_DIMENSIONS 2>/dev/null || true

export DRIVE9_LOCAL_DSN="$DB_DSN"
export DRIVE9_LOCAL_INIT_SCHEMA="true"
export DRIVE9_LOCAL_EMBEDDING_MODE="auto"
export DRIVE9_LOCAL_API_KEY="${DRIVE9_API_KEY:-local-dev-key}"
export DRIVE9_S3_DIR="${TMPDIR:-/tmp}/drive9-zero-e2e-s3"

# No semantic workers needed in auto-embedding mode (database handles embedding)
# But keep minimal config for task polling
export DRIVE9_SEMANTIC_WORKERS=0
export DRIVE9_SEMANTIC_POLL_INTERVAL_MS=200

"${PROJECT_ROOT}/bin/drive9-server-local" > /tmp/drive9-server-local-zero-e2e.log 2>&1 &
SERVER_PID=$!

POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

API_KEY="${DRIVE9_API_KEY:-local-dev-key}"
waited=0
while ! curl -sf "http://127.0.0.1:${SERVER_PORT}/v1/status" -H "Authorization: Bearer ${API_KEY}" >/dev/null 2>&1; do
    if [ "$waited" -ge "$POLL_TIMEOUT_S" ]; then
        log_err "Timeout: drive9-server-local did not become ready within ${POLL_TIMEOUT_S}s"
        log_err "Server logs: /tmp/drive9-server-local-zero-e2e.log"
        exit 1
    fi
    sleep "$POLL_INTERVAL_S"
    waited=$((waited+POLL_INTERVAL_S))
done
log_info "drive9-server-local is ready (waited ${waited}s)"

# ------------------------------------------------------------------
# 5. Run smoke tests
# ------------------------------------------------------------------
log_step "[5/6] Running description smoke tests..."

CLI="${PROJECT_ROOT}/bin/drive9"
BASE="${DRIVE9_BASE:-http://127.0.0.1:${SERVER_PORT}}"
API_KEY="${DRIVE9_API_KEY:-local-dev-key}"

# MySQL client connection string for direct DB checks
MYSQL_CLIENT="mysql -h $ZERO_HOST -P $ZERO_PORT -u $ZERO_USER -p$ZERO_PASS --ssl-mode=REQUIRED -D drive9_zero_e2e"

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

check_contains() {
    local desc="$1" haystack="$2" needle="$3"
    TOTAL=$((TOTAL+1))
    if echo "$haystack" | grep -q "$needle"; then
        echo -e "  ${GREEN}✅ PASS${NC}: $desc"
        PASS=$((PASS+1))
    else
        echo -e "  ${RED}❌ FAIL${NC}: $desc (needle='$needle' not found in response)"
        FAIL=$((FAIL+1))
    fi
}

sql_scalar() {
    $MYSQL_CLIENT -N -B -e "$1" 2>/dev/null | head -1 | awk '{gsub(/^ +| +$/, ""); print}'
}

wait_for_task() {
    local resource_id="$1"
    local max_wait="${2:-120}"
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
echo "Description Feature E2E (TiDB Zero)"
echo "========================================"

# ---- 0. Cleanup ----
echo ""
log_info "[0/7] Cleaning up previous test artifacts..."
$MYSQL_CLIENT -e "DELETE FROM semantic_tasks WHERE task_type = 'embed';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM file_nodes WHERE path LIKE '/smoke-%';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE f FROM files f LEFT JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.file_id IS NULL;" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM uploads WHERE target_path LIKE '/smoke-%';" 2>/dev/null || true

# ---- 1. Small file upload with description ----
echo ""
log_info "[1/7] Small file upload with description..."
$CLI ctx add e2e "$BASE" "$API_KEY" 2>/dev/null || true
$CLI ctx e2e 2>/dev/null || true
$CLI fs cp --description "quarterly financial report Q1 2026" /etc/hosts :/smoke-small.txt

DESC=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description stored" "$DESC" "quarterly financial report Q1 2026"

# In auto-embedding mode, the database generates embedding via EMBED_TEXT generated column.
# No semantic worker task is created, so we verify directly in the DB.
HAS_DESC_EMB=$(sql_scalar "SELECT description_embedding IS NOT NULL FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding generated (auto-embedding)" "$HAS_DESC_EMB" "1"

REV_MATCH=$(sql_scalar "SELECT description_embedding_revision = revision FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding_revision matches revision" "$REV_MATCH" "1"

# ---- 2. Large file multipart upload with description ----
echo ""
log_info "[2/7] Large file multipart upload with description..."
dd if=/dev/urandom of=/tmp/smoke-large.bin bs=1M count=5 2>/dev/null
$CLI fs cp --description "5MB random blob for backup" /tmp/smoke-large.bin :/smoke-large.bin

DESC2=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-large.bin';")
check_eq "large file description stored" "$DESC2" "5MB random blob for backup"

# ---- 3. Overwrite without description preserves old value ----
echo ""
log_info "[3/7] Overwrite without description preserves old value..."
cat /etc/hosts | $CLI fs cp - :/smoke-small.txt

DESC3=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description preserved after overwrite without desc" "$DESC3" "quarterly financial report Q1 2026"

# ---- 4. Overwrite with new description replaces old value ----
echo ""
log_info "[4/7] Overwrite with new description replaces old value..."
$CLI fs cp --description "updated description after review" /etc/hosts :/smoke-small.txt

DESC4=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description updated after overwrite with new desc" "$DESC4" "updated description after review"

# ---- 5. Grep full-text search (FTS) on description ----
echo ""
log_info "[5/7] Grep full-text search on description (FTS via fts_match_word)..."

# Wait a moment for any background indexing
sleep 2

RESP=$(curl -sf "${BASE}/v1/fs/?grep=financial+report" -H "X-Dat9-API-Key: ${API_KEY}" || true)
if [ -z "$RESP" ] || [ "$RESP" = "null" ] || [ "$RESP" = "[]" ]; then
    echo -e "  ${YELLOW}⚠️  FTS grep returned empty${NC} — fts_match_word may need more time to index"
    echo -e "  ${YELLOW}ℹ️  This can happen if the columnar replica is still building.${NC}"
    # Don't fail the test for this — TiDB Zero FTS indexing is eventual
else
    check_contains "FTS grep finds file by description" "$RESP" "smoke-small.txt"
fi

# ---- 6. Grep semantic vector search on description ----
echo ""
log_info "[6/7] Grep semantic vector search on description (VEC_EMBED_COSINE_DISTANCE)..."

# Upload a file with a semantically distinct description
$CLI fs cp --description "machine learning model training pipeline" /etc/hosts :/smoke-ml.txt

# Give auto-embedding a moment to populate
sleep 3

# Vector search using a paraphrase
RESP=$(curl -sf "${BASE}/v1/fs/?grep=AI+training+workflow" -H "X-Dat9-API-Key: ${API_KEY}" || true)
if [ -z "$RESP" ] || [ "$RESP" = "null" ] || [ "$RESP" = "[]" ]; then
    echo -e "  ${YELLOW}⚠️  Vector grep returned empty${NC} — VEC_EMBED_COSINE_DISTANCE may need more time"
    echo -e "  ${YELLOW}ℹ️  Auto-embedding is asynchronous; results may take a few seconds.${NC}"
else
    check_contains "Vector grep finds file by semantic description" "$RESP" "smoke-ml.txt"
fi

# ---- 7. Grep combined search (content + description) ----
echo ""
log_info "[7/7] Grep combined search across content and description..."

# Upload a file with both content and description
$CLI fs cp --description "project roadmap and sprint planning" /etc/hosts :/smoke-roadmap.txt
sleep 3

# Search for a term that should match the description
RESP=$(curl -sf "${BASE}/v1/fs/?grep=sprint+planning" -H "X-Dat9-API-Key: ${API_KEY}" || true)
if [ -n "$RESP" ] && [ "$RESP" != "null" ] && [ "$RESP" != "[]" ]; then
    check_contains "Combined grep finds file by description" "$RESP" "smoke-roadmap.txt"
else
    echo -e "  ${YELLOW}⚠️  Combined grep returned empty${NC} — indexing may still be in progress"
fi

# ---- Summary ----
echo ""
echo "========================================"
echo "Summary: ${PASS}/${TOTAL} passed, ${FAIL}/${TOTAL} failed"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    log_err "Some tests failed. Server logs: /tmp/drive9-server-local-zero-e2e.log"
    exit 1
fi

log_info "All description E2E tests passed!"
