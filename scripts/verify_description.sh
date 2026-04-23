#!/usr/bin/env bash
# verify_description.sh — Automated smoke test for the drive9 description feature.
#
# Prerequisites:
#   - drive9-server-local running on 127.0.0.1:9009
#   - TiDB / MySQL running on 127.0.0.1:4000 with database drive9_local
#   - mycli (or mysql client) available in PATH
#   - jq available in PATH
#   - bin/drive9 CLI built
#
# Usage:
#   ./scripts/verify_description.sh

set -euo pipefail

CLI="${CLI:-./bin/drive9}"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-local-dev-key}"
DB="${DRIVE9_DB:-root@tcp(127.0.0.1:4000)/drive9_local}"
MYSQL_CLIENT="${MYSQL_CLIENT:-mycli --dsn ${DB}}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
    local desc="$1" got="$2" want="$3"
    TOTAL=$((TOTAL+1))
    if [ "$got" = "$want" ]; then
        echo "  ✅ PASS: $desc"
        PASS=$((PASS+1))
    else
        echo "  ❌ FAIL: $desc (want='$want' got='$got')"
        FAIL=$((FAIL+1))
    fi
}

check_cmd() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL+1))
    if "$@" >/dev/null 2>&1; then
        echo "  ✅ PASS: $desc"
        PASS=$((PASS+1))
    else
        echo "  ❌ FAIL: $desc"
        FAIL=$((FAIL+1))
    fi
}

sql_query() {
    $MYSQL_CLIENT -e "$1" --table 2>/dev/null | tail -1 | sed 's/|/ /g' | awk '{$1=$1};1'
}

sql_scalar() {
    $MYSQL_CLIENT -e "$1" --table 2>/dev/null | tail -1 | sed 's/|/ /g' | awk '{$1=$1};1'
}

wait_for_task() {
    local resource_id="$1"
    local max_wait="${2:-30}"
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
            echo "  ❌ Task dead_lettered: $err"
            return 1
        fi
        if [ "$waited" -ge "$max_wait" ]; then
            echo "  ❌ Timeout waiting for embed task after ${max_wait}s"
            return 1
        fi
        sleep 1
        waited=$((waited+1))
    done
}

echo "========================================"
echo "drive9 description feature smoke test"
echo "========================================"

# ------------------------------------------------------------------
# 0. Cleanup previous test artifacts
# ------------------------------------------------------------------
echo ""
echo "[0/5] Cleaning up previous test artifacts..."
$MYSQL_CLIENT -e "DELETE FROM semantic_tasks WHERE task_type = 'embed';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM file_nodes WHERE path LIKE '/smoke-%';" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM files WHERE file_id NOT IN (SELECT file_id FROM file_nodes);" 2>/dev/null || true
$MYSQL_CLIENT -e "DELETE FROM uploads WHERE target_path LIKE '/smoke-%';" 2>/dev/null || true
echo "  Done."

# ------------------------------------------------------------------
# 1. Small file upload with description
# ------------------------------------------------------------------
echo ""
echo "[1/5] Small file upload with description..."
$CLI fs cp --description "quarterly financial report Q1 2026" /etc/hosts :/smoke-small.txt

DESC=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description stored" "$DESC" "quarterly financial report Q1 2026"

FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 30 || true

HAS_DESC_EMB=$(sql_scalar "SELECT description_embedding IS NOT NULL FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding generated" "$HAS_DESC_EMB" "1"

REV_MATCH=$(sql_scalar "SELECT description_embedding_revision = revision FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description_embedding_revision matches revision" "$REV_MATCH" "1"

# ------------------------------------------------------------------
# 2. Large file multipart upload with description
# ------------------------------------------------------------------
echo ""
echo "[2/5] Large file multipart upload with description..."
dd if=/dev/urandom of=/tmp/smoke-large.bin bs=1M count=5 2>/dev/null
$CLI fs cp --description "5MB random blob for backup" /tmp/smoke-large.bin :/smoke-large.bin

DESC2=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-large.bin';")
check_eq "large file description stored" "$DESC2" "5MB random blob for backup"

# ------------------------------------------------------------------
# 3. Overwrite without description preserves old value
# ------------------------------------------------------------------
echo ""
echo "[3/5] Overwrite without description preserves old value..."
# Use stdin upload which does not pass description
cat /etc/hosts | $CLI fs cp - :/smoke-small.txt

# Wait for the new embed task (if any)
FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 30 || true

DESC3=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description preserved after overwrite without desc" "$DESC3" "quarterly financial report Q1 2026"

# ------------------------------------------------------------------
# 4. Overwrite with new description replaces old value
# ------------------------------------------------------------------
echo ""
echo "[4/5] Overwrite with new description replaces old value..."
$CLI fs cp --description "updated description after review" /etc/hosts :/smoke-small.txt

FILE_ID=$(sql_scalar "SELECT f.file_id FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
wait_for_task "$FILE_ID" 30 || true

DESC4=$(sql_scalar "SELECT description FROM files f JOIN file_nodes fn ON f.file_id = fn.file_id WHERE fn.path = '/smoke-small.txt';")
check_eq "description updated after overwrite with new desc" "$DESC4" "updated description after review"

# ------------------------------------------------------------------
# 5. Grep API returns empty on local TiDB (known limitation)
# ------------------------------------------------------------------
echo ""
echo "[5/5] Grep API behavior on local TiDB (known limitation)..."
RESP=$(curl -s "${BASE}/v1/fs/?grep=financial+report" -H "X-Dat9-API-Key: ${API_KEY}")
if [ "$RESP" = "null" ] || [ "$RESP" = "[]" ]; then
    echo "  ⚠️  Grep returns empty — expected on local TiDB (vec_embed_cosine_distance not available)"
    echo "  ℹ️  This is a platform limitation, not a description bug."
else
    echo "  ✅ Grep returned results (unexpected but welcome): $RESP"
fi

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
echo ""
echo "========================================"
echo "Summary: ${PASS}/${TOTAL} passed, ${FAIL}/${TOTAL} failed"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
