#!/usr/bin/env bash
# dat9 API smoke test against a live dat9-server deployment.
#
# Coverage:
#  1) Provision tenant (expect 202, api_key + status only)
#  2) Poll tenant status via GET /v1/status until active
#  3) Root list
#  4) Nested mkdir (multi-level directories)
#  5) Multi-file write/read under nested directories
#  6) Batch small-file write/list/read validation
#  6) Copy, rename, delete
#  7) Final list verification
#  8) 100MB multipart upload with checksum-bound presigned parts

set -euo pipefail

BASE="${DAT9_BASE:-http://127.0.0.1:9009}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
RUN_LARGE_FILE="${RUN_LARGE_FILE:-1}"
LARGE_FILE_MB="${LARGE_FILE_MB:-100}"
BATCH_SMALL_FILE_COUNT="${BATCH_SMALL_FILE_COUNT:-10}"

PASS=0
FAIL=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }
ok() { echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { echo -e "${RED}  FAIL${RESET} $*"; }
info() { echo -e "${CYAN}  ->${RESET} $*"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
    PASS=$((PASS+1))
  else
    fail "$desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    ok "$desc"
    PASS=$((PASS+1))
  else
    fail "$desc"
    FAIL=$((FAIL+1))
  fi
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local data="${4:-}"

  local body_file
  body_file="$(mktemp)"

  if [ -n "$auth" ] && [ -n "$data" ]; then
    local code
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
    cat "$body_file"
    echo
    echo "__HTTP__${code}"
    rm -f "$body_file"
    return
  fi

  if [ -n "$auth" ]; then
    local code
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
    cat "$body_file"
    echo
    echo "__HTTP__${code}"
    rm -f "$body_file"
    return
  fi

  local code
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

echo "========================================================"
echo "  dat9 API smoke test"
echo "  Base URL : $BASE"
echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

TS="$(date +%s)"
ROOT_DIR="team-${TS}"
BACKEND_DIR="${ROOT_DIR}/backend/go"
FRONTEND_DIR="${ROOT_DIR}/frontend/web"
BATCH_DIR="${ROOT_DIR}/batch"
LARGE_FILE_BYTES=$((LARGE_FILE_MB * 1024 * 1024))
LARGE_FILE_LOCAL="/tmp/dat9-e2e-large-${TS}.bin"
LARGE_FILE_DOWNLOADED="/tmp/dat9-e2e-large-${TS}.download.bin"
LARGE_REMOTE_DIR="${ROOT_DIR}/large"
LARGE_REMOTE_FILE="${LARGE_REMOTE_DIR}/blob-${LARGE_FILE_MB}m.bin"

step "1" "Provision tenant"
resp=$(curl_body_code POST "$BASE/v1/provision")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "POST /v1/provision returns 202" "$code" "202"

API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
INIT_STATUS=$(printf '%s' "$body" | jq -r '.status // empty')
check_cmd "response contains api_key" test -n "$API_KEY"
check_eq "provision response status is provisioning" "$INIT_STATUS" "provisioning"
keys=$(printf '%s' "$body" | jq -r 'keys_unsorted | sort | join(",")')
check_eq "provision response only has api_key+status" "$keys" "api_key,status"

step "2" "Poll tenant status via /v1/status"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  LAST_STATUS=$(printf '%s' "$sbody" | jq -r '.status // empty')
  info "status=$LAST_STATUS"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant eventually becomes active" "$LAST_STATUS" "active"

step "3" "Root list"
resp=$(curl_body_code GET "$BASE/v1/fs/?list" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/?list returns 200" "$code" "200"
entries_type=$(printf '%s' "$body" | jq -r '.entries | type')
check_eq "list response contains entries array" "$entries_type" "array"

step "4" "Create nested directories"
for d in "$ROOT_DIR" "${ROOT_DIR}/backend" "$BACKEND_DIR" "${ROOT_DIR}/frontend" "$FRONTEND_DIR" "$BATCH_DIR"; do
  resp=$(curl_body_code POST "$BASE/v1/fs/$d?mkdir" "$API_KEY")
  code=$(http_code "$resp")
  check_eq "POST /v1/fs/$d?mkdir returns 200" "$code" "200"
done

step "5" "Write and read multiple files"
declare -a FILES
FILES=(
  "$ROOT_DIR/README.md|team-root-$TS"
  "$BACKEND_DIR/main.go|package main\n// smoke-$TS\nfunc main() {}\n"
  "$FRONTEND_DIR/index.html|<html><body>smoke-$TS</body></html>"
  "$BACKEND_DIR/config.yaml|env: smoke-$TS"
)

for item in "${FILES[@]}"; do
  path="${item%%|*}"
  payload="${item#*|}"
  resp=$(curl_body_code PUT "$BASE/v1/fs/$path" "$API_KEY" "$payload")
  code=$(http_code "$resp")
  check_eq "PUT /v1/fs/$path returns 200" "$code" "200"

  rresp=$(curl_body_code GET "$BASE/v1/fs/$path" "$API_KEY")
  rcode=$(http_code "$rresp")
  rbody=$(json_body "$rresp")
  check_eq "GET /v1/fs/$path returns 200" "$rcode" "200"
  check_eq "read back content matches for $path" "$rbody" "$payload"
done

step "6" "Batch small-file write/list/read validation"
for i in $(seq 1 "$BATCH_SMALL_FILE_COUNT"); do
  path="$BATCH_DIR/file-${i}.txt"
  payload="batch-$TS-$i"
  resp=$(curl_body_code PUT "$BASE/v1/fs/$path" "$API_KEY" "$payload")
  code=$(http_code "$resp")
  check_eq "PUT /v1/fs/$path returns 200" "$code" "200"
done

resp=$(curl_body_code GET "$BASE/v1/fs/$BATCH_DIR?list" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/$BATCH_DIR?list returns 200" "$code" "200"
batch_count=$(printf '%s' "$body" | jq -r '.entries | length')
check_eq "batch dir entry count matches" "$batch_count" "$BATCH_SMALL_FILE_COUNT"

for i in 1 "$BATCH_SMALL_FILE_COUNT"; do
  path="$BATCH_DIR/file-${i}.txt"
  expected="batch-$TS-$i"
  rresp=$(curl_body_code GET "$BASE/v1/fs/$path" "$API_KEY")
  rcode=$(http_code "$rresp")
  rbody=$(json_body "$rresp")
  check_eq "GET /v1/fs/$path returns 200" "$rcode" "200"
  check_eq "batch file content matches for $path" "$rbody" "$expected"
done

step "7" "Copy, rename, delete"
resp=$(curl -sS -o /tmp/dat9-copy.out -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Copy-Source: /$ROOT_DIR/README.md" "$BASE/v1/fs/$ROOT_DIR/README-copy.md?copy")
check_eq "POST ?copy returns 200" "$resp" "200"

resp=$(curl -sS -o /tmp/dat9-rename.out -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Rename-Source: /$BACKEND_DIR/config.yaml" "$BASE/v1/fs/$BACKEND_DIR/config-renamed.yaml?rename")
check_eq "POST ?rename returns 200" "$resp" "200"

resp=$(curl -sS -o /tmp/dat9-delete.out -w "%{http_code}" -X DELETE -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$ROOT_DIR/README-copy.md")
check_eq "DELETE copied file returns 200" "$resp" "200"

step "8" "Final list verification"
resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?list" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/$ROOT_DIR?list returns 200" "$code" "200"
backend_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="backend" and .isDir==true)')
frontend_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="frontend" and .isDir==true)')
copy_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="README-copy.md")')
check_eq "backend directory still exists" "$backend_exists" "true"
check_eq "frontend directory still exists" "$frontend_exists" "true"
check_eq "copied file removed" "$copy_exists" "false"

if [ "$RUN_LARGE_FILE" = "1" ]; then
  step "9" "Large file multipart upload (${LARGE_FILE_MB}MB)"
  check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'

  resp=$(curl_body_code POST "$BASE/v1/fs/$LARGE_REMOTE_DIR?mkdir" "$API_KEY")
  code=$(http_code "$resp")
  check_eq "POST /v1/fs/$LARGE_REMOTE_DIR?mkdir returns 200" "$code" "200"

  dd if=/dev/zero of="$LARGE_FILE_LOCAL" bs=1M count="$LARGE_FILE_MB" status=none
  check_cmd "local large file created" test -f "$LARGE_FILE_LOCAL"

  PART_CHECKSUMS=$(python3 - "$LARGE_FILE_LOCAL" <<'PY'
import base64
import hashlib
import sys

part_size = 8 * 1024 * 1024
out = []
with open(sys.argv[1], "rb") as f:
    while True:
        chunk = f.read(part_size)
        if not chunk:
            break
        out.append(base64.b64encode(hashlib.sha256(chunk).digest()).decode())
print(",".join(out))
PY
)

  plan_file="$(mktemp)"
  plan_code=$(curl -sS -o "$plan_file" -w "%{http_code}" -X PUT \
    -H "Authorization: Bearer $API_KEY" \
    -H "X-Dat9-Content-Length: $LARGE_FILE_BYTES" \
    -H "X-Dat9-Part-Checksums: $PART_CHECKSUMS" \
    --data-binary "" \
    "$BASE/v1/fs/$LARGE_REMOTE_FILE")
  check_eq "initiate multipart upload returns 202" "$plan_code" "202"

  upload_id=$(jq -r '.upload_id // empty' "$plan_file")
  part_count=$(jq -r '.parts | length' "$plan_file")
  check_cmd "multipart upload_id exists" test -n "$upload_id"
  check_cmd "multipart has presigned parts" test "$part_count" -gt 0

  python3 - "$plan_file" "$LARGE_FILE_LOCAL" <<'PY'
import json
import sys
import urllib.request

plan_path, file_path = sys.argv[1], sys.argv[2]
with open(plan_path, "r", encoding="utf-8") as f:
    plan = json.load(f)

parts = plan.get("parts", [])
with open(file_path, "rb") as data_file:
    for idx, p in enumerate(parts, 1):
        url = p["url"]
        size = int(p["size"])
        data = data_file.read(size)
        if len(data) != size:
            raise SystemExit(f"short read for part {idx}: got {len(data)} expected {size}")

        req = urllib.request.Request(url, data=data, method="PUT")
        req.add_header("Content-Length", str(size))
        for hk, hv in (p.get("headers") or {}).items():
            req.add_header(hk, hv)
        if p.get("checksum_sha256"):
            req.add_header("x-amz-checksum-sha256", p["checksum_sha256"])

        with urllib.request.urlopen(req, timeout=300) as resp:
            status = getattr(resp, "status", 200)
            if status >= 300:
                raise SystemExit(f"part {idx} failed: HTTP {status}")
PY
  check_eq "multipart part upload script exits successfully" "$?" "0"

  resp=$(curl_body_code POST "$BASE/v1/uploads/$upload_id/complete" "$API_KEY")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  complete_status=$(printf '%s' "$body" | jq -r '.status // empty')
  check_eq "POST /v1/uploads/$upload_id/complete returns 200" "$code" "200"
  check_eq "complete response status is ok" "$complete_status" "ok"

  resp=$(curl_body_code GET "$BASE/v1/fs/$LARGE_REMOTE_DIR?list" "$API_KEY")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "GET /v1/fs/$LARGE_REMOTE_DIR?list returns 200" "$code" "200"
  large_exists=$(printf '%s' "$body" | jq -r --arg name "blob-${LARGE_FILE_MB}m.bin" 'any(.entries[]; .name==$name and .isDir==false)')
  large_size=$(printf '%s' "$body" | jq -r --arg name "blob-${LARGE_FILE_MB}m.bin" '.entries[] | select(.name==$name) | .size')
  check_eq "large file appears in list" "$large_exists" "true"
  check_eq "large file size matches" "$large_size" "$LARGE_FILE_BYTES"

  download_code=$(curl -sS -L -o "$LARGE_FILE_DOWNLOADED" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$LARGE_REMOTE_FILE")
  check_eq "download large file returns 200" "$download_code" "200"
  check_cmd "downloaded large file exists" test -f "$LARGE_FILE_DOWNLOADED"
  src_sum=$(sha256sum "$LARGE_FILE_LOCAL" | cut -d' ' -f1)
  dst_sum=$(sha256sum "$LARGE_FILE_DOWNLOADED" | cut -d' ' -f1)
  check_eq "downloaded large file sha256 matches" "$dst_sum" "$src_sum"

  rm -f "$plan_file" "$LARGE_FILE_LOCAL" "$LARGE_FILE_DOWNLOADED"
fi

rm -f /tmp/dat9-copy.out /tmp/dat9-rename.out /tmp/dat9-delete.out

echo
echo "========================================================"
echo "  RESULTS: $PASS / $TOTAL passed, $FAIL failed"
echo "  Base URL : $BASE"
echo "  Finished : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$FAIL" -eq 0 ]; then
  echo -e "  ${GREEN}All tests passed.${RESET}"
else
  echo -e "  ${RED}$FAIL test(s) failed.${RESET}"
fi
echo "========================================================"

exit "$FAIL"
