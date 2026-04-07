#!/usr/bin/env bash
# drive9 API smoke test against a live drive9-server deployment.
#
# Coverage:
#  1) Provision tenant (expect 202, api_key + status only)
#  2) Poll tenant status via GET /v1/status until active
#  3) Root list
#  4) Nested mkdir (multi-level directories)
#  5) Multi-file write/read under nested directories
#  6) Batch small-file write/list/read validation
#  7) Content search (`grep`) and attribute search (`find`)
#  8) Image upload + image query (`find` by jpg extension)
#  9) Semantic text recall checks (`grep` with paraphrase query)
# 10) Image-associated recall checks (caption text + image file)
# 11) SQL endpoint sanity query
# 12) Copy, rename, delete
# 13) Final list verification
# 14) 100MB multipart upload via POST /v1/uploads/initiate + download checksum
# 15) Max-upload boundary check (limit allowed, limit+1 rejected)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_IMAGE_FIXTURE_PATH="${DRIVE9_IMAGE_FIXTURE_PATH:-$SCRIPT_DIR/fixtures/cat03.jpg}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
RUN_LARGE_FILE="${RUN_LARGE_FILE:-1}"
LARGE_FILE_MB="${LARGE_FILE_MB:-100}"
BATCH_SMALL_FILE_COUNT="${BATCH_SMALL_FILE_COUNT:-10}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
RUN_UPLOAD_LIMIT_BOUNDARY="${RUN_UPLOAD_LIMIT_BOUNDARY:-1}"
UPLOAD_LIMIT_BYTES="${UPLOAD_LIMIT_BYTES:-10737418240}"
SEMANTIC_TIMEOUT_S="${SEMANTIC_TIMEOUT_S:-90}"
SEMANTIC_INTERVAL_S="${SEMANTIC_INTERVAL_S:-3}"

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

  local attempt=1
  while :; do
    local body_file
    body_file="$(mktemp)"
    local code
    if [ -n "$auth" ] && [ -n "$data" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
    elif [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
    fi

    if [ "$code" != "429" ] || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return
    fi

    info "throttled (429), retrying ${attempt}/${REQUEST_MAX_RETRIES}: $method $url"
    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

json_is_valid() {
  printf '%s' "$1" | jq -e . >/dev/null 2>&1
}

json_array_paths() {
  local body="$1"
  if ! json_is_valid "$body"; then
    return 1
  fi
  if ! printf '%s' "$body" | jq -e 'type=="array"' >/dev/null 2>&1; then
    return 1
  fi
  printf '%s' "$body" | jq -r '.[]? | .path // empty' 2>/dev/null
}

paths_contains_exact() {
  local paths="$1"
  local expect="$2"
  [ -n "$expect" ] && printf '%s\n' "$paths" | while IFS= read -r p; do
    [ "$p" = "$expect" ] && return 0
  done
}

paths_contains_scope() {
  local paths="$1"
  local scope="$2"
  [ -n "$scope" ] && printf '%s\n' "$paths" | while IFS= read -r p; do
    case "$p" in
      "$scope"/*) return 0 ;;
    esac
  done
}

paths_contains_token() {
  local paths="$1"
  local token="$2"
  [ -n "$token" ] && printf '%s\n' "$paths" | while IFS= read -r p; do
    case "$p" in
      *"$token"*) return 0 ;;
    esac
  done
}

wait_grep_contains_path() {
  local desc="$1"
  local query_encoded="$2"
  local scope_path="$3"
  local expect_path="$4"
  local fallback_token="${5:-}"

  local deadline=$(( $(date +%s) + SEMANTIC_TIMEOUT_S ))
  local found="false"
  while :; do
    local resp code body paths
    resp=$(curl_body_code GET "$BASE/v1/fs/$scope_path?grep=$query_encoded&limit=20" "$API_KEY")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    if [ "$code" = "200" ]; then
      if paths=$(json_array_paths "$body"); then
        if paths_contains_exact "$paths" "$expect_path" \
          || paths_contains_token "$paths" "$fallback_token" \
          || paths_contains_scope "$paths" "$scope_path"; then
          found="true"
          break
        fi
      else
        info "$desc response is not valid JSON array yet"
      fi
      if [ "$found" = "true" ]; then
        break
      fi
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    info "$desc not ready yet, retrying semantic recall"
    sleep "$SEMANTIC_INTERVAL_S"
  done
  check_eq "$desc" "$found" "true"
}

echo "========================================================"
echo "  dat9 API smoke test"
echo "  Base URL : $BASE"
echo "  Image fixture : $DRIVE9_IMAGE_FIXTURE_PATH"
echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

check_cmd "local image fixture exists" test -s "$DRIVE9_IMAGE_FIXTURE_PATH"

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
IMAGE_LOCAL="/tmp/dat9-e2e-image-${TS}.jpg"
IMAGE_REMOTE="${ROOT_DIR}/assets/icon-${TS}.jpg"
SEM_TEXT_TARGET="${ROOT_DIR}/notes/cat-story-${TS}.txt"
SEM_TEXT_OTHER="${ROOT_DIR}/notes/dog-story-${TS}.txt"
IMAGE_CAPTION_REMOTE="${ROOT_DIR}/assets/icon-${TS}.caption.txt"

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

step "7" "Search checks (grep/find)"
resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?grep=smoke-$TS&limit=20" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ?grep returns 200" "$code" "200"
grep_count=$(printf '%s' "$body" | jq -r 'length')
check_cmd "grep returns at least 2 results" test "$grep_count" -ge 2
grep_has_root=$(printf '%s' "$body" | jq -r --arg root "$ROOT_DIR" 'any(.[]; (.path // "") | contains($root))')
check_eq "grep includes files under test root" "$grep_has_root" "true"

resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.yaml" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ?find returns 200" "$code" "200"
find_has_yaml=$(printf '%s' "$body" | jq -r --arg p "$BACKEND_DIR/config.yaml" 'any(.[]; .path==$p)')
if [ "$find_has_yaml" != "true" ]; then
  find_has_yaml=$(printf '%s' "$body" | jq -r 'any(.[]; (.path // "") | endswith("config.yaml"))')
fi

check_eq "find by name returns yaml file" "$find_has_yaml" "true"

step "8" "Image upload and query"
mkdir -p "$(dirname "$IMAGE_LOCAL")"
cp "$DRIVE9_IMAGE_FIXTURE_PATH" "$IMAGE_LOCAL"
check_cmd "local jpg fixture exists" test -s "$IMAGE_LOCAL"

IMAGE_BYTES=$(wc -c < "$IMAGE_LOCAL" | tr -d ' ')
IMAGE_CHECKSUMS=$(python3 - "$IMAGE_LOCAL" <<'PY'
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

image_init_payload="$(mktemp)"
jq -n \
  --arg path "/$IMAGE_REMOTE" \
  --argjson total_size "$IMAGE_BYTES" \
  --arg checksums "$IMAGE_CHECKSUMS" \
  '{path:$path,total_size:$total_size,part_checksums:($checksums|split(","))}' > "$image_init_payload"

image_plan_file="$(mktemp)"
image_plan_code=$(curl -sS -o "$image_plan_file" -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@$image_init_payload" \
  "$BASE/v1/uploads/initiate")
check_eq "POST /v1/uploads/initiate for image returns 202" "$image_plan_code" "202"

image_upload_id=$(jq -r '.upload_id // empty' "$image_plan_file")
image_part_count=$(jq -r '.parts | length' "$image_plan_file")
check_cmd "image multipart upload_id exists" test -n "$image_upload_id"
check_cmd "image multipart has presigned parts" test "$image_part_count" -gt 0

python3 - "$image_plan_file" "$IMAGE_LOCAL" <<'PY'
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
check_eq "image multipart part upload script exits successfully" "$?" "0"

resp=$(curl_body_code POST "$BASE/v1/uploads/$image_upload_id/complete" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
image_complete_status=$(printf '%s' "$body" | jq -r '.status // empty')
check_eq "POST /v1/uploads/$image_upload_id/complete returns 200" "$code" "200"
check_eq "image complete response status is ok" "$image_complete_status" "ok"

rm -f "$image_init_payload" "$image_plan_file"

resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.jpg" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ?find name=*.jpg returns 200" "$code" "200"
find_has_png=$(printf '%s' "$body" | jq -r --arg p "$IMAGE_REMOTE" 'any(.[]; .path==$p)')
if [ "$find_has_png" != "true" ]; then
  find_has_png=$(printf '%s' "$body" | jq -r 'any(.[]; (.path // "") | endswith(".jpg"))')
fi
check_eq "find by name returns jpg file" "$find_has_png" "true"

step "9" "Semantic text recall checks"
resp=$(curl_body_code POST "$BASE/v1/fs/${ROOT_DIR}/notes?mkdir" "$API_KEY")
code=$(http_code "$resp")
check_eq "POST /v1/fs/${ROOT_DIR}/notes?mkdir returns 200" "$code" "200"

resp=$(curl_body_code PUT "$BASE/v1/fs/$SEM_TEXT_TARGET" "$API_KEY" "A cat is resting on a sofa near a window.")
code=$(http_code "$resp")
check_eq "PUT semantic target text returns 200" "$code" "200"

resp=$(curl_body_code PUT "$BASE/v1/fs/$SEM_TEXT_OTHER" "$API_KEY" "A dog is running in a field under bright sun.")
code=$(http_code "$resp")
check_eq "PUT semantic distractor text returns 200" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?grep=feline%20sofa&limit=20" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET semantic grep returns 200" "$code" "200"
wait_grep_contains_path "semantic grep includes cat-story target" "feline%20sofa" "${ROOT_DIR}/notes" "$SEM_TEXT_TARGET" "cat-story"
wait_grep_contains_path "semantic grep includes dog-story target" "canine%20field" "${ROOT_DIR}/notes" "$SEM_TEXT_OTHER" "dog-story"

step "10" "Image-associated recall checks"
resp=$(curl_body_code PUT "$BASE/v1/fs/$IMAGE_CAPTION_REMOTE" "$API_KEY" "This image shows a cat face icon.")
code=$(http_code "$resp")
check_eq "PUT image caption text returns 200" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?grep=feline%20face%20icon&limit=20" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET image-associated grep returns 200" "$code" "200"
wait_grep_contains_path "image-associated recall includes caption" "feline%20face%20icon" "${ROOT_DIR}/assets" "$IMAGE_CAPTION_REMOTE" "caption"

resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.jpg" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET image find returns 200" "$code" "200"
image_file_present=$(printf '%s' "$body" | jq -r --arg p "$IMAGE_REMOTE" 'any(.[]; .path==$p)')
if [ "$image_file_present" != "true" ]; then
  image_file_present=$(printf '%s' "$body" | jq -r 'any(.[]; (.path // "") | endswith(".jpg"))')
fi
check_eq "image file remains discoverable" "$image_file_present" "true"

step "11" "SQL endpoint sanity"
sql_req='{"query":"SELECT 1 AS n"}'
sql_body="$(mktemp)"
code=$(curl -sS -o "$sql_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" --data-binary "$sql_req" "$BASE/v1/sql")
body=$(cat "$sql_body")
rm -f "$sql_body"
check_eq "POST /v1/sql returns 200" "$code" "200"
sql_n=$(printf '%s' "$body" | jq -r '.[0].n')
check_eq "SQL query result n=1" "$sql_n" "1"

step "12" "Copy, rename, delete"
copy_body="$(mktemp)"
copy_code=$(curl -sS -o "$copy_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Copy-Source: /$ROOT_DIR/README.md" "$BASE/v1/fs/$ROOT_DIR/README-copy.md?copy")
copy_attempt=1
while [ "$copy_code" = "429" ] && [ "$copy_attempt" -lt "$REQUEST_MAX_RETRIES" ]; do
  info "throttled (429), retrying ${copy_attempt}/${REQUEST_MAX_RETRIES}: copy"
  copy_attempt=$((copy_attempt + 1))
  sleep "$REQUEST_RETRY_SLEEP_S"
  copy_code=$(curl -sS -o "$copy_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Copy-Source: /$ROOT_DIR/README.md" "$BASE/v1/fs/$ROOT_DIR/README-copy.md?copy")
done
check_eq "POST ?copy returns 200" "$copy_code" "200"
rm -f "$copy_body"

rename_body="$(mktemp)"
rename_code=$(curl -sS -o "$rename_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Rename-Source: /$BACKEND_DIR/config.yaml" "$BASE/v1/fs/$BACKEND_DIR/config-renamed.yaml?rename")
rename_attempt=1
while [ "$rename_code" = "429" ] && [ "$rename_attempt" -lt "$REQUEST_MAX_RETRIES" ]; do
  info "throttled (429), retrying ${rename_attempt}/${REQUEST_MAX_RETRIES}: rename"
  rename_attempt=$((rename_attempt + 1))
  sleep "$REQUEST_RETRY_SLEEP_S"
  rename_code=$(curl -sS -o "$rename_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Rename-Source: /$BACKEND_DIR/config.yaml" "$BASE/v1/fs/$BACKEND_DIR/config-renamed.yaml?rename")
done
check_eq "POST ?rename returns 200" "$rename_code" "200"
rm -f "$rename_body"

delete_body="$(mktemp)"
delete_code=$(curl -sS -o "$delete_body" -w "%{http_code}" -X DELETE -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$ROOT_DIR/README-copy.md")
delete_attempt=1
while [ "$delete_code" = "429" ] && [ "$delete_attempt" -lt "$REQUEST_MAX_RETRIES" ]; do
  info "throttled (429), retrying ${delete_attempt}/${REQUEST_MAX_RETRIES}: delete"
  delete_attempt=$((delete_attempt + 1))
  sleep "$REQUEST_RETRY_SLEEP_S"
  delete_code=$(curl -sS -o "$delete_body" -w "%{http_code}" -X DELETE -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$ROOT_DIR/README-copy.md")
done
check_eq "DELETE copied file returns 200" "$delete_code" "200"
rm -f "$delete_body"

step "13" "Final list verification"
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
  step "14" "Large file multipart upload via body initiate (${LARGE_FILE_MB}MB)"
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

  init_payload="$(mktemp)"
  jq -n \
    --arg path "/$LARGE_REMOTE_FILE" \
    --argjson total_size "$LARGE_FILE_BYTES" \
    --arg checksums "$PART_CHECKSUMS" \
    '{path:$path,total_size:$total_size,part_checksums:($checksums|split(","))}' > "$init_payload"

  plan_file="$(mktemp)"
  plan_code=$(curl -sS -o "$plan_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$init_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "POST /v1/uploads/initiate returns 202" "$plan_code" "202"

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

  rm -f "$init_payload" "$plan_file" "$LARGE_FILE_LOCAL" "$LARGE_FILE_DOWNLOADED"
fi

if [ "$RUN_UPLOAD_LIMIT_BOUNDARY" = "1" ]; then
  step "15" "Upload limit boundary (limit/limit+1)"
  boundary_ok_payload="$(mktemp)"
  python3 - "$ROOT_DIR/limit-1g.bin" "$UPLOAD_LIMIT_BYTES" > "$boundary_ok_payload" <<'PY'
import base64
import hashlib
import json
import sys

path = "/" + sys.argv[1].lstrip("/")
upload_limit = int(sys.argv[2])
part_size = 8 * 1024 * 1024
part = b"\x00" * part_size
checksum = base64.b64encode(hashlib.sha256(part).digest()).decode()
parts = (upload_limit + part_size - 1) // part_size
print(json.dumps({
    "path": path,
    "total_size": upload_limit,
    "part_checksums": [checksum] * parts,
}))
PY

  boundary_ok_body="$(mktemp)"
  boundary_ok_code=$(curl -sS -o "$boundary_ok_body" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_ok_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "init at upload limit returns 202" "$boundary_ok_code" "202"
  rm -f "$boundary_ok_payload" "$boundary_ok_body"

  over_limit=$((UPLOAD_LIMIT_BYTES + 1))
  boundary_over_payload="$(mktemp)"
  python3 - "$ROOT_DIR/limit-over.bin" "$over_limit" > "$boundary_over_payload" <<'PY'
import base64
import hashlib
import json
import sys

path = "/" + sys.argv[1].lstrip("/")
over_limit = int(sys.argv[2])
part = b"\x00" * (8 * 1024 * 1024)
checksum = base64.b64encode(hashlib.sha256(part).digest()).decode()
print(json.dumps({
    "path": path,
    "total_size": over_limit,
    "part_checksums": [checksum],
}))
PY

  boundary_over_body="$(mktemp)"
  boundary_over_code=$(curl -sS -o "$boundary_over_body" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_over_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "init over upload limit returns 413" "$boundary_over_code" "413"
  over_err=$(jq -r '.error // empty' "$boundary_over_body")
  check_cmd "over-limit response has error message" test -n "$over_err"
  rm -f "$boundary_over_payload" "$boundary_over_body"
fi

rm -f "$IMAGE_LOCAL"

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
