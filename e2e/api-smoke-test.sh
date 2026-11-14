#!/usr/bin/env bash
# drive9 API smoke test against a live drive9-server deployment.
#
# Tenant mode:
#  - Fresh (default): POST /v1/provision a new tenant, then run the suite.
#  - Existing (DRIVE9_API_KEY set): skip provision and reuse the tenant the
#    key belongs to. Step 1 skips provision and emits SKIP checks so the
#    PASS/FAIL/SKIP/TOTAL accounting stays consistent. The upload-limit
#    boundary check (step 15) defaults to OFF in this mode because reserving
#    `total_size` against an existing tenant's quota can spuriously fail with
#    507; an explicit RUN_UPLOAD_LIMIT_BOUNDARY=1 still wins. A final
#    cleanup step removes the timestamped test tree (`team-${TS}`) so the
#    existing tenant is not polluted.
#
# Coverage:
#  1) Provision tenant (expect 202, tenant_id + api_key + status)
#     - honors DRIVE9_API_KEY: skips provision when set
#  2) Poll tenant status via GET /v1/status until active, then assert the
#     capability negotiation surface: tenant_id, max_upload_bytes,
#     inline_threshold, migration_capabilities, storage_capabilities. The
#     upload-limit boundary (step 15) is derived from max_upload_bytes
#     instead of a hardcoded constant, so a deployment that changes
#     DRIVE9_MAX_UPLOAD_BYTES cannot turn this suite into a false pass.
#  3) Root list
#  4) Nested mkdir (multi-level directories)
#  5) Multi-file write/read under nested directories
#  6) Batch small-file write/list/read validation
#  7) Content search (`grep`) and attribute search (`find`)
#  8) Image upload + image query (`find` by jpg extension)
#  9) Semantic text recall checks (`grep` with paraphrase query; off unless RUN_SEMANTIC_CHECKS=1)
# 10) Image-associated recall checks (caption text + image file; off unless RUN_SEMANTIC_CHECKS=1)
# 11) SQL endpoint sanity query (off unless RUN_SQL_CHECKS=1)
# 12) Copy, hardlink, rename, delete
# 13) Final list verification
# 14) 100MB multipart upload via POST /v1/uploads/initiate + download checksum
# 15) Max-upload boundary check (limit allowed, limit+1 rejected)
# 16) Wire contract: `?stat` JSON projection and HEAD headers
#     (X-Dat9-Revision/Resource-ID/Mtime/Btime/Storage-Type, strong ETag,
#     Accept-Ranges, root synthetic stat, `?list` entry projection)
# 17) Wire contract: conditional write (X-Dat9-Expected-Revision create-only /
#     match / stale-409, malformed header, X-Dat9-Content-Length mismatch) and
#     Range / If-Range / If-Match read semantics
# 18) Metadata: `?create` (revision 1, conflict, completion identity),
#     PUT X-Dat9-Tag/X-Dat9-Description, `?setmeta`, find-by-tag
# 19) Path handling: unicode, spaces, `#`/`?`, and X-Dat9-Path-Encoding
#     (base64url) source headers for copy/rename
# 20) Delete and link contract (`?kind`, `?recursive`, non-empty 409, symlink)
# 21) Error and routing contract (401/400/404/405/409, /healthz)
# 22) Batch endpoints: `/v1/fs:batch-stat`, `:batch-read-small`, `:batch-write`
#     (per-item status, include_checksum, CAS, per-item 409/413, limits)
# 23) Upload lifecycle: V1 list/resume/abort + V2 presigned round-trip
#     (initiate/presign/presign-batch/part PUT/complete/abort)
# 24) Mount lease lifecycle (register/heartbeat/get/list/release); a server
#     that does not serve /v1/mounts yet SKIPs instead of failing
# 25) Cleanup test tree (existing tenant mode only)
#
# Steps 16-24 are extension groups, each behind its own knob (all default 1):
# RUN_CONTRACT_CHECKS, RUN_BATCH_CHECKS, RUN_UPLOAD_LIFECYCLE_CHECKS,
# RUN_MOUNT_LEASE_CHECKS. They assert the current server contract and never
# weaken an assertion to accommodate an older server: probe_features() checks
# what this deployment serves (btime, HEAD read validators, Range, If-Match,
# batch-stat checksums) and only the dependent checks SKIP when it does not;
# a missing route SKIPs and a present-but-wrong answer FAILs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
DRIVE9_IMAGE_FIXTURE_PATH="${DRIVE9_IMAGE_FIXTURE_PATH:-$SCRIPT_DIR/fixtures/cat03.jpg}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-300}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
RUN_LARGE_FILE="${RUN_LARGE_FILE:-1}"
LARGE_FILE_MB="${LARGE_FILE_MB:-100}"
BATCH_SMALL_FILE_COUNT="${BATCH_SMALL_FILE_COUNT:-10}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
# Upload-limit boundary check defaults to OFF in existing-tenant mode: reserving
# `total_size` against a tenant that already consumed quota can spuriously fail
# with 507. An explicit RUN_UPLOAD_LIMIT_BOUNDARY still wins.
if [ -z "${RUN_UPLOAD_LIMIT_BOUNDARY:-}" ]; then
  if [ -n "$API_KEY" ]; then
    RUN_UPLOAD_LIMIT_BOUNDARY=0
  else
    RUN_UPLOAD_LIMIT_BOUNDARY=1
  fi
fi
# Empty means "derive from GET /v1/status max_upload_bytes" once the tenant is
# active; an explicit value wins so a caller can probe a known limit.
UPLOAD_LIMIT_BYTES="${UPLOAD_LIMIT_BYTES:-}"
SEMANTIC_TIMEOUT_S="${SEMANTIC_TIMEOUT_S:-90}"
SEMANTIC_INTERVAL_S="${SEMANTIC_INTERVAL_S:-3}"
RUN_SEMANTIC_CHECKS="${RUN_SEMANTIC_CHECKS:-0}"
RUN_SQL_CHECKS="${RUN_SQL_CHECKS:-0}"
RUN_CONTRACT_CHECKS="${RUN_CONTRACT_CHECKS:-1}"
RUN_BATCH_CHECKS="${RUN_BATCH_CHECKS:-1}"
RUN_UPLOAD_LIFECYCLE_CHECKS="${RUN_UPLOAD_LIFECYCLE_CHECKS:-1}"
RUN_MOUNT_LEASE_CHECKS="${RUN_MOUNT_LEASE_CHECKS:-1}"

# Server-negotiated facts, filled in by step 2. Empty/"true"/"false" strings
# so an older server that omits a field makes the dependent check SKIP.
SERVER_TENANT_ID=""
SERVER_MAX_UPLOAD_BYTES=""
SERVER_INLINE_THRESHOLD=""
CAP_BATCH_WRITE_MODE="unknown"
CAP_COMPLETION_IDENTITY="unknown"
CAP_APPEND_LOG="unknown"
CAP_BATCH_UPLOAD_PRESIGNED="unknown"

# Feature probes. The drive9 repository also embeds a server snapshot that the
# Local E2E job runs, and some data-plane features exist only in the server
# repository. probe_features() asks the deployment what it actually does, once,
# and every dependent check SKIPs when the answer is "not here" -- a deployment
# that does serve the feature is still hard-asserted. A missing capability is
# never mistaken for a passing one.
PROBES_DONE=0
PROBE_STATUS_TENANT_ID="unknown"
PROBE_STATUS_MIGRATION_CAPS="unknown"
PROBE_STAT_BTIME="unknown"
PROBE_HEAD_VALIDATORS="unknown"
PROBE_RANGE="unknown"
PROBE_IF_MATCH="unknown"
PROBE_BATCH_CHECKSUM="unknown"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }
ok() { echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { echo -e "${RED}  FAIL${RESET} $*"; }
skip_check() {
  local desc="$1"
  TOTAL=$((TOTAL+1))
  SKIP=$((SKIP+1))
  echo -e "${YELLOW}  SKIP${RESET} $desc"
}
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

header_value() {
  local header_file="$1"
  local header_name="$2"
  awk -F': *' -v name="$header_name" 'tolower($1)==tolower(name){v=$2} END{gsub(/\r/,"",v); print v}' "$header_file"
}

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

# ---------------------------------------------------------------------------
# Generic request helper for the contract checks.
#
#   request <METHOD> <URL> [--auth KEY] [--hdr "Name: value"]... \
#           [--data BODY] [--data-file PATH] [--max-time SECONDS]
#
# Prints the response body followed by a "__HTTP__<code>" trailer (same shape
# as curl_body_code so http_code/json_body work unchanged) and leaves the
# response headers in $REQ_HDR_FILE for resp_hdr(). 429 responses are retried
# REQUEST_MAX_RETRIES times, like the rest of the suite.
#
# HEAD is sent with -I, not -X HEAD: the latter makes curl wait for a body the
# server never sends.
# ---------------------------------------------------------------------------
# The response-header file is created once, at this level: `resp=$(request ...)`
# runs request() in a subshell, so a variable assigned inside it could never
# reach the caller that reads the headers. request() truncates this file
# instead, which keeps resp_hdr() working across the substitution.
REQ_HDR_FILE="$(mktemp)"
request() {
  local method="$1" url="$2"
  shift 2
  local auth="" data="" data_file="" max_time="300"
  local -a hdrs=()
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --auth) auth="${2:-}"; shift 2 ;;
      --hdr) hdrs+=("${2:-}"); shift 2 ;;
      --data) data="${2:-}"; shift 2 ;;
      --data-file) data_file="${2:-}"; shift 2 ;;
      --max-time) max_time="${2:-300}"; shift 2 ;;
      *)
        echo "request helper: unknown option '$1'" >&2
        return 2
        ;;
    esac
  done

  local attempt=1
  while :; do
    local body_file code
    body_file="$(mktemp)"
    : >"$REQ_HDR_FILE"
    local -a args=(-sS -o "$body_file" -D "$REQ_HDR_FILE" -w "%{http_code}" --max-time "$max_time")
    if [ "$method" = "HEAD" ]; then
      args+=(-I)
    else
      args+=(-X "$method")
    fi
    if [ -n "$auth" ]; then
      args+=(-H "Authorization: Bearer $auth")
    fi
    local h
    for h in ${hdrs[@]+"${hdrs[@]}"}; do
      args+=(-H "$h")
    done
    if [ -n "$data_file" ]; then
      args+=(--data-binary "@$data_file")
    elif [ -n "$data" ]; then
      args+=(--data-binary "$data")
    fi

    code=$(curl "${args[@]}" "$url")

    if [ "$code" = "429" ] && [ "$attempt" -lt "$REQUEST_MAX_RETRIES" ]; then
      info "throttled (429), retrying ${attempt}/${REQUEST_MAX_RETRIES}: $method $url"
      rm -f "$body_file"
      attempt=$((attempt + 1))
      sleep "$REQUEST_RETRY_SLEEP_S"
      continue
    fi

    cat "$body_file"
    echo
    echo "__HTTP__${code}"
    rm -f "$body_file"
    return 0
  done
}

# resp_hdr <Name> reads a response header from the last request() call.
resp_hdr() {
  if [ -z "${REQ_HDR_FILE:-}" ] || [ ! -f "${REQ_HDR_FILE:-}" ]; then
    return 0
  fi
  header_value "$REQ_HDR_FILE" "$1"
}

check_status() {
  local desc="$1" resp="$2" want="$3"
  check_eq "$desc" "$(http_code "$resp")" "$want"
}

probe_features() {
  if [ "$PROBES_DONE" = "1" ]; then
    return 0
  fi
  PROBES_DONE=1
  local dir="${ROOT_DIR}/probe"
  local path="${dir}/f.txt"
  request POST "$BASE/v1/fs/$dir?mkdir" --auth "$API_KEY" >/dev/null
  request PUT "$BASE/v1/fs/$path" --auth "$API_KEY" --data "probe-0123456789" >/dev/null

  local resp body
  resp=$(request GET "$BASE/v1/fs/$path?stat" --auth "$API_KEY")
  body=$(json_body "$resp")
  if [ "$(http_code "$resp")" = "200" ]; then
    PROBE_STAT_BTIME=$(printf '%s' "$body" | jq -r 'if has("btime") and .btime != null then "true" else "false" end')
  fi

  resp=$(request HEAD "$BASE/v1/fs/$path" --auth "$API_KEY")
  if [ "$(resp_hdr 'ETag')" != "" ]; then
    PROBE_HEAD_VALIDATORS="true"
  else
    PROBE_HEAD_VALIDATORS="false"
  fi

  resp=$(request GET "$BASE/v1/fs/$path" --auth "$API_KEY" --hdr "Range: bytes=0-3")
  if [ "$(http_code "$resp")" = "206" ]; then
    PROBE_RANGE="true"
  else
    PROBE_RANGE="false"
  fi

  resp=$(request GET "$BASE/v1/fs/$path" --auth "$API_KEY" --hdr 'If-Match: "probe-stale"')
  if [ "$(http_code "$resp")" = "412" ]; then
    PROBE_IF_MATCH="true"
  else
    PROBE_IF_MATCH="false"
  fi

  resp=$(request POST "$BASE/v1/fs:batch-stat" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg p "/$path" '{paths:[$p],include_checksum:true}')")
  if [ "$(json_body "$resp" | jq -r '(.results[0].checksum_sha256 // empty)')" != "" ]; then
    PROBE_BATCH_CHECKSUM="true"
  else
    PROBE_BATCH_CHECKSUM="false"
  fi

  info "features: stat_btime=$PROBE_STAT_BTIME head_validators=$PROBE_HEAD_VALIDATORS range=$PROBE_RANGE if_match=$PROBE_IF_MATCH batch_checksum=$PROBE_BATCH_CHECKSUM"
}

# check_json <desc> <json body> <jq filter> <expected string form>
check_json() {
  local desc="$1" body="$2" filter="$3" want="$4"
  local got
  if ! got=$(printf '%s' "$body" | jq -r "$filter" 2>/dev/null); then
    got="<jq-error>"
  fi
  check_eq "$desc" "$got" "$want"
}

# body_has_path <json array body> <path>: the array holds an element with .path.
body_has_path() {
  printf '%s' "$1" | jq -e --arg p "$2" 'any(.[]; .path==$p)' >/dev/null 2>&1
}

# body_lacks_path <json array body> <path>: the inverse, used for negative
# search assertions (a wrong tag value must NOT return the tagged file).
body_lacks_path() {
  if printf '%s' "$1" | jq -e --arg p "$2" 'any(.[]; .path==$p)' >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

url_encode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

b64url() {
  python3 -c 'import base64, sys; print(base64.urlsafe_b64encode(sys.argv[1].encode()).decode().rstrip("="))' "$1"
}

# put_presigned_part <presign-json-file> <data-file>
# PUTs the bytes to the presigned URL (verbatim signed headers plus the
# checksum header only when the server did not presign one) and echoes the
# object store's ETag, which the multipart complete step must reference.
put_presigned_part() {
  python3 - "$1" "$2" <<'PY'
import json
import sys
import urllib.request

with open(sys.argv[1], "r", encoding="utf-8") as f:
    part = json.load(f)
data = open(sys.argv[2], "rb").read()

req = urllib.request.Request(part["url"], data=data, method="PUT")
req.add_header("Content-Length", str(len(data)))
headers = part.get("headers") or {}
for hk, hv in headers.items():
    req.add_header(hk, hv)
lower_headers = {k.lower() for k in headers}
if part.get("checksum_crc32c") and "x-amz-checksum-crc32c" not in lower_headers and "x-goog-hash" not in lower_headers:
    req.add_header("x-amz-checksum-crc32c", part["checksum_crc32c"])
elif part.get("checksum_sha256") and "x-amz-checksum-sha256" not in lower_headers and "x-goog-meta-drive9-sha256" not in lower_headers:
    req.add_header("x-amz-checksum-sha256", part["checksum_sha256"])

with urllib.request.urlopen(req, timeout=300) as resp:
    status = getattr(resp, "status", 200)
    if status >= 300:
        raise SystemExit(f"part PUT failed: HTTP {status}")
    print(resp.headers.get("ETag") or "")
PY
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

if [ -n "$API_KEY" ]; then
  TENANT_MODE="existing (DRIVE9_API_KEY)"
else
  TENANT_MODE="fresh provision"
fi

echo "========================================================"
echo "  drive9 API smoke test"
echo "  Base URL : $BASE"
echo "  Tenant   : $TENANT_MODE"
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
LARGE_FILE_LOCAL="/tmp/drive9-e2e-large-${TS}.bin"
LARGE_FILE_DOWNLOADED="/tmp/drive9-e2e-large-${TS}.download.bin"
LARGE_REMOTE_DIR="${ROOT_DIR}/large"
LARGE_REMOTE_FILE="${LARGE_REMOTE_DIR}/blob-${LARGE_FILE_MB}m.bin"
IMAGE_LOCAL="/tmp/drive9-e2e-image-${TS}.jpg"
IMAGE_REMOTE="${ROOT_DIR}/assets/icon-${TS}.jpg"
SEM_TEXT_TARGET="${ROOT_DIR}/notes/cat-story-${TS}.txt"
SEM_TEXT_OTHER="${ROOT_DIR}/notes/dog-story-${TS}.txt"
IMAGE_CAPTION_REMOTE="${ROOT_DIR}/assets/icon-${TS}.caption.txt"

step "1" "Provision tenant"
if [ -z "$API_KEY" ]; then
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"

  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  TENANT_ID=$(printf '%s' "$body" | jq -r '.tenant_id // empty')
  INIT_STATUS=$(printf '%s' "$body" | jq -r '.status // empty')
  check_cmd "response contains tenant_id" test -n "$TENANT_ID"
  check_cmd "response contains api_key" test -n "$API_KEY"
  check_cmd "provision response status is provisioning or active (got=$INIT_STATUS)" bash -c 'case "$1" in provisioning|active) exit 0;; *) exit 1;; esac' _ "$INIT_STATUS"
  check_cmd "provision response contains api_key, status, tenant_id" bash -c "echo \"\$1\" | jq -e '.api_key and .status and .tenant_id' >/dev/null" _ "$body"
else
  info "using existing DRIVE9_API_KEY (skip provision)"
  skip_check "POST /v1/provision returns 202"
  skip_check "response contains tenant_id"
  skip_check "response contains api_key"
  skip_check "provision response status is provisioning or active"
  skip_check "provision response contains api_key, status, tenant_id"
fi

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

# Capability negotiation. /v1/status is the owner-readable document that tells a
# client which write protocols and limits this deployment serves; without these
# assertions a deployment could silently stop advertising a capability (or
# change its limits) and every client-side protocol choice would drift with it.
sresp=$(request GET "$BASE/v1/status" --auth "$API_KEY")
sbody=$(json_body "$sresp")
check_status "GET /v1/status returns 200 for the owner key" "$sresp" "200"
SERVER_TENANT_ID=$(printf '%s' "$sbody" | jq -r '.tenant_id // empty')
SERVER_MAX_UPLOAD_BYTES=$(printf '%s' "$sbody" | jq -r '.max_upload_bytes // empty')
SERVER_INLINE_THRESHOLD=$(printf '%s' "$sbody" | jq -r '.inline_threshold // empty')
PROBE_STATUS_TENANT_ID=$(printf '%s' "$sbody" | jq -r 'if has("tenant_id") then "true" else "false" end')
if [ "$PROBE_STATUS_TENANT_ID" = "true" ]; then
  check_cmd "status exposes tenant_id" test -n "$SERVER_TENANT_ID"
  if [ -n "${TENANT_ID:-}" ]; then
    check_eq "status tenant_id matches the provisioned tenant" "$SERVER_TENANT_ID" "$TENANT_ID"
  fi
else
  skip_check "status exposes tenant_id (server does not project tenant_id)"
  skip_check "status tenant_id matches the provisioned tenant (server does not project tenant_id)"
fi
check_cmd "status advertises a positive max_upload_bytes" bash -c 'test "${1:-0}" -gt 0' _ "$SERVER_MAX_UPLOAD_BYTES"
if [ -n "$SERVER_INLINE_THRESHOLD" ]; then
  check_cmd "status advertises a positive inline_threshold" bash -c 'test "${1:-0}" -gt 0' _ "$SERVER_INLINE_THRESHOLD"
else
  skip_check "status advertises inline_threshold (older server omits it)"
fi
PROBE_STATUS_MIGRATION_CAPS=$(printf '%s' "$sbody" | jq -r 'if has("migration_capabilities") and (.migration_capabilities|type) == "object" then "true" else "false" end')
if [ "$PROBE_STATUS_MIGRATION_CAPS" = "true" ]; then
  check_json "status migration_capabilities is an object" "$sbody" '(.migration_capabilities|type)' "object"
else
  skip_check "status migration_capabilities is an object (server does not project it)"
fi
if printf '%s' "$sbody" | jq -e 'has("storage_capabilities")' >/dev/null 2>&1; then
  check_json "status storage_capabilities is an object" "$sbody" '(.storage_capabilities|type)' "object"
  CAP_BATCH_WRITE_MODE=$(printf '%s' "$sbody" | jq -r '.storage_capabilities.batch_write_mode_v1 // false')
  CAP_COMPLETION_IDENTITY=$(printf '%s' "$sbody" | jq -r '.storage_capabilities.completion_identity_v1 // false')
  CAP_APPEND_LOG=$(printf '%s' "$sbody" | jq -r '.storage_capabilities.append_log_v1 // false')
  CAP_BATCH_UPLOAD_PRESIGNED=$(printf '%s' "$sbody" | jq -r '.storage_capabilities.batch_upload_presigned_v1 // false')
  info "capabilities: batch_write_mode=$CAP_BATCH_WRITE_MODE completion_identity=$CAP_COMPLETION_IDENTITY append_log=$CAP_APPEND_LOG batch_upload_presigned=$CAP_BATCH_UPLOAD_PRESIGNED"
else
  skip_check "status advertises storage_capabilities (older server omits it)"
fi
if [ -z "$UPLOAD_LIMIT_BYTES" ]; then
  UPLOAD_LIMIT_BYTES="${SERVER_MAX_UPLOAD_BYTES:-10737418240}"
  info "upload-limit boundary derived from status max_upload_bytes=$UPLOAD_LIMIT_BYTES"
fi

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
batch_count=$(printf '%s' "$body" | jq -r '(.entries // []) | length')
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
grep_count=$(printf '%s' "$body" | jq -r '(. // []) | length')
check_cmd "grep returns at least 2 results" test "$grep_count" -ge 2
grep_has_root=$(printf '%s' "$body" | jq -r --arg root "$ROOT_DIR" '(. // []) | any(.[]; (.path // "") | contains($root))')
check_eq "grep includes files under test root" "$grep_has_root" "true"

resp=$(request GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.yaml" --auth "$API_KEY")
check_status "GET ?find returns 200" "$resp" "200"
body=$(json_body "$resp")
# Search results carry absolute paths, so this must match the exact written
# path. An `endswith` fallback would also pass when any *.yaml exists anywhere
# under the root, which is exactly the drift this check is meant to catch.
check_cmd "find by name returns the exact yaml path" body_has_path "$body" "/$BACKEND_DIR/config.yaml"

step "8" "Image upload and query"
mkdir -p "$(dirname "$IMAGE_LOCAL")"
cp "$DRIVE9_IMAGE_FIXTURE_PATH" "$IMAGE_LOCAL"
check_cmd "local jpg fixture exists" test -s "$IMAGE_LOCAL"

IMAGE_BYTES=$(wc -c < "$IMAGE_LOCAL" | tr -d ' ')
IMAGE_CHECKSUMS=$(python3 - "$IMAGE_LOCAL" <<'PY'
import base64
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

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

# The `|| rc=$?` keeps a failing part upload inside the PASS/FAIL accounting:
# with `set -e` a bare python3 call would abort the whole suite before the
# summary and before the existing-tenant cleanup ran.
image_upload_rc=0
python3 - "$image_plan_file" "$IMAGE_LOCAL" <<'PY' || image_upload_rc=$?
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
        headers = p.get("headers") or {}
        for hk, hv in headers.items():
            req.add_header(hk, hv)
        # Only inject the S3-style checksum header when the server did not
        # already presign a checksum header. Non-S3 backends (e.g. GCS) sign
        # CRC32C into x-goog-hash; adding x-amz-checksum-crc32c on top of that
        # fails with 400 "Header must be signed".
        lower_headers = {k.lower() for k in headers}
        if p.get("checksum_crc32c") and "x-amz-checksum-crc32c" not in lower_headers and "x-goog-hash" not in lower_headers:
            req.add_header("x-amz-checksum-crc32c", p["checksum_crc32c"])
        elif p.get("checksum_sha256") and "x-amz-checksum-sha256" not in lower_headers and "x-goog-meta-drive9-sha256" not in lower_headers:
            req.add_header("x-amz-checksum-sha256", p["checksum_sha256"])

        with urllib.request.urlopen(req, timeout=300) as resp:
            status = getattr(resp, "status", 200)
            if status >= 300:
                raise SystemExit(f"part {idx} failed: HTTP {status}")
PY
check_eq "image multipart part upload script exits successfully" "$image_upload_rc" "0"

resp=$(curl_body_code POST "$BASE/v1/uploads/$image_upload_id/complete" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
image_complete_status=$(printf '%s' "$body" | jq -r '.status // empty')
check_eq "POST /v1/uploads/$image_upload_id/complete returns 200" "$code" "200"
check_eq "image complete response status is ok" "$image_complete_status" "ok"

rm -f "$image_init_payload" "$image_plan_file"

resp=$(request GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.jpg" --auth "$API_KEY")
check_status "GET ?find name=*.jpg returns 200" "$resp" "200"
check_cmd "find by name returns the exact uploaded jpg path" body_has_path "$(json_body "$resp")" "/$IMAGE_REMOTE"

step "9" "Semantic text recall checks"
if [ "$RUN_SEMANTIC_CHECKS" = "1" ]; then
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
else
  info "semantic text recall checks skipped (RUN_SEMANTIC_CHECKS=$RUN_SEMANTIC_CHECKS)"
  skip_check "POST /v1/fs/${ROOT_DIR}/notes?mkdir returns 200"
  skip_check "PUT semantic target text returns 200"
  skip_check "PUT semantic distractor text returns 200"
  skip_check "GET semantic grep returns 200"
  skip_check "semantic grep includes cat-story target"
  skip_check "semantic grep includes dog-story target"
fi

step "10" "Image-associated recall checks"
if [ "$RUN_SEMANTIC_CHECKS" = "1" ]; then
  resp=$(curl_body_code PUT "$BASE/v1/fs/$IMAGE_CAPTION_REMOTE" "$API_KEY" "This image shows a cat face icon.")
  code=$(http_code "$resp")
  check_eq "PUT image caption text returns 200" "$code" "200"

  resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?grep=feline%20face%20icon&limit=20" "$API_KEY")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "GET image-associated grep returns 200" "$code" "200"
  wait_grep_contains_path "image-associated recall includes caption" "feline%20face%20icon" "${ROOT_DIR}/assets" "$IMAGE_CAPTION_REMOTE" "caption"

  resp=$(request GET "$BASE/v1/fs/$ROOT_DIR?find=&name=*.jpg" --auth "$API_KEY")
  check_status "GET image find returns 200" "$resp" "200"
  check_cmd "image file remains discoverable at its exact path" body_has_path "$(json_body "$resp")" "/$IMAGE_REMOTE"
else
  info "image-associated recall checks skipped (RUN_SEMANTIC_CHECKS=$RUN_SEMANTIC_CHECKS)"
  skip_check "PUT image caption text returns 200"
  skip_check "GET image-associated grep returns 200"
  skip_check "image-associated recall includes caption"
  skip_check "GET image find returns 200"
  skip_check "image file remains discoverable at its exact path"
fi

step "11" "SQL endpoint sanity"
if [ "$RUN_SQL_CHECKS" = "1" ]; then
  sql_req='{"query":"SELECT 1 AS n"}'
  sql_body="$(mktemp)"
  code=$(curl -sS -o "$sql_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" --data-binary "$sql_req" "$BASE/v1/sql")
  body=$(cat "$sql_body")
  rm -f "$sql_body"
  check_eq "POST /v1/sql returns 200" "$code" "200"
  sql_n=$(printf '%s' "$body" | jq -r '.[0].n')
  check_eq "SQL query result n=1" "$sql_n" "1"
else
  info "SQL endpoint checks skipped (RUN_SQL_CHECKS=$RUN_SQL_CHECKS)"
  skip_check "POST /v1/sql returns 200"
  skip_check "SQL query result n=1"
fi

step "12" "Copy, hardlink, rename, delete"
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

hardlink_src="${ROOT_DIR}/hardlink-source.txt"
hardlink_dst="${ROOT_DIR}/hardlink-link.txt"
resp=$(curl_body_code PUT "$BASE/v1/fs/$hardlink_src" "$API_KEY" "hardlink-original-${TS}")
code=$(http_code "$resp")
check_eq "PUT hardlink source returns 200" "$code" "200"

hardlink_body="$(mktemp)"
hardlink_code=$(curl -sS -o "$hardlink_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Hardlink-Source: /$hardlink_src" "$BASE/v1/fs/$hardlink_dst?hardlink=1")
hardlink_attempt=1
while [ "$hardlink_code" = "429" ] && [ "$hardlink_attempt" -lt "$REQUEST_MAX_RETRIES" ]; do
  info "throttled (429), retrying ${hardlink_attempt}/${REQUEST_MAX_RETRIES}: hardlink"
  hardlink_attempt=$((hardlink_attempt + 1))
  sleep "$REQUEST_RETRY_SLEEP_S"
  hardlink_code=$(curl -sS -o "$hardlink_body" -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Hardlink-Source: /$hardlink_src" "$BASE/v1/fs/$hardlink_dst?hardlink=1")
done
check_eq "POST ?hardlink returns 200" "$hardlink_code" "200"
rm -f "$hardlink_body"

hardlink_src_headers="$(mktemp)"
hardlink_dst_headers="$(mktemp)"
hardlink_src_head=$(curl -sS -D "$hardlink_src_headers" -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$hardlink_src")
hardlink_dst_head=$(curl -sS -D "$hardlink_dst_headers" -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$hardlink_dst")
check_eq "HEAD hardlink source returns 200" "$hardlink_src_head" "200"
check_eq "HEAD hardlink link returns 200" "$hardlink_dst_head" "200"
hardlink_src_id=$(header_value "$hardlink_src_headers" "X-Dat9-Resource-ID")
hardlink_dst_id=$(header_value "$hardlink_dst_headers" "X-Dat9-Resource-ID")
hardlink_src_nlink=$(header_value "$hardlink_src_headers" "X-Dat9-Nlink")
hardlink_dst_nlink=$(header_value "$hardlink_dst_headers" "X-Dat9-Nlink")
rm -f "$hardlink_src_headers" "$hardlink_dst_headers"
check_cmd "hardlink source resource_id is present" test -n "$hardlink_src_id"
check_eq "hardlink resource_id matches source" "$hardlink_dst_id" "$hardlink_src_id"
check_cmd "hardlink source nlink >= 2" bash -c 'test "${1:-0}" -ge 2' -- "$hardlink_src_nlink"
check_cmd "hardlink link nlink >= 2" bash -c 'test "${1:-0}" -ge 2' -- "$hardlink_dst_nlink"

resp=$(curl_body_code PUT "$BASE/v1/fs/$hardlink_dst" "$API_KEY" "hardlink-updated-${TS}")
code=$(http_code "$resp")
check_eq "PUT hardlink link returns 200" "$code" "200"
resp=$(curl_body_code GET "$BASE/v1/fs/$hardlink_src" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET hardlink source returns 200" "$code" "200"
check_eq "writing hardlink updates source content" "$body" "hardlink-updated-${TS}"

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
hardlink_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="hardlink-link.txt")')
check_eq "backend directory still exists" "$backend_exists" "true"
check_eq "frontend directory still exists" "$frontend_exists" "true"
check_eq "copied file removed" "$copy_exists" "false"
check_eq "hardlink file remains visible" "$hardlink_exists" "true"

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
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

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

  # Same `|| rc=$?` shape as the image upload: a failed part upload must be a
  # counted FAIL, not an errexit that skips the summary and the cleanup.
  large_upload_rc=0
  python3 - "$plan_file" "$LARGE_FILE_LOCAL" <<'PY' || large_upload_rc=$?
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
        headers = p.get("headers") or {}
        for hk, hv in headers.items():
            req.add_header(hk, hv)
        # Only inject the S3-style checksum header when the server did not
        # already presign a checksum header. Non-S3 backends (e.g. GCS) sign
        # CRC32C into x-goog-hash; adding x-amz-checksum-crc32c on top of that
        # fails with 400 "Header must be signed".
        lower_headers = {k.lower() for k in headers}
        if p.get("checksum_crc32c") and "x-amz-checksum-crc32c" not in lower_headers and "x-goog-hash" not in lower_headers:
            req.add_header("x-amz-checksum-crc32c", p["checksum_crc32c"])
        elif p.get("checksum_sha256") and "x-amz-checksum-sha256" not in lower_headers and "x-goog-meta-drive9-sha256" not in lower_headers:
            req.add_header("x-amz-checksum-sha256", p["checksum_sha256"])

        with urllib.request.urlopen(req, timeout=300) as resp:
            status = getattr(resp, "status", 200)
            if status >= 300:
                raise SystemExit(f"part {idx} failed: HTTP {status}")
PY
  check_eq "multipart part upload script exits successfully" "$large_upload_rc" "0"

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
import json
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

path = "/" + sys.argv[1].lstrip("/")
upload_limit = int(sys.argv[2])
part_size = 8 * 1024 * 1024
part = b"\x00" * part_size
checksum = base64.b64encode(struct.pack(">I", crc32c(part))).decode()
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
  boundary_ok_id=$(jq -r '.upload_id // empty' "$boundary_ok_body")
  check_cmd "init at upload limit returns an upload plan" test -n "$boundary_ok_id"
  # Release the reservation immediately. An INITIATED session that is never
  # completed keeps total_size reserved against the tenant quota, and a later
  # run then fails this same check with 507 for a reason that is not a server
  # regression (see e2e/AGENTS.md "Local-server-specific expectations").
  if [ -n "$boundary_ok_id" ]; then
    abort_resp=$(request DELETE "$BASE/v1/uploads/$boundary_ok_id" --auth "$API_KEY")
    check_status "DELETE aborts the limit-reservation upload" "$abort_resp" "200"
  else
    skip_check "DELETE aborts the limit-reservation upload"
  fi
  rm -f "$boundary_ok_payload" "$boundary_ok_body"

  over_limit=$((UPLOAD_LIMIT_BYTES + 1))
  boundary_over_payload="$(mktemp)"
  python3 - "$ROOT_DIR/limit-over.bin" "$over_limit" > "$boundary_over_payload" <<'PY'
import base64
import json
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

path = "/" + sys.argv[1].lstrip("/")
over_limit = int(sys.argv[2])
part = b"\x00" * (8 * 1024 * 1024)
checksum = base64.b64encode(struct.pack(">I", crc32c(part))).decode()
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

# ---------------------------------------------------------------------------
# Extension steps (16-24). Every fixture below lives under $ROOT_DIR so the
# existing-tenant cleanup step (25) removes all of it in one request.
# ---------------------------------------------------------------------------
CONTRACT_DIR="${ROOT_DIR}/contract"
STAT_PATH="${CONTRACT_DIR}/stat.txt"
CAS_PATH="${CONTRACT_DIR}/cas.txt"
RANGE_PATH="${CONTRACT_DIR}/range.txt"
CREATE_PATH="${CONTRACT_DIR}/created.txt"
TAG_PATH="${CONTRACT_DIR}/tagged.txt"
LINK_PATH="${CONTRACT_DIR}/link.txt"

if [ "$RUN_CONTRACT_CHECKS" = "1" ]; then
  step "16" "Wire contract: stat metadata and HEAD headers"
  probe_features
  STAT_PAYLOAD="stat-contract-${TS}"

  resp=$(request POST "$BASE/v1/fs/$CONTRACT_DIR?mkdir" --auth "$API_KEY")
  check_status "mkdir for the contract fixture dir returns 200" "$resp" "200"

  resp=$(request PUT "$BASE/v1/fs/$STAT_PATH" --auth "$API_KEY" --data "$STAT_PAYLOAD")
  check_status "PUT stat fixture returns 200" "$resp" "200"
  PUT_REV=$(json_body "$resp" | jq -r '.revision // 0')

  resp=$(request GET "$BASE/v1/fs/$STAT_PATH?stat" --auth "$API_KEY")
  stat_body=$(json_body "$resp")
  check_status "GET ?stat returns 200" "$resp" "200"
  check_json "stat size matches the written bytes" "$stat_body" '.size' "${#STAT_PAYLOAD}"
  check_json "stat reports isdir=false for a file" "$stat_body" '.isdir' "false"
  check_json "stat revision matches the PUT revision" "$stat_body" '.revision' "$PUT_REV"
  check_json "stat mtime is a positive epoch second" "$stat_body" '(.mtime > 0)' "true"
  if [ "$PROBE_STAT_BTIME" = "true" ]; then
    check_json "stat btime is a positive epoch second" "$stat_body" '(.btime > 0)' "true"
  else
    skip_check "stat btime is a positive epoch second (server does not project btime)"
  fi
  STAT_RESOURCE=$(printf '%s' "$stat_body" | jq -r '.resource_id // empty')
  check_cmd "stat exposes resource_id" test -n "$STAT_RESOURCE"
  check_cmd "stat exposes content_type" test -n "$(printf '%s' "$stat_body" | jq -r '.content_type // empty')"
  check_json "stat tags project as an object (or null when untagged)" "$stat_body" '(.tags == null or (.tags|type) == "object")' "true"
  check_json "stat description projects as a string" "$stat_body" '(.description|type)' "string"

  resp=$(request HEAD "$BASE/v1/fs/$STAT_PATH" --auth "$API_KEY")
  check_status "HEAD stat returns 200" "$resp" "200"
  check_eq "HEAD Content-Length equals the file size" "$(resp_hdr 'Content-Length')" "${#STAT_PAYLOAD}"
  check_eq "HEAD marks the file as not-a-dir" "$(resp_hdr 'X-Dat9-IsDir')" "false"
  check_eq "HEAD X-Dat9-Revision matches the PUT revision" "$(resp_hdr 'X-Dat9-Revision')" "$PUT_REV"
  check_eq "HEAD X-Dat9-Resource-ID matches the stat resource_id" "$(resp_hdr 'X-Dat9-Resource-ID')" "$STAT_RESOURCE"
  check_eq "HEAD X-Dat9-Mtime matches stat mtime" "$(resp_hdr 'X-Dat9-Mtime')" "$(printf '%s' "$stat_body" | jq -r '.mtime')"
  if [ "$PROBE_STAT_BTIME" = "true" ]; then
    check_eq "HEAD X-Dat9-Btime matches stat btime" "$(resp_hdr 'X-Dat9-Btime')" "$(printf '%s' "$stat_body" | jq -r '.btime')"
  else
    skip_check "HEAD X-Dat9-Btime matches stat btime (server does not project btime)"
  fi
  check_cmd "HEAD X-Dat9-Nlink is at least 1" bash -c 'test "${1:-0}" -gt 0' _ "$(resp_hdr 'X-Dat9-Nlink')"
  if [ "$PROBE_HEAD_VALIDATORS" = "true" ]; then
    check_cmd "HEAD advertises a strong ETag" test -n "$(resp_hdr 'ETag')"
    check_eq "HEAD advertises Accept-Ranges: bytes" "$(resp_hdr 'Accept-Ranges')" "bytes"
    check_eq "HEAD reports the single content layout" "$(resp_hdr 'X-Dat9-Content-Layout')" "single"
  else
    skip_check "HEAD advertises a strong ETag (server does not expose read validators on HEAD)"
    skip_check "HEAD advertises Accept-Ranges: bytes (server does not expose read validators on HEAD)"
    skip_check "HEAD reports the single content layout (server does not expose read validators on HEAD)"
  fi
  info "HEAD storage class: $(resp_hdr 'X-Dat9-Storage-Type') (db9 = inline, s3 = object store)"

  resp=$(request GET "$BASE/v1/fs/?stat" --auth "$API_KEY")
  check_status "root ?stat returns 200" "$resp" "200"
  check_json "root ?stat reports isdir=true" "$(json_body "$resp")" '.isdir' "true"
  resp=$(request HEAD "$BASE/v1/fs/" --auth "$API_KEY")
  check_eq "root HEAD marks the root as a directory" "$(resp_hdr 'X-Dat9-IsDir')" "true"

  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR?list" --auth "$API_KEY")
  list_body=$(json_body "$resp")
  check_json "list entry carries the file size" "$list_body" '.entries[]|select(.name=="stat.txt")|.size' "${#STAT_PAYLOAD}"
  check_json "list entry carries isDir=false" "$list_body" '.entries[]|select(.name=="stat.txt")|.isDir' "false"
  check_json "list entry carries the stat resource_id" "$list_body" '.entries[]|select(.name=="stat.txt")|.resource_id' "$STAT_RESOURCE"
  check_json "list entry carries a positive revision" "$list_body" '.entries[]|select(.name=="stat.txt")|(.revision > 0)' "true"
  check_json "list entry carries a boolean hasMode" "$list_body" '.entries[]|select(.name=="stat.txt")|(.hasMode|type)' "boolean"
  check_json "list entry carries nlink >= 1" "$list_body" '.entries[]|select(.name=="stat.txt")|(.nlink >= 1)' "true"

  step "17" "Wire contract: conditional write and range read"
  # X-Dat9-Expected-Revision is a CAS, not a hint: 0 means "this path must not
  # exist", a matching revision overwrites, anything else loses with 409.
  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Expected-Revision: 0" --data "cas-v1")
  check_status "PUT with Expected-Revision 0 creates a missing path" "$resp" "200"
  CAS_REV=$(json_body "$resp" | jq -r '.revision // 0')

  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Expected-Revision: 0" --data "cas-v2")
  check_status "PUT with Expected-Revision 0 over an existing path returns 409" "$resp" "409"

  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Expected-Revision: $CAS_REV" --data "cas-v2")
  check_status "PUT with the current Expected-Revision returns 200" "$resp" "200"
  CAS_REV2=$(json_body "$resp" | jq -r '.revision // 0')
  check_cmd "conditional write bumps the revision" \
    bash -c 'test "${2:-0}" -gt "${1:-0}"' _ "$CAS_REV" "$CAS_REV2"

  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Expected-Revision: $CAS_REV" --data "cas-v3")
  check_status "PUT with a stale Expected-Revision returns 409" "$resp" "409"

  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Expected-Revision: not-a-number" --data "cas-v3")
  check_status "PUT with a malformed Expected-Revision returns 400" "$resp" "400"

  resp=$(request PUT "$BASE/v1/fs/$CAS_PATH" --auth "$API_KEY" --hdr "X-Dat9-Content-Length: $(( ${#STAT_PAYLOAD} + 7 ))" --data "cas-v4")
  check_status "PUT with a mismatched X-Dat9-Content-Length returns 400" "$resp" "400"

  resp=$(request HEAD "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY")
  check_status "HEAD of a not-yet-written range fixture returns 404" "$resp" "404"

  RANGE_TEXT="0123456789"
  resp=$(request PUT "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --data "$RANGE_TEXT")
  check_status "PUT range fixture returns 200" "$resp" "200"
  RANGE_ETAG=$(request HEAD "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" >/dev/null; resp_hdr 'ETag')
  if [ "$PROBE_HEAD_VALIDATORS" = "true" ]; then
    check_cmd "range fixture HEAD exposes an ETag" test -n "$RANGE_ETAG"
  else
    skip_check "range fixture HEAD exposes an ETag (server does not expose read validators on HEAD)"
  fi

  if [ "$PROBE_RANGE" = "true" ]; then
    resp=$(request GET "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --hdr "Range: bytes=2-5")
    check_status "Range read returns 206" "$resp" "206"
    check_eq "Range read returns exactly the requested bytes" "$(json_body "$resp")" "2345"
    check_eq "Range response carries Content-Range" "$(resp_hdr 'Content-Range')" "bytes 2-5/10"

    resp=$(request GET "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --hdr "Range: bytes=50-60")
    check_status "an unsatisfiable Range returns 416" "$resp" "416"
  else
    skip_check "Range read returns 206 (server does not honour Range)"
    skip_check "Range read returns exactly the requested bytes (server does not honour Range)"
    skip_check "Range response carries Content-Range (server does not honour Range)"
    skip_check "an unsatisfiable Range returns 416 (server does not honour Range)"
  fi

  resp=$(request GET "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --hdr "Range: bytes=0-1" --hdr 'If-Range: "stale-validator"')
  check_status "a stale If-Range falls back to the full body" "$resp" "200"
  check_eq "stale If-Range returns the whole file" "$(json_body "$resp")" "$RANGE_TEXT"

  if [ "$PROBE_IF_MATCH" = "true" ]; then
    resp=$(request GET "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --hdr 'If-Match: "stale-etag"')
    check_status "a stale If-Match returns 412" "$resp" "412"

    resp=$(request GET "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --hdr "If-Match: $RANGE_ETAG")
    check_status "a matching If-Match serves the file" "$resp" "200"
  else
    skip_check "a stale If-Match returns 412 (server does not evaluate If-Match)"
    skip_check "a matching If-Match serves the file (server does not evaluate If-Match)"
  fi

  if [ "$PROBE_HEAD_VALIDATORS" = "true" ]; then
    resp=$(request PUT "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --data "0123456789-updated")
    resp=$(request HEAD "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY")
    check_cmd "the ETag changes after an overwrite" bash -c 'test "$1" != "$2"' _ "$RANGE_ETAG" "$(resp_hdr 'ETag')"
  else
    resp=$(request PUT "$BASE/v1/fs/$RANGE_PATH" --auth "$API_KEY" --data "0123456789-updated")
    skip_check "the ETag changes after an overwrite (server does not expose read validators on HEAD)"
  fi

  step "18" "Metadata: create, tags, setmeta and find-by-tag"
  resp=$(request POST "$BASE/v1/fs/$CREATE_PATH?create" --auth "$API_KEY")
  check_status "POST ?create returns 200" "$resp" "200"
  check_json "?create reports revision 1" "$(json_body "$resp")" '.revision' "1"
  if [ "$CAP_COMPLETION_IDENTITY" = "true" ]; then
    check_cmd "?create echoes the created resource_id (completion_identity_v1)" \
      test -n "$(json_body "$resp" | jq -r '.resource_id // empty')"
  else
    skip_check "?create echoes the created resource_id (server does not advertise completion_identity_v1)"
  fi

  resp=$(request POST "$BASE/v1/fs/$CREATE_PATH?create" --auth "$API_KEY")
  check_status "POST ?create over an existing path returns 409" "$resp" "409"

  resp=$(request PUT "$BASE/v1/fs/$TAG_PATH" --auth "$API_KEY" \
    --hdr "X-Dat9-Tag: e2e=live-${TS}" --hdr "X-Dat9-Description: description-${TS}" --data "tagged-body")
  check_status "PUT with X-Dat9-Tag and X-Dat9-Description returns 200" "$resp" "200"

  resp=$(request GET "$BASE/v1/fs/$TAG_PATH?stat" --auth "$API_KEY")
  tagged_stat=$(json_body "$resp")
  check_json "the PUT tag lands in stat tags" "$tagged_stat" '.tags.e2e' "live-${TS}"
  check_json "the PUT description lands in stat" "$tagged_stat" '.description' "description-${TS}"

  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR?find=&tag=e2e=live-${TS}" --auth "$API_KEY")
  check_status "GET ?find by tag returns 200" "$resp" "200"
  check_cmd "find by exact tag value returns the tagged file" body_has_path "$(json_body "$resp")" "/$TAG_PATH"

  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR?find=&tag=e2e" --auth "$API_KEY")
  check_cmd "find by tag key (existence) returns the tagged file" body_has_path "$(json_body "$resp")" "/$TAG_PATH"

  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR?find=&tag=e2e=other-${TS}" --auth "$API_KEY")
  check_cmd "find by a different tag value excludes the file" body_lacks_path "$(json_body "$resp")" "/$TAG_PATH"

  resp=$(request POST "$BASE/v1/fs/$TAG_PATH?setmeta" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg v "live-${TS}" --arg r "$TS" --arg d "setmeta-${TS}" '{tags:{e2e:$v,run:$r},description:$d}')")
  check_status "POST ?setmeta returns 200" "$resp" "200"
  resp=$(request GET "$BASE/v1/fs/$TAG_PATH?stat" --auth "$API_KEY")
  check_json "setmeta adds the new tag key" "$(json_body "$resp")" '.tags.run' "$TS"
  check_json "setmeta replaces the description" "$(json_body "$resp")" '.description' "setmeta-${TS}"

  resp=$(request POST "$BASE/v1/fs/$TAG_PATH?setmeta" --auth "$API_KEY" --hdr 'Content-Type: application/json' --data '{}')
  check_status "POST ?setmeta with no fields returns 400" "$resp" "400"

  resp=$(request POST "$BASE/v1/fs/$CONTRACT_DIR?setmeta" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data '{"tags":{"e2e":"x"}}')
  check_status "POST ?setmeta on a directory returns 400" "$resp" "400"

  resp=$(request POST "$BASE/v1/fs/$TAG_PATH?setmeta" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg d "$(python3 -c 'print("x"*2001)')" '{description:$d}')")
  check_status "POST ?setmeta with a 2001-character description returns 400" "$resp" "400"

  resp=$(request PUT "$BASE/v1/fs/$TAG_PATH" --auth "$API_KEY" --hdr "X-Dat9-Tag: missing-equals" --data "bad-tag")
  check_status "PUT with a malformed X-Dat9-Tag returns 400" "$resp" "400"

  step "19" "Path handling: unicode, spaces, reserved characters, encoded source headers"
  UNI_DIR="${ROOT_DIR}/路径 空格"
  UNI_FILE="${UNI_DIR}/文件 #1?.txt"
  UNI_PAYLOAD="unicode-${TS}"
  UNI_COPY="${UNI_DIR}/copy #2.txt"
  UNI_RENAMED="${UNI_DIR}/renamed.txt"
  enc_uni_dir=$(url_encode "/$UNI_DIR")
  enc_uni_file=$(url_encode "/$UNI_FILE")
  enc_uni_copy=$(url_encode "/$UNI_COPY")

  resp=$(request POST "$BASE/v1/fs/$enc_uni_dir?mkdir" --auth "$API_KEY")
  check_status "mkdir with unicode and spaces returns 200" "$resp" "200"

  resp=$(request PUT "$BASE/v1/fs/$enc_uni_file" --auth "$API_KEY" --data "$UNI_PAYLOAD")
  check_status "PUT with unicode, space, '#' and '?' percent encoding returns 200" "$resp" "200"

  resp=$(request GET "$BASE/v1/fs/$enc_uni_file" --auth "$API_KEY")
  check_eq "round-trip read returns the exact bytes" "$(json_body "$resp")" "$UNI_PAYLOAD"

  resp=$(request GET "$BASE/v1/fs/$enc_uni_dir?list" --auth "$API_KEY")
  check_json "list keeps the unicode file name intact" "$(json_body "$resp")" \
    '[.entries[]|select(.name=="文件 #1?.txt")]|length' "1"

  resp=$(request GET "$BASE/v1/fs/$enc_uni_file?stat" --auth "$API_KEY")
  check_json "stat resolves the unicode path with its exact size" "$(json_body "$resp")" '.size' "${#UNI_PAYLOAD}"

  resp=$(request POST "$BASE/v1/fs/$enc_uni_copy?copy" --auth "$API_KEY" \
    --hdr "X-Dat9-Copy-Source: $(b64url "/$UNI_FILE")" --hdr "X-Dat9-Path-Encoding: base64url")
  check_status "copy with a base64url source header returns 200" "$resp" "200"
  resp=$(request GET "$BASE/v1/fs/$enc_uni_copy" --auth "$API_KEY")
  check_eq "the encoded copy carries the original bytes" "$(json_body "$resp")" "$UNI_PAYLOAD"

  resp=$(request POST "$BASE/v1/fs/$(url_encode "/$UNI_RENAMED")?rename" --auth "$API_KEY" \
    --hdr "X-Dat9-Rename-Source: $(b64url "/$UNI_COPY")" --hdr "X-Dat9-Path-Encoding: base64url")
  check_status "rename with a base64url source header returns 200" "$resp" "200"
  resp=$(request GET "$BASE/v1/fs/$(url_encode "/$UNI_RENAMED")" --auth "$API_KEY")
  check_eq "the renamed file keeps the original bytes" "$(json_body "$resp")" "$UNI_PAYLOAD"
  resp=$(request GET "$BASE/v1/fs/$enc_uni_copy" --auth "$API_KEY")
  check_status "the old path is gone after rename" "$resp" "404"

  resp=$(request POST "$BASE/v1/fs/$enc_uni_copy?copy" --auth "$API_KEY" \
    --hdr "X-Dat9-Copy-Source: $(b64url "/$UNI_FILE")" --hdr "X-Dat9-Path-Encoding: utf8")
  check_status "an unsupported X-Dat9-Path-Encoding returns 400" "$resp" "400"

  step "20" "Delete and link contract"
  DEL_DIR="${CONTRACT_DIR}/del"
  DEL_FILE="${CONTRACT_DIR}/del-file.txt"
  resp=$(request POST "$BASE/v1/fs/$DEL_DIR?mkdir" --auth "$API_KEY")
  check_status "mkdir for the delete fixture returns 200" "$resp" "200"
  resp=$(request PUT "$BASE/v1/fs/$DEL_DIR/child.txt" --auth "$API_KEY" --data "child")
  check_status "PUT a child into the delete fixture returns 200" "$resp" "200"

  resp=$(request DELETE "$BASE/v1/fs/$DEL_DIR" --auth "$API_KEY")
  check_status "deleting a non-empty directory returns 409" "$resp" "409"
  resp=$(request DELETE "$BASE/v1/fs/$DEL_DIR?kind=dir" --auth "$API_KEY")
  check_status "rmdir of a non-empty directory returns 409" "$resp" "409"
  resp=$(request DELETE "$BASE/v1/fs/$DEL_DIR?kind=bogus" --auth "$API_KEY")
  check_status "an invalid delete kind returns 400" "$resp" "400"
  resp=$(request DELETE "$BASE/v1/fs/$DEL_DIR?recursive" --auth "$API_KEY")
  check_status "recursive delete returns 200" "$resp" "200"
  resp=$(request GET "$BASE/v1/fs/$DEL_DIR?list" --auth "$API_KEY")
  check_status "listing the recursively deleted directory returns 200" "$resp" "200"
  check_json "the recursively deleted directory lists empty" "$(json_body "$resp")" '(.entries|length)' "0"
  resp=$(request GET "$BASE/v1/fs/$DEL_DIR?stat" --auth "$API_KEY")
  check_status "stat on the recursively deleted directory returns 404" "$resp" "404"

  resp=$(request PUT "$BASE/v1/fs/$DEL_FILE" --auth "$API_KEY" --data "bye")
  check_status "PUT the single-file delete fixture returns 200" "$resp" "200"
  resp=$(request DELETE "$BASE/v1/fs/$DEL_FILE?kind=file" --auth "$API_KEY")
  check_status "deleting a file with kind=file returns 200" "$resp" "200"
  resp=$(request DELETE "$BASE/v1/fs/$DEL_FILE?kind=file" --auth "$API_KEY")
  check_status "deleting the same file twice returns 404" "$resp" "404"
  resp=$(request DELETE "$BASE/v1/fs/$CONTRACT_DIR/never-created.txt" --auth "$API_KEY")
  check_status "deleting a missing path returns 404" "$resp" "404"

  resp=$(request POST "$BASE/v1/fs/$LINK_PATH?symlink" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg t "/$STAT_PATH" '{target:$t}')")
  check_status "POST ?symlink returns 200" "$resp" "200"
  check_json "?symlink reports revision 1" "$(json_body "$resp")" '.revision' "1"
  resp=$(request POST "$BASE/v1/fs/$LINK_PATH?symlink" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg t "/$STAT_PATH" '{target:$t}')")
  check_status "POST ?symlink over an existing path returns 409" "$resp" "409"
  resp=$(request HEAD "$BASE/v1/fs/$LINK_PATH" --auth "$API_KEY")
  check_status "HEAD of the symlink returns 200" "$resp" "200"
  resp=$(request POST "$BASE/v1/fs/$CONTRACT_DIR/bad-link?symlink" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data '{"target":""}')
  check_status "POST ?symlink with an empty target returns 400" "$resp" "400"
  resp=$(request DELETE "$BASE/v1/fs/$LINK_PATH?kind=file" --auth "$API_KEY")
  check_status "the symlink is deletable" "$resp" "200"

  step "21" "Error and routing contract"
  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR/does-not-exist.txt" --auth "$API_KEY")
  check_status "GET a missing path returns 404" "$resp" "404"
  resp=$(request GET "$BASE/v1/fs/$CONTRACT_DIR/does-not-exist.txt?stat" --auth "$API_KEY")
  check_status "GET ?stat on a missing path returns 404" "$resp" "404"
  resp=$(request HEAD "$BASE/v1/fs/$CONTRACT_DIR/does-not-exist.txt" --auth "$API_KEY")
  check_status "HEAD a missing path returns 404" "$resp" "404"

  resp=$(request POST "$BASE/v1/fs/$STAT_PATH" --auth "$API_KEY")
  check_status "POST with no action selector returns 400" "$resp" "400"
  resp=$(request POST "$BASE/v1/fs/$STAT_PATH?copy" --auth "$API_KEY")
  check_status "POST ?copy without a source header returns 400" "$resp" "400"
  resp=$(request POST "$BASE/v1/fs/$STAT_PATH?rename" --auth "$API_KEY")
  check_status "POST ?rename without a source header returns 400" "$resp" "400"
  resp=$(request POST "$BASE/v1/fs/$CONTRACT_DIR?mkdir" --auth "$API_KEY")
  check_status "mkdir over an existing directory returns 409" "$resp" "409"
  resp=$(request DELETE "$BASE/v1/fs:batch-stat" --auth "$API_KEY")
  check_status "a wrong method on /v1/fs:batch-stat returns 405" "$resp" "405"

  resp=$(request GET "$BASE/v1/fs/$STAT_PATH")
  check_status "an unauthenticated FS read returns 401" "$resp" "401"
  resp=$(request GET "$BASE/v1/fs/$STAT_PATH" --auth "not-a-valid-key")
  check_status "an invalid bearer key returns 401" "$resp" "401"
  resp=$(request GET "$BASE/v1/not-a-route" --auth "$API_KEY")
  check_status "an unknown route returns 404" "$resp" "404"

  resp=$(request GET "$BASE/healthz")
  check_status "/healthz returns 200" "$resp" "200"
  check_json "/healthz reports ok" "$(json_body "$resp")" '.status' "ok"
else
  info "wire/metadata/error contract checks skipped (RUN_CONTRACT_CHECKS=$RUN_CONTRACT_CHECKS)"
  skip_check "wire contract: stat metadata and HEAD headers (RUN_CONTRACT_CHECKS=0)"
  skip_check "wire contract: conditional write and range read (RUN_CONTRACT_CHECKS=0)"
  skip_check "metadata: create, tags, setmeta and find-by-tag (RUN_CONTRACT_CHECKS=0)"
  skip_check "path handling: unicode, spaces, reserved characters (RUN_CONTRACT_CHECKS=0)"
  skip_check "delete and link contract (RUN_CONTRACT_CHECKS=0)"
  skip_check "error and routing contract (RUN_CONTRACT_CHECKS=0)"
fi

if [ "$RUN_BATCH_CHECKS" = "1" ]; then
  step "22" "Batch endpoints: batch-stat, batch-read-small, batch-write"
  BW_DIR="${ROOT_DIR}/batch-wire"
  BW_ONE="${BW_DIR}/one.txt"
  BW_TWO="${BW_DIR}/two.txt"
  BW_MISSING="${BW_DIR}/missing.txt"
  BW_ONE_BODY="batch-one-${TS}"

  resp=$(request POST "$BASE/v1/fs/$BW_DIR?mkdir" --auth "$API_KEY")
  check_status "mkdir for the batch fixture dir returns 200" "$resp" "200"
  resp=$(request PUT "$BASE/v1/fs/$BW_ONE" --auth "$API_KEY" --data "$BW_ONE_BODY")
  check_status "PUT the batch fixture file returns 200" "$resp" "200"
  BW_ONE_REV=$(json_body "$resp" | jq -r '.revision // 0')

  stat_req=$(jq -nc --arg a "/$BW_ONE" --arg b "/$BW_MISSING" --arg c "/$BW_DIR" '{paths:[$a,$b,$c]}')
  resp=$(request POST "$BASE/v1/fs:batch-stat" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data "$stat_req")
  bs_body=$(json_body "$resp")
  check_status "POST /v1/fs:batch-stat returns 200" "$resp" "200"
  check_json "batch-stat returns one result per requested path" "$bs_body" '(.results|length)' "3"
  check_json "batch-stat resolves an existing file" "$bs_body" '.results[0].status' "200"
  check_json "batch-stat reports the file size" "$bs_body" '.results[0].size' "${#BW_ONE_BODY}"
  check_json "batch-stat reports a missing path per item" "$bs_body" '.results[1].status' "404"
  check_json "batch-stat marks a directory" "$bs_body" '.results[2].isDir' "true"
  check_json "batch-stat reports a boolean hasMode per item" "$bs_body" '(.results[0].hasMode|type)' "boolean"
  check_json "batch-stat reports a positive revision" "$bs_body" '.results[0].revision' "$BW_ONE_REV"

  if [ "$PROBE_BATCH_CHECKSUM" = "true" ]; then
    resp=$(request POST "$BASE/v1/fs:batch-stat" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
      --data "$(jq -nc --arg a "/$BW_ONE" '{paths:[$a],include_checksum:true}')")
    check_json "batch-stat include_checksum returns the content sha256" "$(json_body "$resp")" '.results[0].checksum_sha256' \
      "$(printf '%s' "$BW_ONE_BODY" | sha256sum | cut -d' ' -f1)"
  else
    skip_check "batch-stat include_checksum returns the content sha256 (server does not project checksums)"
  fi

  resp=$(request POST "$BASE/v1/fs:batch-stat" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc '[range(0;257) | "/x" + (.|tostring)] | {paths:.}')")
  check_status "batch-stat over the 256-path limit returns 400" "$resp" "400"

  resp=$(request POST "$BASE/v1/fs:batch-read-small" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg a "/$BW_MISSING" '{paths:[$a]}')")
  check_json "batch-read-small reports a missing path per item" "$(json_body "$resp")" '.results[0].status' "404"

  resp=$(request POST "$BASE/v1/fs:batch-read-small" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc '[range(0;129) | "/x" + (.|tostring)] | {paths:.}')")
  check_status "batch-read-small over the 128-path limit returns 400" "$resp" "400"

  # Everything below needs an inline-capable backend: batch-write requires
  # inline storage, and batch-read-small rejects non-inline objects with a
  # per-item 413. /v1/status advertises that readiness as
  # storage_capabilities.batch_write_mode_v1.
  if [ "$CAP_BATCH_WRITE_MODE" = "true" ]; then
    resp=$(request POST "$BASE/v1/fs:batch-read-small" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
      --data "$(jq -nc --arg a "/$BW_ONE" '{paths:[$a]}')")
    brs_body=$(json_body "$resp")
    check_status "POST /v1/fs:batch-read-small returns 200" "$resp" "200"
    check_json "batch-read-small reports a per-item 200" "$brs_body" '.results[0].status' "200"
    check_json "batch-read-small returns the inline bytes as base64" "$brs_body" '.results[0].data' \
      "$(printf '%s' "$BW_ONE_BODY" | base64 | tr -d '\n')"
    check_json "batch-read-small echoes the size" "$brs_body" '.results[0].size' "${#BW_ONE_BODY}"
    resp=$(request POST "$BASE/v1/fs:batch-read-small" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
      --data "$(jq -nc --arg a "/$BW_ONE" '{paths:[$a],max_bytes:2}')")
    check_json "batch-read-small reports a per-item 413 over max_bytes" "$(json_body "$resp")" '.results[0].status' "413"

    bw_new_body="batch-two-${TS}"
    bw_upd_body="${BW_ONE_BODY}-updated"
    bw_req=$(jq -nc \
      --arg create "/$BW_TWO" --arg create_data "$(printf '%s' "$bw_new_body" | base64 | tr -d '\n')" \
      --arg existing "/$BW_ONE" --arg existing_data "$(printf '%s' "$bw_upd_body" | base64 | tr -d '\n')" \
      --arg dir "/$BW_DIR" --arg dir_data "$(printf 'x' | base64 | tr -d '\n')" \
      --argjson rev "$BW_ONE_REV" \
      '{items:[
         {path:$create, expected_revision:0, data:$create_data},
         {path:$existing, expected_revision:$rev, data:$existing_data},
         {path:$dir, expected_revision:$rev, data:$dir_data},
         {path:$create, expected_revision:999, data:$create_data}
       ]}')
    resp=$(request POST "$BASE/v1/fs:batch-write" --auth "$API_KEY" \
      --hdr 'Content-Type: application/json' --data "$bw_req")
    bw_body=$(json_body "$resp")
    check_status "POST /v1/fs:batch-write returns 200" "$resp" "200"
    check_json "batch-write returns one result per item" "$bw_body" '(.results|length)' "4"
    check_json "batch-write creates a missing path with expected_revision 0" "$bw_body" '.results[0].status' "200"
    check_json "batch-write overwrites with the current revision" "$bw_body" '.results[1].status' "200"
    check_json "batch-write rejects a directory target per item" "$bw_body" '.results[2].status' "409"
    check_json "batch-write rejects a stale revision per item" "$bw_body" '.results[3].status' "409"
    check_json "batch-write commits exactly the winning items" "$bw_body" '[.results[]|select(.status==200)]|length' "2"

    resp=$(request GET "$BASE/v1/fs/$BW_TWO" --auth "$API_KEY")
    check_eq "the batch-created file carries the batch bytes" "$(json_body "$resp")" "$bw_new_body"
    resp=$(request GET "$BASE/v1/fs/$BW_ONE" --auth "$API_KEY")
    check_eq "the batch-overwritten file carries the new bytes" "$(json_body "$resp")" "$bw_upd_body"
    resp=$(request GET "$BASE/v1/fs/$BW_ONE?stat" --auth "$API_KEY")
    check_cmd "the batch-overwritten revision advanced past the original" \
      bash -c 'test "${1:-0}" -gt "${2:-0}"' _ "$(json_body "$resp" | jq -r '.revision // 0')" "$BW_ONE_REV"

    resp=$(request POST "$BASE/v1/fs:batch-write" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
      --data "$(jq -nc --arg p "/$BW_ONE" --arg d "$(printf 'z' | base64 | tr -d '\n')" \
        '{items:[{path:$p, expected_revision:0, data:$d}]}')")
    check_json "batch-write treats expected_revision 0 as create-only" "$(json_body "$resp")" '.results[0].status' "409"

    if [ -n "$SERVER_INLINE_THRESHOLD" ] && [ "$SERVER_INLINE_THRESHOLD" -le 1048576 ]; then
      oversize_b64=$(python3 -c 'import base64, sys; print(base64.b64encode(b"x" * int(sys.argv[1])).decode())' "$((SERVER_INLINE_THRESHOLD + 1))")
      resp=$(request POST "$BASE/v1/fs:batch-write" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
        --data "$(jq -nc --arg p "/$BW_DIR/oversize.bin" --arg d "$oversize_b64" \
          '{items:[{path:$p, expected_revision:0, data:$d}]}')")
      check_json "batch-write rejects an item over inline_threshold with per-item 413" "$(json_body "$resp")" '.results[0].status' "413"
    else
      skip_check "batch-write rejects an item over inline_threshold with per-item 413 (inline_threshold not advertised or too large to probe)"
    fi

    resp=$(request POST "$BASE/v1/fs:batch-write" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
      --data "$(jq -nc --arg p "/$BW_DIR/mode.txt" --arg d "$(printf 'mode' | base64 | tr -d '\n')" \
        '{items:[{path:$p, expected_revision:0, data:$d, hasMode:true, mode:420}]}')")
    check_json "batch-write commits an owner hasMode item" "$(json_body "$resp")" '.results[0].status' "200"
    resp=$(request GET "$BASE/v1/fs/$BW_DIR?list" --auth "$API_KEY")
    check_json "the hasMode item exposes its mode in the listing" "$(json_body "$resp")" \
      '.entries[]|select(.name=="mode.txt")|.hasMode' "true"
    check_json "the hasMode item exposes mode 420 (0644)" "$(json_body "$resp")" \
      '.entries[]|select(.name=="mode.txt")|.mode' "420"
  else
    info "batch-write/batch-read-small inline checks skipped (storage_capabilities.batch_write_mode_v1=$CAP_BATCH_WRITE_MODE)"
    skip_check "batch-read-small inline happy path (inline storage not advertised)"
    skip_check "batch-read-small per-item 413 over max_bytes (inline storage not advertised)"
    skip_check "batch-write create/overwrite/CAS matrix (inline storage not advertised)"
    skip_check "batch-write treats expected_revision 0 as create-only (inline storage not advertised)"
    skip_check "batch-write rejects an item over inline_threshold with per-item 413 (inline storage not advertised)"
    skip_check "batch-write owner hasMode item (inline storage not advertised)"
  fi

else
  info "batch endpoint checks skipped (RUN_BATCH_CHECKS=$RUN_BATCH_CHECKS)"
  skip_check "batch endpoints: batch-stat / batch-read-small / batch-write (RUN_BATCH_CHECKS=0)"
fi

if [ "$RUN_UPLOAD_LIFECYCLE_CHECKS" = "1" ]; then
  step "23" "Upload lifecycle: V1 list/resume/abort and V2 presigned round-trip"
  V1_UP_PATH="${CONTRACT_DIR}/v1-lifecycle.bin"
  # 2 MiB = one 8 MiB part. part_checksums is mandatory on V1 initiate and
  # resume (an empty list answers 400 "missing part_checksums"); a 4-byte
  # base64 value is a well-formed CRC32C slot. Nothing is ever uploaded here:
  # the session is aborted, which is the point of this block.
  V1_INIT=$(jq -nc --arg p "/$V1_UP_PATH" '{path:$p,total_size:2097152,part_checksums:["AAAAAA=="]}')
  V1_CHECKSUMS='{"part_checksums":["AAAAAA=="]}'

  resp=$(request POST "$BASE/v1/uploads/initiate" --auth "$API_KEY" --hdr 'Content-Type: application/json' --data "$V1_INIT")
  check_status "V1 initiate with one part checksum returns 202" "$resp" "202"
  check_cmd "V1 initiate returns an upload_id" test -n "$(json_body "$resp" | jq -r '.upload_id // empty')"
  check_cmd "V1 initiate returns presigned parts" \
    test "$(json_body "$resp" | jq -r '(.parts // [])|length')" -gt 0

  resp=$(request GET "$BASE/v1/uploads?path=$(url_encode "/$V1_UP_PATH")" --auth "$API_KEY")
  v1_listing=$(json_body "$resp")
  check_status "GET /v1/uploads?path lists in-progress uploads" "$resp" "200"
  check_json "the listing reports exactly one live session for the path" "$v1_listing" '.uploads|length' "1"
  check_json "the listed session is UPLOADING" "$v1_listing" '.uploads[0].status' "UPLOADING"
  check_json "the listing echoes the session total_size" "$v1_listing" '.uploads[0].total_size' "2097152"
  v1_id=$(printf '%s' "$v1_listing" | jq -r '.uploads[0].upload_id // empty')

  resp=$(request POST "$BASE/v1/uploads/$v1_id/resume" --auth "$API_KEY")
  check_status "V1 resume without part checksums returns 400" "$resp" "400"

  resp=$(request POST "$BASE/v1/uploads/$v1_id/resume" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data "$V1_CHECKSUMS")
  check_status "POST /v1/uploads/{id}/resume returns 200" "$resp" "200"
  check_json "resume echoes the session upload_id" "$(json_body "$resp")" '.upload_id' "$v1_id"

  resp=$(request GET "$BASE/v1/uploads/$v1_id/resume" --auth "$API_KEY" \
    --hdr 'X-Dat9-Part-Checksums: AAAAAA==')
  check_status "GET /v1/uploads/{id}/resume returns 200" "$resp" "200"

  # A second initiate for the same path supersedes the first session instead of
  # failing: exactly one session stays live, under a new id.
  v1_init2=$(jq -nc --arg p "/$V1_UP_PATH" '{path:$p,total_size:2097152,part_checksums:["AAAAAA=="]}')
  resp=$(request POST "$BASE/v1/uploads/initiate" --auth "$API_KEY" --hdr 'Content-Type: application/json' --data "$v1_init2")
  check_status "re-initiating the same path returns 202 (supersedes)" "$resp" "202"
  v1_id2=$(json_body "$resp" | jq -r '.upload_id // empty')
  check_cmd "re-initiate allocates a different upload_id" bash -c 'test "$1" != "$2"' _ "$v1_id" "$v1_id2"

  resp=$(request GET "$BASE/v1/uploads?path=$(url_encode "/$V1_UP_PATH")" --auth "$API_KEY")
  v1_listing2=$(json_body "$resp")
  check_json "only one session stays live after re-initiate" "$v1_listing2" '.uploads|length' "1"
  check_json "the live session is the re-initiated one" "$v1_listing2" '.uploads[0].upload_id' "$v1_id2"

  resp=$(request DELETE "$BASE/v1/uploads/$v1_id2" --auth "$API_KEY")
  check_status "DELETE /v1/uploads/{id} aborts the session" "$resp" "200"
  resp=$(request GET "$BASE/v1/uploads?path=$(url_encode "/$V1_UP_PATH")" --auth "$API_KEY")
  check_json "the aborted session is no longer listed" "$(json_body "$resp")" '(.uploads|length)' "0"
  resp=$(request POST "$BASE/v1/uploads/$v1_id2/resume" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data "$V1_CHECKSUMS")
  check_status "resuming an aborted session returns 409" "$resp" "409"

  V2_PATH="${CONTRACT_DIR}/v2-roundtrip.bin"
  V2_LOCAL="/tmp/drive9-e2e-v2-${TS}.bin"
  V2_DOWNLOADED="/tmp/drive9-e2e-v2-${TS}.download.bin"
  dd if=/dev/zero of="$V2_LOCAL" bs=1M count=1 status=none
  V2_BYTES=$(wc -c < "$V2_LOCAL" | tr -d ' ')

  resp=$(request POST "$BASE/v2/uploads/initiate" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg p "/$V2_PATH" --argjson s "$V2_BYTES" '{path:$p,total_size:$s}')")
  check_status "POST /v2/uploads/initiate returns 202" "$resp" "202"
  v2_plan=$(json_body "$resp")
  v2_id=$(printf '%s' "$v2_plan" | jq -r '.upload_id // empty')
  check_cmd "V2 initiate returns an upload_id" test -n "$v2_id"
  check_json "V2 initiate reports a positive part_size" "$v2_plan" '(.part_size > 0)' "true"
  check_json "V2 initiate reports one part for a 1MiB object" "$v2_plan" '.total_parts' "1"

  resp=$(request POST "$BASE/v2/uploads/$v2_id/presign" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data '{"part_number":1}')
  check_status "POST /v2/uploads/{id}/presign returns 200" "$resp" "200"
  v2_part_file=$(mktemp)
  json_body "$resp" > "$v2_part_file"
  check_cmd "presign returns a PUT url" test -n "$(jq -r '.url // empty' "$v2_part_file")"
  check_json "presign returns the part number" "$(cat "$v2_part_file")" '.number' "1"

  resp=$(request POST "$BASE/v2/uploads/$v2_id/presign-batch" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data '{"parts":[{"part_number":1}]}')
  check_status "POST /v2/uploads/{id}/presign-batch returns 200" "$resp" "200"
  check_json "presign-batch returns one signed part" "$(json_body "$resp")" '(.parts|length)' "1"
  check_json "presign-batch returns the part number" "$(json_body "$resp")" '.parts[0].number' "1"

  v2_etag=""
  v2_put_rc=0
  v2_etag=$(put_presigned_part "$v2_part_file" "$V2_LOCAL") || v2_put_rc=$?
  check_eq "the presigned part PUT exits successfully" "$v2_put_rc" "0"
  check_cmd "the presigned part PUT returns an ETag" test -n "$v2_etag"

  resp=$(request POST "$BASE/v2/uploads/$v2_id/complete" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg e "$v2_etag" '{parts:[{number:1,etag:$e}]}')")
  check_status "POST /v2/uploads/{id}/complete returns 200" "$resp" "200"
  v2_done=$(json_body "$resp")
  check_json "V2 complete reports status completed" "$v2_done" '.status' "completed"
  if [ "$CAP_COMPLETION_IDENTITY" = "true" ]; then
    check_json "V2 complete echoes a positive revision" "$v2_done" '(.revision > 0)' "true"
    check_cmd "V2 complete echoes the published resource_id" test -n "$(printf '%s' "$v2_done" | jq -r '.resource_id // empty')"
  else
    skip_check "V2 complete echoes a positive revision (server does not advertise completion_identity_v1)"
    skip_check "V2 complete echoes the published resource_id (server does not advertise completion_identity_v1)"
  fi

  resp=$(request GET "$BASE/v1/fs/$V2_PATH?stat" --auth "$API_KEY")
  check_json "the V2-uploaded object reports the uploaded size" "$(json_body "$resp")" '.size' "$V2_BYTES"
  v2_download_code=$(curl -sS -L -o "$V2_DOWNLOADED" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$V2_PATH")
  check_eq "downloading the V2-uploaded object returns 200" "$v2_download_code" "200"
  check_eq "the V2 round-trip sha256 matches the source" \
    "$(sha256sum "$V2_DOWNLOADED" | cut -d' ' -f1)" "$(sha256sum "$V2_LOCAL" | cut -d' ' -f1)"

  V2_ABORT_PATH="${CONTRACT_DIR}/v2-abort.bin"
  resp=$(request POST "$BASE/v2/uploads/initiate" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg p "/$V2_ABORT_PATH" --argjson s "$V2_BYTES" '{path:$p,total_size:$s}')")
  v2_abort_id=$(json_body "$resp" | jq -r '.upload_id // empty')
  check_status "V2 initiate for the abort case returns 202" "$resp" "202"
  check_cmd "V2 initiate for the abort case returns an upload_id" test -n "$v2_abort_id"
  resp=$(request POST "$BASE/v2/uploads/$v2_abort_id/abort" --auth "$API_KEY")
  check_status "POST /v2/uploads/{id}/abort returns 200" "$resp" "200"
  resp=$(request POST "$BASE/v2/uploads/$v2_abort_id/complete" --auth "$API_KEY" \
    --hdr 'Content-Type: application/json' --data '{"parts":[{"number":1,"etag":"\"aborted\""}]}')
  check_status "completing an aborted V2 session returns 409 (upload is not active)" "$resp" "409"

  rm -f "$v2_part_file" "$V2_LOCAL" "$V2_DOWNLOADED"
else
  info "upload lifecycle checks skipped (RUN_UPLOAD_LIFECYCLE_CHECKS=$RUN_UPLOAD_LIFECYCLE_CHECKS)"
  skip_check "upload lifecycle: V1 list/resume/abort + V2 round-trip (RUN_UPLOAD_LIFECYCLE_CHECKS=0)"
fi

if [ "$RUN_MOUNT_LEASE_CHECKS" = "1" ]; then
  step "24" "Mount lease lifecycle"
  MOUNT_ID="e2e-${TS}"
  resp=$(request POST "$BASE/v1/mounts" --auth "$API_KEY" --hdr 'Content-Type: application/json' \
    --data "$(jq -nc --arg id "$MOUNT_ID" '{mount_id:$id,mount_point:"/tmp/drive9-e2e",host:"e2e-runner",pid:4242,mode:"read_write",ttl_seconds:600}')")
  if [ "$(http_code "$resp")" = "404" ]; then
    # /v1/mounts is newer than the oldest supported server build. A route the
    # server does not serve is a SKIP, never a FAIL.
    info "server does not serve /v1/mounts; mount lease checks skipped"
    skip_check "POST /v1/mounts registers a lease (route absent)"
    skip_check "GET /v1/mounts/{id} returns the lease (route absent)"
    skip_check "POST /v1/mounts/{id}/heartbeat extends the lease (route absent)"
    skip_check "GET /v1/mounts lists the lease (route absent)"
    skip_check "a heartbeat extends expires_at (route absent)"
    skip_check "DELETE /v1/mounts/{id} releases the lease (route absent)"
    skip_check "the released lease is gone (route absent)"
    skip_check "releasing an unknown lease is idempotent (route absent)"
  else
    check_status "POST /v1/mounts registers a lease" "$resp" "200"
    mount_body=$(json_body "$resp")
    check_json "the lease echoes mount_id" "$mount_body" '.mount_id' "$MOUNT_ID"
    check_json "the lease echoes mode" "$mount_body" '.mode' "read_write"
    check_cmd "the lease carries a tenant_id" test -n "$(printf '%s' "$mount_body" | jq -r '.tenant_id // empty')"
    check_cmd "the lease carries an expires_at" test -n "$(printf '%s' "$mount_body" | jq -r '.expires_at // empty')"
    check_json "the lease advertises a positive heartbeat interval" "$mount_body" '(.heartbeat_interval_seconds > 0)' "true"
    mount_expires=$(printf '%s' "$mount_body" | jq -r '.expires_at')

    resp=$(request GET "$BASE/v1/mounts/$MOUNT_ID" --auth "$API_KEY")
    check_status "GET /v1/mounts/{id} returns the lease" "$resp" "200"
    check_json "the fetched lease echoes mount_id" "$(json_body "$resp")" '.mount_id' "$MOUNT_ID"

    resp=$(request POST "$BASE/v1/mounts/$MOUNT_ID/heartbeat" --auth "$API_KEY" \
      --hdr 'Content-Type: application/json' --data '{"ttl_seconds":900}')
    check_status "POST /v1/mounts/{id}/heartbeat extends the lease" "$resp" "200"
    check_cmd "a heartbeat extends expires_at" bash -c 'test "$1" != "$2"' _ \
      "$mount_expires" "$(json_body "$resp" | jq -r '.expires_at')"

    resp=$(request GET "$BASE/v1/mounts" --auth "$API_KEY")
    check_status "GET /v1/mounts lists the lease" "$resp" "200"
    check_json "the listing includes the active lease" "$(json_body "$resp")" \
      "[.mounts[]|select(.mount_id==\"$MOUNT_ID\")]|length" "1"

    resp=$(request DELETE "$BASE/v1/mounts/$MOUNT_ID" --auth "$API_KEY")
    check_status "DELETE /v1/mounts/{id} releases the lease" "$resp" "204"
    resp=$(request GET "$BASE/v1/mounts/$MOUNT_ID" --auth "$API_KEY")
    check_status "the released lease is gone" "$resp" "404"
    resp=$(request DELETE "$BASE/v1/mounts/$MOUNT_ID" --auth "$API_KEY")
    check_status "releasing an unknown lease is idempotent" "$resp" "204"
  fi
else
  info "mount lease checks skipped (RUN_MOUNT_LEASE_CHECKS=$RUN_MOUNT_LEASE_CHECKS)"
  skip_check "mount lease lifecycle (RUN_MOUNT_LEASE_CHECKS=0)"
fi

rm -f "$IMAGE_LOCAL"

if [ -n "$API_KEY" ] && [ -n "${DRIVE9_API_KEY:-}" ]; then
  step "25" "Cleanup test tree (existing tenant mode)"
  resp=$(curl_body_code DELETE "$BASE/v1/fs/$ROOT_DIR?recursive" "$API_KEY")
  code=$(http_code "$resp")
  check_eq "DELETE /v1/fs/$ROOT_DIR?recursive returns 200" "$code" "200"
fi

echo
echo "========================================================"
echo "  RESULTS: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
echo "  Base URL : $BASE"
echo "  Finished : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$FAIL" -eq 0 ]; then
  if [ "$SKIP" -eq 0 ]; then
    echo -e "  ${GREEN}All tests passed.${RESET}"
  else
    echo -e "  ${GREEN}All executed tests passed.${RESET}"
  fi
else
  echo -e "  ${RED}$FAIL test(s) failed.${RESET}"
fi
echo "========================================================"

exit "$FAIL"
