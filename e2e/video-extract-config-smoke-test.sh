#!/usr/bin/env bash
# Live tenant video-extract config and worker smoke test.
#
# Manual-only: provisions and deletes a real tenant, uploads a caller-provided
# MP4, and calls a billable OpenAI-compatible vision provider. Missing
# configuration skips the suite before any HTTP request is made.

set -euo pipefail

required_env=(
  DRIVE9_BASE
  DRIVE9_TIDBCLOUD_PUBLIC_KEY
  DRIVE9_TIDBCLOUD_PRIVATE_KEY
  DRIVE9_E2E_VIDEO_EXTRACT_API_BASE
  DRIVE9_E2E_VIDEO_EXTRACT_API_KEY
  DRIVE9_E2E_VIDEO_EXTRACT_MODEL
  DRIVE9_E2E_VIDEO_FIXTURE_PATH
  DRIVE9_E2E_VIDEO_EXPECTED_MARKER
)
missing_env=()
for name in "${required_env[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing_env+=("$name")
  fi
done
if [ "${#missing_env[@]}" -gt 0 ]; then
  printf 'SKIP: video extract config smoke requires: %s\n' "${missing_env[*]}"
  exit 0
fi

BASE="${DRIVE9_BASE}"
BASE="${BASE%/}"
VIDEO_FIXTURE="$DRIVE9_E2E_VIDEO_FIXTURE_PATH"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
VIDEO_EXTRACT_TIMEOUT_S="${VIDEO_EXTRACT_TIMEOUT_S:-600}"
VIDEO_EXTRACT_INTERVAL_S="${VIDEO_EXTRACT_INTERVAL_S:-5}"
DISABLED_EXTRACT_WAIT_S="${DISABLED_EXTRACT_WAIT_S:-30}"
CURL_CONNECT_TIMEOUT_S="${CURL_CONNECT_TIMEOUT_S:-10}"
CURL_MAX_TIME_S="${CURL_MAX_TIME_S:-180}"

ROOT_PATH="video-extract-e2e"
VIDEO_PATH="$ROOT_PATH/fixture.mp4"
DISABLED_VIDEO_PATH="$ROOT_PATH/disabled.mp4"
MARKER="$DRIVE9_E2E_VIDEO_EXPECTED_MARKER"

TENANT_ID=""
OWNER_API_KEY=""
CONFIG_ENABLED=0
TREE_CREATED=0

die() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

[[ "$BASE" == https://* ]] || die "DRIVE9_BASE must use https:// for hosted video extract smoke"

for command in curl jq python3; do
  command -v "$command" >/dev/null || die "$command is required"
done
[ -s "$VIDEO_FIXTURE" ] || die "video fixture not found or empty: $VIDEO_FIXTURE"

curl_response() {
  local method="$1"
  local url="$2"
  local auth="$3"
  shift 3

  local body_file
  body_file="$(mktemp)"
  local args=(
    --connect-timeout "$CURL_CONNECT_TIMEOUT_S"
    --max-time "$CURL_MAX_TIME_S"
    -sS -o "$body_file" -w '%{http_code}' -X "$method"
  )
  case "$auth" in
    control-plane)
      args+=(
        -H "X-TiDBCloud-Public-Key: $DRIVE9_TIDBCLOUD_PUBLIC_KEY"
        -H "X-TiDBCloud-Private-Key: $DRIVE9_TIDBCLOUD_PRIVATE_KEY"
      )
      ;;
    owner)
      args+=(-H "Authorization: Bearer ${OWNER_API_KEY}")
      ;;
    *)
      rm -f "$body_file"
      die "unknown authentication mode: $auth"
      ;;
  esac

  local code
  if ! code="$(curl "${args[@]}" "$@" "$url")"; then
    rm -f "$body_file"
    return 1
  fi
  cat "$body_file"
  printf '\n__HTTP__%s\n' "$code"
  rm -f "$body_file"
}

http_code() {
  printf '%s' "$1" | awk -F'__HTTP__' 'NF > 1 {print $2}' | tr -d '\n'
}

json_body() {
  printf '%s' "$1" | sed '/__HTTP__/d'
}

http_failure() {
  local action="$1"
  local code="$2"
  local body="$3"
  local reason
  reason="$(printf '%s' "$body" | jq -r '.error // empty' 2>/dev/null || true)"
  if [ -n "$reason" ]; then
    die "$action returned HTTP $code: $reason"
  fi
  die "$action returned HTTP $code"
}

upload_video_fixture() {
  local remote_path="$1"
  local response code body upload_id plan_file complete_response complete_code complete_body

  response="$(curl_response PUT "$BASE/v1/fs/$remote_path" owner \
    -H 'Content-Type: video/mp4' \
    -H "X-Dat9-Part-Checksums: $VIDEO_PART_CHECKSUMS" \
    --data-binary "@$VIDEO_FIXTURE")" || die "video upload request failed"
  code="$(http_code "$response")"
  body="$(json_body "$response")"
  if [ "$code" = "200" ]; then
    return 0
  fi
  [ "$code" = "202" ] || http_failure "video upload" "$code" "$body"

  upload_id="$(printf '%s' "$body" | jq -r '.upload_id // empty')"
  [ -n "$upload_id" ] || die "video upload response omitted upload_id"
  plan_file="$(mktemp)"
  printf '%s' "$body" >"$plan_file"
  if ! python3 - "$plan_file" "$VIDEO_FIXTURE" <<'PY'
import json
import sys
import urllib.request

plan_path, file_path = sys.argv[1], sys.argv[2]
with open(plan_path, "r", encoding="utf-8") as f:
    plan = json.load(f)

parts = plan.get("parts", [])
if not parts:
    raise SystemExit("video upload plan has no parts")
with open(file_path, "rb") as data_file:
    for idx, part in enumerate(parts, 1):
        size = int(part["size"])
        data = data_file.read(size)
        if len(data) != size:
            raise SystemExit(f"short read for part {idx}: got {len(data)} expected {size}")
        req = urllib.request.Request(part["url"], data=data, method="PUT")
        req.add_header("Content-Length", str(size))
        for key, value in (part.get("headers") or {}).items():
            req.add_header(key, value)
        if part.get("checksum_crc32c"):
            req.add_header("x-amz-checksum-crc32c", part["checksum_crc32c"])
        elif part.get("checksum_sha256"):
            req.add_header("x-amz-checksum-sha256", part["checksum_sha256"])
        with urllib.request.urlopen(req, timeout=300) as resp:
            if getattr(resp, "status", 200) >= 300:
                raise SystemExit(f"part {idx} failed: HTTP {resp.status}")
PY
  then
    rm -f "$plan_file"
    die "video multipart part upload failed"
  fi
  rm -f "$plan_file"

  complete_response="$(curl_response POST "$BASE/v1/uploads/$upload_id/complete" owner)" || die "video upload complete request failed"
  complete_code="$(http_code "$complete_response")"
  complete_body="$(json_body "$complete_response")"
  [ "$complete_code" = "200" ] || http_failure "video upload complete" "$complete_code" "$complete_body"
  [ "$(printf '%s' "$complete_body" | jq -r '.status // empty')" = "ok" ] || die "video upload complete response was unexpected"
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT
  set +e

  if [ "$CONFIG_ENABLED" -eq 1 ] && [ -n "$TENANT_ID" ]; then
    local disable_response disable_code
    disable_response="$(
      printf '%s' '{"enabled":false}' |
        curl_response PUT "$BASE/v1/admin/tenants/$TENANT_ID/extract-config/video" control-plane \
          -H 'Content-Type: application/json' --data-binary @-
    )"
    disable_code="$(http_code "$disable_response")"
    if [ "$disable_code" != "200" ]; then
      printf 'CLEANUP FAIL: disabling video extract config returned HTTP %s\n' "$disable_code" >&2
      cleanup_failed=1
    fi
  fi

  if [ "$TREE_CREATED" -eq 1 ] && [ -n "$OWNER_API_KEY" ]; then
    local tree_response tree_code
    tree_response="$(curl_response DELETE "$BASE/v1/fs/$ROOT_PATH?recursive" owner)"
    tree_code="$(http_code "$tree_response")"
    if [ "$tree_code" != "200" ]; then
      printf 'CLEANUP FAIL: deleting test tree returned HTTP %s\n' "$tree_code" >&2
      cleanup_failed=1
    fi
  fi

  if [ -n "$TENANT_ID" ]; then
    local tenant_response tenant_code
    tenant_response="$(curl_response DELETE "$BASE/v1/admin/tenants/$TENANT_ID" control-plane)"
    tenant_code="$(http_code "$tenant_response")"
    if [ "$tenant_code" != "202" ] && [ "$tenant_code" != "200" ]; then
      printf 'CLEANUP FAIL: deleting tenant %s returned HTTP %s\n' "$TENANT_ID" "$tenant_code" >&2
      cleanup_failed=1
    fi
  fi

  if [ "$status" -eq 0 ] && [ "$cleanup_failed" -ne 0 ]; then
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT

printf '=== tenant video extract config smoke ===\n'
printf 'Base URL: %s\n' "$BASE"
printf 'Video fixture: %s\n' "$VIDEO_FIXTURE"

provision_response="$(curl_response POST "$BASE/v1/provision" control-plane)" || die "tenant provision request failed"
provision_code="$(http_code "$provision_response")"
provision_body="$(json_body "$provision_response")"
[ "$provision_code" = "202" ] || http_failure "tenant provision" "$provision_code" "$provision_body"
TENANT_ID="$(printf '%s' "$provision_body" | jq -r '.tenant_id // empty')"
OWNER_API_KEY="$(printf '%s' "$provision_body" | jq -r '.api_key // empty')"
[ -n "$TENANT_ID" ] || die "tenant provision response omitted tenant_id"
[ -n "$OWNER_API_KEY" ] || die "tenant provision response omitted api_key"
printf 'PASS: provisioned disposable tenant %s\n' "$TENANT_ID"

deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
last_status=""
while :; do
  status_response="$(curl_response GET "$BASE/v1/status" owner)" || die "tenant status request failed"
  status_code="$(http_code "$status_response")"
  status_body="$(json_body "$status_response")"
  if [ "$status_code" = "200" ]; then
    last_status="$(printf '%s' "$status_body" | jq -r '.status // empty')"
    [ "$last_status" = "active" ] && break
  fi
  [ "$(date +%s)" -lt "$deadline" ] || die "tenant did not become active within ${POLL_TIMEOUT_S}s (last status: ${last_status:-unknown})"
  sleep "$POLL_INTERVAL_S"
done
printf 'PASS: tenant became active\n'

config_url="$BASE/v1/admin/tenants/$TENANT_ID/extract-config/video"
config_response="$(
  jq -nc '{
    enabled: true,
    api_base: env.DRIVE9_E2E_VIDEO_EXTRACT_API_BASE,
    api_key: env.DRIVE9_E2E_VIDEO_EXTRACT_API_KEY,
    model: env.DRIVE9_E2E_VIDEO_EXTRACT_MODEL,
    protocol: "openai",
    prompt: "Describe the actual visible content across the supplied video frames in one concise English sentence. Do not invent details."
  }' |
    curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-
)" || die "video extract config PUT request failed"
config_code="$(http_code "$config_response")"
config_body="$(json_body "$config_response")"
[ "$config_code" = "200" ] || http_failure "video extract config PUT/provider validation" "$config_code" "$config_body"
CONFIG_ENABLED=1
if ! printf '%s' "$config_body" | jq -e '
  .enabled == true and .source == "custom" and .protocol == "openai" and
  .api_base == env.DRIVE9_E2E_VIDEO_EXTRACT_API_BASE and
  .model == env.DRIVE9_E2E_VIDEO_EXTRACT_MODEL and
  (.api_key | type == "string" and contains("*") and . != env.DRIVE9_E2E_VIDEO_EXTRACT_API_KEY)
' >/dev/null; then
  die "video extract config response did not return the expected masked custom config"
fi
printf 'PASS: provider validated and custom video config is masked\n'

mkdir_response="$(curl_response POST "$BASE/v1/fs/$ROOT_PATH?mkdir" owner)" || die "test directory creation failed"
mkdir_code="$(http_code "$mkdir_response")"
mkdir_body="$(json_body "$mkdir_response")"
[ "$mkdir_code" = "200" ] || http_failure "test directory creation" "$mkdir_code" "$mkdir_body"
TREE_CREATED=1

VIDEO_PART_CHECKSUMS="$(python3 - "$VIDEO_FIXTURE" <<'PY'
import base64
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
        table.append(crc)
    return table

table = _crc32c_table()
part_size = 8 * 1024 * 1024
checksums = []
with open(sys.argv[1], "rb") as f:
    while chunk := f.read(part_size):
        crc = 0xFFFFFFFF
        for byte in chunk:
            crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
        crc ^= 0xFFFFFFFF
        checksums.append(base64.b64encode(struct.pack(">I", crc)).decode())
print(",".join(checksums))
PY
)" || die "video checksum calculation failed"

upload_video_fixture "$VIDEO_PATH"
printf 'PASS: uploaded video fixture\n'

extract_deadline=$(( $(date +%s) + VIDEO_EXTRACT_TIMEOUT_S ))
semantic_text=""
while :; do
  stat_response="$(curl_response GET "$BASE/v1/fs/$VIDEO_PATH?stat" owner)" || die "video stat request failed"
  stat_code="$(http_code "$stat_response")"
  stat_body="$(json_body "$stat_response")"
  if [ "$stat_code" = "200" ]; then
    semantic_text="$(printf '%s' "$stat_body" | jq -r '.semantic_text // empty')"
    if [[ "$semantic_text" == *"$MARKER"* ]] && [ "${#semantic_text}" -gt "${#MARKER}" ]; then
      break
    fi
  fi
  [ "$(date +%s)" -lt "$extract_deadline" ] || die "video semantic text did not appear within ${VIDEO_EXTRACT_TIMEOUT_S}s"
  sleep "$VIDEO_EXTRACT_INTERVAL_S"
done
printf 'PASS: video worker wrote model-derived semantic text\n'
printf '%s\n%s\n' '--- video semantic_text ---' "$semantic_text"

disable_response="$(printf '%s' '{"enabled":false}' | curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-)" || die "video extract config disable request failed"
disable_code="$(http_code "$disable_response")"
disable_body="$(json_body "$disable_response")"
[ "$disable_code" = "200" ] || http_failure "video extract config disable" "$disable_code" "$disable_body"
if ! printf '%s' "$disable_body" | jq -e '.enabled == false and .source == "custom"' >/dev/null; then
  die "video extract config disable response was unexpected"
fi
CONFIG_ENABLED=0

upload_video_fixture "$DISABLED_VIDEO_PATH"

disabled_deadline=$(( $(date +%s) + DISABLED_EXTRACT_WAIT_S ))
while :; do
  disabled_stat_response="$(curl_response GET "$BASE/v1/fs/$DISABLED_VIDEO_PATH?stat" owner)" || die "disabled video stat request failed"
  disabled_stat_code="$(http_code "$disabled_stat_response")"
  disabled_stat_body="$(json_body "$disabled_stat_response")"
  [ "$disabled_stat_code" = "200" ] || http_failure "disabled video stat" "$disabled_stat_code" "$disabled_stat_body"
  [ -z "$(printf '%s' "$disabled_stat_body" | jq -r '.semantic_text // empty')" ] || die "disabled video unexpectedly received extracted text"
  [ "$(date +%s)" -ge "$disabled_deadline" ] && break
  sleep "$VIDEO_EXTRACT_INTERVAL_S"
done
printf 'PASS: disabled video config produced no extracted text for %ss\n' "$DISABLED_EXTRACT_WAIT_S"
