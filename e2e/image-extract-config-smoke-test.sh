#!/usr/bin/env bash
# Live tenant image-extract config and generated-tag smoke test.
#
# Manual-only (not part of smoke-all / local-e2e): provisions and deletes a
# real tenant and calls a real OpenAI-compatible vision provider. Missing
# configuration skips the entire suite before any HTTP request is made.
#
#   export DRIVE9_BASE="https://..."
#   export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
#   export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
#   export DRIVE9_E2E_IMAGE_EXTRACT_API_BASE="https://..."
#   export DRIVE9_E2E_IMAGE_EXTRACT_API_KEY="..."
#   export DRIVE9_E2E_IMAGE_EXTRACT_MODEL="..."
#   bash e2e/image-extract-config-smoke-test.sh

set -euo pipefail

required_env=(
  DRIVE9_BASE
  DRIVE9_TIDBCLOUD_PUBLIC_KEY
  DRIVE9_TIDBCLOUD_PRIVATE_KEY
  DRIVE9_E2E_IMAGE_EXTRACT_API_BASE
  DRIVE9_E2E_IMAGE_EXTRACT_API_KEY
  DRIVE9_E2E_IMAGE_EXTRACT_MODEL
)
missing_env=()
for name in "${required_env[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing_env+=("$name")
  fi
done
if [ "${#missing_env[@]}" -gt 0 ]; then
  printf 'SKIP: image extract config smoke requires: %s\n' "${missing_env[*]}"
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE%/}"
IMAGE_FIXTURE="${DRIVE9_IMAGE_FIXTURE_PATH:-$SCRIPT_DIR/fixtures/cat03.jpg}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
IMAGE_EXTRACT_TIMEOUT_S="${IMAGE_EXTRACT_TIMEOUT_S:-180}"
IMAGE_EXTRACT_INTERVAL_S="${IMAGE_EXTRACT_INTERVAL_S:-3}"
DISABLED_EXTRACT_WAIT_S="${DISABLED_EXTRACT_WAIT_S:-30}"
CURL_CONNECT_TIMEOUT_S="${CURL_CONNECT_TIMEOUT_S:-10}"
CURL_MAX_TIME_S="${CURL_MAX_TIME_S:-120}"

ROOT_PATH="image-extract-e2e"
IMAGE_PATH="$ROOT_PATH/fixture.jpg"
DISABLED_IMAGE_PATH="$ROOT_PATH/disabled.jpg"
MARKER_KEY="e2e_marker"
UNREACHABLE_API_BASE="${DRIVE9_E2E_UNREACHABLE_API_BASE:-https://example.com:81/v1}"

TENANT_ID=""
OWNER_API_KEY=""
CONFIG_ENABLED=0
TREE_CREATED=0

die() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

[[ "$BASE" == https://* ]] || die "DRIVE9_BASE must use https:// for hosted image extract smoke"

for command in curl jq; do
  command -v "$command" >/dev/null || die "$command is required"
done
[ -s "$IMAGE_FIXTURE" ] || die "image fixture not found or empty: $IMAGE_FIXTURE"

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

assert_config_unchanged() {
  local label="$1"
  local response code body canonical
  response="$(curl_response GET "$config_url" control-plane)" || die "$label config verification GET failed"
  code="$(http_code "$response")"
  body="$(json_body "$response")"
  [ "$code" = "200" ] || http_failure "$label config verification GET" "$code" "$body"
  canonical="$(printf '%s' "$body" | jq -S -c .)" || die "$label config verification returned invalid JSON"
  [ "$canonical" = "$before_canonical" ] || die "$label candidate changed the persisted config"
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
        curl_response PUT "$BASE/v1/admin/tenants/$TENANT_ID/extract-config/image" control-plane \
          -H 'Content-Type: application/json' --data-binary @-
    )"
    disable_code="$(http_code "$disable_response")"
    if [ "$disable_code" != "200" ]; then
      printf 'CLEANUP FAIL: disabling image extract config returned HTTP %s\n' "$disable_code" >&2
      cleanup_failed=1
    fi
  fi

  if [ "$TREE_CREATED" -eq 1 ] && [ -n "$OWNER_API_KEY" ]; then
    local file_response file_code
    file_response="$(curl_response DELETE "$BASE/v1/fs/$ROOT_PATH?recursive" owner)"
    file_code="$(http_code "$file_response")"
    if [ "$file_code" != "200" ]; then
      printf 'CLEANUP FAIL: deleting the test file tree returned HTTP %s\n' "$file_code" >&2
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

printf '=== tenant image extract config smoke ===\n'
printf 'Base URL: %s\n' "$BASE"

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
    if [ "$last_status" = "active" ]; then
      break
    fi
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    die "tenant did not become active within ${POLL_TIMEOUT_S}s (last status: ${last_status:-unknown})"
  fi
  sleep "$POLL_INTERVAL_S"
done
printf 'PASS: tenant became active\n'

config_url="$BASE/v1/admin/tenants/$TENANT_ID/extract-config/image"
before_response="$(curl_response GET "$config_url" control-plane)" || die "initial extract config request failed"
before_code="$(http_code "$before_response")"
before_body="$(json_body "$before_response")"
[ "$before_code" = "200" ] || http_failure "initial extract config GET" "$before_code" "$before_body"
before_source="$(printf '%s' "$before_body" | jq -r '.source // empty')"
case "$before_source" in
  none) ;;
  default) die "tenant has a process default image provider; hosted image-extract smoke requires source=none to prove tenant config usage" ;;
  *) die "new tenant unexpectedly has extract config source ${before_source:-unknown}" ;;
esac
before_canonical="$(printf '%s' "$before_body" | jq -S -c .)" || die "initial extract config GET returned invalid JSON"

invalid_key="drive9-e2e-invalid-provider-key"
invalid_key_response="$(
  jq -nc --arg api_base "$DRIVE9_E2E_IMAGE_EXTRACT_API_BASE" --arg api_key "$invalid_key" \
    --arg model "$DRIVE9_E2E_IMAGE_EXTRACT_MODEL" \
    '{enabled:true, api_base:$api_base, api_key:$api_key, model:$model}' |
    curl_response PUT "$config_url" control-plane \
      -H 'Content-Type: application/json' --data-binary @-
)" || die "invalid API key config PUT request failed"
invalid_key_code="$(http_code "$invalid_key_response")"
invalid_key_body="$(json_body "$invalid_key_response")"
[ "$invalid_key_code" = "400" ] || http_failure "invalid API key config PUT (expected HTTP 400)" "$invalid_key_code" "$invalid_key_body"
invalid_key_error="$(printf '%s' "$invalid_key_body" | jq -r '.error // empty')"
if [[ "$invalid_key_error" != *"image extraction provider validation failed: authentication rejected"* ]]; then
  die "invalid API key config PUT returned an unexpected error"
fi
[[ "$invalid_key_error" != *"$invalid_key"* ]] || die "invalid API key leaked in config PUT error"
assert_config_unchanged "invalid API key"
printf 'PASS: invalid provider API key was rejected without persisting config\n'

unreachable_response="$(
  jq -nc --arg api_base "$UNREACHABLE_API_BASE" --arg api_key "$DRIVE9_E2E_IMAGE_EXTRACT_API_KEY" \
    --arg model "$DRIVE9_E2E_IMAGE_EXTRACT_MODEL" \
    '{enabled:true, api_base:$api_base, api_key:$api_key, model:$model}' |
    curl_response PUT "$config_url" control-plane \
      -H 'Content-Type: application/json' --data-binary @-
)" || die "unreachable API base config PUT request failed"
unreachable_code="$(http_code "$unreachable_response")"
unreachable_body="$(json_body "$unreachable_response")"
if [ "$unreachable_code" != "502" ] && [ "$unreachable_code" != "504" ]; then
  http_failure "unreachable API base config PUT (expected HTTP 502 or 504)" "$unreachable_code" "$unreachable_body"
fi
unreachable_error="$(printf '%s' "$unreachable_body" | jq -r '.error // empty')"
if [[ "$unreachable_error" != "image extraction provider validation failed: "* ]]; then
  die "unreachable API base config PUT returned an unexpected error"
fi
assert_config_unchanged "unreachable API base"
printf 'PASS: unreachable provider API base was rejected without persisting config\n'

config_response="$(
  jq -nc '{
    enabled: true,
    api_base: env.DRIVE9_E2E_IMAGE_EXTRACT_API_BASE,
    api_key: env.DRIVE9_E2E_IMAGE_EXTRACT_API_KEY,
    model: env.DRIVE9_E2E_IMAGE_EXTRACT_MODEL,
    prompt: "Analyze the image and return only one JSON object with a short caption_en and an attributes object. The attributes object MUST contain the required key e2e_marker. Set its value to one short lowercase English noun that best describes the primary visible content of the image. Determine the value from the image itself; do not use a predetermined value. A response without attributes.e2e_marker is invalid."
  }' |
    curl_response PUT "$config_url" control-plane \
      -H 'Content-Type: application/json' --data-binary @-
)" || die "extract config PUT request failed"
config_code="$(http_code "$config_response")"
config_body="$(json_body "$config_response")"
[ "$config_code" = "200" ] || http_failure "extract config PUT/provider validation" "$config_code" "$config_body"
CONFIG_ENABLED=1

if ! printf '%s' "$config_body" | jq -e '
  .enabled == true and
  .source == "custom" and
  .api_base == env.DRIVE9_E2E_IMAGE_EXTRACT_API_BASE and
  .model == env.DRIVE9_E2E_IMAGE_EXTRACT_MODEL and
  (.api_key | type == "string" and contains("*") and . != env.DRIVE9_E2E_IMAGE_EXTRACT_API_KEY)
' >/dev/null; then
  die "extract config PUT response did not return the expected masked custom config"
fi

get_response="$(curl_response GET "$config_url" control-plane)" || die "extract config verification GET failed"
get_code="$(http_code "$get_response")"
get_body="$(json_body "$get_response")"
[ "$get_code" = "200" ] || http_failure "extract config verification GET" "$get_code" "$get_body"
if ! printf '%s' "$get_body" | jq -e '
  .enabled == true and
  .source == "custom" and
  .api_base == env.DRIVE9_E2E_IMAGE_EXTRACT_API_BASE and
  .model == env.DRIVE9_E2E_IMAGE_EXTRACT_MODEL and
  (.api_key | type == "string" and contains("*") and . != env.DRIVE9_E2E_IMAGE_EXTRACT_API_KEY)
' >/dev/null; then
  die "extract config GET did not return the expected masked custom config"
fi
printf 'PASS: provider validated and custom config is masked\n'

mkdir_response="$(curl_response POST "$BASE/v1/fs/$ROOT_PATH?mkdir" owner)" || die "test directory creation failed"
mkdir_code="$(http_code "$mkdir_response")"
mkdir_body="$(json_body "$mkdir_response")"
[ "$mkdir_code" = "200" ] || http_failure "test directory creation" "$mkdir_code" "$mkdir_body"
TREE_CREATED=1

upload_response="$(curl_response PUT "$BASE/v1/fs/$IMAGE_PATH" owner \
  -H 'Content-Type: image/jpeg' --data-binary "@$IMAGE_FIXTURE")" || die "image upload request failed"
upload_code="$(http_code "$upload_response")"
upload_body="$(json_body "$upload_response")"
[ "$upload_code" = "200" ] || http_failure "image upload" "$upload_code" "$upload_body"
printf 'PASS: uploaded image fixture\n'

extract_deadline=$(( $(date +%s) + IMAGE_EXTRACT_TIMEOUT_S ))
marker=""
while :; do
  stat_response="$(curl_response GET "$BASE/v1/fs/$IMAGE_PATH?stat" owner)" || die "image stat request failed"
  stat_code="$(http_code "$stat_response")"
  stat_body="$(json_body "$stat_response")"
  if [ "$stat_code" = "200" ]; then
    marker="$(printf '%s' "$stat_body" | jq -r --arg key "$MARKER_KEY" '.tags[$key] // empty')"
    if [[ "$marker" =~ ^[a-z][a-z0-9_-]{0,31}$ ]]; then
      break
    fi
  fi
  if [ "$(date +%s)" -ge "$extract_deadline" ]; then
    die "generated tag did not appear within ${IMAGE_EXTRACT_TIMEOUT_S}s"
  fi
  sleep "$IMAGE_EXTRACT_INTERVAL_S"
done
printf 'PASS: generated marker tag appeared on image stat\n'

find_response="$(curl_response GET "$BASE/v1/fs/$ROOT_PATH" owner \
  --get --data-urlencode 'find=' --data-urlencode "tag=$MARKER_KEY=$marker")" || die "tag find request failed"
find_code="$(http_code "$find_response")"
find_body="$(json_body "$find_response")"
[ "$find_code" = "200" ] || http_failure "tag find" "$find_code" "$find_body"
if ! printf '%s' "$find_body" | jq -e --arg path "/$IMAGE_PATH" '
  (. // []) | any(.[]; (.path == $path) or ((.path // "") | endswith($path)))
' >/dev/null; then
  die "tag find did not return the extracted image"
fi

printf 'PASS: image extract config and generated tag verified\n'

disable_response="$(
  printf '%s' '{"enabled":false}' |
    curl_response PUT "$config_url" control-plane \
      -H 'Content-Type: application/json' --data-binary @-
)" || die "extract config disable request failed"
disable_code="$(http_code "$disable_response")"
disable_body="$(json_body "$disable_response")"
[ "$disable_code" = "200" ] || http_failure "extract config disable" "$disable_code" "$disable_body"
if ! printf '%s' "$disable_body" | jq -e '
  .enabled == false and
  .source == "custom" and
  (.api_base // "") == "" and
  (.api_key // "") == "" and
  (.model // "") == "" and
  (.prompt // "") == ""
' >/dev/null; then
  die "extract config disable response did not return a cleared custom off-marker"
fi
CONFIG_ENABLED=0

disabled_upload_response="$(curl_response PUT "$BASE/v1/fs/$DISABLED_IMAGE_PATH" owner \
  -H 'Content-Type: image/jpeg' --data-binary "@$IMAGE_FIXTURE")" || die "disabled image upload request failed"
disabled_upload_code="$(http_code "$disabled_upload_response")"
disabled_upload_body="$(json_body "$disabled_upload_response")"
[ "$disabled_upload_code" = "200" ] || http_failure "disabled image upload" "$disabled_upload_code" "$disabled_upload_body"

disabled_deadline=$(( $(date +%s) + DISABLED_EXTRACT_WAIT_S ))
while :; do
  disabled_stat_response="$(curl_response GET "$BASE/v1/fs/$DISABLED_IMAGE_PATH?stat" owner)" || die "disabled image stat request failed"
  disabled_stat_code="$(http_code "$disabled_stat_response")"
  disabled_stat_body="$(json_body "$disabled_stat_response")"
  [ "$disabled_stat_code" = "200" ] || http_failure "disabled image stat" "$disabled_stat_code" "$disabled_stat_body"
  if ! printf '%s' "$disabled_stat_body" | jq -e '
    (.semantic_text // "") == "" and ((.tags // {}) | length) == 0
  ' >/dev/null; then
    die "disabled image unexpectedly received extracted text or tags"
  fi
  if [ "$(date +%s)" -ge "$disabled_deadline" ]; then
    break
  fi
  sleep "$IMAGE_EXTRACT_INTERVAL_S"
done
printf 'PASS: disabled config produced no extracted text or tags for %ss\n' "$DISABLED_EXTRACT_WAIT_S"
