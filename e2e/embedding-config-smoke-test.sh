#!/usr/bin/env bash
# Live tenant embedding-config and semantic processing smoke test.
#
# Manual-only: provisions and deletes a real tenant and calls a billable
# OpenAI-compatible 1024-dimension embedding provider. Missing configuration
# skips the suite before any HTTP request is made.

set -euo pipefail

required_env=(
  DRIVE9_BASE
  DRIVE9_TIDBCLOUD_PUBLIC_KEY
  DRIVE9_TIDBCLOUD_PRIVATE_KEY
  DRIVE9_E2E_EMBED_API_BASE
  DRIVE9_E2E_EMBED_API_KEY
  DRIVE9_E2E_EMBED_MODEL
)
missing_env=()
for name in "${required_env[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing_env+=("$name")
  fi
done
if [ "${#missing_env[@]}" -gt 0 ]; then
  printf 'SKIP: embedding config smoke requires: %s\n' "${missing_env[*]}"
  exit 0
fi

BASE="${DRIVE9_BASE%/}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
EMBED_TIMEOUT_S="${EMBED_TIMEOUT_S:-180}"
EMBED_INTERVAL_S="${EMBED_INTERVAL_S:-3}"
EMBED_CONFIG_PROPAGATION_WAIT_S="${EMBED_CONFIG_PROPAGATION_WAIT_S:-2}"
CURL_CONNECT_TIMEOUT_S="${CURL_CONNECT_TIMEOUT_S:-10}"
CURL_MAX_TIME_S="${CURL_MAX_TIME_S:-120}"
UNREACHABLE_API_BASE="${DRIVE9_E2E_UNREACHABLE_API_BASE:-https://example.com:81/v1}"

ROOT_PATH="embedding-e2e"
TARGET_PATH="$ROOT_PATH/target.txt"
DISTRACTOR_PATH="$ROOT_PATH/distractor.txt"
TARGET_TEXT="A veterinarian carefully treats an injured kitten inside a quiet clinic."
DISTRACTOR_TEXT="A cargo ship carries steel containers across the open ocean."
SEMANTIC_QUERY="animal doctor caring for a wounded feline"

TENANT_ID=""
OWNER_API_KEY=""
CONFIG_ENABLED=0
TREE_CREATED=0

die() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

for command in curl jq; do
  command -v "$command" >/dev/null || die "$command is required"
done

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
    disable_response="$(printf '%s' '{"enabled":false}' | curl_response PUT "$BASE/v1/admin/tenants/$TENANT_ID/embedding-config" control-plane -H 'Content-Type: application/json' --data-binary @-)"
    disable_code="$(http_code "$disable_response")"
    if [ "$disable_code" != "200" ]; then
      printf 'CLEANUP FAIL: disabling embedding config returned HTTP %s\n' "$disable_code" >&2
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

printf '=== tenant embedding config smoke ===\n'
printf 'Base URL: %s\n' "$BASE"
printf 'Target text: %s\n' "$TARGET_TEXT"
printf 'Semantic query: %s\n' "$SEMANTIC_QUERY"

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

config_url="$BASE/v1/admin/tenants/$TENANT_ID/embedding-config"
before_response="$(curl_response GET "$config_url" control-plane)" || die "initial embedding config request failed"
before_code="$(http_code "$before_response")"
before_body="$(json_body "$before_response")"
[ "$before_code" = "200" ] || http_failure "initial embedding config GET" "$before_code" "$before_body"
before_source="$(printf '%s' "$before_body" | jq -r '.source // empty')"
case "$before_source" in
  none|default) ;;
  database_auto) die "tenant uses database auto-embedding; hosted embedding-config smoke requires shared or native fts_only mode" ;;
  *) die "new tenant unexpectedly has embedding config source ${before_source:-unknown}" ;;
esac
before_canonical="$(printf '%s' "$before_body" | jq -S -c .)" || die "initial embedding config GET returned invalid JSON"

invalid_key_response="$(
  jq -nc --arg api_base "$DRIVE9_E2E_EMBED_API_BASE" --arg model "$DRIVE9_E2E_EMBED_MODEL" '{enabled:true, api_base:$api_base, api_key:"drive9-e2e-invalid-provider-key", model:$model}' |
    curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-
)" || die "invalid API key embedding config PUT request failed"
invalid_key_code="$(http_code "$invalid_key_response")"
invalid_key_body="$(json_body "$invalid_key_response")"
[ "$invalid_key_code" = "400" ] || http_failure "invalid API key embedding config PUT (expected HTTP 400)" "$invalid_key_code" "$invalid_key_body"
invalid_key_error="$(printf '%s' "$invalid_key_body" | jq -r '.error // empty')"
[[ "$invalid_key_error" == "embedding provider validation failed: "* ]] || die "invalid API key embedding config PUT returned an unexpected error"
assert_config_unchanged "invalid API key"
printf 'PASS: invalid embedding provider key was rejected without persistence\n'

unreachable_response="$(
  jq -nc --arg api_base "$UNREACHABLE_API_BASE" --arg api_key "$DRIVE9_E2E_EMBED_API_KEY" --arg model "$DRIVE9_E2E_EMBED_MODEL" '{enabled:true, api_base:$api_base, api_key:$api_key, model:$model}' |
    curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-
)" || die "unreachable embedding provider config PUT request failed"
unreachable_code="$(http_code "$unreachable_response")"
unreachable_body="$(json_body "$unreachable_response")"
if [ "$unreachable_code" != "502" ] && [ "$unreachable_code" != "504" ]; then
  http_failure "unreachable embedding provider config PUT (expected HTTP 502 or 504)" "$unreachable_code" "$unreachable_body"
fi
assert_config_unchanged "unreachable API base"
printf 'PASS: unreachable embedding provider was rejected without persistence\n'

config_response="$(
  jq -nc '{enabled:true, api_base:env.DRIVE9_E2E_EMBED_API_BASE, api_key:env.DRIVE9_E2E_EMBED_API_KEY, model:env.DRIVE9_E2E_EMBED_MODEL}' |
    curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-
)" || die "embedding config PUT request failed"
config_code="$(http_code "$config_response")"
config_body="$(json_body "$config_response")"
[ "$config_code" = "200" ] || http_failure "embedding config PUT/provider validation" "$config_code" "$config_body"
CONFIG_ENABLED=1
if ! printf '%s' "$config_body" | jq -e '
  .enabled == true and .source == "custom" and (.generation | type == "number" and . > 0) and
  .api_base == env.DRIVE9_E2E_EMBED_API_BASE and .model == env.DRIVE9_E2E_EMBED_MODEL and
  (.api_key | type == "string" and contains("*") and . != env.DRIVE9_E2E_EMBED_API_KEY)
' >/dev/null; then
  die "embedding config response did not return the expected masked custom config"
fi
printf 'PASS: embedding provider validated and custom config is masked\n'

# Config invalidation is synchronous on the serving pod and propagated to
# other pods by the metadb outbox. Allow that bounded broadcast to settle
# before the upload decides whether to enqueue app-managed embedding work.
sleep "$EMBED_CONFIG_PROPAGATION_WAIT_S"

mkdir_response="$(curl_response POST "$BASE/v1/fs/$ROOT_PATH?mkdir" owner)" || die "test directory creation failed"
mkdir_code="$(http_code "$mkdir_response")"
mkdir_body="$(json_body "$mkdir_response")"
[ "$mkdir_code" = "200" ] || http_failure "test directory creation" "$mkdir_code" "$mkdir_body"
TREE_CREATED=1

target_response="$(curl_response PUT "$BASE/v1/fs/$TARGET_PATH" owner -H 'Content-Type: text/plain' --data-binary "$TARGET_TEXT")" || die "semantic target upload failed"
target_code="$(http_code "$target_response")"
target_body="$(json_body "$target_response")"
[ "$target_code" = "200" ] || http_failure "semantic target upload" "$target_code" "$target_body"
distractor_response="$(curl_response PUT "$BASE/v1/fs/$DISTRACTOR_PATH" owner -H 'Content-Type: text/plain' --data-binary "$DISTRACTOR_TEXT")" || die "semantic distractor upload failed"
distractor_code="$(http_code "$distractor_response")"
distractor_body="$(json_body "$distractor_response")"
[ "$distractor_code" = "200" ] || http_failure "semantic distractor upload" "$distractor_code" "$distractor_body"
printf 'PASS: uploaded semantic target and distractor\n'

embed_deadline=$(( $(date +%s) + EMBED_TIMEOUT_S ))
found="false"
while :; do
  grep_response="$(curl_response GET "$BASE/v1/fs/$ROOT_PATH" owner --get --data-urlencode "grep=$SEMANTIC_QUERY" --data-urlencode 'limit=20')" || die "semantic grep request failed"
  grep_code="$(http_code "$grep_response")"
  grep_body="$(json_body "$grep_response")"
  if [ "$grep_code" = "200" ] && printf '%s' "$grep_body" | jq -e --arg path "/$TARGET_PATH" '
    type == "array" and any(.[]; (.path == $path) or ((.path // "") | endswith($path)))
  ' >/dev/null 2>&1; then
    found="true"
    break
  fi
  [ "$(date +%s)" -lt "$embed_deadline" ] || break
  sleep "$EMBED_INTERVAL_S"
done
[ "$found" = "true" ] || die "semantic query did not find the target within ${EMBED_TIMEOUT_S}s"
printf 'PASS: app-managed embedding worker and semantic query found the target\n'
printf '%s\n' '--- semantic grep result ---'
printf '%s' "$grep_body" | jq .

disable_response="$(printf '%s' '{"enabled":false}' | curl_response PUT "$config_url" control-plane -H 'Content-Type: application/json' --data-binary @-)" || die "embedding config disable request failed"
disable_code="$(http_code "$disable_response")"
disable_body="$(json_body "$disable_response")"
[ "$disable_code" = "200" ] || http_failure "embedding config disable" "$disable_code" "$disable_body"
if ! printf '%s' "$disable_body" | jq -e '.enabled == false and .source == "custom" and (.api_base // "") == "" and (.api_key // "") == "" and (.model // "") == ""' >/dev/null; then
  die "embedding config disable response did not return a cleared custom config"
fi
CONFIG_ENABLED=0
printf 'PASS: embedding config disabled cleanly\n'
