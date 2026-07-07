#!/usr/bin/env bash
# Shared /v1/provision helpers for live e2e scripts.

_drive9_provision_helper_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "${DRIVE9_CLEANUP_HELPER_LOADED:-}" ]; then
  . "$_drive9_provision_helper_dir/cleanup-helper.sh"
fi
drive9_e2e_init_tmpdir

drive9_provision_body() {
  local public_key="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-}"
  local private_key="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-}"
  local spending_limit="${DRIVE9_TIDBCLOUD_SPENDING_LIMIT:-}"

  if [ -z "$public_key" ] && [ -z "$private_key" ] && [ -z "$spending_limit" ]; then
    return 0
  fi
  if [ -z "$public_key" ] || [ -z "$private_key" ]; then
    echo "DRIVE9_TIDBCLOUD_PUBLIC_KEY and DRIVE9_TIDBCLOUD_PRIVATE_KEY must be set together for TiDB Cloud Native provisioning" >&2
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to build TiDB Cloud Native provision request body" >&2
    return 1
  fi

  if [ -n "$spending_limit" ]; then
    case "$spending_limit" in
      *[!0-9]*)
        echo "DRIVE9_TIDBCLOUD_SPENDING_LIMIT must be a non-negative integer" >&2
        return 1
        ;;
    esac
    jq -cn \
      --arg pk "$public_key" \
      --arg sk "$private_key" \
      --argjson limit "$spending_limit" \
      '{public_key: $pk, private_key: $sk, tidbcloud_spending_limit: $limit}'
    return
  fi

  jq -cn --arg pk "$public_key" --arg sk "$private_key" '{public_key: $pk, private_key: $sk}'
}

drive9_provision_to_file() {
  local base="$1"
  local output_file="$2"
  local body max_retries retry_sleep attempt code tenant_id api_key suite

  body="$(drive9_provision_body)" || return 1
  drive9_cleanup_require_ready || return 1
  max_retries="${DRIVE9_PROVISION_MAX_RETRIES:-${REQUEST_MAX_RETRIES:-${CLI_MAX_RETRIES:-1}}}"
  retry_sleep="${DRIVE9_PROVISION_RETRY_SLEEP_S:-${REQUEST_RETRY_SLEEP_S:-${CLI_RETRY_SLEEP_S:-2}}}"
  attempt=1

  while :; do
    if [ -n "$body" ]; then
      code=$(printf '%s' "$body" | curl -sS -o "$output_file" -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        --data-binary @- \
        "$base/v1/provision" || true)
    else
      code=$(curl -sS -o "$output_file" -w "%{http_code}" -X POST "$base/v1/provision" || true)
    fi
    code="$(drive9_provision_normalize_http_code "$code")"

    if ! drive9_provision_retryable_code "$code" || [ "$attempt" -ge "$max_retries" ]; then
      if [ "$code" = "202" ]; then
        tenant_id="$(jq -r '.tenant_id // empty' "$output_file" 2>/dev/null || true)"
        api_key="$(jq -r '.api_key // empty' "$output_file" 2>/dev/null || true)"
        suite="${DRIVE9_E2E_SUITE_NAME:-$(basename "${BASH_SOURCE[1]:-${0:-provision}}")}"
        if drive9_cleanup_is_enabled; then
          if [ -z "$tenant_id" ] || [ -z "$api_key" ]; then
            echo "CRITICAL: provision succeeded but response lacks tenant_id/api_key; cannot register cleanup" >&2
            return 1
          fi
          if ! drive9_cleanup_register_live "$suite" "$base" "$tenant_id" "$api_key"; then
            echo "CRITICAL: provisioned tenant $tenant_id but failed to register cleanup" >&2
            return 1
          fi
        fi
      fi
      printf '%s' "$code"
      return
    fi

    echo "provision returned http=$code, retrying ${attempt}/${max_retries}" >&2
    attempt=$((attempt + 1))
    sleep "$retry_sleep"
  done
}

drive9_provision_normalize_http_code() {
  case "$1" in
    [0-9][0-9][0-9]) printf '%s' "$1" ;;
    *) printf '000' ;;
  esac
}

drive9_provision_retryable_code() {
  case "$1" in
    000|403|429|5??) return 0 ;;
    *) return 1 ;;
  esac
}

drive9_provision_curl_body_code() {
  local base="$1"
  local body_file code

  body_file="$(mktemp)"
  if ! code="$(drive9_provision_to_file "$base" "$body_file")"; then
    rm -f "$body_file"
    return 1
  fi
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}
