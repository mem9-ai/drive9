#!/usr/bin/env bash
# Shared /v1/provision helpers for live e2e scripts.

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
  local body max_retries retry_sleep attempt code

  body="$(drive9_provision_body)" || return 1
  max_retries="${DRIVE9_PROVISION_MAX_RETRIES:-${REQUEST_MAX_RETRIES:-${CLI_MAX_RETRIES:-1}}}"
  retry_sleep="${DRIVE9_PROVISION_RETRY_SLEEP_S:-${REQUEST_RETRY_SLEEP_S:-${CLI_RETRY_SLEEP_S:-2}}}"
  attempt=1

  while :; do
    if [ -n "$body" ]; then
      code=$(curl -sS -o "$output_file" -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        --data-binary "$body" \
        "$base/v1/provision")
    else
      code=$(curl -sS -o "$output_file" -w "%{http_code}" -X POST "$base/v1/provision")
    fi

    if [ "$code" != "429" ] || [ "$attempt" -ge "$max_retries" ]; then
      printf '%s' "$code"
      return
    fi

    echo "provision throttled (429), retrying ${attempt}/${max_retries}" >&2
    attempt=$((attempt + 1))
    sleep "$retry_sleep"
  done
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
