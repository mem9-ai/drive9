#!/usr/bin/env bash
# Validate a pre-existing drive9 API key against deployed endpoints.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-60}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

if [ -z "$API_KEY" ]; then
  echo "DRIVE9_API_KEY is required"
  exit 1
fi

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS+1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

curl_code() {
  local method="$1"
  local url="$2"
  local body_file
  body_file="$(mktemp)"
  local code
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $API_KEY" "$url")
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

echo "Base: $BASE"

resp=$(curl_code GET "$BASE/v1/status")
code=$(http_code "$resp")
body=$(json_body "$resp")
status=$(printf '%s' "$body" | jq -r '.status // empty')
check_eq "GET /v1/status returns 200" "$code" "200"

if [ "$status" = "provisioning" ]; then
  deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    sleep "$POLL_INTERVAL_S"
    resp=$(curl_code GET "$BASE/v1/status")
    body=$(json_body "$resp")
    status=$(printf '%s' "$body" | jq -r '.status // empty')
    echo "status=$status"
    [ "$status" = "active" ] && break
  done
fi

check_eq "tenant status is active" "$status" "active"

resp=$(curl_code GET "$BASE/v1/fs/?list")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/?list returns 200" "$code" "200"
entries_type=$(printf '%s' "$body" | jq -r '.entries | type')
check_eq "entries is array" "$entries_type" "array"

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
