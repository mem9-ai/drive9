#!/usr/bin/env bash
# drive9 journal API smoke test against a live deployment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/provision-helper.sh"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
	local desc="$1" got="$2" want="$3"
	TOTAL=$((TOTAL + 1))
	if [[ "$got" == "$want" ]]; then
		echo "PASS $desc (got=$got)"
		PASS=$((PASS + 1))
	else
		echo "FAIL $desc (want=$want got=$got)"
		FAIL=$((FAIL + 1))
	fi
}

check_cmd() {
	local desc="$1"
	shift
	TOTAL=$((TOTAL + 1))
	if "$@"; then
		echo "PASS $desc"
		PASS=$((PASS + 1))
	else
		echo "FAIL $desc"
		FAIL=$((FAIL + 1))
	fi
}

curl_body_code() {
	local method="$1"
	local url="$2"
	local auth="${3:-}"
	local data="${4:-}"
	local content_type="${5:-application/json}"
	local body_file
	local code

	body_file="$(mktemp)"
	if [[ -n "$auth" && -n "$data" ]]; then
		code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" \
			-H "Authorization: Bearer $auth" \
			-H "Content-Type: $content_type" \
			--data-binary "$data" "$url")
	elif [[ -n "$auth" ]]; then
		code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" \
			-H "Authorization: Bearer $auth" "$url")
	else
		code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
	fi
	cat "$body_file"
	echo
	echo "__HTTP__${code}"
	rm -f "$body_file"
}

curl_append() {
	local url="$1"
	local auth="$2"
	local key="$3"
	local data="$4"
	local body_file
	local code

	body_file="$(mktemp)"
	code=$(curl -sS -o "$body_file" -w "%{http_code}" -X POST \
		-H "Authorization: Bearer $auth" \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: $key" \
		--data-binary "$data" "$url")
	cat "$body_file"
	echo
	echo "__HTTP__${code}"
	rm -f "$body_file"
}

http_code() {
	printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'
}

json_body() {
	printf '%s' "$1" | sed '/__HTTP__/d'
}

provision_key() {
	local resp code body status deadline

	resp=$(drive9_provision_curl_body_code "$BASE" || true)
	code=$(http_code "$resp")
	body=$(json_body "$resp")
	check_eq "POST /v1/provision returns 202" "$code" "202"
	API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
	check_cmd "provision response contains api_key" test -n "$API_KEY"

	deadline=$(($(date +%s) + POLL_TIMEOUT_S))
	while :; do
		resp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
		body=$(json_body "$resp")
		status=$(printf '%s' "$body" | jq -r '.status // empty')
		if [[ "$status" == "active" ]]; then
			break
		fi
		if (($(date +%s) >= deadline)); then
			break
		fi
		sleep "$POLL_INTERVAL_S"
	done
	check_eq "tenant eventually becomes active" "$status" "active"
}

echo "=== drive9 journal smoke test ==="
echo "BASE=$BASE"

if [[ -z "$API_KEY" ]]; then
	provision_key
else
	echo "Using existing DRIVE9_API_KEY"
fi

ts="$(date +%s)"
journal_id="jrn_smoke_${ts}"
append_id="app_smoke_${ts}"

create_body=$(printf '{"journal_id":"%s","kind":"agent","labels":[{"key":"env","value":"prod"},{"key":"env","value":"us-east"},{"key":"suite","value":"journal-smoke"}]}' "$journal_id")
resp=$(curl_body_code POST "$BASE/v1/journals" "$API_KEY" "$create_body")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "POST /v1/journals returns 200" "$code" "200"
created_id=$(printf '%s' "$body" | jq -r '.journal_id // empty')
check_eq "create returns requested journal_id" "$created_id" "$journal_id"

entry_body='{"type":"tool.call.completed","status":"ok","subjects":["tool:exec_command"],"summary":{"cmd":"journal smoke"}}'
resp=$(curl_append "$BASE/v1/journals/$journal_id/entries" "$API_KEY" "$append_id" "$entry_body")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "append returns 200" "$code" "200"
first_seq=$(printf '%s' "$body" | jq -r '.first_seq // empty')
head_hash=$(printf '%s' "$body" | jq -r '.head_hash // empty')
check_eq "append starts at seq 1" "$first_seq" "1"
check_cmd "append returns head hash" test -n "$head_hash"

resp=$(curl_append "$BASE/v1/journals/$journal_id/entries" "$API_KEY" "$append_id" "$entry_body")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "idempotent append retry returns 200" "$code" "200"
retry_head=$(printf '%s' "$body" | jq -r '.head_hash // empty')
check_eq "append retry preserves head hash" "$retry_head" "$head_hash"

resp=$(curl_body_code GET "$BASE/v1/journals/$journal_id/entries?limit=10" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "cat returns 200" "$code" "200"
entry_count=$(printf '%s' "$body" | jq -s 'length')
check_eq "cat returns one entry" "$entry_count" "1"

resp=$(curl_body_code GET "$BASE/v1/journal-entries?meta=env%3Dprod&meta=env%3Dus-east&limit=10" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "find repeated labels returns 200" "$code" "200"
found_id=$(printf '%s' "$body" | jq -rs -r 'map(select(.journal_id == "'"$journal_id"'"))[0].journal_id // ""')
check_eq "find repeated labels includes journal" "$found_id" "$journal_id"
has_zero_time=$(printf '%s' "$body" | grep -c '0001-01-01' || true)
check_eq "find omits zero time values" "$has_zero_time" "0"

resp=$(curl_body_code GET "$BASE/v1/journal-entries?meta=env" "$API_KEY")
code=$(http_code "$resp")
check_eq "malformed metadata query returns 400" "$code" "400"

resp=$(curl_body_code GET "$BASE/v1/journals/jrn_missing_smoke/entries" "$API_KEY")
code=$(http_code "$resp")
check_eq "missing journal cat returns 404" "$code" "404"

resp=$(curl_body_code GET "$BASE/v1/journals/$journal_id/verify" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "verify returns 200" "$code" "200"
verify_ok=$(printf '%s' "$body" | jq -r '.ok')
projection_ok=$(printf '%s' "$body" | jq -r '.projection_ok')
unchecked=$(printf '%s' "$body" | jq -r 'has("seal_ok") or has("artifact_bytes_available")')
check_eq "verify ok is true" "$verify_ok" "true"
check_eq "verify projection_ok is true" "$projection_ok" "true"
check_eq "verify omits unchecked fields" "$unchecked" "false"

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
