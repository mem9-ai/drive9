#!/usr/bin/env bash
# Manual-only S3 Express append-log validation. This suite requires a deployed
# server configured with a dedicated Directory Bucket; it is intentionally not
# wired into local-e2e or smoke-all.

set -u

required_env=(DRIVE9_BASE DRIVE9_API_KEY DRIVE9_E2E_S3_EXPRESS_ENABLED)
missing_env=()
for name in "${required_env[@]}"; do
	if [[ -z "${!name:-}" ]]; then
		missing_env+=("$name")
	fi
done
if ((${#missing_env[@]} > 0)); then
	printf 'SKIP: S3 Express append-log smoke requires: %s\n' "${missing_env[*]}"
	exit 0
fi

require_command() {
	local name="$1"
	if ! command -v "$name" >/dev/null 2>&1; then
		printf 'FAIL: required command %s is unavailable\n' "$name" >&2
		exit 1
	fi
}

base="${DRIVE9_BASE%/}"
api_key="$DRIVE9_API_KEY"
run_root="$(mktemp -d "${TMPDIR:-/tmp}/drive9-s3-express.XXXXXX")"
path="/s3-express-e2e-$RANDOM-$RANDOM-wal"
parts_path="${path}-parts"
response="$run_root/response.json"
headers="$run_root/headers"
body="$run_root/body"

cleanup() {
	local target
	for target in "$path" "$parts_path"; do
		curl --silent --show-error --output /dev/null \
			-X DELETE "$base/v1/fs$target" \
			-H "Authorization: Bearer $api_key" || true
	done
	rm -rf "$run_root"
}

append_status() {
	local data="$1" revision="$2" size="$3" target="$4"
	curl --silent --show-error --output "$response" --write-out '%{http_code}' \
		-X POST "$base/v1/fs$target?append-log" \
		-H "Authorization: Bearer $api_key" \
		-H "X-Dat9-Expected-Revision: $revision" \
		-H "X-Dat9-Expected-Size: $size" \
		-H "Content-Length: ${#data}" \
		--data-binary "$data"
}

append_success() {
	local data="$1" revision="$2" size="$3" target="$4" status
	status="$(append_status "$data" "$revision" "$size" "$target")" || return 1
	if [[ "$status" != 200 ]]; then
		printf 'FAIL: append status=%s response=%s\n' "$status" "$(<"$response")" >&2
		return 1
	fi
}

header_value() {
	local name="$1"
	awk -F ': *' -v name="$name" '
		tolower($1) == name { sub(/\r$/, "", $2); print $2; exit }
	' "$headers"
}

get_status() {
	local target="$1"
	curl --silent --show-error --output "$body" --dump-header "$headers" \
		--write-out '%{http_code}' \
		-H "Authorization: Bearer $api_key" "$base/v1/fs$target"
}

head_status() {
	local target="$1"
	curl --silent --show-error --output /dev/null --dump-header "$headers" \
		--write-out '%{http_code}' \
		-I -H "Authorization: Bearer $api_key" "$base/v1/fs$target"
}

require_command curl
require_command jq
require_command awk

trap cleanup EXIT INT TERM

revision=0
size=0
append_success 'wal-header' "$revision" "$size" "$path" || exit 1
revision="$(jq -er '.revision' "$response")"
size="$(jq -er '.size_bytes' "$response")"
append_success 'frame-1' "$revision" "$size" "$path" || exit 1
revision="$(jq -er '.revision' "$response")"
size="$(jq -er '.size_bytes' "$response")"

status="$(get_status "$path")" || exit 1
if [[ "$status" != 200 || "$(<"$body")" != 'wal-headerframe-1' ]]; then
	printf 'FAIL: bounded append-log GET status=%s body=%s\n' \
		"$status" "$(<"$body")" >&2
	exit 1
fi
if awk -F ': *' 'tolower($1) == "location" { found = 1 } END { exit !found }' "$headers"; then
	printf '%s\n' 'FAIL: append-log GET exposed a redirect or direct object URL' >&2
	exit 1
fi

status="$(curl --silent --show-error --output "$body" --dump-header "$headers" \
	--write-out '%{http_code}' -H "Authorization: Bearer $api_key" \
	-H 'Range: bytes=4-9' "$base/v1/fs$path")" || exit 1
if [[ "$status" != 206 || "$(<"$body")" != 'header' || \
	"$(header_value content-range)" != 'bytes 4-9/17' ]]; then
	printf 'FAIL: append-log range GET status=%s range=%s body=%s\n' \
		"$status" "$(header_value content-range)" "$(<"$body")" >&2
	exit 1
fi

stale_revision=$((revision - 1))
status="$(append_status 'stale' "$stale_revision" "$size" "$path")" || exit 1
if [[ "$status" != 409 || "$(jq -r '.code' "$response")" != append_log_conflict ]]; then
	printf 'FAIL: append-log conflict status=%s response=%s\n' \
		"$status" "$(<"$response")" >&2
	exit 1
fi

status="$(append_status 'x' "$revision" 5000000000 "$path")" || exit 1
if [[ "$status" != 413 || "$(jq -r '.code' "$response")" != append_log_too_large ]]; then
	printf 'FAIL: append-log 5GB limit status=%s response=%s\n' \
		"$status" "$(<"$response")" >&2
	exit 1
fi

status="$(curl --silent --show-error --output "$response" --write-out '%{http_code}' \
	-X PUT "$base/v1/fs$path" -H "Authorization: Bearer $api_key" \
	-H "X-Dat9-Expected-Revision: $revision" --data-binary 'reset')" || exit 1
if [[ "$status" != 200 ]]; then
	printf 'FAIL: append-log full-body PUT status=%s response=%s\n' \
		"$status" "$(<"$response")" >&2
	exit 1
fi
revision="$(jq -er '.revision' "$response")"
size="$(jq -er '.size_bytes' "$response")"
if [[ "$size" != 5 ]]; then
	printf 'FAIL: append-log full-body PUT size=%s want=5\n' "$size" >&2
	exit 1
fi
status="$(get_status "$path")" || exit 1
if [[ "$status" != 200 || "$(<"$body")" != reset ]]; then
	printf 'FAIL: append-log full-body PUT GET status=%s body=%s\n' \
		"$status" "$(<"$body")" >&2
	exit 1
fi

parts_revision=0
parts_size=0
append_success 'x' "$parts_revision" "$parts_size" "$parts_path" || exit 1
parts_revision="$(jq -er '.revision' "$response")"
parts_size="$(jq -er '.size_bytes' "$response")"
rebased=0
for ((attempt = 1; attempt <= 10001; attempt++)); do
	status="$(append_status 'p' "$parts_revision" "$parts_size" "$parts_path")" || exit 1
	if [[ "$status" == 200 ]]; then
		parts_revision="$(jq -er '.revision' "$response")"
		parts_size="$(jq -er '.size_bytes' "$response")"
		if ((attempt % 1000 == 0)); then
			printf 'INFO: completed %d append parts\n' "$attempt"
		fi
		continue
	fi
	if [[ "$status" == 409 && "$(jq -r '.code' "$response")" == append_log_rebased ]]; then
		rebased=1
		break
	fi
	printf 'FAIL: append part %d status=%s response=%s\n' \
		"$attempt" "$status" "$(<"$response")" >&2
	exit 1
done
if ((rebased == 0)); then
	printf '%s\n' 'FAIL: expected TooManyParts rebase was not observed' >&2
	exit 1
fi
status="$(head_status "$parts_path")" || exit 1
if [[ "$status" != 200 ]]; then
	printf 'FAIL: append-log rebase HEAD status=%s\n' "$status" >&2
	exit 1
fi
parts_revision="$(header_value x-dat9-revision)"
parts_size="$(header_value content-length)"
append_success 'p' "$parts_revision" "$parts_size" "$parts_path" || exit 1

printf '%s\n' 'PASS: S3 Express append-log HTTP create/append/conflict/range/413/rewrite/rebase'
