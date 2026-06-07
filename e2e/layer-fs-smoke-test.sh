#!/usr/bin/env bash
# drive9 layer filesystem smoke test against a live deployment.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
CLI_HOME="${DRIVE9_E2E_CLI_HOME:-$(mktemp -d)}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
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

check_cmd_fail() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    echo "FAIL $desc (expected failure)"
    FAIL=$((FAIL + 1))
  else
    echo "PASS $desc"
    PASS=$((PASS + 1))
  fi
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

url_escape() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe=""))
PY
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local data="${4:-}"
  local body_file
  local code
  local curl_rc
  local attempt
  local -a args

  body_file="$(mktemp)"
  attempt=1
  while :; do
    : >"$body_file"
    args=(-sS -o "$body_file" -w "%{http_code}" -X "$method")
    if [ -n "$auth" ]; then
      args+=(-H "Authorization: Bearer $auth")
    fi
    if [ -n "$data" ]; then
      args+=(-H "Content-Type: application/json" --data-binary "$data")
    fi
    if code=$(curl "${args[@]}" "$url"); then
      curl_rc=0
    else
      curl_rc=$?
      code="000"
    fi
    if [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      break
    fi
    case "$code" in
      000|429|5??)
        echo "retrying $method $url after HTTP $code (attempt $attempt/$REQUEST_MAX_RETRIES)" >&2
        sleep "$REQUEST_RETRY_SLEEP_S"
        attempt=$((attempt + 1))
        continue
        ;;
    esac
    if [ "$curl_rc" -eq 0 ]; then
      break
    fi
    echo "retrying $method $url after curl exit $curl_rc (attempt $attempt/$REQUEST_MAX_RETRIES)" >&2
    sleep "$REQUEST_RETRY_SLEEP_S"
    attempt=$((attempt + 1))
  done
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

provision_key() {
  local resp code body status deadline

  resp=$(curl_body_code POST "$BASE/v1/provision")
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
    if [ "$status" = "active" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep "$POLL_INTERVAL_S"
  done
  check_eq "tenant eventually becomes active" "$status" "active"
}

detect_release_target() {
  case "$(uname -s)" in
    Linux) CLI_RELEASE_OS="linux" ;;
    Darwin) CLI_RELEASE_OS="darwin" ;;
    *)
      echo "unsupported OS for official CLI download: $(uname -s)" >&2
      return 1
      ;;
  esac

  case "$(uname -m)" in
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *)
      echo "unsupported architecture for official CLI download: $(uname -m)" >&2
      return 1
      ;;
  esac
}

download_official_cli() {
  local target_version="$CLI_RELEASE_VERSION"
  detect_release_target || return 1
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" | tr -d '[:space:]')
  fi
  if [ -z "$target_version" ]; then
    echo "failed to resolve release version from $CLI_RELEASE_BASE_URL/version" >&2
    return 1
  fi
  curl -fsSL "$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      make build-cli CLI_BIN="$CLI_BIN"
      ;;
    official)
      download_official_cli
      ;;
    *)
      echo "invalid CLI_SOURCE: $CLI_SOURCE (expected build|official)" >&2
      return 1
      ;;
  esac
}

drive9() {
  env -u DRIVE9_VAULT_TOKEN HOME="$CLI_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(drive9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"not found"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $*" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

put_layer_entry() {
  local layer_ref="$1"
  local path="$2"
  local op="$3"
  local kind="$4"
  local text="${5:-}"
  local content_b64=""
  local body resp code ref_escaped

  if [ "$op" = "upsert" ] || [ "$op" = "symlink" ]; then
    content_b64=$(printf '%s' "$text" | base64 | tr -d '\n')
  fi
  body=$(jq -n \
    --arg path "$path" \
    --arg op "$op" \
    --arg kind "$kind" \
    --arg content "$content_b64" \
    --arg text "$text" \
    '{path:$path, op:$op, kind:$kind, mode:420}
      + (if $content != "" then {content:$content, content_text:$text} else {} end)')
  ref_escaped=$(url_escape "$layer_ref")
  resp=$(curl_body_code POST "$BASE/v1/fs-layers/$ref_escaped/entries" "$API_KEY" "$body")
  code=$(http_code "$resp")
  check_eq "upsert layer entry $path via $layer_ref" "$code" "200"
}

echo "=== drive9 layer filesystem smoke test ==="
echo "BASE=$BASE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
fi

if [ -z "$API_KEY" ]; then
  provision_key
else
  echo "Using existing DRIVE9_API_KEY"
fi

prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

ts="$(date +%s)"
root="/layer-smoke-${ts}"
base_file="${root}/base.txt"
new_file="${root}/new.txt"
extra_file="${root}/extra.txt"
rollback_file="${root}/rollback-only.txt"
base_local="/tmp/drive9-layer-base-${ts}.txt"
layer_name="layer-smoke-${ts}"
rollback_name="layer-smoke-rollback-${ts}"
unique_tag="unique_${ts}"
ckpt_id="ckpt_smoke_${ts}"

printf 'base before layer %s\n' "$ts" >"$base_local"
drive9_retry fs mkdir "$root" >/dev/null
drive9_retry fs cp "$base_local" ":$base_file" >/dev/null
check_eq "base file visible before layer" "$(drive9_retry fs cat "$base_file")" "$(cat "$base_local")"

layer_json=$(drive9_retry fs layer create \
  --name "$layer_name" \
  --tag suite=layer-smoke \
  --tag "run=$ts" \
  --tag "${unique_tag}=1" \
  --json \
  ":$root")
layer_id=$(printf '%s' "$layer_json" | jq -r '.layer_id // empty')
check_cmd "layer create returns id" test -n "$layer_id"

status_by_name=$(drive9_retry fs layer status --json "$layer_name")
check_eq "layer status by name resolves id" "$(printf '%s' "$status_by_name" | jq -r '.layer_id')" "$layer_id"

status_by_tag_value=$(drive9_retry fs layer status --json "tag:run=$ts")
check_eq "layer status by tag value resolves id" "$(printf '%s' "$status_by_tag_value" | jq -r '.layer_id')" "$layer_id"

status_by_tag_key=$(drive9_retry fs layer status --json "tag:$unique_tag")
check_eq "layer status by tag key resolves id" "$(printf '%s' "$status_by_tag_key" | jq -r '.layer_id')" "$layer_id"

put_layer_entry "$layer_name" "$base_file" "upsert" "file" "base edited in layer ${ts}"
put_layer_entry "tag:$unique_tag" "$new_file" "upsert" "file" "new file from layer ${ts}"

diff_json=$(drive9_retry fs layer diff --json "tag:run=$ts")
diff_count=$(printf '%s' "$diff_json" | jq '.entries | length')
check_eq "layer diff shows two entries" "$diff_count" "2"
check_eq "base entry content_text present in diff" "$(printf '%s' "$diff_json" | jq -r --arg p "$base_file" '.entries[] | select(.path==$p) | .content_text')" "base edited in layer ${ts}"

checkpoint_json=$(drive9_retry fs layer checkpoint --id "$ckpt_id" --label before-extra --json "$layer_name")
check_eq "checkpoint uses requested id" "$(printf '%s' "$checkpoint_json" | jq -r '.checkpoint_id')" "$ckpt_id"
check_eq "checkpoint durable seq captures first two entries" "$(printf '%s' "$checkpoint_json" | jq -r '.durable_seq')" "2"

checkpoint_ref=$(url_escape "$ckpt_id")
checkpoint_resp=$(curl_body_code GET "$BASE/v1/fs-layer-checkpoints/$checkpoint_ref" "$API_KEY")
check_eq "GET checkpoint returns 200" "$(http_code "$checkpoint_resp")" "200"
check_eq "GET checkpoint resolves layer id" "$(json_body "$checkpoint_resp" | jq -r '.layer_id')" "$layer_id"

put_layer_entry "$layer_name" "$extra_file" "upsert" "file" "extra after checkpoint ${ts}"
diff_after_extra=$(drive9_retry fs layer diff --json "$layer_id")
check_eq "layer diff shows three entries after checkpoint" "$(printf '%s' "$diff_after_extra" | jq '.entries | length')" "3"

rollback_json=$(drive9_retry fs layer create \
  --name "$rollback_name" \
  --tag "rollback_run=$ts" \
  --json \
  ":$root")
rollback_id=$(printf '%s' "$rollback_json" | jq -r '.layer_id // empty')
check_cmd "rollback layer create returns id" test -n "$rollback_id"
put_layer_entry "$rollback_name" "$rollback_file" "upsert" "file" "rollback only ${ts}"
check_eq "rollback command returns ok" "$(drive9_retry fs layer rollback "tag:rollback_run=$ts")" "ok"
rollback_status=$(drive9_retry fs layer status --json "$rollback_name")
check_eq "rollback layer state is abandoned" "$(printf '%s' "$rollback_status" | jq -r '.state')" "abandoned"
check_cmd_fail "abandoned layer file is not visible in base" drive9 fs cat "$rollback_file"

commit_out=$(drive9_retry fs layer commit "tag:$unique_tag")
case "$commit_out" in
  committed\ layer="$layer_id"\ applied=3) commit_status="ok" ;;
  *) commit_status="$commit_out" ;;
esac
check_eq "commit by tag key succeeds" "$commit_status" "ok"

committed_status=$(drive9_retry fs layer status --json "$layer_id")
check_eq "committed layer state is committed" "$(printf '%s' "$committed_status" | jq -r '.state')" "committed"
check_eq "base file updated after commit" "$(drive9_retry fs cat "$base_file")" "base edited in layer ${ts}"
check_eq "new file visible after commit" "$(drive9_retry fs cat "$new_file")" "new file from layer ${ts}"
check_eq "extra file visible after commit" "$(drive9_retry fs cat "$extra_file")" "extra after checkpoint ${ts}"

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
