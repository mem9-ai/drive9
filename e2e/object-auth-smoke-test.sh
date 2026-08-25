#!/usr/bin/env bash
# Manual --auth=server (STS mint) smoke against a hosted tidbcloud-native
# deployment and a real object store.
#
# Manual-only (not part of smoke-all / local-e2e / CI): local provision has no
# org binding, and mint talks to the cloud STS of the registered backend.
# Run this yourself on tidbcloud-native dev after registering a dedicated
# test bucket + IAM/CAM/RAM user.
#
#   export DRIVE9_BASE="http://k8s-drive9ti-drive9se-b6bbe5ba6e-cee81207452d1185.elb.ap-southeast-1.amazonaws.com"
#   export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
#   export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
#   export DRIVE9_E2E_OBJECT_BUCKET="your-bucket"
#   export DRIVE9_E2E_OBJECT_ACCESS_KEY_ID="..."
#   export DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY="..."
#   # optional:
#   # DRIVE9_E2E_OBJECT_SCHEME=s3|cos|tos|oss   (default s3)
#   # DRIVE9_E2E_OBJECT_REGION=us-east-1
#   # DRIVE9_E2E_OBJECT_ENDPOINT=https://...
#   # DRIVE9_E2E_OBJECT_STS_ENDPOINT=https://...
#   # DRIVE9_E2E_OBJECT_ACCOUNT_ID=...          (Tencent APPID)
#   # DRIVE9_E2E_OBJECT_PREFIX=...              (extra prefix above the tenant ns)
#   # DRIVE9_E2E_OBJECT_ROLE_ARN=...
#   # DRIVE9_E2E_OBJECT_FORCE_PATH_STYLE=1
#   bash e2e/object-auth-smoke-test.sh

set -euo pipefail

required_env=(
  DRIVE9_BASE
  DRIVE9_TIDBCLOUD_PUBLIC_KEY
  DRIVE9_TIDBCLOUD_PRIVATE_KEY
  DRIVE9_E2E_OBJECT_BUCKET
  DRIVE9_E2E_OBJECT_ACCESS_KEY_ID
  DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY
)
missing_env=()
for name in "${required_env[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing_env+=("$name")
  fi
done
if [ "${#missing_env[@]}" -gt 0 ]; then
  printf 'SKIP: object-auth smoke requires: %s\n' "${missing_env[*]}"
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE="${DRIVE9_BASE%/}"
PUBLIC_KEY="$DRIVE9_TIDBCLOUD_PUBLIC_KEY"
PRIVATE_KEY="$DRIVE9_TIDBCLOUD_PRIVATE_KEY"
SCHEME="${DRIVE9_E2E_OBJECT_SCHEME:-s3}"
BUCKET="$DRIVE9_E2E_OBJECT_BUCKET"
REGION="${DRIVE9_E2E_OBJECT_REGION:-}"
ENDPOINT="${DRIVE9_E2E_OBJECT_ENDPOINT:-}"
STS_ENDPOINT="${DRIVE9_E2E_OBJECT_STS_ENDPOINT:-}"
ACCOUNT_ID="${DRIVE9_E2E_OBJECT_ACCOUNT_ID:-}"
PREFIX="${DRIVE9_E2E_OBJECT_PREFIX:-}"
ROLE_ARN="${DRIVE9_E2E_OBJECT_ROLE_ARN:-}"
FORCE_PATH_STYLE="${DRIVE9_E2E_OBJECT_FORCE_PATH_STYLE:-0}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
CLI_SOURCE="${CLI_SOURCE:-build}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"

PASS=0
FAIL=0
TOTAL=0
CLI_BIN=""
CLI_HOME=""
TENANT_ID=""
API_KEY=""
BACKEND_ID=""
CREATED=0
NS=""

die() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

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

for command in curl jq; do
  command -v "$command" >/dev/null || die "$command is required"
done

if [ "$CLI_SOURCE" = "build" ]; then
  CLI_BIN="$(mktemp)"
  make -C "$REPO_ROOT" build-cli CLI_BIN="$CLI_BIN" >/dev/null
else
  die "CLI_SOURCE=$CLI_SOURCE not supported (use build)"
fi
chmod +x "$CLI_BIN"

CLI_HOME="$(mktemp -d)"
TS="$(date +%s)"
NS="e2e-obj-${TS}"
TMP_FILE="$(mktemp)"
printf 'object-auth %s\n' "$TS" >"$TMP_FILE"
ADMIN_FLAGS=(--server "$BASE" --tidbcloud-public-key "$PUBLIC_KEY" --tidbcloud-private-key "$PRIVATE_KEY")

drive9() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_admin() {
  env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI_BIN" admin "$@"
}

cleanup() {
  if [ "$SKIP_CLEANUP" != "1" ]; then
    if [ -n "${API_KEY:-}" ] && [ -n "${SCHEME:-}" ] && [ -n "${BUCKET:-}" ] && [ -n "${NS:-}" ]; then
      drive9 fs rm --auth=server -r "${SCHEME}://${BUCKET}/${PREFIX:+$PREFIX/}${NS}/" >/dev/null 2>&1 || true
    fi
    if [ -n "${TENANT_ID:-}" ]; then
      drive9_admin tenant object-namespace clear --tenant-id "$TENANT_ID" "${ADMIN_FLAGS[@]}" >/dev/null 2>&1 || true
    fi
    if [ -n "${BACKEND_ID:-}" ]; then
      drive9_admin object-backend rm --id "$BACKEND_ID" "${ADMIN_FLAGS[@]}" >/dev/null 2>&1 || true
    fi
    if [ "$CREATED" -eq 1 ] && [ -n "${TENANT_ID:-}" ]; then
      env -u DRIVE9_SERVER HOME="$CLI_HOME" "$CLI_BIN" delete \
        --server "$BASE" --api-key "${API_KEY:-}" \
        --tidbcloud-public-key "$PUBLIC_KEY" --tidbcloud-private-key "$PRIVATE_KEY" -y \
        >/dev/null 2>&1 || true
    fi
  fi
  rm -f "$CLI_BIN" "$TMP_FILE"
  rm -rf "$CLI_HOME"
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
}
trap cleanup EXIT

echo "=== drive9 object-auth (server mint) smoke ==="
echo "BASE=$BASE SCHEME=$SCHEME BUCKET=$BUCKET"

create_out="$(drive9_admin tenant create "${ADMIN_FLAGS[@]}" --json 2>/dev/null || true)"
if [ -z "$create_out" ]; then
  create_out="$(env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI_BIN" create \
    --server "$BASE" \
    --tidbcloud-public-key "$PUBLIC_KEY" \
    --tidbcloud-private-key "$PRIVATE_KEY" \
    --json)"
fi
API_KEY="$(printf '%s' "$create_out" | jq -r '.api_key // empty')"
TENANT_ID="$(printf '%s' "$create_out" | jq -r '.tenant_id // empty')"
check_cmd "provisioned tenant" test -n "$TENANT_ID" -a -n "$API_KEY"
CREATED=1

deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sfile="$(mktemp)"
  scode=$(curl -sS -o "$sfile" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/status" || true)
  LAST_STATUS=$(jq -r '.status // empty' "$sfile" 2>/dev/null || true)
  rm -f "$sfile"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant active" "$LAST_STATUS" "active"

add_args=(object-backend add --scheme "$SCHEME" --bucket "$BUCKET" --json "${ADMIN_FLAGS[@]}"
  --access-key-id "$DRIVE9_E2E_OBJECT_ACCESS_KEY_ID"
  --secret-access-key "$DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY")
if [ -n "$REGION" ]; then add_args+=(--region "$REGION"); fi
if [ -n "$ENDPOINT" ]; then add_args+=(--endpoint "$ENDPOINT"); fi
if [ -n "$STS_ENDPOINT" ]; then add_args+=(--sts-endpoint "$STS_ENDPOINT"); fi
if [ -n "$ACCOUNT_ID" ]; then add_args+=(--account-id "$ACCOUNT_ID"); fi
if [ -n "$PREFIX" ]; then add_args+=(--prefix "$PREFIX"); fi
if [ -n "$ROLE_ARN" ]; then add_args+=(--credential-kind role --role-arn "$ROLE_ARN"); else add_args+=(--credential-kind static); fi
if [ "$FORCE_PATH_STYLE" = "1" ]; then add_args+=(--force-path-style); fi
add_args+=(--name "e2e-${TS}")

add_out="$(drive9_admin "${add_args[@]}")"
BACKEND_ID="$(printf '%s' "$add_out" | jq -r '.id // empty')"
check_cmd "object-backend add" test -n "$BACKEND_ID"

get_out="$(drive9_admin object-backend get --id "$BACKEND_ID" --json "${ADMIN_FLAGS[@]}")"
check_cmd "object-backend get" bash -c 'echo "$1" | jq -e .id >/dev/null' _ "$get_out"

upd_out="$(drive9_admin object-backend update --id "$BACKEND_ID" --region "${REGION:-us-east-1}" \
  --secret-access-key "$DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY" --json "${ADMIN_FLAGS[@]}")"
check_cmd "object-backend update" bash -c 'echo "$1" | jq -e .id >/dev/null' _ "$upd_out"

drive9_admin tenant object-namespace set --tenant-id "$TENANT_ID" --namespace-id "$NS" "${ADMIN_FLAGS[@]}" >/dev/null
ns_got="$(drive9_admin tenant object-namespace get --tenant-id "$TENANT_ID" --json "${ADMIN_FLAGS[@]}" | jq -r '.namespace_id')"
check_eq "object-namespace set" "$ns_got" "$NS"

uri_root="${SCHEME}://${BUCKET}/"
if [ -n "$PREFIX" ]; then
  uri_root="${uri_root}${PREFIX}/"
fi
uri_root="${uri_root}${NS}"
file_uri="${uri_root}/hello.txt"
outside_uri="${SCHEME}://${BUCKET}/not-${NS}/hello.txt"

drive9 fs cp "$TMP_FILE" "$file_uri" --force >/dev/null
ls_out="$(drive9 fs ls "${uri_root}/")"
check_cmd "minted ls lists hello.txt" bash -c 'echo "$1" | grep -q hello.txt' _ "$ls_out"
cat_out="$(drive9 fs cat "$file_uri")"
check_cmd "minted cat returns body" bash -c 'echo "$1" | grep -q "object-auth"' _ "$cat_out"
check_cmd_fail "minted write outside namespace is denied" drive9 fs cp "$TMP_FILE" "$outside_uri" --force

echo "=== object-auth smoke done PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL ==="
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
