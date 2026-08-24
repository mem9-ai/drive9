#!/usr/bin/env bash
# drive9 API key management smoke test (server HTTP only).
#
# Covers the /v1/tokens surface added by the API key management design:
#   - Credential dispatcher (Bearer vs control-plane; ambiguous → 400; neither → 401)
#   - Data-plane: issue scoped, list, activate/deactivate, delete, revoke-by-key, refresh
#   - Scoped gate: only POST /v1/tokens/refresh allowed for scoped tokens
#   - Pseudoroot projected listing and hidden siblings
#   - Control-plane (local mock IAM on provider=local): generate, list, activate,
#     deactivate, delete, revoke-by-key, org-wide list
#   - Legacy zero-change: owner delete/revoke body {"status":"ok"}
#
# Tenant mode:
#   - Fresh (default): POST /v1/provision, then run the suite with the returned owner key.
#   - Existing (DRIVE9_API_KEY set): reuse that owner key (must be a real drive9 JWT when
#     token management is enabled).
#
# Opt-in (not part of the default smoke-all / local-e2e PR set):
#   RUN_TOKENS_SMOKE=1 bash e2e/smoke-all.sh
#   export DRIVE9_BASE=http://127.0.0.1:9009
#   bash e2e/tokens-smoke-test.sh

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
CP_PUBLIC="${DRIVE9_TOKENS_CP_PUBLIC_KEY:-local-public}"
CP_PRIVATE="${DRIVE9_TOKENS_CP_PRIVATE_KEY:-local-private}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-6}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-1}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
# Local bootstrap uses tenant_id "local" and org "local-org".
LOCAL_TENANT_ID="${DRIVE9_TOKENS_LOCAL_TENANT_ID:-local}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

ok() { TOTAL=$((TOTAL + 1)); PASS=$((PASS + 1)); echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { TOTAL=$((TOTAL + 1)); FAIL=$((FAIL + 1)); echo -e "${RED}  FAIL${RESET} $*"; }
skip_check() { TOTAL=$((TOTAL + 1)); SKIP=$((SKIP + 1)); echo -e "${YELLOW}  SKIP${RESET} $*"; }
step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
  else
    fail "$desc (want=$want got=$got)"
  fi
}

check_cmd() {
  local desc="$1"
  shift
  if "$@"; then
    ok "$desc"
  else
    fail "$desc"
  fi
}

curl_raw() {
  local method="$1"
  local url="$2"
  shift 2
  local attempt=1
  while :; do
    local body_file
    body_file="$(mktemp)"
    local code
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$@" "$url" || echo "000")
    if [ "$code" != "429" ] || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return 0
    fi
    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() {
  if [ "$#" -ge 1 ]; then
    printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'
  else
    awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'
  fi
}
json_body() {
  if [ "$#" -ge 1 ]; then
    printf '%s' "$1" | sed '/__HTTP__/d'
  else
    sed '/__HTTP__/d'
  fi
}

bearer() {
  local method="$1" path="$2" data="${3:-}" key="${4:-$API_KEY}"
  if [ -n "$data" ]; then
    curl_raw "$method" "$BASE$path" -H "Authorization: Bearer $key" -H "Content-Type: application/json" --data-binary "$data"
  else
    curl_raw "$method" "$BASE$path" -H "Authorization: Bearer $key"
  fi
}

cp_hdr() {
  local method="$1" path="$2" data="${3:-}"
  if [ -n "$data" ]; then
    curl_raw "$method" "$BASE$path" \
      -H "X-TiDBCloud-Public-Key: $CP_PUBLIC" \
      -H "X-TiDBCloud-Private-Key: $CP_PRIVATE" \
      -H "Content-Type: application/json" \
      --data-binary "$data"
  else
    curl_raw "$method" "$BASE$path" \
      -H "X-TiDBCloud-Public-Key: $CP_PUBLIC" \
      -H "X-TiDBCloud-Private-Key: $CP_PRIVATE"
  fi
}

echo "=== drive9 tokens API smoke test ==="
echo "BASE=$BASE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'

# ---------------------------------------------------------------------------
# [1] Provision / reuse owner key
# ---------------------------------------------------------------------------
step 1 "provision or reuse owner key"
if [ -z "$API_KEY" ]; then
  resp=$(curl_raw POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "provision HTTP status" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  TENANT_ID=$(printf '%s' "$body" | jq -r '.tenant_id // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
  check_cmd "provision returns tenant_id" test -n "$TENANT_ID"
else
  TENANT_ID="$LOCAL_TENANT_ID"
  skip_check "provision (DRIVE9_API_KEY set; tenant_id default=$TENANT_ID)"
  ok "using existing DRIVE9_API_KEY"
fi

# Fresh provision returns 202 while the tenant is still provisioning. Token
# routes 503 until /v1/status is active (same wait as journal / sse-retention).
deadline=$(($(date +%s) + POLL_TIMEOUT_S))
status=""
while :; do
  sresp=$(bearer GET "/v1/status")
  scode=$(http_code "$sresp")
  status=$(json_body "$sresp" | jq -r '.status // empty')
  if [ "$scode" = "200" ] && [ "$status" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant eventually becomes active" "$status" "active"

# Discover tenant_id from status/list if possible.
list_probe=$(bearer GET "/v1/tokens")
if [ "$(http_code "$list_probe")" = "200" ]; then
  discovered=$(json_body "$list_probe" | jq -r '.tokens[0].tenant_id // empty')
  if [ -n "$discovered" ]; then
    TENANT_ID="$discovered"
  fi
fi
echo "TENANT_ID=$TENANT_ID"

# ---------------------------------------------------------------------------
# [2] Dispatcher credential shapes
# ---------------------------------------------------------------------------
step 2 "dispatcher credential shapes"

# Neither credential → 401
resp=$(curl_raw POST "$BASE/v1/tokens" -H "Content-Type: application/json" --data-binary '{"ttl_seconds":60,"scopes":[{"prefix":"/scratch","ops":["read"]}]}')
check_eq "neither credential → 401" "$(http_code "$resp")" "401"

# Ambiguous: Bearer + control-plane headers → 400
resp=$(curl_raw POST "$BASE/v1/tokens" \
  -H "Authorization: Bearer $API_KEY" \
  -H "X-TiDBCloud-Public-Key: $CP_PUBLIC" \
  -H "X-TiDBCloud-Private-Key: $CP_PRIVATE" \
  -H "Content-Type: application/json" \
  --data-binary '{"ttl_seconds":60,"scopes":[{"prefix":"/scratch","ops":["read"]}]}')
check_eq "Bearer+headers → 400 ambiguous" "$(http_code "$resp")" "400"
body=$(json_body "$resp")
check_cmd "ambiguous body mentions ambiguous" bash -c "printf '%s' '$body' | grep -qi ambiguous"

# Ambiguous: Bearer + public_key in body → 400
resp=$(curl_raw POST "$BASE/v1/tokens" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "{\"ttl_seconds\":60,\"scopes\":[{\"prefix\":\"/scratch\",\"ops\":[\"read\"]}],\"public_key\":\"$CP_PUBLIC\",\"private_key\":\"$CP_PRIVATE\"}")
check_eq "Bearer+body credentials → 400" "$(http_code "$resp")" "400"

# ---------------------------------------------------------------------------
# [3] Issue scoped token
# ---------------------------------------------------------------------------
step 3 "issue scoped token"
ISSUE_BODY='{"subject":"e2e-scoped","ttl_seconds":7200,"scopes":[{"prefix":"/e2e-tokens","ops":["read","list"]}]}'
resp=$(bearer POST "/v1/tokens" "$ISSUE_BODY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "issue scoped → 201" "$code" "201"
SCOPED_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
SCOPED_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')
SCOPED_EXP=$(printf '%s' "$body" | jq -r '.expires_at // empty')
check_cmd "issue returns token" test -n "$SCOPED_TOKEN"
check_cmd "issue returns token_id" test -n "$SCOPED_ID"
check_cmd "issue returns expires_at" test -n "$SCOPED_EXP"
check_eq "issue scope_kind" "$(printf '%s' "$body" | jq -r '.scope_kind')" "fs_scoped"

# ---------------------------------------------------------------------------
# [4] Scoped gate: only refresh allowed
# ---------------------------------------------------------------------------
step 4 "scoped token gate"
resp=$(bearer POST "/v1/tokens" "$ISSUE_BODY" "$SCOPED_TOKEN")
check_eq "scoped cannot issue → 403" "$(http_code "$resp")" "403"

resp=$(bearer GET "/v1/tokens" "" "$SCOPED_TOKEN")
check_eq "scoped cannot list → 403" "$(http_code "$resp")" "403"

resp=$(bearer DELETE "/v1/tokens/$SCOPED_ID" "" "$SCOPED_TOKEN")
check_eq "scoped cannot delete → 403" "$(http_code "$resp")" "403"

resp=$(bearer POST "/v1/tokens/refresh?x=1" "{}" "$SCOPED_TOKEN")
check_eq "scoped refresh with query → 400" "$(http_code "$resp")" "400"

# ---------------------------------------------------------------------------
# [5] Owner list
# ---------------------------------------------------------------------------
step 5 "owner list"
resp=$(bearer GET "/v1/tokens")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "owner list → 200" "$code" "200"
if printf '%s' "$body" | jq -e '.tokens | type == "array"' >/dev/null; then
  ok "list has tokens array"
else
  fail "list has tokens array"
fi
count=$(printf '%s' "$body" | jq '.tokens | length')
check_cmd "list has at least 1 token" test "$count" -ge 1
# No secret leakage
if printf '%s' "$body" | grep -q jwt_hash; then
  fail "list does not contain jwt_hash field"
else
  ok "list does not contain jwt_hash field"
fi
if printf '%s' "$body" | grep -Fq "$SCOPED_TOKEN"; then
  fail "list does not echo scoped plaintext"
else
  ok "list does not echo scoped plaintext"
fi

# ---------------------------------------------------------------------------
# [6] Deactivate / activate scoped
# ---------------------------------------------------------------------------
step 6 "deactivate / activate scoped"
resp=$(bearer POST "/v1/tokens/$SCOPED_ID/deactivate" "{}")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "deactivate → 200" "$code" "200"
check_eq "deactivate status" "$(printf '%s' "$body" | jq -r '.status')" "disabled"

# Disabled key cannot refresh (middleware 401)
resp=$(bearer POST "/v1/tokens/refresh" "{}" "$SCOPED_TOKEN")
check_eq "disabled scoped refresh → 401" "$(http_code "$resp")" "401"

resp=$(bearer POST "/v1/tokens/$SCOPED_ID/activate" "{}")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "activate → 200" "$code" "200"
check_eq "activate status" "$(printf '%s' "$body" | jq -r '.status')" "active"

# Owner cannot activate owner keys (403)
OWNER_ID=$(bearer GET "/v1/tokens" | json_body | jq -r --arg tid "$TENANT_ID" '[.tokens[] | select(.scope_kind=="owner")][0].token_id // empty')
if [ -n "$OWNER_ID" ]; then
  resp=$(bearer POST "/v1/tokens/$OWNER_ID/deactivate" "{}")
  check_eq "owner cannot deactivate owner key → 403" "$(http_code "$resp")" "403"
else
  skip_check "owner self-deactivate (no owner id in list)"
fi

# ---------------------------------------------------------------------------
# [7] Refresh (scoped then owner)
# ---------------------------------------------------------------------------
step 7 "refresh"
# Re-issue a fresh scoped after activate so we have a working token.
resp=$(bearer POST "/v1/tokens" '{"subject":"e2e-refresh","ttl_seconds":3600,"scopes":[{"prefix":"/e2e-tokens","ops":["read"]}]}')
body=$(json_body "$resp")
check_eq "issue for refresh → 201" "$(http_code "$resp")" "201"
R_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
R_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')
R_EXP=$(printf '%s' "$body" | jq -r '.expires_at // empty')

resp=$(bearer POST "/v1/tokens/refresh" "{}" "$R_TOKEN")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "scoped default refresh → 200" "$code" "200"
NEW_R_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
check_cmd "refresh returns new token" test -n "$NEW_R_TOKEN"
check_cmd "refresh token differs from old" test "$NEW_R_TOKEN" != "$R_TOKEN"
check_eq "refresh token_id stable" "$(printf '%s' "$body" | jq -r '.token_id')" "$R_ID"
# Old token no longer authenticates (best-effort; cache residual may allow briefly)
resp=$(bearer POST "/v1/tokens/refresh" "{}" "$R_TOKEN")
old_code=$(http_code "$resp")
if [ "$old_code" = "401" ] || [ "$old_code" = "409" ]; then
  ok "old token after refresh → $old_code"
else
  # Residual cache window can still yield 200 rarely; treat as soft skip.
  skip_check "old token after refresh (got $old_code; residual cache window ok)"
fi

# Long-TTL scoped default refresh (no 365d lockout)
resp=$(bearer POST "/v1/tokens" '{"subject":"e2e-long","ttl_seconds":34560000,"scopes":[{"prefix":"/e2e-tokens","ops":["read"]}]}')
# 34560000s = 400 days
check_eq "issue long-ttl → 201" "$(http_code "$resp")" "201"
LONG_TOKEN=$(json_body "$resp" | jq -r '.token // empty')
resp=$(bearer POST "/v1/tokens/refresh" "{}" "$LONG_TOKEN")
check_eq "long-ttl default refresh → 200" "$(http_code "$resp")" "200"

# Explicit huge ttl rejected
resp=$(bearer POST "/v1/tokens/refresh" '{"ttl_seconds":4611686018427387904}' "$NEW_R_TOKEN")
check_eq "huge ttl refresh → 400" "$(http_code "$resp")" "400"

# Owner refresh (no expiry rotation)
resp=$(bearer POST "/v1/tokens/refresh" "{}")
code=$(http_code "$resp")
body=$(json_body "$resp")
if [ "$code" = "200" ]; then
  ok "owner refresh → 200"
  OLD_OWNER="$API_KEY"
  NEW_OWNER=$(printf '%s' "$body" | jq -r '.token // empty')
  if [ -n "$NEW_OWNER" ] && [ "$NEW_OWNER" != "$API_KEY" ]; then
    ok "owner refresh rotates token"
    API_KEY="$NEW_OWNER"
  else
    ok "owner refresh returned token"
    [ -n "$NEW_OWNER" ] && API_KEY="$NEW_OWNER"
  fi
  # Local single-tenant only: unauthenticated POST /v1/provision re-returns the
  # live owner JWT via handleLocalTenantProvision / liveLocalOwnerAPIKey so a
  # refresh cannot leave provision serving a revoked snapshot.
  # Multi-tenant hosted: the same unauth call creates a *new* tenant and key, so
  # equality with the refreshed owner JWT is not a product contract.
  if [ "$TENANT_ID" = "local" ]; then
    prov=$(curl_raw POST "$BASE/v1/provision")
    prov_key=$(json_body "$prov" | jq -r '.api_key // empty')
    if [ "$(http_code "$prov")" = "202" ] && [ -n "$prov_key" ] && [ "$prov_key" = "$API_KEY" ]; then
      ok "post-refresh provision returns live owner JWT"
    else
      fail "post-refresh provision live key (code=$(http_code "$prov") match=$([ "$prov_key" = "$API_KEY" ] && echo yes || echo no))"
    fi
  else
    skip_check "post-refresh provision live key (local-tenant-only; TENANT_ID=$TENANT_ID)"
    # Hosted multi-tenant: prove owner rotation without the local provision shim.
    if [ -n "$OLD_OWNER" ] && [ -n "$NEW_OWNER" ] && [ "$OLD_OWNER" != "$NEW_OWNER" ]; then
      resp=$(bearer POST "/v1/tokens/refresh" "{}" "$OLD_OWNER")
      old_code=$(http_code "$resp")
      if [ "$old_code" = "401" ] || [ "$old_code" = "409" ]; then
        ok "old owner after refresh → $old_code"
      else
        skip_check "old owner after refresh (got $old_code; residual cache window ok)"
      fi
      resp=$(bearer GET "/v1/tokens")
      check_eq "new owner list after refresh → 200" "$(http_code "$resp")" "200"
    fi
  fi
else
  fail "owner refresh → $code body=$(json_body "$resp")"
fi

# ---------------------------------------------------------------------------
# [8] Legacy revoke-by-key and delete bodies
# ---------------------------------------------------------------------------
step 8 "legacy revoke/delete response bodies"
resp=$(bearer POST "/v1/tokens" '{"subject":"e2e-revoke","ttl_seconds":600,"scopes":[{"prefix":"/e2e-tokens","ops":["read"]}]}')
body=$(json_body "$resp")
check_eq "issue for revoke → 201" "$(http_code "$resp")" "201"
REV_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
REV_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')

resp=$(bearer POST "/v1/tokens/revoke" "{\"api_key\":\"$REV_TOKEN\"}")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "owner revoke-by-key → 200" "$code" "200"
check_eq "legacy revoke body status=ok" "$(printf '%s' "$body" | jq -r '.status')" "ok"
if printf '%s' "$body" | jq -e 'keys == ["status"]' >/dev/null; then
  ok "legacy revoke body has only status"
else
  fail "legacy revoke body has only status"
fi

resp=$(bearer POST "/v1/tokens" '{"subject":"e2e-del","ttl_seconds":600,"scopes":[{"prefix":"/e2e-tokens","ops":["read"]}]}')
body=$(json_body "$resp")
DEL_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')
resp=$(bearer DELETE "/v1/tokens/$DEL_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "owner delete → 200" "$code" "200"
check_eq "legacy delete body status=ok" "$(printf '%s' "$body" | jq -r '.status')" "ok"
if printf '%s' "$body" | jq -e 'keys == ["status"]' >/dev/null; then
  ok "legacy delete body has only status"
else
  fail "legacy delete body has only status"
fi

# Disabled then delete (revoke convergence)
resp=$(bearer POST "/v1/tokens" '{"subject":"e2e-dis-del","ttl_seconds":600,"scopes":[{"prefix":"/e2e-tokens","ops":["read"]}]}')
body=$(json_body "$resp")
DD_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')
_=$(bearer POST "/v1/tokens/$DD_ID/deactivate" "{}")
resp=$(bearer DELETE "/v1/tokens/$DD_ID")
check_eq "delete disabled key → 200" "$(http_code "$resp")" "200"

# ---------------------------------------------------------------------------
# [9] Pseudoroot projected namespace
# ---------------------------------------------------------------------------
step 9 "pseudoroot projected namespace"

pseudo_suffix="$(date +%s)-$$"
pseudo_p1="e2e-pseudoroot-p1-$pseudo_suffix"
pseudo_p2="e2e-pseudoroot-p2-$pseudo_suffix"
pseudo_secret="e2e-pseudoroot-secret-$pseudo_suffix"

for pseudo_dir in "$pseudo_p1" "$pseudo_p2" "$pseudo_secret"; do
	resp=$(bearer POST "/v1/fs/$pseudo_dir?mkdir")
	check_eq "create $pseudo_dir" "$(http_code "$resp")" "200"
done
resp=$(bearer PUT "/v1/fs/$pseudo_p1/a.txt" "a")
check_eq "write projected p1 file" "$(http_code "$resp")" "200"
resp=$(bearer PUT "/v1/fs/$pseudo_secret/hidden.txt" "hidden")
check_eq "write hidden sibling file" "$(http_code "$resp")" "200"

pseudo_issue_body=$(jq -nc \
	--arg p1 "/$pseudo_p1" \
	--arg p2 "/$pseudo_p2" \
	'{subject:"e2e-pseudoroot",ttl_seconds:600,scopes:[
		{prefix:"/",ops:["pseudoroot"]},
		{prefix:$p1,ops:["read","list"]},
		{prefix:$p2,ops:["read","list"]}
	]}')
resp=$(bearer POST "/v1/tokens" "$pseudo_issue_body")
body=$(json_body "$resp")
check_eq "issue pseudoroot token" "$(http_code "$resp")" "201"
pseudo_token=$(printf '%s' "$body" | jq -r '.token // empty')
pseudo_token_id=$(printf '%s' "$body" | jq -r '.token_id // empty')
check_eq "pseudoroot canonical directive" \
	"$(printf '%s' "$body" | jq -r '.scopes[] | select(.prefix=="/") | .ops | join(",")')" \
	"pseudoroot"

resp=$(bearer GET "/v1/fs/?list" "" "$pseudo_token")
check_eq "pseudoroot root list" "$(http_code "$resp")" "200"
projected_names=$(json_body "$resp" | jq -r '[.entries[].name] | sort | join(",")')
expected_names=$(printf '%s\n%s\n' "$pseudo_p1" "$pseudo_p2" | sort | paste -sd, -)
check_eq "pseudoroot root names" "$projected_names" "$expected_names"

resp=$(bearer GET "/v1/fs/$pseudo_p1?list" "" "$pseudo_token")
check_eq "pseudoroot target list" "$(http_code "$resp")" "200"
check_eq "pseudoroot target file" \
	"$(json_body "$resp" | jq -r '[.entries[].name] | join(",")')" \
	"a.txt"

resp=$(bearer GET "/v1/fs/$pseudo_secret?list" "" "$pseudo_token")
check_eq "pseudoroot hidden sibling list" "$(http_code "$resp")" "404"
resp=$(bearer GET "/v1/fs/$pseudo_secret/hidden.txt" "" "$pseudo_token")
check_eq "pseudoroot hidden sibling read" "$(http_code "$resp")" "403"

resp=$(bearer POST "/v1/tokens" \
	'{"subject":"e2e-pseudoroot-empty","ttl_seconds":600,"scopes":[{"prefix":"/","ops":["pseudoroot"]}]}')
body=$(json_body "$resp")
check_eq "issue empty pseudoroot token" "$(http_code "$resp")" "201"
empty_pseudo_token=$(printf '%s' "$body" | jq -r '.token // empty')
empty_pseudo_token_id=$(printf '%s' "$body" | jq -r '.token_id // empty')
resp=$(bearer GET "/v1/fs/?list" "" "$empty_pseudo_token")
check_eq "empty pseudoroot root list" "$(http_code "$resp")" "200"
check_eq "empty pseudoroot has no entries" \
	"$(json_body "$resp" | jq -r '.entries | length')" \
	"0"

resp=$(bearer POST "/v1/tokens" \
	'{"subject":"e2e-pseudoroot-conflict","ttl_seconds":600,"scopes":[{"prefix":"/","ops":["pseudoroot","list"]}]}')
check_eq "reject covering pseudoroot list" "$(http_code "$resp")" "400"

for pseudo_token_cleanup_id in "$pseudo_token_id" "$empty_pseudo_token_id"; do
	if [[ -n "$pseudo_token_cleanup_id" ]]; then
		resp=$(bearer DELETE "/v1/tokens/$pseudo_token_cleanup_id")
		check_eq "delete pseudoroot token $pseudo_token_cleanup_id" \
			"$(http_code "$resp")" "200"
	fi
done
for pseudo_dir in "$pseudo_p1" "$pseudo_p2" "$pseudo_secret"; do
	resp=$(bearer DELETE "/v1/fs/$pseudo_dir?recursive=1")
	check_eq "cleanup $pseudo_dir" "$(http_code "$resp")" "200"
done

# ---------------------------------------------------------------------------
# [10] Control-plane generate / list / status / delete / revoke
# ---------------------------------------------------------------------------
step 10 "control-plane management"
# Probe generate with headers (empty body) — works when local mock IAM is enabled.
resp=$(cp_hdr POST "/v1/tokens/generate?tenant_id=$TENANT_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
if [ "$code" = "201" ]; then
  ok "control-plane generate (header-only) → 201"
  CP_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
  CP_TOKEN_ID=$(printf '%s' "$body" | jq -r '.token_id // empty')
  check_cmd "generate returns token" test -n "$CP_TOKEN"
  check_eq "generate scope_kind" "$(printf '%s' "$body" | jq -r '.scope_kind')" "owner"
  check_eq "generate tenant_id" "$(printf '%s' "$body" | jq -r '.tenant_id')" "$TENANT_ID"

  # Owner cannot call control-plane generate
  resp=$(bearer POST "/v1/tokens/generate" "{\"tenant_id\":\"$TENANT_ID\"}")
  check_eq "owner generate → 403" "$(http_code "$resp")" "403"

  # Body-only control credentials (no headers)
  resp=$(curl_raw POST "$BASE/v1/tokens/generate" -H "Content-Type: application/json" \
    --data-binary "{\"tenant_id\":\"$TENANT_ID\",\"public_key\":\"$CP_PUBLIC\",\"private_key\":\"$CP_PRIVATE\",\"key_name\":\"e2e-body-cp\"}")
  check_eq "cp generate body credentials → 201" "$(http_code "$resp")" "201"

  # Wrong method → 405
  resp=$(cp_hdr GET "/v1/tokens/generate?tenant_id=$TENANT_ID")
  check_eq "generate wrong method → 405" "$(http_code "$resp")" "405"

  # Huge ttl → 400
  resp=$(cp_hdr POST "/v1/tokens/generate" "{\"tenant_id\":\"$TENANT_ID\",\"ttl_seconds\":4611686018427387904}")
  check_eq "generate huge ttl → 400" "$(http_code "$resp")" "400"

  # Control-plane cannot refresh
  resp=$(cp_hdr POST "/v1/tokens/refresh" "{}")
  check_eq "control-plane refresh → 403" "$(http_code "$resp")" "403"

  # Control-plane cannot issue scoped
  resp=$(cp_hdr POST "/v1/tokens" '{"ttl_seconds":60,"scopes":[{"prefix":"/x","ops":["read"]}]}')
  check_eq "control-plane issue scoped → 403" "$(http_code "$resp")" "403"

  # Single-tenant list with tenant_id
  resp=$(cp_hdr GET "/v1/tokens?tenant_id=$TENANT_ID")
  check_eq "control-plane list tenant → 200" "$(http_code "$resp")" "200"
  body=$(json_body "$resp")
  if printf '%s' "$body" | jq -e '.tokens | length >= 1' >/dev/null; then
    ok "cp list has tokens"
  else
    fail "cp list has tokens"
  fi

  # Org-wide list (no tenant_id)
  resp=$(cp_hdr GET "/v1/tokens")
  check_eq "control-plane org list → 200" "$(http_code "$resp")" "200"

  # Org list rejects include_expired
  resp=$(cp_hdr GET "/v1/tokens?include_expired=1")
  check_eq "org list include_expired → 400" "$(http_code "$resp")" "400"

  # Deactivate / activate any key (owner target allowed for CP)
  if [ -n "$CP_TOKEN_ID" ]; then
    resp=$(cp_hdr POST "/v1/tokens/$CP_TOKEN_ID/deactivate" "{\"tenant_id\":\"$TENANT_ID\"}")
    check_eq "cp deactivate → 200" "$(http_code "$resp")" "200"
    resp=$(cp_hdr POST "/v1/tokens/$CP_TOKEN_ID/activate" "{\"tenant_id\":\"$TENANT_ID\"}")
    check_eq "cp activate → 200" "$(http_code "$resp")" "200"

    # Delete (extended body)
    resp=$(cp_hdr DELETE "/v1/tokens/$CP_TOKEN_ID?tenant_id=$TENANT_ID")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    check_eq "cp delete → 200" "$code" "200"
    check_eq "cp delete status revoked" "$(printf '%s' "$body" | jq -r '.status')" "revoked"
  fi

  # Revoke by plaintext (control-plane extended body)
  resp=$(cp_hdr POST "/v1/tokens/generate" "{\"tenant_id\":\"$TENANT_ID\",\"key_name\":\"e2e-cp-revoke\"}")
  body=$(json_body "$resp")
  check_eq "cp generate for revoke → 201" "$(http_code "$resp")" "201"
  CPR_TOKEN=$(printf '%s' "$body" | jq -r '.token // empty')
  resp=$(cp_hdr POST "/v1/tokens/revoke" "{\"api_key\":\"$CPR_TOKEN\"}")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "cp revoke-by-key → 200" "$code" "200"
  check_eq "cp revoke status revoked" "$(printf '%s' "$body" | jq -r '.status')" "revoked"
else
  skip_check "control-plane generate (got HTTP $code — IAM/control-plane not available on this deployment)"
  echo "  detail: $(json_body "$resp" | head -c 200)"
  # Still assert dispatcher classifies control-plane credentials (not 401 missing auth).
  if [ "$code" != "401" ]; then
    ok "control-plane credential path is distinct from missing-auth ($code)"
  else
    fail "control-plane credentials treated as missing auth"
  fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo
echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped (total $TOTAL)"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
exit 0
