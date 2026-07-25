#!/usr/bin/env bash
# Shared helpers for shared-smoke-test (meta + metrics + provision gates).
# shellcheck disable=SC2034,SC2155

set -euo pipefail

SHARED_SMOKE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SHARED_SMOKE_LIB_DIR/../.." && pwd)"

BASE="${DRIVE9_BASE:-${BASE:-}}"
META_DSN="${DRIVE9_META_DSN:-${META_DSN:-}}"
PUBLIC_KEY="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-${DRIVE9_PUBLIC_KEY:-local}}"
PRIVATE_KEY="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-${DRIVE9_PRIVATE_KEY:-local}}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-300}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
SOFT_CAP="${SHARED_SOFT_CAP:-${DRIVE9_TIDBCLOUD_NATIVE_SHARED_MAX_TENANTS:-2}}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

step() {
  local title="${1:-}"
  local detail="${2:-}"
  echo -e "\n${YELLOW}[${title}]${RESET} ${detail}"
}
info() { echo -e "${CYAN}  ->${RESET} $*"; }

ok() {
  TOTAL=$((TOTAL + 1))
  PASS=$((PASS + 1))
  echo -e "${GREEN}  PASS${RESET} $*"
}

fail() {
  TOTAL=$((TOTAL + 1))
  FAIL=$((FAIL + 1))
  echo -e "${RED}  FAIL${RESET} $*"
}

skip_check() {
  TOTAL=$((TOTAL + 1))
  SKIP=$((SKIP + 1))
  echo -e "${YELLOW}  SKIP${RESET} $*"
}

check_eq() {
  local desc="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
  else
    fail "$desc (want=$want got=$got)"
  fi
}

require_base() {
  if [ -z "$BASE" ]; then
    echo "DRIVE9_BASE is required" >&2
    exit 1
  fi
  BASE="${BASE%/}"
}

require_cmds() {
  local c
  for c in "$@"; do
    if ! command -v "$c" >/dev/null 2>&1; then
      echo "required command not found: $c" >&2
      exit 1
    fi
  done
}

summary() {
  echo ""
  echo "========================================================"
  echo "  RESULTS: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
  echo "========================================================"
  if [ "$FAIL" -gt 0 ]; then
    return 1
  fi
  return 0
}

# http_json METHOD PATH [API_KEY] [BODY]
# Prints: code\tbody
http_json() {
  local method="$1" path="$2"
  local key="${3:-}"
  local body="${4:-}"
  local args=(-sS -X "$method" -w $'\t%{http_code}')
  args+=(-H "Content-Type: application/json")
  if [ -n "$key" ]; then
    args+=(-H "Authorization: Bearer $key")
  fi
  if [ -n "${PUBLIC_KEY}" ] && [ -n "${PRIVATE_KEY}" ]; then
    args+=(-H "X-TiDBCloud-Public-Key: ${PUBLIC_KEY}")
    args+=(-H "X-TiDBCloud-Private-Key: ${PRIVATE_KEY}")
  fi
  if [ -n "$body" ]; then
    args+=(-d "$body")
  fi
  local out
  out="$(curl "${args[@]}" "${BASE}${path}")"
  local code="${out##*$'\t'}"
  local resp="${out%$'\t'*}"
  printf '%s\t%s\n' "$code" "$resp"
}

json_field() {
  local json="$1" field="$2"
  printf '%s' "$json" | jq -r --arg f "$field" 'if type=="object" then .[$f] // empty else empty end'
}

wait_active() {
  local key="$1"
  local deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  local status=""
  while :; do
    local line code body
    line="$(http_json GET /v1/status "$key")"
    code="${line%%$'\t'*}"
    body="${line#*$'\t'}"
    status="$(json_field "$body" status)"
    info "status=$status http=$code"
    if [ "$code" = "200" ] && [ "$status" = "active" ]; then
      echo "$status"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "$status"
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
}

provision_tenant() {
  local body="{}"
  if [ -n "$PUBLIC_KEY" ] && [ -n "$PRIVATE_KEY" ]; then
    body="$(jq -n --arg pk "$PUBLIC_KEY" --arg sk "$PRIVATE_KEY" \
      '{public_key:$pk, private_key:$sk}')"
  fi
  local line code resp
  line="$(http_json POST /v1/provision "" "$body")"
  code="${line%%$'\t'*}"
  resp="${line#*$'\t'}"
  if [ "$code" != "202" ] && [ "$code" != "200" ]; then
    echo "provision failed http=$code body=$resp" >&2
    return 1
  fi
  local key tid st
  key="$(json_field "$resp" api_key)"
  tid="$(json_field "$resp" tenant_id)"
  st="$(json_field "$resp" status)"
  if [ -z "$key" ] || [ -z "$tid" ]; then
    echo "provision missing api_key/tenant_id: $resp" >&2
    return 1
  fi
  if [ "$st" != "active" ]; then
    if ! wait_active "$key" >/dev/null; then
      echo "tenant $tid did not become active" >&2
      return 1
    fi
  fi
  printf '%s\t%s\n' "$tid" "$key"
}

delete_tenant() {
  local key="$1"
  http_json DELETE /v1/tenant "$key" '{}' >/dev/null || true
}

meta_available() {
  [ -n "$META_DSN" ] && command -v mysql >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1
}

meta_query() {
  local sql="$1"
  if ! meta_available; then
    echo "META_DSN or mysql/python3 unavailable" >&2
    return 1
  fi
  python3 - "$META_DSN" "$sql" <<'PY'
import re, subprocess, sys
dsn, sql = sys.argv[1], sys.argv[2]
m = re.match(r"(?P<user>[^:]+):(?P<pw>.*)@tcp\((?P<host>[^:]+):(?P<port>\d+)\)/(?P<db>[^?]+)", dsn)
if not m:
    m = re.match(r"(?P<user>[^:]+):(?P<pw>.*)@(?P<host>[^:]+):(?P<port>\d+)/(?P<db>[^?]+)", dsn)
if not m:
    sys.stderr.write(f"cannot parse META_DSN: {dsn!r}\n")
    sys.exit(2)
g = m.groupdict()
cmd = [
    "mysql", "-N", "-B",
    "-h", g["host"], "-P", g["port"],
    "-u", g["user"], f"-p{g['pw']}",
    g["db"], "-e", sql,
]
p = subprocess.run(cmd, capture_output=True, text=True)
sys.stdout.write(p.stdout)
if p.returncode != 0:
    sys.stderr.write(p.stderr)
    sys.exit(p.returncode)
PY
}

meta_tenant_provider() {
  local tid="$1"
  meta_query "SELECT provider FROM tenants WHERE id='${tid//\'/\\\'}' LIMIT 1;" | tr -d '[:space:]'
}

meta_placement() {
  local tid="$1"
  meta_query "
SELECT p.db_id, p.placement, p.schema_shape, p.status
FROM tenant_placements p
JOIN fs_registry f ON f.fs_id = p.fs_id
WHERE f.tenant_id='${tid//\'/\\\'}'
LIMIT 1;"
}

meta_db_id() {
  local tid="$1"
  meta_placement "$tid" | awk '{print $1}'
}

meta_tenant_count() {
  local db_id="$1"
  meta_query "SELECT tenant_count FROM db_pool WHERE db_id=${db_id};" | tr -d '[:space:]'
}

meta_soft_cap_reached() {
  local db_id="$1"
  meta_query "SELECT soft_cap_reached FROM db_pool WHERE db_id=${db_id};" | tr -d '[:space:]'
}

meta_max_tenants() {
  local db_id="$1"
  meta_query "SELECT max_tenants FROM db_pool WHERE db_id=${db_id};" | tr -d '[:space:]'
}

meta_pool_status() {
  local db_id="$1"
  meta_query "SELECT status FROM db_pool WHERE db_id=${db_id};" | tr -d '[:space:]'
}

meta_active_shared_pool_count() {
  meta_query "SELECT COUNT(*) FROM db_pool WHERE status='active';" | tr -d '[:space:]'
}

meta_has_org_binding() {
  local tid="$1"
  local n
  n="$(meta_query "SELECT COUNT(*) FROM tenant_tidbcloud_org_bindings WHERE tenant_id='${tid//\'/\\\'}';" | tr -d '[:space:]')"
  [ "${n:-0}" != "0" ]
}

metrics_available() {
  command -v curl >/dev/null 2>&1
}

# Fetch /metrics text (best-effort).
metrics_get() {
  curl -sS "${BASE}/metrics" 2>/dev/null || curl -sS "${BASE}/v1/metrics" 2>/dev/null || true
}

metrics_has_series() {
  local text="$1" needle="$2"
  printf '%s' "$text" | grep -q "$needle"
}

# Raw body PUT (filesystem). Prints http_code only; body discarded.
put_raw() {
  local path="$1" key="$2" data="$3"
  curl -sS -o /tmp/shared-smoke-put.out -w "%{http_code}" -X PUT \
    -H "Authorization: Bearer $key" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "$data" \
    "${BASE}/v1/fs/${path#/}"
}

get_raw() {
  local path="$1" key="$2"
  local tmp code
  tmp="$(mktemp)"
  code="$(curl -sS -o "$tmp" -w "%{http_code}" -X GET \
    -H "Authorization: Bearer $key" \
    "${BASE}/v1/fs/${path#/}")"
  printf '%s\t%s\n' "$code" "$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
}

# Extra headers: name value pairs after key/body
http_hdr() {
  local method="$1" path="$2" key="$3"
  shift 3
  local args=(-sS -o /tmp/shared-smoke-hdr.out -w "%{http_code}" -X "$method")
  args+=(-H "Authorization: Bearer $key")
  while [ "$#" -ge 2 ]; do
    args+=(-H "$1: $2")
    shift 2
  done
  curl "${args[@]}" "${BASE}${path}"
}

list_has_name() {
  local body="$1" name="$2"
  printf '%s' "$body" | python3 -c '
import json,sys
name=sys.argv[1]
try:
  d=json.load(sys.stdin)
  entries=d.get("entries") if isinstance(d,dict) else None
  if not isinstance(entries,list):
    print("false"); raise SystemExit
  for e in entries:
    n=e.get("name") if isinstance(e,dict) else None
    if n==name or (isinstance(n,str) and n.rstrip("/")==name.rstrip("/")):
      print("true"); raise SystemExit
  print("false")
except Exception:
  print("false")
' "$name"
}

search_has_path() {
  local body="$1" path="$2"
  printf '%s' "$body" | python3 -c '
import json,sys
want=sys.argv[1].lstrip("/")
try:
  d=json.load(sys.stdin)
  items=d if isinstance(d,list) else d.get("results") or d.get("entries") or d.get("hits") or []
  if not isinstance(items,list):
    print("false"); raise SystemExit
  def norm(p):
    return (p or "").lstrip("/")
  print("true" if any(isinstance(e,dict) and norm(e.get("path"))==want for e in items) else "false")
except Exception:
  print("false")
' "$path"
}

print_env_banner() {
  local title="$1"
  echo "========================================================"
  echo "  $title"
  echo "  BASE     : ${BASE:-<unset>}"
  echo "  META_DSN : $([ -n "$META_DSN" ] && echo set || echo unset)"
  echo "  SOFT_CAP : $SOFT_CAP"
  echo "  CLOUD_KEY: $([ -n "$PUBLIC_KEY" ] && [ -n "$PRIVATE_KEY" ] && echo set || echo unset)"
  echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "========================================================"
}
