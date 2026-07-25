#!/usr/bin/env bash
# lifecycle-delete-and-count: delete tenant → placement gone, tenant_count drops, peer intact.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

require_base
require_cmds curl jq

print_env_banner "case: lifecycle-delete-and-count"

step "provision" "tenants A and B"
if ! A_LINE="$(provision_tenant)"; then fail "provision A"; summary; exit 1; fi
A_TENANT="${A_LINE%%$'\t'*}"; A_KEY="${A_LINE#*$'\t'}"
if ! B_LINE="$(provision_tenant)"; then fail "provision B"; summary; exit 1; fi
B_TENANT="${B_LINE%%$'\t'*}"; B_KEY="${B_LINE#*$'\t'}"
ok "A=$A_TENANT B=$B_TENANT"

A_DB=""
COUNT_BEFORE=""
if meta_available; then
  A_DB="$(meta_db_id "$A_TENANT" || true)"
  COUNT_BEFORE="$(meta_tenant_count "$A_DB" 2>/dev/null || true)"
  info "A db_id=$A_DB tenant_count=$COUNT_BEFORE"
else
  skip_check "meta unavailable for count baseline"
fi

step "delete" "tenant A"
line="$(http_json DELETE /v1/tenant "$A_KEY" '{}')"
code="${line%%$'\t'*}"
if [ "$code" = "202" ] || [ "$code" = "200" ]; then
  ok "DELETE /v1/tenant A http=$code"
else
  fail "DELETE A http=$code ${line#*$'\t'}"
fi
sleep 2

line="$(http_json GET /v1/status "$A_KEY")"
code="${line%%$'\t'*}"
if [ "$code" = "401" ] || [ "$code" = "403" ]; then
  ok "A key revoked http=$code"
else
  fail "A key still usable http=$code"
fi

line="$(http_json GET /v1/status "$B_KEY")"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "B still active HTTP" "$code" "200"
check_eq "B status active" "$(json_field "$body" status)" "active"

if meta_available && [ -n "$A_DB" ]; then
  pl="$(meta_placement "$A_TENANT" || true)"
  if [ -z "$pl" ]; then
    ok "A placement removed"
  else
    fail "A placement still present: $pl"
  fi
  COUNT_AFTER="$(meta_tenant_count "$A_DB" || true)"
  if [ -n "$COUNT_BEFORE" ] && [ -n "$COUNT_AFTER" ]; then
    if [ "$COUNT_AFTER" -lt "$COUNT_BEFORE" ]; then
      ok "tenant_count $COUNT_BEFORE -> $COUNT_AFTER"
    else
      fail "tenant_count did not decrease ($COUNT_BEFORE -> $COUNT_AFTER)"
    fi
  fi
fi

delete_tenant "$B_KEY"
summary
exit $?
