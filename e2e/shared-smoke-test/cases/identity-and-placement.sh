#!/usr/bin/env bash
# identity-and-placement: shared provider + placement/schema_shape + co-location.
# Requires: DRIVE9_BASE; DRIVE9_META_DSN strongly recommended.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

require_base
require_cmds curl jq

print_env_banner "case: identity-and-placement"

step "provision" "two shared tenants A/B"
if ! A_LINE="$(provision_tenant)"; then
  fail "provision A"
  summary; exit 1
fi
A_TENANT="${A_LINE%%$'\t'*}"; A_KEY="${A_LINE#*$'\t'}"
ok "A tenant_id=$A_TENANT"

if ! B_LINE="$(provision_tenant)"; then
  fail "provision B"
  summary; exit 1
fi
B_TENANT="${B_LINE%%$'\t'*}"; B_KEY="${B_LINE#*$'\t'}"
ok "B tenant_id=$B_TENANT"

step "meta" "provider + placement"
if meta_available; then
  for tid in "$A_TENANT" "$B_TENANT"; do
    p="$(meta_tenant_provider "$tid" || true)"
    check_eq "$tid provider" "$p" "tidb_cloud_native_shared"
    pl="$(meta_placement "$tid" || true)"
    if [ -z "$pl" ]; then
      fail "$tid missing placement"
    else
      placement="$(echo "$pl" | awk '{print $2}')"
      shape="$(echo "$pl" | awk '{print $3}')"
      check_eq "$tid placement" "$placement" "shared"
      check_eq "$tid schema_shape" "$shape" "shared"
      info "$tid placement: $pl"
    fi
    if meta_has_org_binding "$tid"; then
      fail "$tid still has dedicated tenant_tidbcloud_org_bindings"
    else
      ok "$tid has no dedicated org binding row"
    fi
  done
  A_DB="$(meta_db_id "$A_TENANT" || true)"
  B_DB="$(meta_db_id "$B_TENANT" || true)"
  if [ -n "$A_DB" ] && [ "$A_DB" = "$B_DB" ]; then
    ok "A and B co-located on db_id=$A_DB"
  elif [ -n "$A_DB" ] && [ -n "$B_DB" ]; then
    info "A db_id=$A_DB B db_id=$B_DB (ok if soft-cap forced a second pool)"
    ok "both tenants placed on some db_pool"
  else
    fail "could not resolve db_ids"
  fi
  for tid in "$A_TENANT" "$B_TENANT"; do
    db="$(meta_db_id "$tid" || true)"
    st="$(meta_pool_status "$db" || true)"
    check_eq "db_pool $db status active" "$st" "active"
  done
else
  skip_check "meta unavailable (set DRIVE9_META_DSN + mysql client)"
fi

step "cleanup" "delete test tenants"
delete_tenant "$A_KEY"
delete_tenant "$B_KEY"
ok "deleted A/B (best-effort)"

summary
exit $?
