#!/usr/bin/env bash
# metrics-consistency: sample /metrics for shared db pool series after provision.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

metrics_has_tenant_labeled_control_success() {
  local text="$1"
  printf '%s\n' "$text" | grep -Eq '^drive9_tenant_requests_total\{[^}]*status_class="2xx"[^}]*surface="(provision|status|quota|tenant|tokens|vault|other)"[^}]*tenant_id='
}

require_base
require_cmds curl jq

print_env_banner "case: metrics-consistency"

if ! metrics_available; then
  skip_check "curl unavailable"
  summary; exit 0
fi

step "provision" "one tenant to populate metrics"
if ! LINE="$(provision_tenant)"; then
  fail "provision"
  summary; exit 1
fi
TID="${LINE%%$'\t'*}"; KEY="${LINE#*$'\t'}"
ok "tenant_id=$TID"
sleep 1

step "metrics" "shared db pool gauges present"
TEXT="$(metrics_get)"
if [ -z "$TEXT" ]; then
  skip_check "/metrics empty or not exposed"
else
	if metrics_has_tenant_labeled_control_success "$TEXT"; then
		fail "tenant_requests_total control-plane 2xx series unexpectedly carries tenant labels"
	else
		ok "tenant_requests_total control-plane 2xx series is aggregate-only"
	fi

  if metrics_has_series "$TEXT" "drive9_shared_db_pool"; then
    ok "drive9_shared_db_pool_* series present"
  else
    # Older builds may only have tenant gauges
    if metrics_has_series "$TEXT" "drive9_tenant_count"; then
      ok "drive9_tenant_count present (shared series optional on this build)"
    else
      fail "no shared_db_pool or tenant_count metrics found"
    fi
  fi
  if meta_available; then
    db="$(meta_db_id "$TID" || true)"
    cnt="$(meta_tenant_count "$db" || true)"
    info "meta db_id=$db tenant_count=$cnt"
    if [ -n "$cnt" ] && metrics_has_series "$TEXT" "drive9_shared_db_pool_tenants"; then
      ok "meta count available and metrics expose shared pool tenants"
    else
      skip_check "could not cross-check meta count vs metrics tenants gauge"
    fi
  else
    skip_check "meta unavailable for cross-check"
  fi
fi

delete_tenant "$KEY"
summary
exit $?
