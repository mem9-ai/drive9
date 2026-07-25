#!/usr/bin/env bash
# soft-cap-and-new-pool: with small soft cap, overflowing tenants open a new db_pool
# (or latch soft_cap / hard over-fill on the current pool).
# Best run self-contained with SOFT_CAP=2 (default harness).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

require_base
require_cmds curl jq

print_env_banner "case: soft-cap-and-new-pool"

if ! meta_available; then
  skip_check "meta required for soft-cap case"
  summary
  exit 0
fi

declare -a TIDS KEYS DBS

cleanup_tenants() {
  local k
  for k in "${KEYS[@]+"${KEYS[@]}"}"; do
    delete_tenant "$k"
  done
}
trap cleanup_tenants EXIT

step "baseline" "active shared pool count"
POOLS_BEFORE="$(meta_active_shared_pool_count || echo 0)"
info "active pools before=$POOLS_BEFORE"

# Provision soft_cap+1 tenants (SOFT_CAP=2 → 3 tenants).
# With HARD_CAP_RATIO≈1.5 this may hard-overfill one pool and/or open a second.
N=$((SOFT_CAP + 1))
if [ "$N" -lt 2 ]; then N=3; fi
info "provisioning $N tenants (soft_cap=$SOFT_CAP)"

for i in $(seq 1 "$N"); do
  if ! LINE="$(provision_tenant)"; then
    fail "provision tenant $i"
    summary
    exit 1
  fi
  TIDS+=("${LINE%%$'\t'*}")
  KEYS+=("${LINE#*$'\t'}")
  ok "tenant $i id=${TIDS[$((i - 1))]}"
  DBS+=("$(meta_db_id "${TIDS[$((i - 1))]}" || true)")
done

step "assert" "multiple pools or soft_cap latch"
UNIQUE_DBS="$(printf '%s\n' "${DBS[@]}" | sort -u | grep -c . || true)"
info "unique db_ids=${UNIQUE_DBS}: ${DBS[*]}"
if [ "${UNIQUE_DBS:-0}" -ge 2 ]; then
  ok "overflow created/used >=2 physical pools"
else
  # With packing/hard over-fill, first pool may latch; check soft_cap_reached.
  db0="${DBS[0]}"
  if [ -n "$db0" ]; then
    reached="$(meta_soft_cap_reached "$db0" || true)"
    cnt="$(meta_tenant_count "$db0" || true)"
    max="$(meta_max_tenants "$db0" || true)"
    info "db=$db0 count=$cnt max=$max soft_cap_reached=$reached"
    if [ "$reached" = "1" ] || { [ -n "$max" ] && [ -n "$cnt" ] && [ "$cnt" -ge "$max" ]; }; then
      ok "single pool latched at soft cap (count=$cnt max=$max)"
    else
      fail "expected second pool or soft_cap latch (unique_dbs=$UNIQUE_DBS count=$cnt max=$max)"
    fi
  else
    fail "no db_id resolved"
  fi
fi

POOLS_AFTER="$(meta_active_shared_pool_count || echo 0)"
info "active pools after=$POOLS_AFTER"
if [ "${POOLS_AFTER:-0}" -ge "${POOLS_BEFORE:-0}" ]; then
  ok "pool inventory non-decreasing ($POOLS_BEFORE -> $POOLS_AFTER)"
else
  fail "pool count dropped ($POOLS_BEFORE -> $POOLS_AFTER)"
fi

step "cleanup" "delete test tenants"
cleanup_tenants
trap - EXIT
ok "deleted $N tenants"

summary
exit $?
