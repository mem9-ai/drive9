#!/usr/bin/env bash
# gates-fork-sql: shared capability gates (fork 409, sql 400).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

require_base
require_cmds curl jq

print_env_banner "case: gates-fork-sql"

step "provision" "one shared tenant"
if ! LINE="$(provision_tenant)"; then
  fail "provision"
  summary; exit 1
fi
TID="${LINE%%$'\t'*}"; KEY="${LINE#*$'\t'}"
ok "tenant_id=$TID"

step "gates" "fork + sql"
line="$(http_json POST /v1/fork "$KEY" '{}')"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "shared fork HTTP" "$code" "409"
if echo "$body" | grep -qi 'shared-pool\|shared pool\|not supported'; then
  ok "fork body mentions shared unsupported"
else
  fail "fork unexpected body: $body"
fi

line="$(http_json POST /v1/sql "$KEY" '{"query":"SELECT 1"}')"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "shared sql HTTP" "$code" "400"
if echo "$body" | grep -qi 'shared-schema\|not supported'; then
  ok "sql body mentions shared-schema unsupported"
else
  fail "sql unexpected body: $body"
fi

delete_tenant "$KEY"
summary
exit $?
