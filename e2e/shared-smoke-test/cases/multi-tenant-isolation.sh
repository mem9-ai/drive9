#!/usr/bin/env bash
# multi-tenant-isolation: cross-tenant fs isolation on shared (same paths, list/grep/find/mutations).
# Multi-tenant data plane — not single-tenant api/cli suite coverage.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/lib.sh"

require_base
require_cmds curl jq python3

TS="$(date +%s)"
print_env_banner "case: multi-tenant-isolation"

step "provision" "tenants A and B"
if ! A_LINE="$(provision_tenant)"; then fail "provision A"; summary; exit 1; fi
A_TENANT="${A_LINE%%$'\t'*}"; A_KEY="${A_LINE#*$'\t'}"
if ! B_LINE="$(provision_tenant)"; then fail "provision B"; summary; exit 1; fi
B_TENANT="${B_LINE%%$'\t'*}"; B_KEY="${B_LINE#*$'\t'}"
ok "A=$A_TENANT B=$B_TENANT"

step "same-path" "writes are tenant-scoped"
code="$(put_raw "a-${TS}.txt" "$A_KEY" "hello from A")"
check_eq "A write" "$code" "200"
code="$(put_raw "a-${TS}.txt" "$B_KEY" "hello from B")"
check_eq "B write same path" "$code" "200"
line="$(get_raw "a-${TS}.txt" "$A_KEY")"
check_eq "A reads own" "${line#*$'\t'}" "hello from A"
line="$(get_raw "a-${TS}.txt" "$B_KEY")"
check_eq "B reads own" "${line#*$'\t'}" "hello from B"

# Single mkdir — a second POST on the same path returns 409 (already exists).
code="$(curl -sS -o /dev/null -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $A_KEY" \
  "${BASE}/v1/fs/docs-${TS}?mkdir")"
check_eq "A mkdir" "$code" "200"
code="$(put_raw "docs-${TS}/note.txt" "$A_KEY" "nested note")"
check_eq "A write nested" "$code" "200"
line="$(get_raw "docs-${TS}/note.txt" "$B_KEY")"
check_eq "B cannot read A nested" "${line%%$'\t'*}" "404"

step "list-grep-find" "no cross-tenant leakage"
MARKER="shared-iso-A-${TS}"
code="$(put_raw "docs-${TS}/secret.txt" "$A_KEY" "$MARKER")"
check_eq "A write marker" "$code" "200"

line="$(http_json GET "/v1/fs/?list" "$B_KEY")"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "B list 200" "$code" "200"
check_eq "B list has no docs-${TS}" "$(list_has_name "$body" "docs-${TS}")" "false"

line="$(http_json GET "/v1/fs/?grep=${MARKER}&limit=20" "$B_KEY")"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "B grep 200" "$code" "200"
check_eq "B grep misses A path" "$(search_has_path "$body" "/docs-${TS}/secret.txt")" "false"

line="$(http_json GET "/v1/fs/?find=&name=note.txt" "$B_KEY")"
code="${line%%$'\t'*}"; body="${line#*$'\t'}"
check_eq "B find note 200" "$code" "200"
check_eq "B find misses A note" "$(search_has_path "$body" "/docs-${TS}/note.txt")" "false"

step "mutations" "cross-tenant copy/delete denied"
code="$(http_hdr POST "/v1/fs/stolen-${TS}.txt?copy" "$B_KEY" "X-Dat9-Copy-Source" "/docs-${TS}/note.txt")"
check_eq "B copy from A → 404" "$code" "404"
code="$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE \
  -H "Authorization: Bearer $B_KEY" \
  "${BASE}/v1/fs/docs-${TS}/note.txt")"
check_eq "B delete A path → 404" "$code" "404"
line="$(get_raw "docs-${TS}/note.txt" "$A_KEY")"
check_eq "A note intact" "${line#*$'\t'}" "nested note"

step "same-path-delete" "B delete own does not wipe A"
code="$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE \
  -H "Authorization: Bearer $B_KEY" \
  "${BASE}/v1/fs/a-${TS}.txt")"
check_eq "B delete own" "$code" "200"
line="$(get_raw "a-${TS}.txt" "$A_KEY")"
check_eq "A same path intact" "${line#*$'\t'}" "hello from A"

delete_tenant "$A_KEY"
delete_tenant "$B_KEY"
summary
exit $?
