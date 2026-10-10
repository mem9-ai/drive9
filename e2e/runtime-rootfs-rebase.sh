#!/usr/bin/env bash
# Paired cross-repository proof that one Drive9 full-root change layer keeps
# overlaying the workspace after the server restarts with a different base
# image/profile. The server workflow runs phase=seed with image A, then
# phase=verify with image B against the same TiDB and object-store state.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
PHASE="${DRIVE9_RUNTIME_REBASE_PHASE:-}"
STATE_FILE="${DRIVE9_RUNTIME_REBASE_STATE_FILE:-}"
ROOT="${DRIVE9_RUNTIME_REBASE_ROOT:-/runtime-rootfs-rebase}"
API_KEY=""
KEEP_ROOT=0

fail() {
  printf 'FAIL runtime rootfs rebase (%s): %s\n' "$PHASE" "$1" >&2
  exit 1
}

cleanup() {
  if [ "$KEEP_ROOT" = 0 ] && [ -n "$API_KEY" ]; then
    curl -sS --max-time 20 -X DELETE \
      -H "Authorization: Bearer ${API_KEY}" \
      "${BASE}/v1/fs/${ROOT#/}?recursive" >/dev/null 2>&1 || true
  fi
  if [ "$KEEP_ROOT" = 0 ] && [ -n "$STATE_FILE" ]; then
    rm -f "$STATE_FILE"
  fi
}
trap cleanup EXIT

wait_for_tenant() {
  local deadline status
  deadline=$((SECONDS + 120))
  while :; do
    status="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
      "${BASE}/v1/status" | jq -r '.status // empty')" || status=""
    [ "$status" = active ] && return
    [ "$SECONDS" -lt "$deadline" ] || fail "tenant did not become active"
    sleep 2
  done
}

require_v6() {
  curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/runtime/capabilities" | jq -e \
      'any(.providers[].candidates[];
        .persistence == "full_root"
        and .rootfs.capability_version == "drive9_rootfs.user_union.extent.v6")' \
      >/dev/null || fail "Runtime does not advertise full-root capability v6"
}

[ -x "$CLI_BIN" ] || fail "DRIVE9_CLI_BIN is not executable"
[ "$PHASE" = seed ] || [ "$PHASE" = verify ] || fail "phase must be seed or verify"
[ -n "$STATE_FILE" ] || fail "DRIVE9_RUNTIME_REBASE_STATE_FILE is required"

if [ "$PHASE" = seed ]; then
  provision_json="$(curl -fsS --max-time 30 -X POST "${BASE}/v1/provision")" \
    || fail "tenant provision failed"
  API_KEY="$(printf '%s' "$provision_json" | jq -r '.api_key // empty')"
  [ -n "$API_KEY" ] || fail "tenant provision returned no API key"
  umask 077
  printf '%s' "$API_KEY" >"$STATE_FILE"
  chmod 0600 "$STATE_FILE"
  wait_for_tenant
  require_v6
  curl -fsS --max-time 20 -X POST -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/fs/${ROOT#/}?mkdir" >/dev/null || fail "workspace root creation failed"

  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
    --workspace "$ROOT" --timeout 30s -- \
    /bin/sh -c 'set -eu
      [ "$(cat /etc/drive9-base-id)" = image-a ]
      [ "$(cat /etc/drive9-rebase-copyup)" = image-a-lower ]
      [ "$(cat /etc/drive9-rebase-whiteout)" = image-a-lower ]
      printf image-a-change > /etc/drive9-rebase-copyup
      rm /etc/drive9-rebase-whiteout' \
    >/dev/null || fail "image A seed execution failed"
  KEEP_ROOT=1
  printf 'PASS runtime rootfs rebase seed\n'
  exit 0
fi

[ -f "$STATE_FILE" ] || fail "seed state file is missing"
API_KEY="$(cat "$STATE_FILE")"
[ -n "$API_KEY" ] || fail "seed state file is empty"
wait_for_tenant
require_v6

DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s -- \
  /bin/sh -c 'set -eu
    [ "$(cat /etc/drive9-base-id)" = image-b ]
    [ -f /etc/drive9-base-b ]
    [ ! -e /etc/drive9-base-a ]
    [ "$(cat /etc/drive9-rebase-copyup)" = image-a-change ]
    [ ! -e /etc/drive9-rebase-whiteout ]' \
  >/dev/null || fail "image B did not retain image A copy-up and whiteout"

rm -f "$STATE_FILE"
printf 'PASS runtime rootfs rebase preserves persistent change layer\n'
