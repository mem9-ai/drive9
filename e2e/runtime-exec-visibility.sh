#!/usr/bin/env bash
# Cross-repository Runtime proof against a real drive9-server, FUSE mount, and
# Docker process. The exec request is intentionally issued exactly once.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
RUN_ID="$(date +%s)-$$"
ROOT_REL="runtime-visibility-${RUN_ID}"
ROOT="/${ROOT_REL}"
INBOUND_PATH="${ROOT_REL}/inbound.txt"
OUTBOUND_PATH="${ROOT_REL}/outbound.txt"
INBOUND_PAYLOAD="api-to-exec-${RUN_ID}"
OUTBOUND_PAYLOAD="exec-to-api-${RUN_ID}"
WORK_DIR="$(mktemp -d)"

cleanup() {
  if [ -n "$API_KEY" ]; then
    curl -sS --max-time 20 -X DELETE \
      -H "Authorization: Bearer ${API_KEY}" \
      "${BASE}/v1/fs/${ROOT_REL}?recursive" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

fail() {
  printf 'FAIL runtime exec visibility: %s\n' "$1" >&2
  if [ -s "$WORK_DIR/exec.stderr" ]; then
    sed -n '1,80p' "$WORK_DIR/exec.stderr" >&2
  fi
  exit 1
}

if [ ! -x "$CLI_BIN" ]; then
  fail "DRIVE9_CLI_BIN is not executable"
fi

if [ -z "$API_KEY" ]; then
  provision_json="$(curl -fsS --max-time 30 -X POST "${BASE}/v1/provision")" \
    || fail "tenant provision failed"
  API_KEY="$(printf '%s' "$provision_json" | jq -r '.api_key // empty')"
  [ -n "$API_KEY" ] || fail "tenant provision returned no API key"
fi

deadline=$((SECONDS + POLL_TIMEOUT_S))
while :; do
  status="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/status" | jq -r '.status // empty')" || status=""
  if [ "$status" = "active" ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "tenant did not become active"
  fi
  sleep "$POLL_INTERVAL_S"
done

capabilities="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/runtime/capabilities")" || fail "Runtime capabilities unavailable"
printf '%s' "$capabilities" | jq -e \
  '.streaming == true and .separate_stdout_stderr == true and .cancel == true and .detached == false and .replay == false' \
  >/dev/null || fail "Runtime capabilities do not match the one-shot contract"

curl -fsS --max-time 20 -X POST -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${ROOT_REL}?mkdir" >/dev/null \
  || fail "workspace root creation failed"

# Direction 1: an acknowledged FS API write must be observed byte-for-byte by
# the subsequently created Runtime mount and process.
printf '%s' "$INBOUND_PAYLOAD" | curl -fsS --max-time 20 -X PUT \
  -H "Authorization: Bearer ${API_KEY}" --data-binary @- \
  "${BASE}/v1/fs/${INBOUND_PATH}" >/dev/null \
  || fail "FS API inbound write failed"

if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s --env "OUTBOUND_PAYLOAD=${OUTBOUND_PAYLOAD}" -- \
  /bin/sh -c 'set -eu; cat inbound.txt; printf "%s" "$OUTBOUND_PAYLOAD" > outbound.txt' \
  >"$WORK_DIR/exec.stdout" 2>"$WORK_DIR/exec.stderr"; then
  fail "one-shot Runtime exec failed"
fi

actual_inbound="$(cat "$WORK_DIR/exec.stdout")"
[ "$actual_inbound" = "$INBOUND_PAYLOAD" ] \
  || fail "FS API write was not visible to the Runtime process"

# Direction 2: a process write followed by the service's mandatory drain and
# unmount must be immediately readable through the FS API. Do not poll: a
# successful terminal frame is itself the durability/visibility boundary.
actual_outbound="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${OUTBOUND_PATH}")" \
  || fail "Runtime write was not readable through the FS API"
[ "$actual_outbound" = "$OUTBOUND_PAYLOAD" ] \
  || fail "Runtime write did not match through the FS API"

printf 'PASS runtime exec bidirectional workspace visibility\n'
