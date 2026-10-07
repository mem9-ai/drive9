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
INBOUND_PATH="${ROOT_REL}/upper/etc/api-inbound.txt"
OUTBOUND_PATH="${ROOT_REL}/upper/workspace/outbound.txt"
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
  '.streaming == true and .separate_stdout_stderr == true and .cancel == true and .detached == false and .replay == false
   and any(.providers[].candidates[]; .execution_class == "linux-full"
     and .rootfs.capability_version == "drive9_rootfs.user_union.extent.v3"
     and .capabilities["drive9.extent_xattr.v1"] == true
     and .production_eligible == true and .bounded_selection_eligible == true)' \
  >/dev/null || fail "Runtime capabilities do not match the one-shot contract"

curl -fsS --max-time 20 -X POST -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${ROOT_REL}?mkdir" >/dev/null \
  || fail "workspace root creation failed"

# First sandbox: execute a lower-image binary and persist changes across the
# literal Linux root. This invocation also initializes the immutable rootfs
# binding and Drive9 upper/work backing.
if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s -- \
  /bin/sh -c 'set -eu
    test -x /bin/sh
    test "$(stat -c %u:%g /bin/sh)" = 0:0
    test "$(id -u)" = 65532
    test "$(awk '\''$1 == "CapEff:" { print $2 }'\'' /proc/self/status)" = 0000000000000002
    printf lower-image-ok
    printf root-state > /root/persist.txt
    printf home-state > /home/agent/persist.txt
    printf etc-state > /etc/drive9.conf
    printf workspace-state > /workspace/persist.txt
    printf rename-state > /workspace/rename-from.txt
    mv /workspace/rename-from.txt /workspace/rename-to.txt
    printf delete-state > /workspace/deleted.txt
    rm /workspace/deleted.txt
    rm /etc/drive9-lower-delete
    if chmod 0600 /etc/drive9-lower-rename 2>/dev/null; then exit 1; fi
    mv /etc/drive9-lower-rename /etc/drive9-lower-renamed
    test "$(stat -c %u:%g /etc/drive9-lower-renamed)" = 0:0
    test "$(stat -c %u:%g /etc/drive9.conf)" = 65532:65532
    chmod 0640 /etc/drive9.conf
    ln -s ../etc/drive9.conf /root/config-link
    ln /workspace/persist.txt /workspace/persist-hardlink.txt
    printf transient > /tmp/not-persistent
    printf transient > /run/not-persistent' \
  >"$WORK_DIR/exec.stdout" 2>"$WORK_DIR/exec.stderr"; then
  fail "first rootfs Runtime exec failed"
fi

[ "$(cat "$WORK_DIR/exec.stdout")" = lower-image-ok ] \
  || fail "provider lower-image /bin/sh did not execute"

# Direction 1: after rootfs initialization, an acknowledged FS API write into
# the durable upper must be observed at the corresponding merged-root path by
# a new sandbox.
printf '%s' "$INBOUND_PAYLOAD" | curl -fsS --max-time 20 -X PUT \
  -H "Authorization: Bearer ${API_KEY}" --data-binary @- \
  "${BASE}/v1/fs/${INBOUND_PATH}" >/dev/null \
  || fail "FS API inbound upper write failed"

if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s --env "INBOUND_PAYLOAD=${INBOUND_PAYLOAD}" --env "OUTBOUND_PAYLOAD=${OUTBOUND_PAYLOAD}" -- \
  /bin/sh -c 'set -eu
    test "$(stat -c %u:%g /bin/sh)" = 0:0
    test "$(cat /etc/api-inbound.txt)" = "$INBOUND_PAYLOAD"
    test "$(cat /root/persist.txt)" = root-state
    test "$(cat /home/agent/persist.txt)" = home-state
    test "$(cat /etc/drive9.conf)" = etc-state
    test "$(cat /workspace/persist.txt)" = workspace-state
    test "$(cat /workspace/rename-to.txt)" = rename-state
    test ! -e /workspace/rename-from.txt
    test ! -e /workspace/deleted.txt
    test ! -e /etc/drive9-lower-delete
    test "$(cat /etc/drive9-lower-renamed)" = rename-lower-state
    test "$(stat -c %u:%g /etc/drive9-lower-renamed)" = 0:0
    test "$(stat -c %u:%g /etc/drive9.conf)" = 65532:65532
    test "$(stat -c %a /etc/drive9.conf)" = 640
    test "$(readlink /root/config-link)" = ../etc/drive9.conf
    test "$(stat -c %i /workspace/persist.txt)" = "$(stat -c %i /workspace/persist-hardlink.txt)"
    test ! -e /tmp/not-persistent
    test ! -e /run/not-persistent
    for environment in /proc/[0-9]*/environ; do
      test -r "$environment" || continue
      if tr "\000" "\n" < "$environment" | grep -Eq "^DRIVE9_(API_KEY|SERVER|VAULT_TOKEN)="; then
        exit 1
      fi
    done
    printf "%s" "$OUTBOUND_PAYLOAD" > /workspace/outbound.txt' \
  >"$WORK_DIR/exec-2.stdout" 2>"$WORK_DIR/exec-2.stderr"; then
  cp "$WORK_DIR/exec-2.stderr" "$WORK_DIR/exec.stderr"
  fail "second rootfs Runtime exec failed"
fi

# Direction 2: a process write followed by the service's mandatory drain and
# unmount must be immediately readable through the FS API. Do not poll: a
# successful terminal frame is itself the durability/visibility boundary.
actual_outbound="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${OUTBOUND_PATH}")" \
  || fail "Runtime write was not readable through the FS API"
[ "$actual_outbound" = "$OUTBOUND_PAYLOAD" ] \
  || fail "Runtime write did not match through the FS API"

printf 'PASS runtime exec bidirectional workspace visibility\n'
