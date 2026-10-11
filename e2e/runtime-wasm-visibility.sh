#!/usr/bin/env bash
# Cross-repository WASM Runtime proof against a real drive9-server, fresh
# Drive9 FUSE mount, pinned Wasmtime process, and public CLI/API surface.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
WASM_COMMAND="${DRIVE9_RUNTIME_WASM_E2E_COMMAND:-wasi-e2e}"
RUN_ID="$(date +%s)-$$"
ROOT_REL="runtime-wasm-${RUN_ID}"
ROOT="/${ROOT_REL}"
INPUT_PAYLOAD="api-to-wasm-${RUN_ID}"
EXPECTED_RESULT="drive9-wasm-e2e"
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
  printf 'FAIL runtime WASM visibility: %s\n' "$1" >&2
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
  '.streaming == true and .separate_stdout_stderr == true and .cancel == true
   and .detached == false and .replay == false
   and any(.providers[]; .provider == "wasm"
     and any(.candidates[]; .execution_class == "wasi-command"
       and .persistence == "workspace"
       and .rootfs.driver == "workspace-mount"
       and .rootfs.capability_version == "drive9_workspace.mount.v1"
       and .capabilities["workspace_persistence"] == true
       and .capabilities["wasi_command"] == true
       and .capabilities["full_root_persistence"] != true
       and .production_eligible == true))' >/dev/null \
  || fail "Runtime capabilities do not expose the bounded WASI provider"

curl -fsS --max-time 20 -X POST -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${ROOT_REL}?mkdir" >/dev/null \
  || fail "workspace root creation failed"
printf '%s\n' "$INPUT_PAYLOAD" | curl -fsS --max-time 20 -X PUT \
  -H "Authorization: Bearer ${API_KEY}" --data-binary @- \
  "${BASE}/v1/fs/${ROOT_REL}/input.txt" >/dev/null \
  || fail "FS API input write failed"

# Issue the process exactly once. Success means the server confirmed Wasmtime
# exit, drained the Drive9 mount, and completed cleanup before returning.
if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --persistence workspace --timeout 30s -- \
  "$WASM_COMMAND" >"$WORK_DIR/exec.stdout" 2>"$WORK_DIR/exec.stderr"; then
  fail "WASM Runtime exec failed"
fi

expected_stdout="${INPUT_PAYLOAD}
${EXPECTED_RESULT}"
actual_stdout="$(cat "$WORK_DIR/exec.stdout")"
[ "$actual_stdout" = "$expected_stdout" ] \
  || fail "WASI stdout did not prove API-to-sandbox workspace visibility"

# Do not poll or retry this read. The successful terminal frame is the
# durability boundary, so the first independent read must see the exact bytes.
actual_result="$(DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" \
  "$CLI_BIN" fs cat ":${ROOT}/result.txt")" \
  || fail "WASM result was not readable after the Runtime drain"
[ "$actual_result" = "$EXPECTED_RESULT" ] \
  || fail "WASM-to-API workspace bytes differ after drain"

printf 'PASS runtime WASM visibility: API -> fresh WASI mount -> drain -> API\n'
