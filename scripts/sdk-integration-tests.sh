#!/usr/bin/env bash
#
# sdk-integration-tests.sh — one-click cross-SDK integration suite for drive9.
#
# Expects TiDB on DRIVE9_LOCAL_DSN (default 127.0.0.1:4000), starts drive9-server
# (provider=local, embedding mode app) against it,
# points every drive9 SDK (Go, TypeScript, Rust, Python, Kotlin, Swift) at the
# live server via DRIVE9_SERVER/DRIVE9_API_KEY, runs each SDK's integration
# suite, prints a summary, and tears everything down on exit.
#
# Each SDK constructs its client via the default-client constructor
# (Client.default / Client.defaultClient / Client::default_client), which reads
# DRIVE9_SERVER + DRIVE9_API_KEY first and ~/.drive9/config second — so the
# real config-resolution path is exercised end to end.
#
# Usage:
#   make sdk-integration-tests
#   bash scripts/sdk-integration-tests.sh
#   bash scripts/sdk-integration-tests.sh --only go,ts
#   bash scripts/sdk-integration-tests.sh --keep-server        # leave server up for debugging
#   bash scripts/sdk-integration-tests.sh --port 19009         # override listen port
#   bash scripts/sdk-integration-tests.sh --no-build           # reuse bin/drive9-server
#   DRIVE9_SERVER_BIN=/path/to/drive9-server bash scripts/sdk-integration-tests.sh --no-build
#
# Exit code is non-zero if any enabled SDK suite fails.
#
# NOTE: written to be compatible with the macOS system bash 3.2 (no assoc
# arrays, no `mapfile`).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
LISTEN_PORT="${DRIVE9_SDK_INTG_PORT:-19009}"
LISTEN_ADDR="127.0.0.1:${LISTEN_PORT}"
DEFAULT_LOCAL_DSN="root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
API_KEY=""
KEEP_SERVER=0
DO_BUILD=1
SERVER_BIN="${DRIVE9_SERVER_BIN:-$ROOT/bin/drive9-server}"
ONLY=""
ALL_SDKS="go ts rs py kt swift"

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --only)        ONLY="$2"; shift 2 ;;
    --keep-server) KEEP_SERVER=1; shift ;;
    --no-build)    DO_BUILD=0; shift ;;
    --port)        LISTEN_PORT="$2"; LISTEN_ADDR="127.0.0.1:${LISTEN_PORT}"; shift 2 ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [ -n "$ONLY" ]; then
  SDKS="$(echo "$ONLY" | tr ',' ' ')"
  # Validate tokens so a typo (e.g. --only go,typescript) doesn't silently
  # run nothing for the unrecognized SDK.
  for s in $SDKS; do
    if ! echo "$ALL_SDKS" | grep -qw "$s"; then
      echo "unknown SDK in --only: '$s' (known: $(echo $ALL_SDKS))" >&2
      exit 2
    fi
  done
else
  SDKS="$ALL_SDKS"
fi

# ---------------------------------------------------------------------------
# State that needs cleanup
# ---------------------------------------------------------------------------
SERVER_PID=""
S3_DIR=""
WORK_DIR="$(mktemp -d -t drive9-sdk-intg-XXXXXX)"
cleanup_done=0

cleanup() {
  if [ "$cleanup_done" = 1 ]; then return; fi
  cleanup_done=1
  echo ""
  echo "=== teardown ==="
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "stopping server (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$S3_DIR" ] && [ -d "$S3_DIR" ]; then
    rm -rf "$S3_DIR" || true
  fi
  rm -rf "$WORK_DIR" || true
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { printf '\n\033[1m>>> %s\033[0m\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

wait_for_http() {
  local url="$1" tries="${2:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -sf -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

provision_owner_key() {
  local base="$1" body code key status tries=90
  body="$(mktemp)"
  code="$(curl -sS -o "$body" -w "%{http_code}" -X POST "$base/v1/provision" || true)"
  if [ "$code" != "202" ] && [ "$code" != "200" ]; then
    echo "POST $base/v1/provision failed: HTTP ${code:-000}" >&2
    cat "$body" >&2 || true
    rm -f "$body"
    exit 1
  fi
  key="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("api_key") or "")' "$body")"
  rm -f "$body"
  if [ -z "$key" ]; then
    echo "provision response missing api_key" >&2
    exit 1
  fi
  while [ "$tries" -gt 0 ]; do
    status="$(curl -sS -H "Authorization: Bearer ${key}" "$base/v1/status" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status") or "")' 2>/dev/null || true)"
    if [ "$status" = "active" ]; then
      printf '%s\n' "$key"
      return
    fi
    tries=$((tries - 1))
    sleep 1
  done
  echo "tenant not active after provision (status=${status:-unknown})" >&2
  exit 1
}

wait_for_tidb() {
  local dsn="$1" tries="${2:-90}"
  local host port
  host="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/", sys.argv[1]); print(m.group(1) if m else "")' "$dsn")"
  port="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/", sys.argv[1]); print(m.group(2) if m else "")' "$dsn")"
  [ -n "$host" ] && [ -n "$port" ] || return 1
  for _ in $(seq 1 "$tries"); do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2)
s.settimeout(3)
data = s.recv(64)
s.close()
sys.exit(0 if len(data) > 4 else 1)
PY
    then
      return 0
    fi
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------------------
# Toolchain probe — only require toolchains for SDKs we will actually run.
# Results are stored in plain vars (portable across bash 3.2..5):
#   RUN_<sdk>   = 1 if the toolchain is present and the SDK is selected
#   SKIP_<sdk>  = reason string if the SDK is skipped
# ---------------------------------------------------------------------------
sdk_enabled() {
  local s="$1"
  for x in $SDKS; do
    if [ "$x" = "$s" ]; then return 0; fi
  done
  return 1
}

RUN_GO=0;   SKIP_GO=""
RUN_TS=0;   SKIP_TS=""
RUN_RS=0;   SKIP_RS=""
RUN_PY=0;   SKIP_PY=""
RUN_KT=0;   SKIP_KT=""
RUN_SWIFT=0; SKIP_SWIFT=""

probe_toolchains() {
  log "probing toolchains"
  if sdk_enabled go; then
    if have go; then RUN_GO=1; else SKIP_GO="go not installed"; fi
  fi
  if sdk_enabled ts; then
    if have node && (have pnpm || have npm); then RUN_TS=1; else SKIP_TS="node/npm not installed"; fi
  fi
  if sdk_enabled rs; then
    if have cargo; then RUN_RS=1; else SKIP_RS="cargo not installed"; fi
  fi
  if sdk_enabled py; then
    if have python3 && have pip3; then RUN_PY=1; else SKIP_PY="python3/pip3 not installed"; fi
  fi
  if sdk_enabled kt; then
    if have gradle && have java; then RUN_KT=1; else SKIP_KT="gradle/java not installed"; fi
  fi
  if sdk_enabled swift; then
    if have swift; then RUN_SWIFT=1; else SKIP_SWIFT="swift not installed"; fi
  fi

  local running=""
  if [ "$RUN_GO" = 1 ]; then running="$running go"; fi
  if [ "$RUN_TS" = 1 ]; then running="$running ts"; fi
  if [ "$RUN_RS" = 1 ]; then running="$running rs"; fi
  if [ "$RUN_PY" = 1 ]; then running="$running py"; fi
  if [ "$RUN_KT" = 1 ]; then running="$running kt"; fi
  if [ "$RUN_SWIFT" = 1 ]; then running="$running swift"; fi
  echo "will run:${running:- <none>}"
  [ -n "$SKIP_GO" ] && echo "skip go: $SKIP_GO" || true
  [ -n "$SKIP_TS" ] && echo "skip ts: $SKIP_TS" || true
  [ -n "$SKIP_RS" ] && echo "skip rs: $SKIP_RS" || true
  [ -n "$SKIP_PY" ] && echo "skip py: $SKIP_PY" || true
  [ -n "$SKIP_KT" ] && echo "skip kt: $SKIP_KT" || true
  [ -n "$SKIP_SWIFT" ] && echo "skip swift: $SKIP_SWIFT" || true
  true
}

# ---------------------------------------------------------------------------
# Bootstrap: TiDB, server build, server start
# ---------------------------------------------------------------------------
bootstrap_db() {
  : "${DRIVE9_LOCAL_DSN:=$DEFAULT_LOCAL_DSN}"
  export DRIVE9_LOCAL_DSN
  log "waiting for TiDB at $DRIVE9_LOCAL_DSN"
  if ! wait_for_tidb "$DRIVE9_LOCAL_DSN" 90; then
    echo "TiDB is not reachable; start playground on 127.0.0.1:4000 or set DRIVE9_LOCAL_DSN" >&2
    exit 1
  fi
  local host port db
  host="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(1) if m else "")' "$DRIVE9_LOCAL_DSN")"
  port="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(2) if m else "")' "$DRIVE9_LOCAL_DSN")"
  db="$(python3 -c 'import re,sys; m=re.search(r"@tcp\(([^:]+):(\d+)\)/([^?]*)", sys.argv[1]); print(m.group(3) if m else "drive9_local")' "$DRIVE9_LOCAL_DSN")"
  if have mysql; then
    mysql --protocol=tcp -h "$host" -P "$port" -u root -e "CREATE DATABASE IF NOT EXISTS \`$db\`;" >/dev/null
  fi
  echo "TiDB is ready"
}

build_server() {
  if [ -n "${DRIVE9_SERVER_BIN:-}" ]; then
    if [ ! -x "$SERVER_BIN" ]; then
      echo "DRIVE9_SERVER_BIN is not executable: $SERVER_BIN" >&2
      exit 1
    fi
    log "using DRIVE9_SERVER_BIN=$SERVER_BIN"
    return
  fi
  if [ "$DO_BUILD" -eq 0 ] && [ -x "$SERVER_BIN" ]; then
    log "reusing existing $SERVER_BIN (--no-build)"
    return
  fi
  log "building drive9-server"
  make build-server
  SERVER_BIN="$ROOT/bin/drive9-server"
}

start_server() {
  log "starting drive9-server on $LISTEN_ADDR"
  S3_DIR="$WORK_DIR/s3"
  mkdir -p "$S3_DIR"

  # Compose the env explicitly so the run is hermetic.
  export DRIVE9_LISTEN_ADDR="$LISTEN_ADDR"
  export DRIVE9_PUBLIC_URL="http://$LISTEN_ADDR"
  export DRIVE9_TENANT_PROVIDER="${DRIVE9_TENANT_PROVIDER:-local}"
  export DRIVE9_LOCAL_DSN="${DRIVE9_LOCAL_DSN:-$DEFAULT_LOCAL_DSN}"
  export DRIVE9_META_DSN="${DRIVE9_META_DSN:-$DRIVE9_LOCAL_DSN}"
  export DRIVE9_LOCAL_MYSQL_DSN="${DRIVE9_LOCAL_MYSQL_DSN:-$DRIVE9_LOCAL_DSN}"
  export DRIVE9_LOCAL_EMBEDDING_MODE="${DRIVE9_LOCAL_EMBEDDING_MODE:-app}"
  export DRIVE9_S3_DIR="$S3_DIR"

  # Redirect server logs to a file for debugging.
  "$SERVER_BIN" >"$WORK_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  echo "server pid: $SERVER_PID  (logs: $WORK_DIR/server.log)"

  if ! wait_for_http "http://$LISTEN_ADDR/healthz" 60; then
    echo "server did not become healthy in time" >&2
    tail -50 "$WORK_DIR/server.log" >&2 || true
    exit 1
  fi
  echo "server is healthy"

  API_KEY="$(provision_owner_key "http://$LISTEN_ADDR")"
  export DRIVE9_API_KEY="$API_KEY"
  echo "provisioned owner API key"
}

# ---------------------------------------------------------------------------
# Per-SDK runners. Each exports DRIVE9_SERVER/DRIVE9_API_KEY and invokes the
# SDK's own test command, scoped to the integration suite.
# Result vars: RESULT_<sdk> = pass|fail
# ---------------------------------------------------------------------------
export DRIVE9_SERVER="http://$LISTEN_ADDR"
export DRIVE9_INTEGRATION=1   # TS gate; harmless elsewhere

RESULT_GO=""; RESULT_TS=""; RESULT_RS=""; RESULT_PY=""; RESULT_KT=""; RESULT_SWIFT=""

run_go() {
  log "Go SDK integration suite"
  if (cd "$ROOT" && go test -tags=integration -timeout=10m -v ./pkg/client/... 2>&1); then
    RESULT_GO=pass
  else
    RESULT_GO=fail
  fi
}

run_ts() {
  log "TypeScript SDK integration suite"
  local pkgdir="$ROOT/clients/drive9-js"
  if have pnpm; then
    (cd "$pkgdir" && pnpm install --frozen-lockfile >/dev/null 2>&1 || pnpm install >/dev/null 2>&1 || true)
  else
    (cd "$pkgdir" && npm install >/dev/null 2>&1 || true)
  fi
  if (cd "$pkgdir" && npx vitest run tests/integration.test.ts 2>&1); then
    RESULT_TS=pass
  else
    RESULT_TS=fail
  fi
}

run_rs() {
  log "Rust SDK integration suite"
  local dir="$ROOT/clients/drive9-rs"
  if (cd "$dir" && cargo test --test integration -- --ignored --test-threads=1 2>&1); then
    RESULT_RS=pass
  else
    RESULT_RS=fail
  fi
}

run_py() {
  log "Python SDK integration suite"
  local dir="$ROOT/clients/drive9-py"
  # Use a dedicated venv to avoid PEP 668 externally-managed-Environment errors.
  local venv="$dir/.venv-it"
  if [ ! -x "$venv/bin/python" ]; then
    python3 -m venv "$venv" >/dev/null 2>&1 || true
  fi
  (cd "$dir" && "$venv/bin/pip" install -e ".[dev]" >/dev/null 2>&1 || true)
  if (cd "$dir" && "$venv/bin/python" -m pytest -v tests/test_integration.py 2>&1); then
    RESULT_PY=pass
  else
    RESULT_PY=fail
  fi
}

run_kt() {
  log "Kotlin SDK integration suite"
  local dir="$ROOT/clients/drive9-kotlin"
  if (cd "$dir" && gradle test --tests "com.drive9.mobile.Drive9IntegrationTest" --no-daemon --console=plain 2>&1); then
    RESULT_KT=pass
  else
    RESULT_KT=fail
  fi
}

run_swift() {
  log "Swift SDK integration suite"
  local dir="$ROOT/clients/drive9-swift"
  if (cd "$dir" && swift test --filter Drive9IntegrationTests 2>&1); then
    RESULT_SWIFT=pass
  else
    RESULT_SWIFT=fail
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
probe_toolchains

bootstrap_db
build_server
start_server

if [ "$RUN_GO" = 1 ]; then run_go; fi
if [ "$RUN_TS" = 1 ]; then run_ts; fi
if [ "$RUN_RS" = 1 ]; then run_rs; fi
if [ "$RUN_PY" = 1 ]; then run_py; fi
if [ "$RUN_KT" = 1 ]; then run_kt; fi
if [ "$RUN_SWIFT" = 1 ]; then run_swift; fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "==================== SDK integration summary ===================="
exit_code=0
for s in $ALL_SDKS; do
  eval "skip_val=\"\$SKIP_$(echo $s | tr '[:lower:]' '[:upper:]')\""
  eval "result_val=\"\$RESULT_$(echo $s | tr '[:lower:]' '[:upper:]')\""
  if [ -n "$skip_val" ]; then
    printf '  %-7s SKIP   (%s)\n' "$s" "$skip_val"
  elif [ "$result_val" = "pass" ]; then
    printf '  %-7s PASS\n' "$s"
  elif [ "$result_val" = "fail" ]; then
    printf '  %-7s FAIL\n' "$s"
    exit_code=1
  else
    printf '  %-7s SKIP   (not selected)\n' "$s"
  fi
done
echo "================================================================"

if [ "$KEEP_SERVER" -eq 1 ]; then
  echo "--keep-server: leaving server (pid $SERVER_PID) running"
  echo "  DRIVE9_SERVER=$DRIVE9_SERVER  DRIVE9_API_KEY=$DRIVE9_API_KEY"
  echo "  server logs: $WORK_DIR/server.log"
  # Detach cleanup so the trap does not kill them.
  cleanup_done=1
  SERVER_PID=""
fi

exit "$exit_code"