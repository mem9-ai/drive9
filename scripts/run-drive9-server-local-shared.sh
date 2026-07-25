#!/usr/bin/env bash
# Build and run drive9-server (not drive9-server-local) with:
#   - MySQL 8.4 in Docker/Podman as control-plane meta DB
#   - LocalClustersAPI managing one TiDB container per shared/physical pool
#   - optional warm tenant-pool pre-create via Admin API when WARM_POOL_SIZE>0
#
# On exit (Ctrl+C, failure, or normal stop) the script removes:
#   - the meta MySQL container
#   - all tenant TiDB containers labeled for this session
#   - the session docker network
#   - anonymous volumes attached to those containers (via rm -v)
#
# Usage:
#   bash scripts/run-drive9-server-local-shared.sh
#   WARM_POOL_SIZE=2 SOFT_CAP=2 HARD_CAP_RATIO=1.5 bash scripts/run-drive9-server-local-shared.sh
#   KEEP_CONTAINERS=1 bash scripts/run-drive9-server-local-shared.sh   # skip cleanup
#
# Optional env overrides are documented below.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# --- knobs ---
DB_RUNTIME="${DRIVE9_LOCAL_SHARED_DB_RUNTIME:-}"
MYSQL_IMAGE="${DRIVE9_LOCAL_SHARED_MYSQL_IMAGE:-mysql:8.4}"
TIDB_IMAGE="${DRIVE9_TIDBCLOUD_LOCAL_TIDB_IMAGE:-pingcap/tidb:v8.5.6}"
DB_PASSWORD="${DRIVE9_LOCAL_SHARED_DB_PASSWORD:-drive9pass}"
META_DB="${DRIVE9_LOCAL_SHARED_META_DB:-drive9_meta}"
LISTEN_ADDR="${DRIVE9_LISTEN_ADDR:-127.0.0.1:9009}"
PUBLIC_URL="${DRIVE9_PUBLIC_URL:-http://${LISTEN_ADDR}}"
# Health probes should hit a loopback URL on the host that runs this script.
# PUBLIC_URL may be host.orb.internal (for clients inside an Orb Linux machine);
# that name is not a reliable loopback health target on macOS.
HEALTH_URL="${DRIVE9_HEALTH_URL:-}"
if [ -z "$HEALTH_URL" ]; then
  case "$LISTEN_ADDR" in
    0.0.0.0:*|\[::\]:*)
      HEALTH_URL="http://127.0.0.1:${LISTEN_ADDR##*:}"
      ;;
    *)
      HEALTH_URL="http://${LISTEN_ADDR}"
      ;;
  esac
fi
SOFT_CAP="${DRIVE9_TIDBCLOUD_NATIVE_SHARED_MAX_TENANTS:-${SOFT_CAP:-2}}"
HARD_CAP_RATIO="${DRIVE9_TIDBCLOUD_NATIVE_SHARED_HARD_CAP_RATIO:-${HARD_CAP_RATIO:-1.5}}"
REOPEN_RATIO="${DRIVE9_TIDBCLOUD_NATIVE_SHARED_REOPEN_RATIO:-${REOPEN_RATIO:-0.8}}"
TENANT_POOL_MAX_SIZE="${DRIVE9_TENANT_POOL_MAX_SIZE:-${TENANT_POOL_MAX_SIZE:-2}}"
# Pre-create free warm-pool tenants after server is healthy. 0 = skip.
# Default 2 free tenant slots (not necessarily 2 physical TiDB instances; see banner).
WARM_POOL_SIZE="${DRIVE9_LOCAL_SHARED_WARM_POOL_SIZE:-${WARM_POOL_SIZE:-2}}"
WARM_POOL_TIMEOUT_S="${WARM_POOL_TIMEOUT_S:-600}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"
# If set, run this command after server (and optional warm pool) is ready,
# then exit cleanly (used by e2e/shared-smoke-test.sh). Example:
#   POST_CMD='bash e2e/shared-smoke-test.sh cases-only'
POST_CMD="${POST_CMD:-}"

SESSION_ID="${DRIVE9_TIDBCLOUD_LOCAL_SESSION_ID:-drive9-local-shared-$$-$(date +%s)}"
RUN_DIR="${DRIVE9_LOCAL_SHARED_RUN_DIR:-$(mktemp -d -t drive9-local-shared.XXXXXX)}"
SERVER_BIN="${DRIVE9_LOCAL_SHARED_SERVER_BIN:-$RUN_DIR/drive9-server}"
SERVER_LOG="${DRIVE9_LOCAL_SHARED_SERVER_LOG:-$RUN_DIR/drive9-server.log}"
S3_DIR="${DRIVE9_S3_DIR:-$RUN_DIR/s3}"

MYSQL_CONTAINER=""
NETWORK=""
SERVER_PID=""
META_PORT=""
META_DSN=""
CLEANED=0
# Set by signal handlers so EXIT cleanup treats Ctrl+C as a clean stop.
CLEAN_EXIT=0

log() { echo "[local-shared] $*"; }
die() { echo "[local-shared] ERROR: $*" >&2; exit 1; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

detect_runtime() {
  if [ -n "$DB_RUNTIME" ]; then
    need_cmd "$DB_RUNTIME"
    return
  fi
  if command -v docker >/dev/null 2>&1; then
    DB_RUNTIME=docker
    return
  fi
  if command -v podman >/dev/null 2>&1; then
    DB_RUNTIME=podman
    return
  fi
  die "docker or podman is required"
}

pick_host_port_from_container() {
  local container="$1" private_port="$2"
  "$DB_RUNTIME" port "$container" "${private_port}/tcp" 2>/dev/null | awk -F: 'END{print $NF}'
}

meta_exec() {
  "$DB_RUNTIME" exec "$MYSQL_CONTAINER" mysql -uroot -p"$DB_PASSWORD" -N -e "$1" 2>/dev/null
}

# http_json METHOD PATH [BODY] → prints "HTTP_CODE\tBODY"
# Host-local admin/health calls use HEALTH_URL (loopback), not PUBLIC_URL
# (which may be host.orb.internal for clients inside an Orb Linux machine).
# Admin tenant-pool GET uses TiDB Cloud credential headers (not JSON body).
http_json() {
  local method="$1" path="$2" body="${3:-}"
  local tmp code base="${HEALTH_URL:-$PUBLIC_URL}"
  tmp="$(mktemp)"
  if [ "$method" = "GET" ]; then
    code="$(curl -sS -o "$tmp" -w "%{http_code}" -X GET \
      -H "X-TiDBCloud-Public-Key: local" \
      -H "X-TiDBCloud-Private-Key: local" \
      "${base}${path}" 2>/dev/null || echo "000")"
  else
    code="$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" \
      -H "Content-Type: application/json" \
      --data-binary "$body" \
      "${base}${path}" 2>/dev/null || echo "000")"
  fi
  printf '%s\t%s' "$code" "$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
}

ensure_warm_pool() {
  local size="$1"
  if ! [[ "$size" =~ ^[0-9]+$ ]]; then
    die "WARM_POOL_SIZE must be a non-negative integer, got: $size"
  fi
  if [ "$size" -le 0 ]; then
    log "WARM_POOL_SIZE=$size — skip warm pool create"
    return 0
  fi
  if [ "$size" -gt "$TENANT_POOL_MAX_SIZE" ]; then
    die "WARM_POOL_SIZE=$size exceeds TENANT_POOL_MAX_SIZE=$TENANT_POOL_MAX_SIZE (raise the max or lower warm size)"
  fi

  # local backend accepts any non-empty keys; values are not validated against Cloud.
  local body
  body="$(printf '{"public_key":"local","private_key":"local","pool_size":%d}' "$size")"

  log "creating warm tenant-pool size=$size (starts TiDB via LocalClustersAPI; may take minutes)..."
  local line code resp
  line="$(http_json POST /v1/admin/tenant-pool "$body")"
  code="${line%%$'\t'*}"
  resp="${line#*$'\t'}"

  if [ "$code" = "202" ] || [ "$code" = "200" ] || [ "$code" = "201" ]; then
    log "warm pool create accepted http=$code body=$resp"
  elif [ "$code" = "409" ]; then
    log "warm pool already exists (409); PATCHing pool_size=$size"
    line="$(http_json PATCH /v1/admin/tenant-pool "$body")"
    code="${line%%$'\t'*}"
    resp="${line#*$'\t'}"
    if [ "$code" != "202" ] && [ "$code" != "200" ]; then
      tail -n 80 "$SERVER_LOG" >&2 || true
      die "warm pool update failed http=$code body=$resp"
    fi
    log "warm pool update accepted http=$code body=$resp"
  else
    tail -n 80 "$SERVER_LOG" >&2 || true
    die "warm pool create failed http=$code body=$resp"
  fi

  # Wait until free_size reaches target (create is mostly sync but status may lag).
  local deadline free
  deadline=$(($(date +%s) + WARM_POOL_TIMEOUT_S))
  while :; do
    line="$(http_json GET /v1/admin/tenant-pool)"
    code="${line%%$'\t'*}"
    resp="${line#*$'\t'}"
    if [ "$code" = "200" ]; then
      free="$(printf '%s' "$resp" | python3 -c 'import sys,json
try:
 d=json.load(sys.stdin); print(d.get("free_size",""))
except Exception:
 print("")' 2>/dev/null || true)"
      if [ -n "$free" ] && [ "$free" -ge "$size" ] 2>/dev/null; then
        log "warm pool ready free_size=$free (target=$size) body=$resp"
        return 0
      fi
      log "waiting warm pool free_size=${free:-?} target=$size ..."
    else
      log "waiting warm pool GET http=$code body=$resp ..."
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      tail -n 80 "$SERVER_LOG" >&2 || true
      die "server died while waiting for warm pool"
    fi
    [ "$(date +%s)" -lt "$deadline" ] || {
      tail -n 80 "$SERVER_LOG" >&2 || true
      die "timed out waiting for warm pool free_size>=$size (last GET http=$code body=$resp)"
    }
    sleep "$POLL_INTERVAL_S"
  done
}

request_clean_exit() {
  CLEAN_EXIT=1
  # EXIT trap will run cleanup with CLEAN_EXIT=1.
  exit 0
}

cleanup() {
  local rc=$?
  if [ "$CLEANED" = "1" ]; then
    return
  fi
  CLEANED=1

  # Ctrl+C / SIGTERM: treat as intentional stop (not a failed run).
  if [ "$CLEAN_EXIT" = "1" ] || [ "$rc" -eq 130 ] || [ "$rc" -eq 143 ]; then
    rc=0
  fi

  if [ "$KEEP_CONTAINERS" = "1" ]; then
    log "KEEP_CONTAINERS=1 — leaving containers; session=$SESSION_ID run_dir=$RUN_DIR"
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
      log "server still running pid=$SERVER_PID (log: $SERVER_LOG)"
    fi
    return
  fi

  log "cleaning up session=$SESSION_ID ..."

  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""

  local ids
  ids="$("$DB_RUNTIME" ps -aq --filter "label=drive9.local.session=${SESSION_ID}" 2>/dev/null || true)"
  if [ -n "$ids" ]; then
    log "removing session containers (meta + tidb): $(echo "$ids" | tr '\n' ' ')"
    # shellcheck disable=SC2086
    "$DB_RUNTIME" rm -fv $ids >/dev/null 2>&1 || true
  fi

  if [ -n "$MYSQL_CONTAINER" ]; then
    "$DB_RUNTIME" rm -fv "$MYSQL_CONTAINER" >/dev/null 2>&1 || true
  fi

  if [ -n "$NETWORK" ]; then
    "$DB_RUNTIME" network rm "$NETWORK" >/dev/null 2>&1 || true
  fi

  if [ -d "$RUN_DIR" ] && [[ "$RUN_DIR" == *drive9-local-shared* ]]; then
    if [ "$rc" -eq 0 ]; then
      rm -rf "$RUN_DIR" 2>/dev/null || true
    else
      log "failed (rc=$rc); artifacts kept at $RUN_DIR (server log: $SERVER_LOG)"
    fi
  fi

  log "cleanup done"
  if [ "$rc" -ne 0 ]; then
    exit "$rc"
  fi
}

trap cleanup EXIT
trap request_clean_exit INT TERM

print_banner() {
  cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 drive9-server (shared + LocalClustersAPI)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  listen:       $LISTEN_ADDR
  public:       $PUBLIC_URL
  health:       $HEALTH_URL
  meta DSN:     $META_DSN
  soft cap:     $SOFT_CAP tenants/pool
  hard ratio:   $HARD_CAP_RATIO  (hard ≈ ceil(soft×ratio))
  reopen:       $REOPEN_RATIO
  pool max:     $TENANT_POOL_MAX_SIZE (admin warm pool ceiling)
  warm size:    $WARM_POOL_SIZE  free tenant slots (0=skip; shared: N slots ≠ N TiDBs)
  session:      $SESSION_ID
  runtime:      $DB_RUNTIME
  tidb image:   $TIDB_IMAGE
  server log:   $SERVER_LOG
  run dir:      $RUN_DIR

  Note (shared mode):
    warm pool = pre-created FREE TENANTS ready to claim (skip cold create path).
    soft cap  = max tenants per physical shared TiDB (db_pool).
    So warm=2 + soft=2 usually means 2 free tenants on 1 TiDB, not 2 TiDBs.
    hard cap ratio = emergency over-fill when soft is full and new physical create fails.

  provision:
    curl -sS -X POST ${PUBLIC_URL}/v1/provision -H 'Content-Type: application/json' -d '{}'

  health:
    curl -sS ${PUBLIC_URL}/healthz

  Press Ctrl+C to stop and clean up containers/volumes.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EOF
}

# --- main ---
need_cmd go
need_cmd openssl
need_cmd curl
need_cmd python3
detect_runtime

mkdir -p "$RUN_DIR" "$S3_DIR"

log "building drive9-server → $SERVER_BIN"
CGO_ENABLED=0 go build -o "$SERVER_BIN" ./cmd/drive9-server

# docker name charset: [a-zA-Z0-9][a-zA-Z0-9_.-]+
sanitize_name() { echo "$1" | tr -c 'A-Za-z0-9_.-' '-' | sed 's/^-*//;s/-*$//'; }

NETWORK="$(sanitize_name "drive9-local-shared-net-${SESSION_ID}")"
MYSQL_CONTAINER="$(sanitize_name "drive9-meta-${SESSION_ID}")"

log "creating network $NETWORK"
"$DB_RUNTIME" network create "$NETWORK" >/dev/null

log "starting MySQL $MYSQL_IMAGE as $MYSQL_CONTAINER"
"$DB_RUNTIME" run -d \
  --name "$MYSQL_CONTAINER" \
  --network "$NETWORK" \
  --label "drive9.local.session=${SESSION_ID}" \
  --label "drive9.local.role=meta" \
  -e MYSQL_ROOT_PASSWORD="$DB_PASSWORD" \
  -p 127.0.0.1::3306 \
  "$MYSQL_IMAGE" >/dev/null

deadline=$(($(date +%s) + POLL_TIMEOUT_S))
while :; do
  META_PORT="$(pick_host_port_from_container "$MYSQL_CONTAINER" 3306 || true)"
  [ -n "$META_PORT" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || die "timed out waiting for MySQL published port"
  sleep "$POLL_INTERVAL_S"
done

while :; do
  if meta_exec "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  [ "$(date +%s)" -lt "$deadline" ] || {
    "$DB_RUNTIME" logs --tail 40 "$MYSQL_CONTAINER" >&2 || true
    die "timed out waiting for MySQL readiness"
  }
  sleep "$POLL_INTERVAL_S"
done

meta_exec "CREATE DATABASE IF NOT EXISTS \`${META_DB}\`;" >/dev/null
META_DSN="root:${DB_PASSWORD}@tcp(127.0.0.1:${META_PORT})/${META_DB}?parseTime=true"
log "meta ready on 127.0.0.1:${META_PORT} db=${META_DB}"

MASTER_KEY="$(openssl rand -hex 32)"
TOKEN_KEY="$(openssl rand -hex 32)"

log "starting drive9-server (provider=tidb_cloud_native_shared, clusters=local)"
env \
  DRIVE9_LISTEN_ADDR="$LISTEN_ADDR" \
  DRIVE9_PUBLIC_URL="$PUBLIC_URL" \
  DRIVE9_META_DSN="$META_DSN" \
  DRIVE9_ENCRYPT_TYPE=local_aes \
  DRIVE9_MASTER_KEY="$MASTER_KEY" \
  DRIVE9_TOKEN_SIGNING_KEY="$TOKEN_KEY" \
  DRIVE9_TENANT_PROVIDER=tidb_cloud_native_shared \
  DRIVE9_TIDBCLOUD_CLUSTERS_BACKEND=local \
  DRIVE9_TIDBCLOUD_LOCAL_RUNTIME="$DB_RUNTIME" \
  DRIVE9_TIDBCLOUD_LOCAL_TIDB_IMAGE="$TIDB_IMAGE" \
  DRIVE9_TIDBCLOUD_LOCAL_HOST=127.0.0.1 \
  DRIVE9_TIDBCLOUD_LOCAL_SESSION_ID="$SESSION_ID" \
  DRIVE9_TIDBCLOUD_NATIVE_SHARED_MAX_TENANTS="$SOFT_CAP" \
  DRIVE9_TIDBCLOUD_NATIVE_SHARED_HARD_CAP_RATIO="$HARD_CAP_RATIO" \
  DRIVE9_TIDBCLOUD_NATIVE_SHARED_REOPEN_RATIO="$REOPEN_RATIO" \
  DRIVE9_TENANT_POOL_MAX_SIZE="$TENANT_POOL_MAX_SIZE" \
  DRIVE9_S3_DIR="$S3_DIR" \
  DRIVE9_DISABLE_AUTO_EMBEDDING=true \
  DRIVE9_LEADER_DISABLED=1 \
  "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

deadline=$(($(date +%s) + POLL_TIMEOUT_S))
while :; do
  if curl -sf "${HEALTH_URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    tail -n 50 "$SERVER_LOG" >&2 || true
    die "drive9-server exited early (see $SERVER_LOG)"
  fi
  [ "$(date +%s)" -lt "$deadline" ] || {
    tail -n 50 "$SERVER_LOG" >&2 || true
    die "timed out waiting for /healthz (HEALTH_URL=$HEALTH_URL PUBLIC_URL=$PUBLIC_URL)"
  }
  sleep "$POLL_INTERVAL_S"
done
log "drive9-server is up (pid=$SERVER_PID) health=$HEALTH_URL public=$PUBLIC_URL"

ensure_warm_pool "$WARM_POOL_SIZE"

print_banner

ENV_FILE="$RUN_DIR/env.sh"
cat >"$ENV_FILE" <<EOF
# Generated by run-drive9-server-local-shared.sh — valid while the server is up.
export DRIVE9_BASE=${PUBLIC_URL}
export DRIVE9_META_DSN='${META_DSN}'
export DRIVE9_TIDBCLOUD_LOCAL_SESSION_ID='${SESSION_ID}'
export DRIVE9_TIDBCLOUD_NATIVE_SHARED_MAX_TENANTS=${SOFT_CAP}
export DRIVE9_TIDBCLOUD_NATIVE_SHARED_HARD_CAP_RATIO=${HARD_CAP_RATIO}
export DRIVE9_LOCAL_SHARED_WARM_POOL_SIZE=${WARM_POOL_SIZE}
EOF
log "companion env written to $ENV_FILE (source it from another terminal)"

# Export stable names for e2e/shared-smoke-test and other POST_CMD consumers.
export DRIVE9_BASE="$PUBLIC_URL"
export DRIVE9_META_DSN="$META_DSN"
export DRIVE9_TIDBCLOUD_LOCAL_SESSION_ID="$SESSION_ID"

if [ -n "$POST_CMD" ]; then
  log "running POST_CMD: $POST_CMD"
  bash -c "$POST_CMD"
  request_clean_exit
fi

# Wait until interrupted; trap performs cleanup.
while kill -0 "$SERVER_PID" 2>/dev/null; do
  sleep 1
done
log "server process exited unexpectedly"
tail -n 30 "$SERVER_LOG" >&2 || true
exit 1
