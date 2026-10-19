#!/usr/bin/env bash
#
# local-object-store.sh — shared object store helper for local drive9-server (e2e / blackbox).
#
# Usage:
#   bash scripts/local-object-store.sh ensure   # start if needed, create bucket
#   eval "$(bash scripts/local-object-store.sh env)"
#   eval "$(bash scripts/local-object-store.sh apply)"  # pick S3 vs mock; print exports
#   bash scripts/local-object-store.sh status
#   bash scripts/local-object-store.sh stop     # only the named container / recorded pid
#
# Configuration: DRIVE9_OBJECT_STORE_{IMAGE,FALLBACK_IMAGE,BINARY,PORT,BIND,
# ACCESS_KEY,SECRET_KEY,BUCKET,CONTAINER,PID_FILE,DATA_DIR,DOWNLOAD_URL,
# HEALTH_PATH,HEALTH_TIMEOUT_S,HEALTH_CONNECT_TIMEOUT_S,HEALTH_PROBE_TIMEOUT_S}.
# Defaults: pinned RustFS image, no alternate image, rustfs binary on PATH,
# port 19000 on loopback, /health/ready, bucket drive9-local.
# DRIVE9_S3_BACKEND=auto|s3|mock selects real S3 with optional mock fallback.
# DRIVE9_S3_BUCKET already set makes apply a no-op; DRIVE9_S3_ENDPOINT can
# advertise a VM/container endpoint. See docs/guides/local-object-store.md.
#
# Compatible with macOS bash 3.2.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/object-store-config.sh"

PORT="${DRIVE9_OBJECT_STORE_PORT:-19000}"
BIND="${DRIVE9_OBJECT_STORE_BIND:-127.0.0.1}"
ACCESS_KEY="${DRIVE9_OBJECT_STORE_ACCESS_KEY:-drive9local}"
SECRET_KEY="${DRIVE9_OBJECT_STORE_SECRET_KEY:-drive9local}"
BUCKET="${DRIVE9_OBJECT_STORE_BUCKET:-drive9-local}"
CONTAINER="${DRIVE9_OBJECT_STORE_CONTAINER:-drive9-local-object-store}"
IMAGE="${DRIVE9_OBJECT_STORE_IMAGE:-$OBJECT_STORE_DEFAULT_IMAGE}"
FALLBACK_IMAGE="${DRIVE9_OBJECT_STORE_FALLBACK_IMAGE-}"
PID_FILE="${DRIVE9_OBJECT_STORE_PID_FILE:-${TMPDIR:-/tmp}/drive9-local-object-store.pid}"
DATA_DIR="${DRIVE9_OBJECT_STORE_DATA_DIR:-${TMPDIR:-/tmp}/drive9-local-object-store-data}"

OBJECT_STORE_IMAGE_CANDIDATES="$IMAGE"
if [ -n "$FALLBACK_IMAGE" ] && [ "$FALLBACK_IMAGE" != "$IMAGE" ]; then
  OBJECT_STORE_IMAGE_CANDIDATES="$OBJECT_STORE_IMAGE_CANDIDATES $FALLBACK_IMAGE"
fi
# Seconds to wait for one candidate's health endpoint; overridable for
# hermetic tests (a candidate only holds the loop this long when something
# answers slowly — a refused connection fails immediately).
DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S="${DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S:-40}"
# Per-probe bounds. Without them a server that accepts TCP but never answers
# the health request would stall `curl` past any deadline: the overall
# --max-time is what makes DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S effective.
DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S="${DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S:-2}"
# Cap for one-shot probes (reuse checks, status). Retry loops pass their
# remaining budget explicitly instead of using this default.
DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S="${DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S:-5}"
# curl treats --max-time 0 as "no limit", so every knob must be a positive
# integer or the bounded-probe contract silently breaks. Invalid values fall
# back to the defaults instead.
positive_int() {
  case "${1:-}" in
    '' | *[!0-9]*) return 1 ;;
    *) [ "$1" -ge 1 ] ;;
  esac
}
while :; do
  if positive_int "$DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S"; then
    :
  else
    echo "warning: DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S='$DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S' is not a positive integer; using 40" >&2
    DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S=40
  fi
  if positive_int "$DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S"; then
    :
  else
    echo "warning: DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S='$DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S' is not a positive integer; using 2" >&2
    DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S=2
  fi
  if positive_int "$DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S"; then
    :
  else
    echo "warning: DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S='$DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S' is not a positive integer; using 5" >&2
    DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S=5
  fi
  break
done

HEALTH_HOST="$BIND"
if [ "$BIND" = "0.0.0.0" ] || [ "$BIND" = "::" ] || [ "$BIND" = "[::]" ]; then
  HEALTH_HOST="127.0.0.1"
fi
HEALTH_URL="http://${HEALTH_HOST}:${PORT}"
ADVERTISED="${DRIVE9_S3_ENDPOINT:-http://${HEALTH_HOST}:${PORT}}"

have() { command -v "$1" >/dev/null 2>&1; }

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

url_healthy() {
  # Bounded probe: connect-timeout covers a black-holed peer, and --max-time
  # (default: the full health budget) covers a server that accepts TCP but
  # stalls the HTTP response. $2 overrides the cap for callers with a running
  # deadline (wait_healthy passes the seconds it has left).
  local url="$1"
  local max_time="${2:-$DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S}"
  curl -sf --connect-timeout "$DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S" --max-time "$max_time" "$url$DRIVE9_OBJECT_STORE_HEALTH_PATH" >/dev/null 2>&1
}

candidate_runtimes() {
  if [ -n "${DRIVE9_LOCAL_E2E_DB_RUNTIME:-}" ] && have "${DRIVE9_LOCAL_E2E_DB_RUNTIME}"; then
    echo "${DRIVE9_LOCAL_E2E_DB_RUNTIME}"
    return 0
  fi
  local found=0
  if have docker; then
    echo docker
    found=1
  fi
  if have podman; then
    echo podman
    found=1
  fi
  [ "$found" = 1 ]
}

create_bucket() {
  local base="$1"
  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_REGION="${DRIVE9_S3_REGION:-us-east-1}" \
    python3 - "$base" "$BUCKET" <<'PY'
import os, sys, datetime, hashlib, hmac, urllib.error, urllib.parse, urllib.request

base, bucket = sys.argv[1], sys.argv[2]
parsed = urllib.parse.urlparse(base)
scheme = parsed.scheme or "http"
host = parsed.netloc
if not host:
    sys.stderr.write("invalid object store URL: %s\n" % base)
    sys.exit(1)
access = os.environ["AWS_ACCESS_KEY_ID"]
secret = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ.get("AWS_REGION", "us-east-1")
service = "s3"
method = "PUT"
path = "/" + bucket
now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
amzdate = now.strftime("%Y%m%dT%H%M%SZ")
datestamp = now.strftime("%Y%m%d")
payload_hash = hashlib.sha256(b"").hexdigest()
canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
signed_headers = "host;x-amz-content-sha256;x-amz-date"
canonical_request = f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
scope = f"{datestamp}/{region}/{service}/aws4_request"
string_to_sign = "AWS4-HMAC-SHA256\n" + amzdate + "\n" + scope + "\n" + hashlib.sha256(canonical_request.encode()).hexdigest()

def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

k_date = _sign(("AWS4" + secret).encode(), datestamp)
k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
auth = (
    f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
    f"SignedHeaders={signed_headers}, Signature={signature}"
)
req = urllib.request.Request(
    f"{scheme}://{host}{path}",
    method="PUT",
    headers={
        "x-amz-date": amzdate,
        "x-amz-content-sha256": payload_hash,
        "Authorization": auth,
    },
)
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        sys.exit(0 if 200 <= resp.status < 300 else 1)
except urllib.error.HTTPError as err:
    if err.code in (200, 409):
        sys.exit(0)
    sys.stderr.write(err.read().decode("utf-8", "replace") + "\n")
    sys.exit(1)
PY
}

wait_healthy() {
  local url="$1"
  local deadline=$(($(date +%s) + DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S))
  local remaining
  while :; do
    remaining=$((deadline - $(date +%s)))
    if [ "$remaining" -le 0 ]; then
      return 1
    fi
    if url_healthy "$url" "$remaining"; then
      return 0
    fi
    sleep 0.3
  done
}

print_env() {
  echo "unset DRIVE9_S3_DIR"
  echo "export DRIVE9_S3_BUCKET=$(shell_quote "$BUCKET")"
  echo "export DRIVE9_S3_REGION=$(shell_quote "${DRIVE9_S3_REGION:-us-east-1}")"
  echo "export DRIVE9_S3_ENDPOINT=$(shell_quote "$ADVERTISED")"
  echo "export DRIVE9_S3_FORCE_PATH_STYLE='true'"
  echo "export DRIVE9_S3_ACCESS_KEY_ID=$(shell_quote "$ACCESS_KEY")"
  echo "export DRIVE9_S3_SECRET_ACCESS_KEY=$(shell_quote "$SECRET_KEY")"
}

start_container() {
  local runtime="$1"
  # A candidate is accepted only when its container actually turns healthy:
  # `run -d` succeeding says nothing about the server inside (entrypoint or
  # platform mismatches exit right after start). An unhealthy candidate is
  # logged, removed, and replaced by the next one; the caller falls back to
  # the binary only after every candidate failed this way. This also applies
  # to a pre-existing same-name container that is not healthy.
  local image
  for image in $OBJECT_STORE_IMAGE_CANDIDATES; do
    if "$runtime" inspect "$CONTAINER" >/dev/null 2>&1; then
      if [ "$("$runtime" inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != "true" ]; then
        "$runtime" start "$CONTAINER" >/dev/null 2>&1 || true
      fi
      if wait_healthy "$HEALTH_URL"; then
        return 0
      fi
      echo "object store container ($image via $runtime) is not healthy; removing and trying the next candidate" >&2
      "$runtime" logs "$CONTAINER" 2>&1 | tail -5 >&2 || true
      "$runtime" rm -f "$CONTAINER" >/dev/null 2>&1 || true
      continue
    fi
    if "$runtime" run -d --name "$CONTAINER" \
      -p "${BIND}:${PORT}:9000" \
      -e "RUSTFS_ACCESS_KEY=${ACCESS_KEY}" \
      -e "RUSTFS_SECRET_KEY=${SECRET_KEY}" \
      -e "MINIO_ROOT_USER=${ACCESS_KEY}" \
      -e "MINIO_ROOT_PASSWORD=${SECRET_KEY}" \
      "$image" server /data >/dev/null 2>&1 \
      && wait_healthy "$HEALTH_URL"; then
      return 0
    fi
    echo "object store image $image did not become healthy via $runtime; trying the next candidate" >&2
    "$runtime" logs "$CONTAINER" 2>&1 | tail -5 >&2 || true
    "$runtime" rm -f "$CONTAINER" >/dev/null 2>&1 || true
  done
  return 1
}

start_binary() {
  local bin=""
  if command -v "$DRIVE9_OBJECT_STORE_BINARY" >/dev/null 2>&1; then
    bin="$(command -v "$DRIVE9_OBJECT_STORE_BINARY")"
  elif [ -n "${DRIVE9_OBJECT_STORE_DOWNLOAD_URL:-}" ]; then
    bin="${DATA_DIR}/object-store"
    mkdir -p "$DATA_DIR"
    if [ ! -x "$bin" ]; then
      echo "fetching object-store binary from $DRIVE9_OBJECT_STORE_DOWNLOAD_URL" >&2
      curl -fsSL "$DRIVE9_OBJECT_STORE_DOWNLOAD_URL" -o "$bin" || return 1
      chmod +x "$bin"
    fi
  else
    echo "no object-store binary; set DRIVE9_OBJECT_STORE_BINARY or DRIVE9_OBJECT_STORE_DOWNLOAD_URL" >&2
    return 1
  fi
  mkdir -p "$DATA_DIR/data"
  RUSTFS_ACCESS_KEY="$ACCESS_KEY" RUSTFS_SECRET_KEY="$SECRET_KEY" \
  MINIO_ROOT_USER="$ACCESS_KEY" MINIO_ROOT_PASSWORD="$SECRET_KEY" \
    "$bin" server "$DATA_DIR/data" --address "${BIND}:${PORT}" >"${DATA_DIR}/object-store.log" 2>&1 &
  echo $! >"$PID_FILE"
}

ensure_bucket() {
  if url_healthy "$ADVERTISED"; then
    create_bucket "$ADVERTISED"
    return 0
  fi
  create_bucket "$HEALTH_URL"
}

cmd_ensure() {
  if url_healthy "$ADVERTISED"; then
    echo "reusing object store at $ADVERTISED" >&2
    create_bucket "$ADVERTISED"
    return 0
  fi
  if url_healthy "$HEALTH_URL"; then
    echo "reusing object store at $HEALTH_URL" >&2
    create_bucket "$HEALTH_URL"
    return 0
  fi

  local runtime
  local runtimes=""
  runtimes="$(candidate_runtimes || true)"
  for runtime in $runtimes; do
    echo "starting object store via $runtime on ${BIND}:${PORT}" >&2
    if start_container "$runtime"; then
      ensure_bucket
      return 0
    fi
    echo "object store via $runtime was not healthy at $HEALTH_URL" >&2
  done

  echo "starting object store binary on ${BIND}:${PORT}" >&2
  if start_binary && wait_healthy "$HEALTH_URL"; then
    ensure_bucket
    return 0
  fi
  if [ -f "${DATA_DIR}/object-store.log" ]; then
    tail -20 "${DATA_DIR}/object-store.log" >&2 || true
  fi
  echo "failed to start object store (advertised $ADVERTISED, local $HEALTH_URL)" >&2
  return 1
}

apply_mock() {
  local dir="${DRIVE9_S3_DIR:-${TMPDIR:-/tmp}/drive9-local-s3}"
  mkdir -p "$dir"
  echo "using local S3 mock at $dir" >&2
  echo "export DRIVE9_S3_DIR=$(shell_quote "$dir")"
}

cmd_apply() {
  if [ -n "${DRIVE9_S3_BUCKET:-}" ]; then
    echo "using existing DRIVE9_S3_BUCKET=$DRIVE9_S3_BUCKET" >&2
    return 0
  fi
  local backend="${DRIVE9_S3_BACKEND:-auto}"
  case "$backend" in
    mock)
      apply_mock
      ;;
    s3|minio)
      cmd_ensure
      echo "using object store at $ADVERTISED bucket=$BUCKET" >&2
      print_env
      ;;
    auto|"")
      if cmd_ensure; then
        echo "using object store at $ADVERTISED bucket=$BUCKET" >&2
        print_env
        return 0
      fi
      echo "object store unavailable; falling back to local S3 mock" >&2
      apply_mock
      ;;
    *)
      echo "unknown DRIVE9_S3_BACKEND=$backend (expected auto|s3|mock)" >&2
      return 2
      ;;
  esac
}

cmd_stop() {
  local runtime=""
  local runtimes=""
  runtimes="$(candidate_runtimes || true)"
  for runtime in $runtimes; do
    if "$runtime" inspect "$CONTAINER" >/dev/null 2>&1; then
      "$runtime" rm -f "$CONTAINER" >/dev/null 2>&1 || true
    fi
  done
  if [ -f "$PID_FILE" ]; then
    kill "$(cat "$PID_FILE")" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
}

cmd="${1:-}"
case "$cmd" in
  ensure) cmd_ensure ;;
  env) print_env ;;
  apply) cmd_apply ;;
  status)
    if url_healthy "$ADVERTISED"; then
      echo "object store healthy at $ADVERTISED"
      exit 0
    fi
    if url_healthy "$HEALTH_URL"; then
      echo "object store healthy at $HEALTH_URL (advertised $ADVERTISED)"
      exit 0
    fi
    echo "object store not healthy at $ADVERTISED (local $HEALTH_URL)" >&2
    exit 1
    ;;
  stop) cmd_stop ;;
  -h|--help|help|"")
    sed -n '2,/^set -euo pipefail$/p' "$0" | sed '/^set -euo pipefail$/d; s/^# \{0,1\}//'
    ;;
  *)
    echo "unknown command: $cmd (expected ensure|env|apply|status|stop)" >&2
    exit 2
    ;;
esac
