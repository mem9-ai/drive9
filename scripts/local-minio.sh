#!/usr/bin/env bash
#
# local-minio.sh — shared MinIO helper for local drive9-server (e2e / blackbox).
#
# Usage:
#   bash scripts/local-minio.sh ensure   # start if needed, create bucket
#   eval "$(bash scripts/local-minio.sh env)"
#   eval "$(bash scripts/local-minio.sh apply)"  # pick minio vs mock; print exports
#   bash scripts/local-minio.sh status
#   bash scripts/local-minio.sh stop     # only the named container / recorded pid
#
# Env:
#   DRIVE9_S3_BACKEND          auto (default) | minio | mock
#   DRIVE9_S3_BUCKET           if already set, apply is a no-op
#   DRIVE9_S3_DIR              mock root when backend=mock (or auto fallback)
#   DRIVE9_MINIO_PORT          (default 19000)
#   DRIVE9_MINIO_BIND          (default 127.0.0.1; 0.0.0.0 for Orb/VMs)
#   DRIVE9_MINIO_USER          (default drive9minio)
#   DRIVE9_MINIO_PASSWORD      (default drive9minio)
#   DRIVE9_MINIO_BUCKET        (default drive9-local)
#   DRIVE9_MINIO_CONTAINER     (default drive9-local-minio)
#   DRIVE9_MINIO_IMAGE         (default minio/minio:RELEASE.2024-12-18T13-15-44Z)
#   DRIVE9_S3_ENDPOINT         advertised endpoint (presign / clients; e.g. Orb)
#
# Compatible with macOS bash 3.2.

set -euo pipefail

PORT="${DRIVE9_MINIO_PORT:-19000}"
BIND="${DRIVE9_MINIO_BIND:-127.0.0.1}"
ACCESS_KEY="${DRIVE9_MINIO_USER:-drive9minio}"
SECRET_KEY="${DRIVE9_MINIO_PASSWORD:-drive9minio}"
BUCKET="${DRIVE9_MINIO_BUCKET:-drive9-local}"
CONTAINER="${DRIVE9_MINIO_CONTAINER:-drive9-local-minio}"
IMAGE="${DRIVE9_MINIO_IMAGE:-minio/minio:RELEASE.2024-12-18T13-15-44Z}"
PID_FILE="${DRIVE9_MINIO_PID_FILE:-${TMPDIR:-/tmp}/drive9-local-minio.pid}"
DATA_DIR="${DRIVE9_MINIO_DATA_DIR:-${TMPDIR:-/tmp}/drive9-local-minio-data}"

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
  curl -sf "$1/minio/health/live" >/dev/null 2>&1
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
    sys.stderr.write("invalid MinIO URL: %s\n" % base)
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
  local deadline=$(($(date +%s) + 40))
  while :; do
    if url_healthy "$url"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
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
  if "$runtime" inspect "$CONTAINER" >/dev/null 2>&1; then
    if [ "$("$runtime" inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" = "true" ]; then
      return 0
    fi
    "$runtime" start "$CONTAINER" >/dev/null
    return 0
  fi
  "$runtime" run -d --name "$CONTAINER" \
    -p "${BIND}:${PORT}:9000" \
    -e "MINIO_ROOT_USER=${ACCESS_KEY}" \
    -e "MINIO_ROOT_PASSWORD=${SECRET_KEY}" \
    "$IMAGE" server /data >/dev/null
}

minio_arch() {
  local os arch
  case "$(uname -s)" in
    Darwin) os="darwin" ;;
    Linux) os="linux" ;;
    *) return 1 ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *) return 1 ;;
  esac
  echo "${os}-${arch}"
}

start_binary() {
  local bin=""
  if have minio; then
    bin="$(command -v minio)"
  else
    local arch
    arch="$(minio_arch)" || return 1
    bin="${DATA_DIR}/minio"
    mkdir -p "$DATA_DIR"
    if [ ! -x "$bin" ]; then
      echo "fetching MinIO $arch" >&2
      curl -fsSL "https://dl.min.io/server/minio/release/${arch}/minio" -o "$bin"
      chmod +x "$bin"
    fi
  fi
  mkdir -p "$DATA_DIR/data"
  MINIO_ROOT_USER="$ACCESS_KEY" MINIO_ROOT_PASSWORD="$SECRET_KEY" \
    "$bin" server "$DATA_DIR/data" --address "${BIND}:${PORT}" >"${DATA_DIR}/minio.log" 2>&1 &
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
    echo "reusing MinIO at $ADVERTISED" >&2
    create_bucket "$ADVERTISED"
    return 0
  fi
  if url_healthy "$HEALTH_URL"; then
    echo "reusing MinIO at $HEALTH_URL" >&2
    create_bucket "$HEALTH_URL"
    return 0
  fi

  local runtime
  local runtimes=""
  runtimes="$(candidate_runtimes || true)"
  for runtime in $runtimes; do
    echo "starting MinIO via $runtime on ${BIND}:${PORT}" >&2
    if start_container "$runtime" && wait_healthy "$HEALTH_URL"; then
      ensure_bucket
      return 0
    fi
    echo "MinIO via $runtime was not healthy at $HEALTH_URL" >&2
    if [ -n "$runtime" ]; then
      "$runtime" logs "$CONTAINER" 2>&1 | tail -20 >&2 || true
    fi
  done

  echo "starting MinIO binary on ${BIND}:${PORT}" >&2
  if start_binary && wait_healthy "$HEALTH_URL"; then
    ensure_bucket
    return 0
  fi
  if [ -f "${DATA_DIR}/minio.log" ]; then
    tail -20 "${DATA_DIR}/minio.log" >&2 || true
  fi
  echo "failed to start MinIO (advertised $ADVERTISED, local $HEALTH_URL)" >&2
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
    minio)
      cmd_ensure
      echo "using MinIO at $ADVERTISED bucket=$BUCKET" >&2
      print_env
      ;;
    auto|"")
      if cmd_ensure; then
        echo "using MinIO at $ADVERTISED bucket=$BUCKET" >&2
        print_env
        return 0
      fi
      echo "MinIO unavailable; falling back to local S3 mock" >&2
      apply_mock
      ;;
    *)
      echo "unknown DRIVE9_S3_BACKEND=$backend (expected auto|minio|mock)" >&2
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
      echo "minio healthy at $ADVERTISED"
      exit 0
    fi
    if url_healthy "$HEALTH_URL"; then
      echo "minio healthy at $HEALTH_URL (advertised $ADVERTISED)"
      exit 0
    fi
    echo "minio not healthy at $ADVERTISED (local $HEALTH_URL)" >&2
    exit 1
    ;;
  stop) cmd_stop ;;
  -h|--help|help|"")
    sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown command: $cmd (expected ensure|env|apply|status|stop)" >&2
    exit 2
    ;;
esac
