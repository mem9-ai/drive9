#!/usr/bin/env bash
# Regression checks for the shared local MinIO image and its override.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAKE_BIN="$(mktemp -d)"
ARGS_FILE="$FAKE_BIN/docker-args"
READY_FILE="$FAKE_BIN/ready"
trap 'rm -rf "$FAKE_BIN"' EXIT

cat >"$FAKE_BIN/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  inspect)
    exit 1
    ;;
  run)
    shift
    printf '%s\n' "$@" >"${DOCKER_ARGS_FILE:?}"
    : >"${MINIO_READY_FILE:?}"
    printf 'fake-container-id\n'
    ;;
  *)
    exit 0
    ;;
esac
SH

cat >"$FAKE_BIN/curl" <<'SH'
#!/usr/bin/env bash
[ -f "${MINIO_READY_FILE:?}" ]
SH

cat >"$FAKE_BIN/python3" <<'SH'
#!/usr/bin/env bash
exit 0
SH

chmod +x "$FAKE_BIN/docker" "$FAKE_BIN/curl" "$FAKE_BIN/python3"

run_ensure() {
  local expected_image="$1"
  shift
  rm -f "$ARGS_FILE" "$READY_FILE"
  env \
    "PATH=$FAKE_BIN:$PATH" \
    "DOCKER_ARGS_FILE=$ARGS_FILE" \
    "MINIO_READY_FILE=$READY_FILE" \
    DRIVE9_LOCAL_E2E_DB_RUNTIME=docker \
    DRIVE9_MINIO_CONTAINER=drive9-local-minio-image-test \
    "$@" \
    bash "$REPO_ROOT/scripts/local-minio.sh" ensure >/dev/null

  grep -Fxq "$expected_image" "$ARGS_FILE" || {
    printf 'FAIL: expected MinIO image %s\n' "$expected_image" >&2
    cat "$ARGS_FILE" >&2
    exit 1
  }
  grep -Fxq 'server' "$ARGS_FILE" || {
    printf 'FAIL: local MinIO did not preserve the server command\n' >&2
    exit 1
  }
  grep -Fxq '/data' "$ARGS_FILE" || {
    printf 'FAIL: local MinIO did not preserve the /data directory\n' >&2
    exit 1
  }
}

# shellcheck source=scripts/minio-defaults.sh
source "$REPO_ROOT/scripts/minio-defaults.sh"
run_ensure "$DRIVE9_DEFAULT_MINIO_IMAGE"
run_ensure 'example.invalid/minio:test' DRIVE9_MINIO_IMAGE=example.invalid/minio:test

printf 'PASS: local MinIO uses the shared pullable default and honors overrides\n'
