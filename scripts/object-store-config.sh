#!/usr/bin/env bash
# Shared local S3 test settings. Source this file from the bootstrap/smoke scripts.
# New names win even when explicitly empty (notably the fallback image).
object_store_legacy_setting() {
  local target="$1" legacy
  shift
  if declare -p "$target" >/dev/null 2>&1; then return; fi
  for legacy in "$@"; do
    if declare -p "$legacy" >/dev/null 2>&1; then
      printf -v "$target" '%s' "${!legacy}"
      OBJECT_STORE_LEGACY_SETTINGS=1
      echo "warning: $legacy is deprecated; use $target" >&2
      return
    fi
  done
}

object_store_legacy_setting DRIVE9_OBJECT_STORE_IMAGE MINIO_IMAGE DRIVE9_MINIO_IMAGE
object_store_legacy_setting DRIVE9_OBJECT_STORE_FALLBACK_IMAGE DRIVE9_MINIO_FALLBACK_IMAGE
object_store_legacy_setting DRIVE9_OBJECT_STORE_PORT MINIO_PORT DRIVE9_MINIO_PORT
object_store_legacy_setting DRIVE9_OBJECT_STORE_BIND DRIVE9_MINIO_BIND
object_store_legacy_setting DRIVE9_OBJECT_STORE_ACCESS_KEY MINIO_ROOT_USER DRIVE9_MINIO_USER
object_store_legacy_setting DRIVE9_OBJECT_STORE_SECRET_KEY MINIO_ROOT_PASSWORD DRIVE9_MINIO_PASSWORD
object_store_legacy_setting DRIVE9_OBJECT_STORE_BUCKET MINIO_BUCKET DRIVE9_MINIO_BUCKET
object_store_legacy_setting DRIVE9_OBJECT_STORE_CONTAINER DRIVE9_MINIO_CONTAINER
object_store_legacy_setting DRIVE9_OBJECT_STORE_PID_FILE DRIVE9_MINIO_PID_FILE
object_store_legacy_setting DRIVE9_OBJECT_STORE_DATA_DIR DRIVE9_MINIO_DATA_DIR
object_store_legacy_setting DRIVE9_OBJECT_STORE_DOWNLOAD_URL MINIO_DOWNLOAD_URL
object_store_legacy_setting DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S MINIO_HEALTH_TIMEOUT_S
object_store_legacy_setting DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S MINIO_HEALTH_CONNECT_TIMEOUT_S
object_store_legacy_setting DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S MINIO_HEALTH_PROBE_TIMEOUT_S

OBJECT_STORE_DEFAULT_IMAGE='rustfs/rustfs:1.0.3@sha256:9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b'
# Preserve the old launcher's binary/probe defaults only for legacy callers.
if [ "${OBJECT_STORE_LEGACY_SETTINGS:-0}" = 1 ]; then
  : "${DRIVE9_OBJECT_STORE_BINARY:=minio}"
  : "${DRIVE9_OBJECT_STORE_HEALTH_PATH:=/minio/health/live}"
  : "${DRIVE9_OBJECT_STORE_ACCESS_KEY:=drive9minio}"
  : "${DRIVE9_OBJECT_STORE_SECRET_KEY:=drive9minio}"
  : "${DRIVE9_OBJECT_STORE_CONTAINER:=drive9-local-minio}"
  : "${DRIVE9_OBJECT_STORE_PID_FILE:=${TMPDIR:-/tmp}/drive9-local-minio.pid}"
  : "${DRIVE9_OBJECT_STORE_DATA_DIR:=${TMPDIR:-/tmp}/drive9-local-minio-data}"
fi
: "${DRIVE9_OBJECT_STORE_BINARY:=rustfs}"
: "${DRIVE9_OBJECT_STORE_HEALTH_PATH:=/health/ready}"
