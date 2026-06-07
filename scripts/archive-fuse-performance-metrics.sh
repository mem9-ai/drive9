#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "fuse-perf-archive: $*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

sanitize_component() {
  printf '%s' "$1" | sed -E 's#[^A-Za-z0-9._-]+#-#g; s#^-+##; s#-+$##'
}

remote_ref() {
  printf 'drive9:%s' "$1"
}

download_drive9_cli() {
  local bin os arch
  bin="$(mktemp)"
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    *) die "unsupported OS for drive9 CLI download: $(uname -s)" ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) die "unsupported architecture for drive9 CLI download: $(uname -m)" ;;
  esac
  curl -fsSL "https://drive9.ai/releases/drive9-${os}-${arch}" -o "$bin"
  chmod +x "$bin"
  printf '%s' "$bin"
}

ensure_remote_dir() {
  local path="$1"
  local current=""
  local component
  IFS='/' read -r -a components <<<"${path#/}"
  for component in "${components[@]}"; do
    [ -n "$component" ] || continue
    current="${current}/${component}"
    "$DRIVE9_CLI_BIN" fs mkdir "$(remote_ref "$current")" >/dev/null 2>&1 || true
  done
}

upload_file() {
  local local_path="$1"
  local remote_path="$2"
  info "upload ${remote_path}"
  "$DRIVE9_CLI_BIN" fs cp "$local_path" "$(remote_ref "$remote_path")"
}

SOURCE_DIR="${DRIVE9_PERF_SOURCE_DIR:-${FUSE_PERF_ARTIFACT_DIR:-}}"
ARCHIVE_ROOT="${DRIVE9_PERF_ARCHIVE_ROOT:-/benchmarks/fuse-performance}"
ARCHIVE_ROOT="${ARCHIVE_ROOT%/}"
DRY_RUN="${DRIVE9_PERF_ARCHIVE_DRY_RUN:-0}"
ALLOW_EMPTY="${DRIVE9_PERF_ARCHIVE_ALLOW_EMPTY:-0}"
REPOSITORY="${DRIVE9_PERF_REPOSITORY:-${GITHUB_REPOSITORY:-mem9-ai/drive9}}"
BRANCH="${DRIVE9_PERF_BRANCH:-${GITHUB_REF_NAME:-unknown}}"
SHA="${DRIVE9_PERF_SHA:-${GITHUB_SHA:-unknown}}"
RUN_ID="${DRIVE9_PERF_RUN_ID:-${GITHUB_RUN_ID:-manual}}"
RUN_ATTEMPT="${DRIVE9_PERF_RUN_ATTEMPT:-${GITHUB_RUN_ATTEMPT:-1}}"
WORKFLOW="${DRIVE9_PERF_WORKFLOW:-${GITHUB_WORKFLOW:-local-e2e}}"
EVENT_NAME="${DRIVE9_PERF_EVENT_NAME:-${GITHUB_EVENT_NAME:-manual}}"

[ -n "$SOURCE_DIR" ] || die "DRIVE9_PERF_SOURCE_DIR or FUSE_PERF_ARTIFACT_DIR is required"
if [ ! -d "$SOURCE_DIR" ]; then
  if [ "$ALLOW_EMPTY" = "1" ]; then
    info "skip archive; performance artifact directory does not exist: $SOURCE_DIR"
    exit 0
  fi
  die "performance artifact directory does not exist: $SOURCE_DIR"
fi

require_cmd curl
require_cmd python3
require_cmd sed

metrics_count="$(find "$SOURCE_DIR" -type f -name 'performance-metrics-*.json' | wc -l | tr -d '[:space:]')"
if [ "$metrics_count" -eq 0 ]; then
  if [ "$ALLOW_EMPTY" = "1" ]; then
    info "skip archive; no performance-metrics-*.json files found in $SOURCE_DIR"
    exit 0
  fi
  die "no performance-metrics-*.json files found in $SOURCE_DIR"
fi

date_path="$(date -u '+%Y/%m/%d')"
branch_slug="$(sanitize_component "$BRANCH")"
[ -n "$branch_slug" ] || branch_slug=unknown
short_sha="$(printf '%s' "$SHA" | cut -c1-12)"
remote_dir="${ARCHIVE_ROOT}/${date_path}/${branch_slug}/${short_sha}/${RUN_ID}-${RUN_ATTEMPT}"
branch_latest_dir="${ARCHIVE_ROOT}/branches/${branch_slug}"
manifest="$(mktemp)"
latest_manifest="$(mktemp)"

python3 - "$SOURCE_DIR" "$manifest" <<'PY'
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

source = Path(sys.argv[1]).resolve()
manifest_path = Path(sys.argv[2])
files = []
for path in sorted(p for p in source.rglob("*") if p.is_file()):
    data = path.read_bytes()
    files.append({
        "path": path.relative_to(source).as_posix(),
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    })

doc = {
    "schema": "drive9-fuse-performance-archive/v1",
    "repository": os.environ.get("DRIVE9_PERF_REPOSITORY") or os.environ.get("GITHUB_REPOSITORY") or "mem9-ai/drive9",
    "branch": os.environ.get("DRIVE9_PERF_BRANCH") or os.environ.get("GITHUB_REF_NAME") or "unknown",
    "commit_sha": os.environ.get("DRIVE9_PERF_SHA") or os.environ.get("GITHUB_SHA") or "unknown",
    "run_id": os.environ.get("DRIVE9_PERF_RUN_ID") or os.environ.get("GITHUB_RUN_ID") or "manual",
    "run_attempt": os.environ.get("DRIVE9_PERF_RUN_ATTEMPT") or os.environ.get("GITHUB_RUN_ATTEMPT") or "1",
    "workflow": os.environ.get("DRIVE9_PERF_WORKFLOW") or os.environ.get("GITHUB_WORKFLOW") or "local-e2e",
    "event_name": os.environ.get("DRIVE9_PERF_EVENT_NAME") or os.environ.get("GITHUB_EVENT_NAME") or "manual",
    "archived_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "files": files,
}
manifest_path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
cp "$manifest" "$latest_manifest"

if [ "$DRY_RUN" = "1" ]; then
  info "dry run; would archive ${SOURCE_DIR} to ${remote_dir}"
  python3 -m json.tool "$manifest" >/dev/null
  exit 0
fi

[ -n "${DRIVE9_SERVER:-}" ] || die "DRIVE9_SERVER is required"
[ -n "${DRIVE9_API_KEY:-}" ] || die "DRIVE9_API_KEY is required"

if [ -n "${DRIVE9_CLI_BIN:-}" ]; then
  [ -x "$DRIVE9_CLI_BIN" ] || die "DRIVE9_CLI_BIN is not executable: $DRIVE9_CLI_BIN"
elif command -v drive9 >/dev/null 2>&1; then
  DRIVE9_CLI_BIN="$(command -v drive9)"
else
  DRIVE9_CLI_BIN="$(download_drive9_cli)"
fi

ensure_remote_dir "$remote_dir"
ensure_remote_dir "$branch_latest_dir"

while IFS= read -r -d '' path; do
  rel="${path#"$SOURCE_DIR"/}"
  upload_file "$path" "${remote_dir}/${rel}"
done < <(find "$SOURCE_DIR" -type f -print0 | sort -z)

upload_file "$manifest" "${remote_dir}/archive-manifest.json"
upload_file "$latest_manifest" "${ARCHIVE_ROOT}/latest.json"
upload_file "$latest_manifest" "${branch_latest_dir}/latest.json"

info "archived metrics to ${remote_dir}"
