#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "fuse-perf-compare: $*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

sanitize_component() {
  printf '%s' "$1" | sed -E 's#[^A-Za-z0-9._-]+#-#g; s#^-+##; s#-+$##'
}

remote_ref() {
  printf ':%s' "$1"
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

copy_from_drive9() {
  local remote_path="$1"
  local local_path="$2"
  local out rc
  set +e
  out=$("$DRIVE9_CLI_BIN" fs cp "$(remote_ref "$remote_path")" "$local_path" 2>&1)
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    return 0
  fi
  if printf '%s' "$out" | grep -Eiq '(^|[^0-9])404([^0-9]|$)|not found|no such file|does not exist'; then
    return 1
  fi
  printf '%s\n' "$out" >&2
  return 2
}

SOURCE_DIR="${DRIVE9_PERF_SOURCE_DIR:-${FUSE_PERF_ARTIFACT_DIR:-}}"
ARCHIVE_ROOT="${DRIVE9_PERF_ARCHIVE_ROOT:-/benchmarks/fuse-performance}"
ARCHIVE_ROOT="${ARCHIVE_ROOT%/}"
BRANCH="${DRIVE9_PERF_BRANCH:-${GITHUB_REF_NAME:-unknown}}"
SHA="${DRIVE9_PERF_SHA:-${GITHUB_SHA:-unknown}}"
RUN_ID="${DRIVE9_PERF_RUN_ID:-${GITHUB_RUN_ID:-manual}}"
RUN_ATTEMPT="${DRIVE9_PERF_RUN_ATTEMPT:-${GITHUB_RUN_ATTEMPT:-1}}"
WARN_RATIO="${FUSE_PERF_COMPARE_WARN_RATIO:-0.30}"
FAIL_ON_REGRESSION="${FUSE_PERF_COMPARE_FAIL_ON_REGRESSION:-1}"

[ -n "$SOURCE_DIR" ] || die "DRIVE9_PERF_SOURCE_DIR or FUSE_PERF_ARTIFACT_DIR is required"
[ -d "$SOURCE_DIR" ] || die "performance artifact directory does not exist: $SOURCE_DIR"
case "$FAIL_ON_REGRESSION" in
  0|1) ;;
  *) die "FUSE_PERF_COMPARE_FAIL_ON_REGRESSION must be 0 or 1" ;;
esac

require_cmd curl
require_cmd python3
require_cmd sed

metrics_count="$(find "$SOURCE_DIR" -type f -name 'performance-metrics-*.json' | wc -l | tr -d '[:space:]')"
if [ "$metrics_count" -ne 1 ]; then
  die "expected exactly one performance-metrics-*.json in $SOURCE_DIR, found ${metrics_count}"
fi
current_metrics="$(find "$SOURCE_DIR" -type f -name 'performance-metrics-*.json' | sort | head -n 1)"

report_slug="${RUN_ID}-${RUN_ATTEMPT}"
output_json="$SOURCE_DIR/performance-compare-${report_slug}.json"
output_md="$SOURCE_DIR/performance-compare-${report_slug}.md"
current_ref="${BRANCH}@$(printf '%s' "$SHA" | cut -c1-12) run ${RUN_ID}-${RUN_ATTEMPT}"

[ -n "${DRIVE9_SERVER:-}" ] || die "DRIVE9_SERVER is required when comparing archived performance metrics"
[ -n "${DRIVE9_API_KEY:-}" ] || die "DRIVE9_API_KEY is required when comparing archived performance metrics"

if [ -n "${DRIVE9_CLI_BIN:-}" ]; then
  [ -x "$DRIVE9_CLI_BIN" ] || die "DRIVE9_CLI_BIN is not executable: $DRIVE9_CLI_BIN"
elif command -v drive9 >/dev/null 2>&1; then
  DRIVE9_CLI_BIN="$(command -v drive9)"
else
  DRIVE9_CLI_BIN="$(download_drive9_cli)"
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

branch_slug="$(sanitize_component "$BRANCH")"
[ -n "$branch_slug" ] || branch_slug=unknown
branch_latest="${ARCHIVE_ROOT}/branches/${branch_slug}/latest.json"
global_latest="${ARCHIVE_ROOT}/latest.json"
baseline_manifest="$tmp_dir/baseline-manifest.json"
baseline_metrics="$tmp_dir/baseline-metrics.json"
baseline_ref=""
missing_reason=""

if copy_from_drive9 "$branch_latest" "$baseline_manifest"; then
  baseline_ref="$branch_latest"
else
  rc=$?
  [ "$rc" -eq 1 ] || die "failed to fetch archived branch baseline manifest ${branch_latest}"
fi

if [ -z "$baseline_ref" ]; then
  if copy_from_drive9 "$global_latest" "$baseline_manifest"; then
    baseline_ref="$global_latest"
  else
    rc=$?
    [ "$rc" -eq 1 ] || die "failed to fetch archived global baseline manifest ${global_latest}"
    missing_reason="no archived baseline found at ${branch_latest} or ${global_latest}"
  fi
fi

if [ -n "$baseline_ref" ]; then
  baseline_run_id="$(python3 - "$baseline_manifest" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    doc = json.load(handle)
print(f"{doc.get('run_id', '')}-{doc.get('run_attempt', '')}")
PY
)"
  if [ "$baseline_run_id" = "${RUN_ID}-${RUN_ATTEMPT}" ]; then
    missing_reason="latest archived baseline points at the current run (${RUN_ID}-${RUN_ATTEMPT}); skipping self-compare"
    baseline_ref=""
  else
    baseline_remote_metrics="$(python3 scripts/compare_fuse_performance_metrics.py manifest-metrics-path --manifest "$baseline_manifest" --archive-root "$ARCHIVE_ROOT")"
    if ! copy_from_drive9 "$baseline_remote_metrics" "$baseline_metrics"; then
      die "failed to fetch baseline metrics ${baseline_remote_metrics} from ${baseline_ref}"
    fi
    baseline_ref="${baseline_ref} -> ${baseline_remote_metrics}"
  fi
fi

if [ -n "$baseline_ref" ]; then
  info "compare ${current_metrics} against ${baseline_ref}"
  compare_args=()
  if [ "$FAIL_ON_REGRESSION" = "1" ]; then
    compare_args+=(--fail-on-regression)
  fi
  python3 scripts/compare_fuse_performance_metrics.py compare \
    --current "$current_metrics" \
    --baseline "$baseline_metrics" \
    --output-json "$output_json" \
    --output-markdown "$output_md" \
    --warning-ratio "$WARN_RATIO" \
    "${compare_args[@]}" \
    --current-ref "$current_ref" \
    --baseline-ref "$baseline_ref"
else
  info "write baseline-missing compare report: ${missing_reason}"
  compare_args=()
  if [ "$FAIL_ON_REGRESSION" = "1" ]; then
    compare_args+=(--fail-on-regression)
  fi
  python3 scripts/compare_fuse_performance_metrics.py compare \
    --current "$current_metrics" \
    --output-json "$output_json" \
    --output-markdown "$output_md" \
    --warning-ratio "$WARN_RATIO" \
    "${compare_args[@]}" \
    --current-ref "$current_ref" \
    --missing-baseline-reason "$missing_reason"
fi

info "wrote ${output_json}"
info "wrote ${output_md}"
