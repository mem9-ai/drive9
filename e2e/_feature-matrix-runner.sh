#!/usr/bin/env bash
# Internal Drive9 feature-matrix runner.
#
# Public entrypoints are posix-feature-matrix.sh and git-feature-matrix.sh.
# Keep this file shared so the two live E2E matrices use the same provisioning,
# FUSE, CLI, and reporting machinery without exposing a combined runner.
#
# This script is intentionally correctness-oriented. It probes supported
# features and records stable unsupported behavior in a Markdown matrix rather
# than treating every unsupported POSIX/Git edge as a performance/regression
# issue. The POSIX suite itself is pjdfstest-based; Drive9-specific FUSE
# integration checks belong in the FUSE smoke/release scripts.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-25}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
REMOTE_VISIBILITY_TIMEOUT_S="${REMOTE_VISIBILITY_TIMEOUT_S:-20}"
REMOTE_VISIBILITY_INTERVAL_S="${REMOTE_VISIBILITY_INTERVAL_S:-0.5}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
FEATURE_MATRIX_STRICT_ALL="${FEATURE_MATRIX_STRICT_ALL:-0}"
FEATURE_MATRIX_REPORT_DIR="${FEATURE_MATRIX_REPORT_DIR:-}"
GIT_MATRIX_TIMEOUT_S="${GIT_MATRIX_TIMEOUT_S:-240}"
GIT_MATRIX_RUN_OVERSIZED="${GIT_MATRIX_RUN_OVERSIZED:-1}"
FEATURE_MATRIX_SUITE="${FEATURE_MATRIX_SUITE:-}"
PJDFSTEST_DIR="${PJDFSTEST_DIR:-}"
PJDFSTEST_TESTS="${PJDFSTEST_TESTS:-}"
PJDFSTEST_BIN="${PJDFSTEST_BIN:-}"
PJDFSTEST_TIMEOUT_S="${PJDFSTEST_TIMEOUT_S:-900}"
PJDFSTEST_ALLOW_NONROOT="${PJDFSTEST_ALLOW_NONROOT:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FEATURE_MATRIX_REPORT_DIR="${FEATURE_MATRIX_REPORT_DIR:-$REPO_ROOT/e2e/reports}"
TS="$(date +%Y%m%d-%H%M%S)"
RUN_ROOT=""
RESULTS_TSV=""
REPORT_PATH=""
CLI_BIN=""
API_KEY=""
MOUNT_POINTS=()

sanitize_tsv_field() {
  printf '%s' "$1" | tr '\t\r\n' '   ' | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//' | cut -c 1-1200
}

record() {
  local status="$1" category="$2" feature="$3" detail="${4:-}"
  printf '%s\t%s\t%s\t%s\n' \
    "$(sanitize_tsv_field "$status")" \
    "$(sanitize_tsv_field "$category")" \
    "$(sanitize_tsv_field "$feature")" \
    "$(sanitize_tsv_field "$detail")" >>"$RESULTS_TSV"
}

write_report() {
  mkdir -p "$FEATURE_MATRIX_REPORT_DIR"
  python3 - "$RESULTS_TSV" "$REPORT_PATH" "$BASE" "$CLI_SOURCE" "$FEATURE_MATRIX_STRICT_ALL" "$FEATURE_MATRIX_SUITE" <<'PY'
import collections
import datetime as dt
import json
import platform
import sys

results_path, report_path, base, cli_source, strict_all, suite = sys.argv[1:7]
rows = []
pjdfstest_summary = None
with open(results_path, encoding="utf-8") as fh:
    for raw in fh:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        parts = raw.split("\t", 3)
        while len(parts) < 4:
            parts.append("")
        row = tuple(parts)
        if row[0] == "META" and row[1] == "pjdfstest" and row[2] == "summary":
            try:
                pjdfstest_summary = json.loads(row[3])
            except json.JSONDecodeError:
                pjdfstest_summary = None
            continue
        rows.append(row)

def is_pjdfstest_row(row):
    return row[1] == "pjdfstest" or row[1].startswith("pjdfstest/")

def is_posix_infra_failure(row):
    if row[0] == "PASS":
        return False
    haystack = " ".join(row[1:]).lower()
    markers = ("provision", "prereq", "prerequisite", "mount")
    return any(marker in haystack for marker in markers)

if suite == "posix":
    rows = [row for row in rows if is_pjdfstest_row(row) or is_posix_infra_failure(row)]

counts = collections.Counter(row[0] for row in rows)
categories = collections.defaultdict(list)
for row in rows:
    categories[row[1]].append(row)

def checkbox(status):
    return "- [x]" if status == "PASS" else "- [ ]"

titles = {
    "posix": "Drive9 POSIX pjdfstest Matrix Report",
    "git": "Drive9 Git Feature Matrix Report",
}

with open(report_path, "w", encoding="utf-8") as out:
    out.write(f"# {titles.get(suite, 'Drive9 Feature Matrix Report')}\n\n")
    out.write(f"**Date:** {dt.datetime.utcnow().replace(microsecond=0).isoformat()}Z\n")
    out.write(f"**Suite:** `{suite}`\n")
    out.write(f"**Base:** `{base}`\n")
    out.write(f"**CLI source:** `{cli_source}`\n")
    out.write(f"**Host:** `{platform.platform()}`\n")
    out.write(f"**Strict unchecked mode:** `{strict_all}`\n\n")
    out.write("## Summary\n\n")
    if suite == "posix" and pjdfstest_summary:
        out.write("| Metric | Count |\n|---|---:|\n")
        out.write(f"| Total cases | {pjdfstest_summary.get('total_cases', 0)} |\n")
        out.write(f"| Passed cases | {pjdfstest_summary.get('passed_cases', 0)} |\n")
        out.write(f"| Failed cases | {pjdfstest_summary.get('failed_cases', 0)} |\n")
        out.write(f"| Total files | {pjdfstest_summary.get('total_files', 0)} |\n")
        out.write(f"| Passed files | {pjdfstest_summary.get('passed_files', 0)} |\n")
        out.write(f"| Failed files | {pjdfstest_summary.get('failed_files', 0)} |\n")
        out.write(f"| Result | {pjdfstest_summary.get('result', 'UNKNOWN')} |\n\n")
        if pjdfstest_summary.get("log"):
            out.write(f"**pjdfstest log:** `{pjdfstest_summary['log']}`\n\n")
    else:
        out.write("| Status | Count |\n|---|---:|\n")
        for status in ("PASS", "FAIL", "UNSUPPORTED", "SKIP"):
            out.write(f"| {status} | {counts.get(status, 0)} |\n")
        out.write(f"| TOTAL | {len(rows)} |\n\n")
    out.write("## Matrix\n\n")
    for category in sorted(categories):
        out.write(f"### {category}\n\n")
        for status, _, feature, detail in categories[category]:
            suffix = f" - {status}: {detail}" if detail else f" - {status}"
            out.write(f"{checkbox(status)} {feature}{suffix}\n")
        out.write("\n")
PY
}

finish() {
  local rc=$?
  local report_rc=0
  local mounts_still_attached=0
  set +e
  for mp in "${MOUNT_POINTS[@]:-}"; do
    stop_mount "$mp" >/dev/null 2>&1 || true
    if [ -n "$mp" ] && is_mounted "$mp"; then
      mounts_still_attached=1
      echo "MOUNT_STILL_ATTACHED=$mp"
    fi
  done
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN" >/dev/null 2>&1 || true
  fi
  if [ -n "${REPORT_PATH:-}" ] && [ -n "${RESULTS_TSV:-}" ] && [ -f "$RESULTS_TSV" ]; then
    write_report || report_rc=$?
    echo "REPORT=$REPORT_PATH"
    if [ -f "$REPORT_PATH" ]; then
      awk '
        /^## Summary/ {in_summary=1; next}
        /^## / && in_summary {exit}
        in_summary {print}
      ' "$REPORT_PATH"
    fi
  fi
  if [ -n "${RUN_ROOT:-}" ] && [ -d "$RUN_ROOT" ]; then
    if [ "$rc" -eq 0 ] && [ "$report_rc" -eq 0 ] && [ "$mounts_still_attached" -eq 0 ]; then
      rm -rf "$RUN_ROOT" >/dev/null 2>&1 || true
    else
      echo "RUN_ROOT=$RUN_ROOT"
    fi
  fi
  exit "$rc"
}
trap finish EXIT

fail_fast() {
  local category="$1" feature="$2" detail="$3"
  record "FAIL" "$category" "$feature" "$detail"
  exit 1
}

detect_release_target() {
  case "$(uname -s)" in
    Linux) CLI_RELEASE_OS="linux" ;;
    Darwin) CLI_RELEASE_OS="darwin" ;;
    *) return 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *) return 1 ;;
  esac
}

download_official_cli() {
  local target_version="$CLI_RELEASE_VERSION"
  detect_release_target || return 1
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" | tr -d '[:space:]')
  fi
  curl -fsSL "$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
  if [ -n "$CLI_RELEASE_VERSION" ]; then
    local actual_version
    actual_version="$($CLI_BIN --version 2>/dev/null | awk '{print $2}')"
    [ "$actual_version" = "$CLI_RELEASE_VERSION" ]
  fi
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      make -C "$REPO_ROOT" build-cli CLI_BIN="$CLI_BIN"
      ;;
    official)
      download_official_cli
      ;;
    *)
      return 1
      ;;
  esac
  test -x "$CLI_BIN"
}

curl_body_code() {
  local method="$1" url="$2" auth="${3:-}" data="${4:-}"
  local body_file code rc
  body_file="$(mktemp)"
  set +e
  if [ -n "$auth" ] && [ -n "$data" ]; then
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
  elif [ -n "$auth" ]; then
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
  else
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
  fi
  rc=$?
  set -e
  cat "$body_file"
  echo
  if [ "$rc" -eq 0 ]; then
    echo "__HTTP__${code}"
  else
    echo "__HTTP__curl-rc-${rc}-${code:-000}"
  fi
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1 out rc
  while :; do
    set +e
    out=$(drive9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

run_with_timeout_capture() {
  local seconds="$1" out_file="$2"
  shift 2
  python3 - "$seconds" "$out_file" "$@" <<'PY'
import os
import signal
import subprocess
import sys
import time

seconds = float(sys.argv[1])
out_file = sys.argv[2]
cmd = sys.argv[3:]

with open(out_file, "wb") as out:
    proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + seconds
    while True:
        rc = proc.poll()
        if rc is not None:
            raise SystemExit(rc if rc >= 0 else 128 + abs(rc))
        if time.monotonic() >= deadline:
            break
        time.sleep(0.2)

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        raise SystemExit(124)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(124)
        time.sleep(0.2)

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()
    raise SystemExit(124)
PY
}

record_cmd() {
  local category="$1" feature="$2" timeout_s="$3"
  shift 3
  local out_file rc out
  out_file="$(mktemp)"
  set +e
  run_with_timeout_capture "$timeout_s" "$out_file" "$@"
  rc=$?
  set -e
  out="$(tail -c 600 "$out_file" 2>/dev/null || true)"
  rm -f "$out_file"
  if [ "$rc" -eq 0 ]; then
    record "PASS" "$category" "$feature" "ok"
  else
    record "FAIL" "$category" "$feature" "rc=$rc ${out:-<no output>}"
  fi
  return 0
}

resolve_pjdfstest_tests() {
  if [ -n "$PJDFSTEST_TESTS" ] && [ -d "$PJDFSTEST_TESTS" ]; then
    (cd "$PJDFSTEST_TESTS" && pwd -P)
    return 0
  fi
  if [ -n "$PJDFSTEST_DIR" ] && [ -d "$PJDFSTEST_DIR/tests" ]; then
    (cd "$PJDFSTEST_DIR/tests" && pwd -P)
    return 0
  fi
  local candidate
  for candidate in \
    "$REPO_ROOT/third_party/pjdfstest/tests" \
    "$REPO_ROOT/pjdfstest/tests" \
    "/usr/local/share/pjdfstest/tests" \
    "/opt/pjdfstest/tests"; do
    if [ -d "$candidate" ]; then
      (cd "$candidate" && pwd -P)
      return 0
    fi
  done
  return 1
}

resolve_pjdfstest_bin() {
  local tests_dir="$1" suite_root
  suite_root="$(cd "$tests_dir/.." && pwd -P)"
  if [ -n "$PJDFSTEST_BIN" ] && [ -x "$PJDFSTEST_BIN" ]; then
    printf '%s/%s\n' "$(cd "$(dirname "$PJDFSTEST_BIN")" && pwd -P)" "$(basename "$PJDFSTEST_BIN")"
    return 0
  fi
  if [ -x "$suite_root/pjdfstest" ]; then
    printf '%s\n' "$suite_root/pjdfstest"
    return 0
  fi
  if command -v pjdfstest >/dev/null 2>&1; then
    command -v pjdfstest
    return 0
  fi
  return 1
}

record_pjdfstest_result() {
  local status="$1" log_file="$2" rc="${3:-0}"
  python3 - "$status" "$log_file" "$RESULTS_TSV" "$rc" "$PJDFSTEST_TIMEOUT_S" <<'PY'
import json
import re
import sys
from pathlib import Path

status, log_path, results_path, rc, timeout_s = sys.argv[1:6]
text = Path(log_path).read_text(encoding="utf-8", errors="replace")

def clean(value: str) -> str:
    return " ".join(value.replace("\t", " ").split())[:1200]

def emit(row_status: str, category: str, feature: str, detail: str) -> None:
    with open(results_path, "a", encoding="utf-8") as out:
        out.write("\t".join([clean(row_status), clean(category), clean(feature), clean(detail)]) + "\n")

total_files = total_cases = None
files_line = re.search(r"Files=(\d+),\s*Tests=(\d+),[^\n]*", text)
if files_line:
    total_files = int(files_line.group(1))
    total_cases = int(files_line.group(2))

result_match = re.search(r"Result:\s*(\S+)", text)
result = result_match.group(1) if result_match else ("PASS" if status == "PASS" else "FAIL")

plans = {}
order = []
current = None
file_start_re = re.compile(r"^(?P<path>\S+/tests/(?P<rel>[^ ]+?\.t))\s+\.*\s*$")
plan_re = re.compile(r"^1\.\.(\d+)\s*$")
failed_file_re = re.compile(r"^\S+/tests/(?P<rel>[^ ]+?\.t)\s+\(Wstat:\s*\d+\s+Tests:\s*(?P<tests>\d+)\s+Failed:\s*(?P<failed>\d+)\)")

for line in text.splitlines():
    start = file_start_re.match(line)
    if start:
        current = start.group("rel")
        if current not in plans:
            order.append(current)
        continue
    if current:
        plan = plan_re.match(line)
        if plan:
            plans[current] = int(plan.group(1))

failed_files = {}
for line in text.splitlines():
    match = failed_file_re.match(line)
    if match:
        rel = match.group("rel")
        failed_files[rel] = (int(match.group("tests")), int(match.group("failed")))
        if rel not in plans:
            plans[rel] = int(match.group("tests"))
        if rel not in order:
            order.append(rel)

failed_cases = sum(failed for _, failed in failed_files.values())
if status != "PASS" and failed_cases == 0:
    failed_cases = sum(1 for line in text.splitlines() if re.match(r"^not ok\s+\d+(?:\b|\s|$)", line))
if total_cases is None:
    total_cases = sum(plans.values())
if status == "PASS":
    failed_cases = 0
passed_cases = max(total_cases - failed_cases, 0)

if total_files is None:
    total_files = len(order)
failed_file_count = len(failed_files) if status != "PASS" else 0
passed_file_count = max(total_files - failed_file_count, 0)

summary = {
    "total_cases": total_cases,
    "passed_cases": passed_cases,
    "failed_cases": failed_cases,
    "total_files": total_files,
    "passed_files": passed_file_count,
    "failed_files": failed_file_count,
    "result": "TIMEOUT" if rc == "124" else result,
    "rc": int(rc) if rc.isdigit() else rc,
    "log": log_path,
}
emit("META", "pjdfstest", "summary", json.dumps(summary, sort_keys=True, separators=(",", ":")))

if not order:
    tail = clean("\n".join(text.splitlines()[-20:])) or "no prove summary found"
    if rc == "124":
        tail = f"timeout after {timeout_s}s; {tail}"
    emit("FAIL" if status != "PASS" else "PASS", "pjdfstest", "pjdfstest run", f"rc={rc}; {tail}; log={log_path}")
    raise SystemExit(0)

if status != "PASS":
    detail = (
        f"rc={rc}; Result={summary['result']}; "
        f"Tests={total_cases} Passed={passed_cases} Failed={failed_cases}; log={log_path}"
    )
    if rc == "124":
        detail = f"timeout after {timeout_s}s; {detail}"
    emit("FAIL", "pjdfstest", "pjdfstest run result", detail)

for rel in order:
    tests = plans.get(rel)
    failed = 0
    if rel in failed_files:
        tests, failed = failed_files[rel]
    tests = tests if tests is not None else 0
    passed = max(tests - failed, 0)
    row_status = "FAIL" if failed else "PASS"
    group = rel.split("/", 1)[0] if "/" in rel else "misc"
    detail = f"Tests={tests} Passed={passed} Failed={failed}; log={log_path}"
    emit(row_status, f"pjdfstest/{group}", rel, detail)
PY
}

run_pjdfstest_suite() {
  local mount_point="$1" root_rel="$2"
  local tests_dir bin suite_root work_dir log_file rc
  if ! command -v prove >/dev/null 2>&1; then
    record "SKIP" "pjdfstest" "pjdfstest full suite" "prove not found"
    return 0
  fi
  if ! tests_dir="$(resolve_pjdfstest_tests)"; then
    record "SKIP" "pjdfstest" "pjdfstest full suite" "PJDFSTEST_TESTS/PJDFSTEST_DIR not set and no local pjdfstest tests found"
    return 0
  fi
  if ! bin="$(resolve_pjdfstest_bin "$tests_dir")"; then
    record "SKIP" "pjdfstest" "pjdfstest full suite" "pjdfstest binary not found or not executable"
    return 0
  fi
  if [ "$PJDFSTEST_ALLOW_NONROOT" != "1" ] && [ "$(id -u)" -ne 0 ]; then
    record "SKIP" "pjdfstest" "pjdfstest full suite" "pjdfstest requires root; rerun as root or set PJDFSTEST_ALLOW_NONROOT=1"
    return 0
  fi
  suite_root="$(cd "$tests_dir/.." && pwd -P)"
  work_dir="$mount_point/$root_rel/pjdfstest"
  mkdir -p "$work_dir"
  log_file="$FEATURE_MATRIX_REPORT_DIR/pjdfstest-$TS.log"
  set +e
  run_with_timeout_capture "$PJDFSTEST_TIMEOUT_S" "$log_file" bash -c \
    'cd "$1" && PATH="$2:$3:$PATH" prove --recurse --verbose "$4"' \
    bash "$work_dir" "$(dirname "$bin")" "$suite_root" "$tests_dir"
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    record_pjdfstest_result "PASS" "$log_file" "$rc"
  else
    record_pjdfstest_result "FAIL" "$log_file" "$rc"
  fi
  return 0
}

record_drive9_cmd() {
  local category="$1" feature="$2" timeout_s="$3"
  shift 3
  record_cmd "$category" "$feature" "$timeout_s" env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" "$@"
}

is_mounted() {
  local mount_point="$1"
  local physical_mount_point
  physical_mount_point="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  mount | awk -v mp="$mount_point" -v pmp="$physical_mount_point" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

wait_mount_state() {
  local mount_point="$1" expect="$2"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted "$mount_point"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted "$mount_point"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

force_unmount() {
  local mount_point="$1"
  case "$(uname -s)" in
    Darwin)
      umount "$mount_point" >/dev/null 2>&1 || diskutil unmount force "$mount_point" >/dev/null 2>&1 || true
      ;;
    Linux)
      fusermount3 -uz "$mount_point" >/dev/null 2>&1 || fusermount -uz "$mount_point" >/dev/null 2>&1 || umount -l "$mount_point" >/dev/null 2>&1 || true
      ;;
  esac
}

start_mount() {
  local mount_point="$1" log_file="$2"
  shift 2
  mkdir -p "$mount_point"
  mount_point="$(cd "$mount_point" && pwd -P)"
  {
    echo "=== drive9 feature-matrix mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    printf 'args:'
    printf ' %q' "$@"
    printf '\n'
  } >>"$log_file"
  env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" mount "$@" >>"$log_file" 2>&1 &
  local mount_pid=$!
  if wait_mount_state "$mount_point" mounted; then
    MOUNT_POINTS+=("$mount_point")
    return 0
  fi
  kill "$mount_pid" >/dev/null 2>&1 || true
  if [ -f "$log_file" ]; then
    tail -n 80 "$log_file" >&2 || true
  fi
  return 1
}

stop_mount() {
  local mount_point="$1"
  local umount_rc=0
  set +e
  if [ -n "$mount_point" ] && is_mounted "$mount_point"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$mount_point" >/dev/null 2>&1
    umount_rc=$?
    wait_mount_state "$mount_point" unmounted >/dev/null 2>&1 || true
    if is_mounted "$mount_point"; then
      force_unmount "$mount_point"
      wait_mount_state "$mount_point" unmounted >/dev/null 2>&1 || true
    fi
  fi
  set -e
  return "$umount_rc"
}

wait_file_content() {
  local path="$1" want="$2"
  local deadline
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    if [ -f "$path" ] && [ "$(cat "$path" 2>/dev/null || true)" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

wait_drive9_cat() {
  local remote_path="$1" want="$2"
  local deadline out rc
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    set +e
    out=$(drive9 fs cat "$remote_path" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ "$out" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

wait_api_get() {
  local path="$1" want="$2"
  path="${path#/}"
  local deadline resp code body
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    resp=$(curl_body_code GET "$BASE/v1/fs/$path" "$API_KEY")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    if [ "$code" = "200" ] && [ "$body" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

sha256_file() {
  python3 - "$1" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
}

wait_drive9_copy_hash() {
  local remote_path="$1" local_path="$2" want_hash="$3"
  local deadline got_hash rc
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    rm -f "$local_path"
    set +e
    drive9 fs cp "$remote_path" "$local_path" >/dev/null 2>&1
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ -f "$local_path" ]; then
      got_hash="$(sha256_file "$local_path" 2>/dev/null || true)"
      if [ "$got_hash" = "$want_hash" ]; then
        return 0
      fi
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

local_mode() {
  local path="$1"
  if [ "$(uname -s)" = "Darwin" ]; then
    stat -f "%Lp" "$path"
  else
    stat -c "%a" "$path"
  fi
}

local_size() {
  local path="$1"
  if [ "$(uname -s)" = "Darwin" ]; then
    stat -f "%z" "$path"
  else
    stat -c "%s" "$path"
  fi
}

head_mode() {
  local path="$1"
  path="${path#/}"
  local out code
  out=$(curl -sS -w "__HTTP__%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$path")
  code=$(printf '%s' "$out" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n')
  if [ "$code" != "200" ]; then
    return 1
  fi
  printf '%s' "$out" | grep -i '^x-dat9-mode:' | head -1 | cut -d: -f2- | sed 's/^[[:space:]]*//' | tr -d '\r\n'
}

git_cmd_record() {
  local repo="$1" category="$2" feature="$3"
  shift 3
  record_cmd "$category" "$feature" "$GIT_MATRIX_TIMEOUT_S" git -C "$repo" "$@"
}

git_output() {
  local repo="$1"
  shift
  git -C "$repo" "$@" 2>/dev/null | tr -d '\r'
}

record_git_clean() {
  local repo="$1" feature="$2"
  local out
  if out="$(git_output "$repo" status --porcelain=v1)" && [ -z "$out" ]; then
    record "PASS" "Git Clean Repo Readiness" "$feature" "status clean"
  else
    record "FAIL" "Git Clean Repo Readiness" "$feature" "status=${out:-<command failed>}"
  fi
}

record_status_contains() {
  local repo="$1" category="$2" feature="$3" pattern="$4"
  local out
  if out="$(git_output "$repo" status --porcelain=v1)" && grep -Eq "$pattern" <<<"$out"; then
    record "PASS" "$category" "$feature" "matched $pattern"
  else
    record "FAIL" "$category" "$feature" "status=${out:-<empty or command failed>} pattern=$pattern"
  fi
}

configure_git_identity() {
  local repo="$1"
  git -C "$repo" config user.email "drive9-matrix@example.test" >/dev/null 2>&1 || return 1
  git -C "$repo" config user.name "Drive9 Matrix" >/dev/null 2>&1 || return 1
}

repo_ready() {
  local repo="$1" category="$2" feature="$3"
  if [ -d "$repo/.git" ] && git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return 0
  fi
  record "FAIL" "$category" "$feature" "repo unavailable: $repo"
  return 1
}

make_remote_ahead_commit() {
  local bare_repo="$1" branch="$2" peer="$3"
  git clone "$bare_repo" "$peer" >/dev/null 2>&1 || return 1
  configure_git_identity "$peer" || return 1
  git -C "$peer" checkout "$branch" >/dev/null 2>&1 || return 1
  printf 'remote ahead\n' > "$peer/remote-ahead.txt"
  git -C "$peer" add remote-ahead.txt >/dev/null 2>&1 || return 1
  git -C "$peer" commit -m "remote ahead" >/dev/null 2>&1 || return 1
  git -C "$peer" push origin "$branch" >/dev/null 2>&1 || return 1
}

clone_drive9_repo() {
  local feature="$1" repo_url="$2" target="$3"
  shift 3
  record_drive9_cmd "Git Clone Modes" "$feature" "$GIT_MATRIX_TIMEOUT_S" git clone --fast "$@" "$repo_url" "$target"
}

run_git_readiness_checks() {
  local repo="$1"
  git_cmd_record "$repo" "Git Clean Repo Readiness" ".git directory is usable" rev-parse --is-inside-work-tree
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git log reads latest commit" log --oneline -1
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git show reads HEAD" show --stat --oneline -1
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git ls-files lists manifest" ls-files
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git cat-file reads clean blob" cat-file -e HEAD:README.md
  record_git_clean "$repo" "git status clean after fast clone"
  if [ -x "$repo/script.sh" ] && git -C "$repo" ls-files -s script.sh | grep -q '^100755 '; then
    record "PASS" "Git Clean Repo Readiness" "executable bit visible" "script.sh executable"
  else
    record "FAIL" "Git Clean Repo Readiness" "executable bit visible" "script.sh is not executable or index mode mismatch"
  fi
  if [ -L "$repo/link-to-readme" ] && [ "$(readlink "$repo/link-to-readme")" = "README.md" ]; then
    record "PASS" "Git Clean Repo Readiness" "symlink visible" "link-to-readme -> README.md"
  else
    record "FAIL" "Git Clean Repo Readiness" "symlink visible" "missing or wrong link"
  fi
  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
data = Path(sys.argv[1]).read_bytes()
raise SystemExit(0 if data == bytes(range(32)) else 1)
PY
  then
    record "PASS" "Git Clean Repo Readiness" "binary file visible" "binary bytes match"
  else
    record "FAIL" "Git Clean Repo Readiness" "binary file visible" "binary bytes mismatch"
  fi
  if git -C "$repo" tag --list v0.1.0 | grep -q '^v0.1.0$'; then
    record "PASS" "Git Clean Repo Readiness" "tag visibility" "v0.1.0"
  else
    record "FAIL" "Git Clean Repo Readiness" "tag visibility" "v0.1.0 missing"
  fi
}

run_git_ops_suite() {
  local repo="$1" bare_repo="$2" ts="$3"
  if ! repo_ready "$repo" "Git Prerequisites" "ops repo ready"; then
    return
  fi
  if configure_git_identity "$repo"; then
    record "PASS" "Git Prerequisites" "configure git identity for ops repo" "ok"
  else
    record "FAIL" "Git Prerequisites" "configure git identity for ops repo" "git config failed"
    return
  fi

  printf '\ntracked edit %s\n' "$ts" >> "$repo/README.md"
  record_status_contains "$repo" "Git Working Tree Operations" "modify tracked file" '^ M README\.md$'
  git_cmd_record "$repo" "Git Index Operations" "git add individual path" add README.md
  printf 'extra unstaged %s\n' "$ts" >> "$repo/README.md"
  record_status_contains "$repo" "Git Index Operations" "staged vs unstaged status accuracy" '^MM README\.md$'
  git_cmd_record "$repo" "Git Index Operations" "git restore --staged" restore --staged README.md
  record_status_contains "$repo" "Git Index Operations" "unstaged status after restore --staged" '^ M README\.md$'

  mkdir -p "$repo/generated/dir"
  printf 'generated %s\n' "$ts" > "$repo/generated/dir/new.txt"
  record_status_contains "$repo" "Git Working Tree Operations" "create files/directories" '^\?\? generated/'

  git_cmd_record "$repo" "Git Working Tree Operations" "git mv tracked file" mv src/app.py src/app_renamed.py
  git_cmd_record "$repo" "Git Working Tree Operations" "git rm tracked file" rm docs/guide.md
  if chmod 0644 "$repo/script.sh"; then
    record_status_contains "$repo" "Git Working Tree Operations" "chmod executable bit change" '^ M script\.sh$'
  else
    record "FAIL" "Git Working Tree Operations" "chmod executable bit change" "chmod failed"
  fi
  rm -f "$repo/link-to-readme"
  if ! ln -s src/app_renamed.py "$repo/link-to-app"; then
    record "FAIL" "Git Working Tree Operations" "symlink changes" "ln -s failed"
  fi
  record_status_contains "$repo" "Git Working Tree Operations" "symlink changes" '^( D link-to-readme|\?\? link-to-app)'
  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
p = Path(sys.argv[1])
data = bytearray(p.read_bytes())
data[0:4] = b"D9MX"
p.write_bytes(data)
PY
  then
    record_status_contains "$repo" "Git Working Tree Operations" "binary file modification" '^ M binary\.bin$'
  else
    record "FAIL" "Git Working Tree Operations" "binary file modification" "binary edit failed"
  fi
  mkdir -p "$repo/ignored-build"
  printf 'ignored\n' > "$repo/ignored-build/cache.tmp"
  if git -C "$repo" check-ignore -q ignored-build/cache.tmp; then
    record "PASS" "Git Working Tree Operations" "ignored local-only generated files" "git check-ignore accepted"
  else
    record "FAIL" "Git Working Tree Operations" "ignored local-only generated files" "ignored-build/cache.tmp was not ignored"
  fi

  printf 'reset me\n' > "$repo/generated/reset.txt"
  git_cmd_record "$repo" "Git Index Operations" "git add -A" add -A
  git_cmd_record "$repo" "Git Index Operations" "git reset path" reset HEAD generated/reset.txt
  record_status_contains "$repo" "Git Index Operations" "git reset leaves path unstaged" '^\?\? generated/reset\.txt$'
  git_cmd_record "$repo" "Git Index Operations" "git add -A after reset" add -A

  git_cmd_record "$repo" "Git Diff And Patch" "git diff --cached" diff --cached --stat
  git_cmd_record "$repo" "Git Commit History" "git commit" commit --no-verify -m "drive9 matrix ops"
  record_git_clean "$repo" "clean status after commit"

  printf '\npatch text %s\n' "$ts" >> "$repo/README.md"
  if git -C "$repo" diff > "$repo/../text.patch"; then
    record "PASS" "Git Diff And Patch" "generate text patch" "ok"
  else
    record "FAIL" "Git Diff And Patch" "generate text patch" "git diff failed"
  fi
  git_cmd_record "$repo" "Git Diff And Patch" "restore before text patch apply" checkout -- README.md
  git_cmd_record "$repo" "Git Diff And Patch" "git apply text patch" apply "$repo/../text.patch"
  if git -C "$repo" diff -- README.md | grep -q 'patch text'; then
    record "PASS" "Git Diff And Patch" "git diff nonempty" "patch text visible"
  else
    record "FAIL" "Git Diff And Patch" "git diff nonempty" "expected patch text not visible"
  fi
  git_cmd_record "$repo" "Git Index Operations" "stage text patch result" add README.md

  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
p = Path(sys.argv[1])
data = bytearray(p.read_bytes())
data[-4:] = b"BIN!"
p.write_bytes(data)
PY
  then
    record "PASS" "Git Diff And Patch" "prepare binary patch edit" "ok"
  else
    record "FAIL" "Git Diff And Patch" "prepare binary patch edit" "binary edit failed"
  fi
  if git -C "$repo" diff --binary > "$repo/../binary.patch"; then
    record "PASS" "Git Diff And Patch" "generate binary patch" "ok"
  else
    record "FAIL" "Git Diff And Patch" "generate binary patch" "git diff --binary failed"
  fi
  git_cmd_record "$repo" "Git Diff And Patch" "restore before binary patch apply" checkout -- binary.bin
  git_cmd_record "$repo" "Git Diff And Patch" "git apply binary patch" apply "$repo/../binary.patch"
  git_cmd_record "$repo" "Git Index Operations" "stage binary patch result" add binary.bin
  git_cmd_record "$repo" "Git Commit History" "git commit --amend" commit --amend --no-edit --no-verify
  record_git_clean "$repo" "clean status after amend"

  local branch="drive9-matrix-$ts"
  git_cmd_record "$repo" "Git Commit History" "branch create/switch" switch -c "$branch"
  printf 'branch work\n' > "$repo/branch-work.txt"
  git_cmd_record "$repo" "Git Index Operations" "stage branch work" add branch-work.txt
  git_cmd_record "$repo" "Git Commit History" "branch commit" commit --no-verify -m "branch work"
  git_cmd_record "$repo" "Git Remote Operations" "push branch to local bare remote" push -u origin HEAD
  git_cmd_record "$repo" "Git Remote Operations" "fetch from local bare remote" fetch origin
  local tag="matrix-$ts"
  git_cmd_record "$repo" "Git Remote Operations" "create local tag" tag "$tag"
  git_cmd_record "$repo" "Git Remote Operations" "push tag to local bare remote" push origin "refs/tags/$tag"
  if make_remote_ahead_commit "$bare_repo" "$branch" "$RUN_ROOT/peer-$ts"; then
    record "PASS" "Git Remote Operations" "remote ahead fixture commit" "ok"
  else
    record "FAIL" "Git Remote Operations" "remote ahead fixture commit" "failed to update bare remote"
  fi
  git_cmd_record "$repo" "Git Remote Operations" "pull from local bare remote" pull --ff-only origin "$branch"
}

clone_for_flow() {
  local name="$1" file_url="$2" mount_point="$3"
  local target="$mount_point/$name"
  record_drive9_cmd "Git Clone Modes" "$name clone for flow" "$GIT_MATRIX_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$target"
  if ! configure_git_identity "$target"; then
    record "FAIL" "Git Prerequisites" "$name git identity configured" "git config failed"
  fi
  printf '%s' "$target"
}

run_git_flow_suite() {
  local mount_point="$1" file_url="$2"
  local repo

  repo="$(clone_for_flow merge-flow "$file_url" "$mount_point")"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "merge-flow repo ready"; then
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "clean merge" merge origin/feature/clean-merge --no-edit
  fi

  repo="$(clone_for_flow conflict-flow "$file_url" "$mount_point")"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "conflict-flow repo ready"; then
    printf 'local conflict\n' > "$repo/README.md"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "conflict fixture stage local edit" add README.md
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "conflict fixture local commit" commit --no-verify -m "local conflict"
    if git -C "$repo" merge origin/feature/conflict >/dev/null 2>&1; then
      record "FAIL" "Git Merge/Rebase/Stash" "conflict detection" "merge unexpectedly succeeded"
    else
      record_status_contains "$repo" "Git Merge/Rebase/Stash" "conflict detection" '^UU README\.md$'
      git -C "$repo" merge --abort >/dev/null 2>&1 || true
    fi
  fi

  repo="$(clone_for_flow rebase-flow "$file_url" "$mount_point")"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "rebase-flow repo ready"; then
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture branch create" switch -c local-rebase
    mkdir -p "$repo/docs"
    printf 'local rebase\n' > "$repo/docs/rebase-local.md"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture stage local file" add docs/rebase-local.md
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture local commit" commit --no-verify -m "local rebase"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "simple rebase" rebase origin/feature/rebase
  fi

  repo="$(clone_for_flow stash-flow "$file_url" "$mount_point")"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "stash-flow repo ready"; then
    printf 'stash edit\n' >> "$repo/README.md"
    printf 'stash untracked\n' > "$repo/stash-new.txt"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash push -u" stash push -u -m "matrix stash"
    record_git_clean "$repo" "clean status after stash push"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash apply" stash apply
    record_status_contains "$repo" "Git Merge/Rebase/Stash" "dirty status after stash apply" 'README\.md'
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash drop" stash drop
  fi
}

run_restore_suite() {
  local git_root_rel="$1" file_url="$2" mount_point="$3" local_root_a="$4" log_file_a="$5"
  local restore_repo="$mount_point/restore-workspace"
  record_drive9_cmd "Git Clone Modes" "restore workspace clone" "$GIT_MATRIX_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$restore_repo"
  if ! repo_ready "$restore_repo" "Sandbox Restore" "restore workspace repo ready"; then
    return
  fi
  if configure_git_identity "$restore_repo"; then
    record "PASS" "Git Prerequisites" "configure git identity for restore repo" "ok"
  else
    record "FAIL" "Git Prerequisites" "configure git identity for restore repo" "git config failed"
    return
  fi

  printf 'committed restore\n' > "$restore_repo/committed-local.txt"
  git_cmd_record "$restore_repo" "Sandbox Restore" "stage committed local state before remount" add committed-local.txt
  git_cmd_record "$restore_repo" "Sandbox Restore" "commit local state before remount" commit --no-verify -m "restore local commit"
  local restore_head
  restore_head="$(git_output "$restore_repo" rev-parse HEAD || true)"

  printf 'unstaged restore\n' >> "$restore_repo/README.md"
  mkdir -p "$restore_repo/restore-dir"
  printf 'overlay dir file\n' > "$restore_repo/restore-dir/file.txt"
  git_cmd_record "$restore_repo" "Drive9 Git Workspace Behavior" "prepare overlay whiteout" rm docs/guide.md
  if chmod 0644 "$restore_repo/script.sh"; then
    record "PASS" "Drive9 Git Workspace Behavior" "prepare overlay chmod" "script.sh mode set to 644"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "prepare overlay chmod" "chmod failed"
  fi
  rm -f "$restore_repo/link-to-readme"
  if ln -s README.md "$restore_repo/restore-link"; then
    record "PASS" "Drive9 Git Workspace Behavior" "prepare overlay symlink" "restore-link -> README.md"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "prepare overlay symlink" "ln -s failed"
  fi
  printf 'small staged object\n' > "$restore_repo/small-staged.txt"
  git_cmd_record "$restore_repo" "Sandbox Restore" "stage small local object before remount" add small-staged.txt
  if [ "$GIT_MATRIX_RUN_OVERSIZED" = "1" ]; then
    python3 - "$restore_repo/oversized-staged.bin" <<'PY'
import sys
from pathlib import Path
Path(sys.argv[1]).write_bytes(b"D9" * (3 * 1024 * 1024))
PY
    git_cmd_record "$restore_repo" "Drive9 Git Workspace Behavior" "stage oversized object before remount" add oversized-staged.bin
  else
    record "SKIP" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "GIT_MATRIX_RUN_OVERSIZED=0"
  fi
  record_status_contains "$restore_repo" "Sandbox Restore" "dirty status before remount" 'README\.md'

  if stop_mount "$mount_point" && ! is_mounted "$mount_point"; then
    record "PASS" "Drive9 Git Workspace Behavior" "unmount drains git workspace state" "rw coding-agent mount unmounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "unmount drains git workspace state" "rw coding-agent mount did not gracefully unmount"
  fi

  local mount_point_b="$RUN_ROOT/git-mount-b"
  local local_root_b="$RUN_ROOT/git-local-b"
  local log_file_b="$RUN_ROOT/git-mount-b.log"
  mkdir -p "$mount_point_b" "$local_root_b"
  if start_mount "$mount_point_b" "$log_file_b" --mode=fuse --profile=coding-agent --local-root "$local_root_b" --durability=interactive ":/$git_root_rel" "$mount_point_b"; then
    record "PASS" "Drive9 Git Workspace Behavior" "fresh local-root remount starts" "mounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "fresh local-root remount starts" "see $log_file_b"
    return
  fi

  restore_repo="$mount_point_b/restore-workspace"
  if [ -d "$restore_repo/.git" ] && git -C "$restore_repo" status --porcelain=v1 >/dev/null 2>&1; then
    record "PASS" "Drive9 Git Workspace Behavior" ".git checkpoint restored" "git status works"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" ".git checkpoint restored" "missing .git or git status failed"
  fi
  if grep -q "unstaged restore" "$restore_repo/README.md" && [ -f "$restore_repo/restore-dir/file.txt" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay upsert/dir survives remount" "README and restore-dir restored"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay upsert/dir survives remount" "missing edited README or restore-dir"
  fi
  if [ ! -e "$restore_repo/docs/guide.md" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay whiteout survives remount" "docs/guide.md absent"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay whiteout survives remount" "docs/guide.md still exists"
  fi
  if [ "$(local_mode "$restore_repo/script.sh")" = "644" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay chmod survives remount" "script.sh mode 644"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay chmod survives remount" "mode=$(local_mode "$restore_repo/script.sh" 2>/dev/null || echo missing)"
  fi
  if [ -L "$restore_repo/restore-link" ] && [ "$(readlink "$restore_repo/restore-link")" = "README.md" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay symlink survives remount" "restore-link -> README.md"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay symlink survives remount" "missing or wrong link"
  fi
  if [ -n "$restore_head" ] && [ "$(git_output "$restore_repo" rev-parse HEAD || true)" = "$restore_head" ] && [ -f "$restore_repo/committed-local.txt" ]; then
    record "PASS" "Sandbox Restore" "committed local state survives fresh local-root remount" "HEAD=$restore_head"
  else
    record "FAIL" "Sandbox Restore" "committed local state survives fresh local-root remount" "HEAD=$(git_output "$restore_repo" rev-parse HEAD || true)"
  fi
  record_status_contains "$restore_repo" "Sandbox Restore" "unstaged edits survive fresh local-root remount" 'README\.md'
  record_status_contains "$restore_repo" "Sandbox Restore" "small staged object preserved" '^A  small-staged\.txt$'
  if [ "$GIT_MATRIX_RUN_OVERSIZED" = "1" ]; then
    local status
    status="$(git_output "$restore_repo" status --porcelain=v1 || true)"
    if grep -Eq '^(\?\?| A|AM| M) oversized-staged\.bin$' <<<"$status" && ! grep -q '^A  oversized-staged\.bin$' <<<"$status"; then
      record "PASS" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "status downgraded: $(grep 'oversized-staged.bin' <<<"$status" | head -1)"
    else
      record "FAIL" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "status=${status:-<empty>}"
    fi
  fi
  mkdir -p "$restore_repo/ignored-build"
  printf 'local ignored\n' > "$restore_repo/ignored-build/cache.tmp"
  stop_mount "$mount_point_b" >/dev/null 2>&1 || true
  mkdir -p "$mount_point_b" "$RUN_ROOT/git-local-c"
  if start_mount "$mount_point_b" "$RUN_ROOT/git-mount-c.log" --mode=fuse --profile=coding-agent --local-root "$RUN_ROOT/git-local-c" --durability=interactive ":/$git_root_rel" "$mount_point_b"; then
    if [ ! -e "$mount_point_b/restore-workspace/ignored-build/cache.tmp" ]; then
      record "PASS" "Sandbox Restore" "ignored generated files are non-durable by design" "ignored-build/cache.tmp absent after fresh local root"
    else
      record "FAIL" "Sandbox Restore" "ignored generated files are non-durable by design" "ignored file unexpectedly restored"
    fi
  else
    record "FAIL" "Sandbox Restore" "ignored generated files are non-durable by design" "remount failed"
  fi
  stop_mount "$mount_point_b" >/dev/null 2>&1 || true
  [ -n "$log_file_a" ] && [ -f "$log_file_a" ] && : >"$log_file_a"
  [ -n "$local_root_a" ] && [ -d "$local_root_a" ] && : >"$local_root_a/.keep" 2>/dev/null || true
}

record_fuse_prereq_skips() {
  local reason="$1"
  if [ "$FEATURE_MATRIX_SUITE" != "git" ]; then
    record "SKIP" "pjdfstest" "pjdfstest full suite" "$reason"
  fi
  if [ "$FEATURE_MATRIX_SUITE" != "posix" ]; then
    record "SKIP" "Git Clone Modes" "all drive9 git clone modes" "$reason"
    record "SKIP" "Drive9 Git Workspace Behavior" "sandbox restore" "$reason"
  fi
}

main() {
  mkdir -p "$FEATURE_MATRIX_REPORT_DIR"
  local report_prefix
  case "$FEATURE_MATRIX_SUITE" in
    posix) report_prefix="posix-feature-report" ;;
    git) report_prefix="git-feature-report" ;;
    *) report_prefix="feature-matrix-report" ;;
  esac
  REPORT_PATH="$FEATURE_MATRIX_REPORT_DIR/$report_prefix-$TS.md"
  RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-feature-matrix.XXXXXX")"
  RUN_ROOT="$(cd "$RUN_ROOT" && pwd -P)"
  RESULTS_TSV="$RUN_ROOT/results.tsv"
  : >"$RESULTS_TSV"

  case "$FEATURE_MATRIX_SUITE" in
    posix|git) ;;
    *) fail_fast "Prerequisites" "FEATURE_MATRIX_SUITE valid" "got ${FEATURE_MATRIX_SUITE:-<empty>}, want posix|git; use e2e/posix-feature-matrix.sh or e2e/git-feature-matrix.sh" ;;
  esac

  echo "=== drive9 $FEATURE_MATRIX_SUITE feature matrix ==="
  echo "SUITE=$FEATURE_MATRIX_SUITE"
  echo "BASE=$BASE"
  echo "CLI_SOURCE=$CLI_SOURCE"
  echo "REPORT_PATH=$REPORT_PATH"

  record "PASS" "Prerequisites" "feature matrix suite selected" "$FEATURE_MATRIX_SUITE"

  command -v python3 >/dev/null 2>&1 || fail_fast "Prerequisites" "python3 available" "python3 not found"
  record "PASS" "Prerequisites" "python3 available" "ok"
  command -v curl >/dev/null 2>&1 || fail_fast "Prerequisites" "curl available" "curl not found"
  record "PASS" "Prerequisites" "curl available" "ok"
  command -v jq >/dev/null 2>&1 || fail_fast "Prerequisites" "jq available" "jq not found"
  record "PASS" "Prerequisites" "jq available" "ok"
  if [ "$FEATURE_MATRIX_SUITE" != "posix" ]; then
    command -v git >/dev/null 2>&1 || fail_fast "Prerequisites" "git available" "git not found"
    record "PASS" "Prerequisites" "git available" "ok"
  fi
  if [ "$CLI_SOURCE" = "build" ]; then
    command -v go >/dev/null 2>&1 || fail_fast "Prerequisites" "go available for CLI build" "go not found"
    record "PASS" "Prerequisites" "go available for CLI build" "ok"
  fi

  if prepare_cli_binary; then
    record "PASS" "Prerequisites" "drive9 CLI ready" "$CLI_BIN"
  else
    fail_fast "Prerequisites" "drive9 CLI ready" "failed to prepare CLI"
  fi

  if [ -n "$DRIVE9_API_KEY" ]; then
    API_KEY="$DRIVE9_API_KEY"
    record "PASS" "Provisioning" "use provided DRIVE9_API_KEY" "provided"
  else
    local resp code body
    resp=$(curl_body_code POST "$BASE/v1/provision")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    if [ "$code" = "202" ]; then
      record "PASS" "Provisioning" "POST /v1/provision returns 202" "ok"
    else
      fail_fast "Provisioning" "POST /v1/provision returns 202" "code=$code body=$body"
    fi
    API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty' 2>/dev/null || true)
    [ -n "$API_KEY" ] || fail_fast "Provisioning" "provision returns api_key" "$body"
    record "PASS" "Provisioning" "provision returns api_key" "ok"
  fi

  local deadline state scode sbody sresp
  deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  state=""
  while :; do
    sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
    scode=$(http_code "$sresp")
    sbody=$(json_body "$sresp")
    state=$(printf '%s' "$sbody" | jq -r '.status // empty' 2>/dev/null || true)
    if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep "$POLL_INTERVAL_S"
  done
  if [ "$state" = "active" ]; then
    record "PASS" "Provisioning" "tenant becomes active" "active"
  else
    fail_fast "Provisioning" "tenant becomes active" "status=$scode:$state body=$sbody"
  fi

  if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
    record_fuse_prereq_skips "unsupported OS $(uname -s)"
    if [ "$FUSE_STRICT_PREREQS" = "1" ] || [ "$FEATURE_MATRIX_STRICT_ALL" = "1" ]; then
      exit 1
    fi
    return
  fi
  if [ "$(uname -s)" = "Linux" ]; then
    if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
      record_fuse_prereq_skips "fusermount/fusermount3 missing"
      if [ "$FUSE_STRICT_PREREQS" = "1" ] || [ "$FEATURE_MATRIX_STRICT_ALL" = "1" ]; then
        exit 1
      fi
      return
    fi
    if [ ! -e /dev/fuse ]; then
      record_fuse_prereq_skips "/dev/fuse missing"
      if [ "$FUSE_STRICT_PREREQS" = "1" ] || [ "$FEATURE_MATRIX_STRICT_ALL" = "1" ]; then
        exit 1
      fi
      return
    fi
  fi
  record "PASS" "Prerequisites" "FUSE host prerequisites" "ok"

  if [ "$FEATURE_MATRIX_SUITE" != "git" ]; then
  local root_rel="feature-matrix-$TS"
  drive9_retry fs mkdir ":/$root_rel" >/dev/null

  local rw_mount="$RUN_ROOT/mount-rw"
  local rw_log="$RUN_ROOT/mount-rw.log"
  if start_mount "$rw_mount" "$rw_log" --mode=fuse --durability=write-sync ":/" "$rw_mount"; then
    :
  else
    record "FAIL" "pjdfstest" "pjdfstest setup mount" "rw mount failed; see $rw_log"
    return
  fi

  run_pjdfstest_suite "$rw_mount" "$root_rel"
  stop_mount "$rw_mount" >/dev/null 2>&1 || true
  fi

  if [ "$FEATURE_MATRIX_SUITE" != "posix" ]; then
  local fixture_json fixture_root bare_repo file_url
  fixture_root="$RUN_ROOT/git-fixture"
  fixture_json="$(python3 "$SCRIPT_DIR/git_fixture.py" "$fixture_root")"
  bare_repo="$(printf '%s' "$fixture_json" | jq -r '.bare_repo')"
  file_url="$(printf '%s' "$fixture_json" | jq -r '.file_url')"
  record "PASS" "Git Fixture" "local bare fixture repo generated" "$bare_repo"

  local git_root_rel="git-feature-matrix-$TS"
  drive9_retry fs mkdir ":/$git_root_rel" >/dev/null
  local git_mount="$RUN_ROOT/git-mount-a"
  local git_local="$RUN_ROOT/git-local-a"
  mkdir -p "$git_mount" "$git_local"
  if start_mount "$git_mount" "$RUN_ROOT/git-mount-a.log" --mode=fuse --profile=coding-agent --local-root "$git_local" --durability=interactive ":/$git_root_rel" "$git_mount"; then
    record "PASS" "Drive9 Git Workspace Behavior" "coding-agent mount starts" "mounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "coding-agent mount starts" "mount failed"
    return
  fi

  clone_drive9_repo "drive9 git clone --fast" "$bare_repo" "$git_mount/fast-full"
  clone_drive9_repo "drive9 git clone --fast --blobless --hydrate=off" "$file_url" "$git_mount/blobless-off" --blobless --hydrate=off
  clone_drive9_repo "drive9 git clone --fast --blobless --hydrate=sync" "$file_url" "$git_mount/blobless-sync" --blobless --hydrate=sync
  clone_drive9_repo "drive9 git clone --fast --blobless then explicit hydrate" "$file_url" "$git_mount/explicit-hydrate" --blobless --hydrate=off
  record_drive9_cmd "Git Clone Modes" "drive9 git hydrate explicit" "$GIT_MATRIX_TIMEOUT_S" git hydrate "$git_mount/explicit-hydrate"

  local ops_repo="$git_mount/ops"
  record_drive9_cmd "Git Clone Modes" "ops clone for full Git operation suite" "$GIT_MATRIX_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$ops_repo"
  run_git_readiness_checks "$ops_repo"
  run_git_ops_suite "$ops_repo" "$bare_repo" "$TS"
  run_git_flow_suite "$git_mount" "$file_url"

  local ws_json ws_id
  ws_json="$(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/git-workspaces?root_path=/$git_root_rel/ops/" || true)"
  ws_id="$(printf '%s' "$ws_json" | jq -r '.workspace_id // empty' 2>/dev/null || true)"
  if [ -n "$ws_id" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "tree manifest registered" "workspace_id=$ws_id"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "tree manifest registered" "$ws_json"
  fi

  run_restore_suite "$git_root_rel" "$file_url" "$git_mount" "$git_local" "$RUN_ROOT/git-mount-a.log"
  fi

  local fail_count unchecked_count
  fail_count="$(awk -F'\t' '$1=="FAIL"{c++} END{print c+0}' "$RESULTS_TSV")"
  unchecked_count="$(awk -F'\t' '$1!="PASS" && $1!="META"{c++} END{print c+0}' "$RESULTS_TSV")"
  if [ "$fail_count" -ne 0 ]; then
    return 1
  fi
  if [ "$FEATURE_MATRIX_STRICT_ALL" = "1" ] && [ "$unchecked_count" -ne 0 ]; then
    return 1
  fi
  return 0
}

main "$@"
