#!/usr/bin/env bash
#
# Regression tests for scripts/publish-agent-archive.sh retention, incomplete
# commit cleanup, latest.json protection, cross-branch ranking, failure
# isolation, and post-prune async-GC upload retry.
#
# Usage: scripts/test-publish-agent-archive.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/publish-agent-archive.sh"
WORK="$(mktemp -d)"
BG_PGIDS_FILE="${WORK}/.bg-pgids"
RELEASE_FILES_FILE="${WORK}/.release-files"
: >"$BG_PGIDS_FILE"
: >"$RELEASE_FILES_FILE"
LAST_BG_PID=""

register_bg_pgid() {
  printf '%s\n' "$1" >>"$BG_PGIDS_FILE"
}

register_release_file() {
  printf '%s\n' "$1" >>"$RELEASE_FILES_FILE"
}

pass() {
  echo "   ok: $*"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_pgid_isolation() {
  if ! command -v python3 >/dev/null 2>&1 && ! command -v setsid >/dev/null 2>&1; then
    fail "process-group isolation requires python3 or setsid"
  fi
}

require_pgid_isolation

spawn_isolated_bg() {
  local out_file=$1
  local err_file=$2
  shift 2
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "$@" >"$out_file" 2>"$err_file" &
  elif command -v setsid >/dev/null 2>&1; then
    setsid "$@" >"$out_file" 2>"$err_file" &
  else
    fail "process-group isolation requires python3 or setsid"
  fi
  LAST_BG_PID=$!
  register_bg_pgid "$LAST_BG_PID"
}

terminate_pgid() {
  local pgid=$1
  kill -TERM -- "-${pgid}" 2>/dev/null || kill -TERM "$pgid" 2>/dev/null || true
}

kill_pgid() {
  local pgid=$1
  kill -KILL -- "-${pgid}" 2>/dev/null || kill -KILL "$pgid" 2>/dev/null || true
}

cleanup_test_harness() {
  local f pgid
  if [ -f "$RELEASE_FILES_FILE" ]; then
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      touch "$f" 2>/dev/null || true
    done <"$RELEASE_FILES_FILE"
  fi
  if [ -f "$BG_PGIDS_FILE" ]; then
    while IFS= read -r pgid; do
      [ -n "$pgid" ] || continue
      terminate_pgid "$pgid"
    done <"$BG_PGIDS_FILE"
    sleep 0.15
    while IFS= read -r pgid; do
      [ -n "$pgid" ] || continue
      kill_pgid "$pgid"
      wait "$pgid" 2>/dev/null || true
    done <"$BG_PGIDS_FILE"
  fi
  rm -rf "$WORK"
}
trap cleanup_test_harness EXIT INT TERM

# Self-test mode invoked by Scenario 19 to verify this harness's exact
# spawn_isolated_bg + cleanup_test_harness EXIT path reaps nested descendants.
if [ -n "${TEST_HARNESS_CLEANUP_SELFTEST_PID_FILE:-}" ]; then
  spawn_isolated_bg /dev/null /dev/null \
    bash -c 'bash -c "sleep 300 & echo \$! >\"$1\"; wait"' _ "$TEST_HARNESS_CLEANUP_SELFTEST_PID_FILE"
  selftest_i=0
  while [ ! -s "$TEST_HARNESS_CLEANUP_SELFTEST_PID_FILE" ]; do
    selftest_i=$((selftest_i + 1))
    [ "$selftest_i" -lt 100 ] || fail "timed out waiting for selftest grandchild pid"
    sleep 0.05
  done
  fail "simulated early test harness failure"
fi

assert_exists() {
  [ -e "$1" ] || fail "expected path to exist: $1"
}

assert_missing() {
  [ ! -e "$1" ] || fail "expected path to be absent: $1"
}

# Build a lightweight fake drive9 CLI backed by $FAKE_REMOTE_ROOT.
FAKE_CLI="${WORK}/fake-drive9"
cat >"$FAKE_CLI" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

root="${FAKE_REMOTE_ROOT:?}"
ops_log="${FAKE_OPS_LOG:?}"
fail_quota_file="${FAKE_FAIL_QUOTA_COUNT_FILE:-}"
fail_rm_pattern="${FAKE_FAIL_RM_PATTERN:-}"
fail_stat_pattern="${FAKE_FAIL_STAT_PATTERN:-}"
fail_ls_call="${FAKE_FAIL_LS_ON_CALL:-}"
ls_count_file="${FAKE_LS_COUNT_FILE:-}"
fail_revoke_pattern="${FAKE_FAIL_REVOKE_PATTERN:-}"
pause_upload_pattern="${FAKE_PAUSE_AFTER_UPLOAD_PATTERN:-}"
pause_signal_file="${FAKE_PAUSE_SIGNAL_FILE:-}"
pause_release_file="${FAKE_PAUSE_RELEASE_FILE:-}"
pause_after_auth_rm_pattern="${FAKE_PAUSE_AFTER_AUTH_RM_PATTERN:-}"
pause_after_auth_cp_pattern="${FAKE_PAUSE_AFTER_AUTH_CP_PATTERN:-}"
ambiguous_after_auth_rm_pattern="${FAKE_AMBIGUOUS_AFTER_AUTH_RM_PATTERN:-}"
pause_auth_signal_file="${FAKE_PAUSE_AUTH_SIGNAL_FILE:-}"
pause_auth_release_file="${FAKE_PAUSE_AUTH_RELEASE_FILE:-}"

strip_ref() {
  local ref=$1
  ref="${ref#:}"
  ref="${ref#/}"
  printf '%s\n' "$ref"
}

file_mtime() {
  stat -c %Y "$1" 2>/dev/null || stat -f %m "$1"
}

fake_now() {
  if [ -n "${DRIVE9_ARCHIVE_NOW_FILE:-}" ] && [ -f "${DRIVE9_ARCHIVE_NOW_FILE}" ]; then
    local fixed
    fixed="$(cat "${DRIVE9_ARCHIVE_NOW_FILE}" 2>/dev/null || true)"
    if [[ "$fixed" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$fixed"
      return 0
    fi
  fi
  date -u '+%s'
}

epoch_to_iso() {
  local ep=$1
  jq -nr --argjson ep "$ep" '$ep | todateiso8601'
}

verify_fs_auth() {
  local key="${DRIVE9_API_KEY:-}"
  if [ "$key" = "test-key" ]; then
    return 0
  fi
  if [[ "$key" == drive9_scoped_* ]]; then
    local tid="${key#drive9_scoped_}"
    local rec="${root}/.tokens/${tid}"
    if [ ! -f "$rec" ]; then
      echo "HTTP 401: invalid API key" >&2
      exit 1
    fi
    local exp status
    IFS=$'\t' read -r exp status <"$rec"
    if [ "$status" != "active" ]; then
      echo "HTTP 401: invalid API key (token revoked)" >&2
      exit 1
    fi
    local now
    now="$(fake_now)"
    if [ "$now" -ge "$exp" ]; then
      echo "HTTP 401: invalid API key (token expired)" >&2
      exit 1
    fi
    return 0
  fi
  echo "HTTP 401: invalid API key" >&2
  exit 1
}

cmd="${1:-}"
sub="${2:-}"
if [ "$cmd" = "token" ]; then
  if [ "${DRIVE9_API_KEY:-}" != "test-key" ]; then
    echo "HTTP 403: scoped token cannot manage tokens" >&2
    exit 1
  fi
  shift 2
  case "$sub" in
  issue)
    ttl_sec=0
    while [ "$#" -gt 0 ]; do
      case "$1" in
      --ttl)
        ttl_raw="${2%s}"
        ttl_sec="$ttl_raw"
        shift 2
        ;;
      --subject|--allow)
        shift 2
        ;;
      --json)
        shift
        ;;
      *)
        shift
        ;;
      esac
    done
    now="$(fake_now)"
    exp=$((now + ttl_sec))
    tok_id="tok_${now}_$$_${RANDOM:-0}"
    mkdir -p "${root}/.tokens"
    printf '%s\tactive\n' "$exp" >"${root}/.tokens/${tok_id}"
    exp_iso="$(epoch_to_iso "$exp")"
    printf 'token-issue %s exp=%s\n' "$tok_id" "$exp" >>"$ops_log"
    jq -n \
      --arg tok "drive9_scoped_${tok_id}" \
      --arg tid "$tok_id" \
      --arg exp_iso "$exp_iso" \
      '{token: $tok, token_id: $tid, scope_kind: "fs_scoped", expires_at: $exp_iso}'
    exit 0
    ;;
  revoke)
    tok_id="${1:-}"
    if [ -n "$fail_revoke_pattern" ] && [[ "$tok_id" == *"$fail_revoke_pattern"* ]]; then
      printf 'token-revoke-fail %s\n' "$tok_id" >>"$ops_log"
      echo "token revoke: HTTP 503: upstream unavailable" >&2
      exit 1
    fi
    rec="${root}/.tokens/${tok_id}"
    if [ ! -f "$rec" ]; then
      echo "HTTP 404: token not found or already revoked" >&2
      exit 1
    fi
    IFS=$'\t' read -r exp _ <"$rec"
    printf '%s\trevoked\n' "$exp" >"$rec"
    printf 'token-revoke %s\n' "$tok_id" >>"$ops_log"
    exit 0
    ;;
  *)
    echo "unsupported token subcommand: $sub" >&2
    exit 1
    ;;
  esac
fi

if [ "$cmd" != "fs" ]; then
  echo "unsupported command: $*" >&2
  exit 1
fi
verify_fs_auth
shift 2

case "$sub" in
ls)
  rel="$(strip_ref "$1")"
  if [ -n "$ls_count_file" ] && [[ "$rel" == *"commits"* ]]; then
    count=0
    if [ -f "$ls_count_file" ]; then
      count="$(cat "$ls_count_file")"
    fi
    count=$((count + 1))
    printf '%d\n' "$count" >"$ls_count_file"
    if [ -n "$fail_ls_call" ] && [ "$count" -eq "$fail_ls_call" ]; then
      printf 'ls-fail %s\n' "$rel" >>"$ops_log"
      echo "fs ls: HTTP 503: upstream unavailable" >&2
      exit 1
    fi
  fi
  target="${root}/${rel}"
  if [ ! -d "$target" ]; then
    echo "fs ls: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'ls %s\n' "$rel" >>"$ops_log"
  for entry in "$target"/*; do
    [ -e "$entry" ] || continue
    base="$(basename "$entry")"
    if [ -d "$entry" ]; then
      printf '%s/\n' "$base"
    else
      printf '%s\n' "$base"
    fi
  done | LC_ALL=C sort
  ;;
stat)
  output_format="text"
  if [ "${1:-}" = "-o" ]; then
    output_format="${2:-}"
    shift 2
  fi
  rel="$(strip_ref "$1")"
  if [ -n "$fail_stat_pattern" ] && [[ "$rel" == *"$fail_stat_pattern"* ]]; then
    printf 'stat-fail %s\n' "$rel" >>"$ops_log"
    echo "fs stat: HTTP 503: upstream unavailable" >&2
    exit 1
  fi
  target="${root}/${rel}"
  if [ ! -e "$target" ]; then
    echo "fs stat: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'stat %s\n' "$rel" >>"$ops_log"
  mtime="$(file_mtime "$target")"
  if [ "$output_format" = "json" ]; then
    if [ -d "$target" ]; then
      printf '{"isdir":true,"mtime":%s}\n' "$mtime"
    else
      printf '{"isdir":false,"mtime":%s}\n' "$mtime"
    fi
  else
    if [ -d "$target" ]; then
      printf 'isdir: true\n'
    else
      printf 'isdir: false\n'
    fi
  fi
  ;;
cat)
  rel="$(strip_ref "$1")"
  target="${root}/${rel}"
  if [ ! -f "$target" ]; then
    echo "fs cat: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'cat %s\n' "$rel" >>"$ops_log"
  cat "$target"
  ;;
mkdir)
  rel="$(strip_ref "$1")"
  target="${root}/${rel}"
  mkdir -p "$(dirname "$target")"
  if ! mkdir "$target" 2>/dev/null; then
    echo "fs mkdir: path already exists" >&2
    exit 1
  fi
  printf 'mkdir %s\n' "$rel" >>"$ops_log"
  ;;
mv)
  old_rel="$(strip_ref "$1")"
  new_rel="$(strip_ref "$2")"
  old_target="${root}/${old_rel}"
  new_target="${root}/${new_rel}"
  if [ ! -e "$old_target" ]; then
    echo "fs mv: HTTP 404: not found" >&2
    exit 1
  fi
  if [ -e "$new_target" ]; then
    echo "fs mv: HTTP 409: path already exists" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$new_target")"
  if ! mv "$old_target" "$new_target" 2>/dev/null; then
    echo "fs mv: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'mv %s %s\n' "$old_rel" "$new_rel" >>"$ops_log"
  ;;
rm)
  recursive=0
  if [ "${1:-}" = "-r" ] || [ "${1:-}" = "--recursive" ]; then
    recursive=1
    shift
  fi
  rel="$(strip_ref "$1")"
  # Server-side post-auth / pre-mutation barrier: verify_fs_auth has ALREADY succeeded.
  if [ -n "$pause_after_auth_rm_pattern" ] && [[ "$rel" == *"$pause_after_auth_rm_pattern"* ]]; then
    if [ -n "$pause_auth_signal_file" ]; then
      touch "$pause_auth_signal_file"
    fi
    if [ -n "$pause_auth_release_file" ]; then
      while [ ! -f "$pause_auth_release_file" ] && [ -d "$(dirname "$pause_auth_release_file")" ]; do
        sleep 0.05
      done
    fi
  fi
  # Simulate an admitted server-side rm -r that remains paused on the server
  # while the CLI returns an ambiguous transport EOF error without any signal
  # to the parent publisher.
  if [ -n "$ambiguous_after_auth_rm_pattern" ] && [[ "$rel" == *"$ambiguous_after_auth_rm_pattern"* ]]; then
    target="${root}/${rel}"
    (
      exec >/dev/null 2>&1
      if [ -n "$pause_auth_signal_file" ]; then
        touch "$pause_auth_signal_file"
      fi
      if [ -n "$pause_auth_release_file" ]; then
        while [ ! -f "$pause_auth_release_file" ] && [ -d "$(dirname "$pause_auth_release_file")" ]; do
          sleep 0.05
        done
      fi
      if [ -e "$target" ]; then
        printf 'rm-delayed-server-commit %s\n' "$rel" >>"$ops_log"
        rm -rf "$target"
      fi
    ) &
    printf 'rm-ambiguous-eof %s\n' "$rel" >>"$ops_log"
    echo "fs rm: unexpected EOF (connection reset by peer)" >&2
    exit 1
  fi
  if [ -n "$fail_rm_pattern" ] && [[ "$rel" == *"$fail_rm_pattern"* ]]; then
    printf 'rm-fail %s\n' "$rel" >>"$ops_log"
    echo "fs rm: HTTP 503: upstream timeout" >&2
    exit 1
  fi
  target="${root}/${rel}"
  if [ ! -e "$target" ]; then
    echo "fs rm: HTTP 404: not found" >&2
    exit 1
  fi
  if [ "$recursive" -eq 1 ]; then
    printf 'rm %s\n' "$rel" >>"$ops_log"
    rm -rf "$target"
  elif [ -d "$target" ]; then
    if ! rmdir "$target" 2>/dev/null; then
      echo "fs rm: HTTP 409: directory not empty" >&2
      exit 1
    fi
    printf 'rmdir %s\n' "$rel" >>"$ops_log"
  else
    printf 'rm %s\n' "$rel" >>"$ops_log"
    rm -f "$target"
  fi
  ;;
cp)
  src=$1
  dst=$2
  if [[ "$dst" == :* ]]; then
    rel="$(strip_ref "$dst")"
    # Server-side post-auth / pre-mutation barrier: verify_fs_auth has ALREADY succeeded.
    if [ -n "$pause_after_auth_cp_pattern" ] && [[ "$rel" == *"$pause_after_auth_cp_pattern"* ]]; then
      if [ -n "$pause_auth_signal_file" ]; then
        touch "$pause_auth_signal_file"
      fi
      if [ -n "$pause_auth_release_file" ]; then
        while [ ! -f "$pause_auth_release_file" ] && [ -d "$(dirname "$pause_auth_release_file")" ]; do
          sleep 0.05
        done
      fi
    fi
    printf 'upload %s\n' "$rel" >>"$ops_log"
    if [ -n "$fail_quota_file" ] && [ -f "$fail_quota_file" ]; then
      remaining="$(cat "$fail_quota_file")"
      if [ "$remaining" -gt 0 ]; then
        printf '%d\n' "$((remaining - 1))" >"$fail_quota_file"
        echo "fs cp: tenant storage quota exceeded: server limit=100 used=100 reserved=0 pending=0 delta=10" >&2
        exit 1
      fi
    fi
    target="${root}/${rel}"
    mkdir -p "$(dirname "$target")"
    cp "$src" "$target"
    if [ -n "$pause_upload_pattern" ] && [[ "$rel" == *"$pause_upload_pattern"* ]]; then
      if [ -n "$pause_signal_file" ]; then
        touch "$pause_signal_file"
      fi
      if [ -n "$pause_release_file" ]; then
        while [ ! -f "$pause_release_file" ] && [ -d "$(dirname "$pause_release_file")" ]; do
          sleep 0.05
        done
      fi
    fi
  elif [[ "$src" == :* ]]; then
    rel="$(strip_ref "$src")"
    target="${root}/${rel}"
    if [ ! -f "$target" ]; then
      echo "fs cp: HTTP 404: not found" >&2
      exit 1
    fi
    printf 'download %s\n' "$rel" >>"$ops_log"
    mkdir -p "$(dirname "$dst")"
    cp "$target" "$dst"
  else
    echo "unsupported cp args: $src $dst" >&2
    exit 1
  fi
  ;;
*)
  echo "unsupported fs subcommand: $sub" >&2
  exit 1
  ;;
esac
EOF
chmod +x "$FAKE_CLI"

# Create a minimal git repo with a fast stub Makefile for build-cli-release.
ORIGIN_GIT="${WORK}/origin.git"
REPO="${WORK}/repo"
git init -q --bare "$ORIGIN_GIT"
git init -q "$REPO"
git -C "$REPO" config user.email test@example.com
git -C "$REPO" config user.name test
git -C "$REPO" remote add origin "$ORIGIN_GIT"

cat >"${REPO}/go.mod" <<'EOF'
module example.com/test-archive

go 1.25.0
EOF

cat >"${REPO}/Makefile" <<'EOF'
SHELL := /bin/bash
DIST_DIR ?= dist
VERSION ?= dev
CLI_TARGETS ?= linux/amd64

build-cli-release:
	@set -eu; \
	mkdir -p "$(DIST_DIR)"; \
	for target in $(CLI_TARGETS); do \
		os="$${target%/*}"; \
		arch="$${target#*/}"; \
		ext=""; \
		if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
		printf 'bin-%s-%s-%s\n' "$$os" "$$arch" "$(VERSION)" >"$(DIST_DIR)/drive9-$${os}-$${arch}$${ext}"; \
	done
EOF

make_commit() {
  local label=$1
  printf '%s\n' "$label" >"${REPO}/state.txt"
  git -C "$REPO" add -A
  git -C "$REPO" commit -qm "commit ${label}"
  git -C "$REPO" push -q origin HEAD:main
  git -C "$REPO" rev-parse HEAD
}

C1="$(make_commit c1)"
C2="$(make_commit c2)"
C3="$(make_commit c3)"
C4="$(make_commit c4)"
C5="$(make_commit c5)"

seed_complete_commit() {
  local remote_root=$1
  local sha=$2
  local published_at=${3:-2026-10-04T00:00:00Z}
  local dir="${remote_root}/drive9/commits/${sha}"
  mkdir -p "${dir}/bin"
  printf 'src-%s\n' "$sha" >"${dir}/source.tar.gz"
  printf 'bin-%s\n' "$sha" >"${dir}/bin/drive9-linux-amd64"
  local src_sha bin_sha
  src_sha="$(sha256sum "${dir}/source.tar.gz" | awk '{print $1}')"
  bin_sha="$(sha256sum "${dir}/bin/drive9-linux-amd64" | awk '{print $1}')"
  printf '%s  source.tar.gz\n%s  bin/drive9-linux-amd64\n' "$src_sha" "$bin_sha" >"${dir}/checksums.txt"
  jq -n \
    --arg sha "$sha" \
    --arg short "${sha:0:7}" \
    --arg published_at "$published_at" \
    --arg src_sha "$src_sha" \
    --arg bin_sha "$bin_sha" \
    '{
      schema_version: 1,
      repository: "mem9-ai/drive9",
      branch: "main",
      commit_sha: $sha,
      short_sha: $short,
      published_at: $published_at,
      source_archive: "source.tar.gz",
      checksums: {
        "source.tar.gz": $src_sha,
        "bin/drive9-linux-amd64": $bin_sha
      },
      binaries: [
        {path: "bin/drive9-linux-amd64", goos: "linux", goarch: "amd64"}
      ]
    }' >"${dir}/manifest.json"
}

seed_latest_index() {
  local remote_root=$1
  local sha=$2
  mkdir -p "${remote_root}/drive9"
  jq -n --arg sha "$sha" '{schema_version: 1, commit_sha: $sha}' >"${remote_root}/drive9/latest.json"
}

run_publisher_in() {
  local repo_dir=$1
  local remote_root=$2
  local ops_log=$3
  shift 3
  (
    cd "$repo_dir"
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    RUNNER_TEMP="${WORK}/runner" \
    FAKE_REMOTE_ROOT="$remote_root" \
    FAKE_OPS_LOG="$ops_log" \
    "$@" \
    "$SCRIPT"
  )
}

run_publisher() {
  local remote_root=$1
  local ops_log=$2
  shift 2
  run_publisher_in "$REPO" "$remote_root" "$ops_log" "$@"
}

echo "== 1. Pre-publish prune removes incomplete target, aged incomplete, and oldest expired commits; preserves latest and non-SHA entries"
R1="${WORK}/remote1"
L1="${WORK}/ops1.log"
: >"$L1"
seed_complete_commit "$R1" "$C1"
seed_complete_commit "$R1" "$C2"
seed_complete_commit "$R1" "$C3"
seed_latest_index "$R1" "$C3"
# Aged incomplete directory from a failed prior run (older than default 3600s grace window).
mkdir -p "${R1}/drive9/commits/${C4}/bin"
printf 'partial\n' >"${R1}/drive9/commits/${C4}/source.tar.gz"
touch -t 202001010000 "${R1}/drive9/commits/${C4}"
# Incomplete target directory with stale leftover file from a failed retry.
mkdir -p "${R1}/drive9/commits/${C5}/bin"
printf 'stale-extra\n' >"${R1}/drive9/commits/${C5}/bin/drive9-stale-old"
# Non-SHA entries must never be pruned.
mkdir -p "${R1}/drive9/commits/notes"
printf 'keep\n' >"${R1}/drive9/commits/README.txt"

run_publisher "$R1" "$L1" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 >/dev/null

assert_missing "${R1}/drive9/commits/${C1}"
assert_missing "${R1}/drive9/commits/${C2}"
assert_missing "${R1}/drive9/commits/${C4}"
assert_missing "${R1}/drive9/commits/${C5}/bin/drive9-stale-old"
assert_exists "${R1}/drive9/commits/${C3}/manifest.json"
assert_exists "${R1}/drive9/commits/${C5}/manifest.json"
assert_exists "${R1}/drive9/commits/notes"
assert_exists "${R1}/drive9/commits/README.txt"
[ "$(jq -r '.commit_sha' "${R1}/drive9/latest.json")" = "$C5" ] || fail "latest.json did not advance to C5"

# Verify deletion order: incomplete target C5, aged incomplete C4, then expired oldest-first (C1 before C2), all before upload.
rm_lines="$(grep '^rm ' "$L1")"
want_rm="$(printf 'rm drive9/commits/%s\nrm drive9/commits/%s\nrm drive9/commits/%s\nrm drive9/commits/%s' "$C5" "$C4" "$C1" "$C2")"
[ "$rm_lines" = "$want_rm" ] || fail "unexpected rm order:\ngot:\n${rm_lines}\nwant:\n${want_rm}"
first_upload_line="$(grep -n '^upload ' "$L1" | head -n 1 | cut -d: -f1)"
last_rm_line="$(grep -n '^rm ' "$L1" | tail -n 1 | cut -d: -f1)"
[ "$last_rm_line" -lt "$first_upload_line" ] || fail "expected pruning to finish before first upload"
pass "pruned incomplete target C5, aged incomplete C4, and expired C1/C2 oldest-first before upload"

echo "== 2. Idempotent rerun on already-complete commit prunes nothing"
L2="${WORK}/ops2.log"
: >"$L2"
run_publisher "$R1" "$L2" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 >/dev/null
assert_exists "${R1}/drive9/commits/${C3}/manifest.json"
assert_exists "${R1}/drive9/commits/${C5}/manifest.json"
if grep -q '^rm ' "$L2"; then
  fail "idempotent rerun unexpectedly pruned commits: $(grep '^rm ' "$L2")"
fi
pass "idempotent rerun kept both C3 and C5 with zero deletes"

echo "== 3. retain_commits=1 protects current latest.json until new commit publishes, then prunes old latest"
C6="$(make_commit c6)"
L3="${WORK}/ops3.log"
: >"$L3"
run_publisher "$R1" "$L3" env DRIVE9_ARCHIVE_RETAIN_COMMITS=1 >/dev/null
assert_missing "${R1}/drive9/commits/${C3}"
assert_missing "${R1}/drive9/commits/${C5}"
assert_exists "${R1}/drive9/commits/${C6}/manifest.json"
[ "$(jq -r '.commit_sha' "${R1}/drive9/latest.json")" = "$C6" ] || fail "latest.json did not advance to C6"

rm_c3_line="$(grep -n "^rm drive9/commits/${C3}$" "$L3" | head -n 1 | cut -d: -f1)"
latest_upload_line="$(grep -n '^upload drive9/latest.json$' "$L3" | head -n 1 | cut -d: -f1)"
rm_c5_line="$(grep -n "^rm drive9/commits/${C5}$" "$L3" | head -n 1 | cut -d: -f1)"
[ "$rm_c3_line" -lt "$latest_upload_line" ] || fail "expected C3 pruned before latest.json update"
[ "$rm_c5_line" -gt "$latest_upload_line" ] || fail "expected old latest C5 pruned only AFTER latest.json update"
pass "protected C5 until latest.json pointed to C6, then pruned C5"

echo "== 4. Post-prune upload retries transient quota exceeded (including incomplete target cleanup retry)"
C7="$(make_commit c7)"
L4="${WORK}/ops4.log"
: >"$L4"
Q4="${WORK}/quota-fail-count"
printf '2\n' >"$Q4"
# Only an incomplete leftover of target C7 exists (no other expired/incomplete commits); cleaning it must enable quota retry.
mkdir -p "${R1}/drive9/commits/${C7}/bin"
printf 'stale-from-aborted-run\n' >"${R1}/drive9/commits/${C7}/bin/drive9-stale"
run_publisher "$R1" "$L4" env DRIVE9_ARCHIVE_RETAIN_COMMITS=5 FAKE_FAIL_QUOTA_COUNT_FILE="$Q4" >/dev/null
assert_missing "${R1}/drive9/commits/${C7}/bin/drive9-stale"
assert_exists "${R1}/drive9/commits/${C7}/manifest.json"
assert_exists "${R1}/drive9/commits/${C6}/manifest.json"
[ "$(cat "$Q4")" = "0" ] || fail "expected both transient quota errors to be retried after incomplete target cleanup"
pass "cleaned incomplete target C7, removed stale files, and retried quota exceeded"

echo "== 5. Without prior pruning, quota exceeded fails immediately without retry"
C8="$(make_commit c8)"
L5="${WORK}/ops5.log"
: >"$L5"
Q5="${WORK}/quota-fail-no-prune"
printf '2\n' >"$Q5"
set +e
run_publisher "$R1" "$L5" env DRIVE9_ARCHIVE_RETAIN_COMMITS=10 FAKE_FAIL_QUOTA_COUNT_FILE="$Q5" >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || fail "expected failure on quota exceeded without pruning"
[ "$(cat "$Q5")" = "1" ] || fail "expected fail-fast (1 attempt), remaining=$(cat "$Q5")"
pass "failed fast without retrying when nothing was pruned"

echo "== 6. DRIVE9_ARCHIVE_RETAIN_COMMITS=0 disables pruning and invalid/overflowing values fail fast"
L6="${WORK}/ops6.log"
: >"$L6"
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_RETAIN_COMMITS=0 >/dev/null
if grep -q '^rm ' "$L6"; then
  fail "expected no pruning when DRIVE9_ARCHIVE_RETAIN_COMMITS=0"
fi
assert_exists "${R1}/drive9/commits/${C7}/manifest.json"
assert_exists "${R1}/drive9/commits/${C8}/manifest.json"

err6_ttl="${WORK}/err6_ttl.log"
err6_break_bad="${WORK}/err6_break_bad.log"
err6_break_init="${WORK}/err6_break_init.log"
err6_break_free="${WORK}/err6_break_free.log"
set +e
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_RETAIN_COMMITS=invalid >/dev/null 2>&1
bad_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_RETAIN_COMMITS=999999999999999999999999 >/dev/null 2>&1
overflow_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS=999999999999999999999999 >/dev/null 2>&1
bad_grace_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS=0 >/dev/null 2>"$err6_ttl"
bad_ttl_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="bad/owner" >/dev/null 2>"$err6_break_bad"
bad_break_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="init" >/dev/null 2>"$err6_break_init"
bad_break_init_rc=$?
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="free-abc" >/dev/null 2>"$err6_break_free"
bad_break_free_rc=$?
set -e
[ "$bad_rc" -ne 0 ] || fail "expected invalid DRIVE9_ARCHIVE_RETAIN_COMMITS to fail"
[ "$overflow_rc" -ne 0 ] || fail "expected overflowing DRIVE9_ARCHIVE_RETAIN_COMMITS to fail"
[ "$bad_grace_rc" -ne 0 ] || fail "expected overflowing DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS to fail"
[ "$bad_ttl_rc" -ne 0 ] || fail "expected DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS=0 to fail"
grep -q "invalid DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS" "$err6_ttl" || fail "expected token TTL validation message, got: $(cat "$err6_ttl")"
[ "$bad_break_rc" -ne 0 ] || fail "expected invalid DRIVE9_ARCHIVE_LOCK_BREAK_OWNER to fail"
grep -q "invalid DRIVE9_ARCHIVE_LOCK_BREAK_OWNER" "$err6_break_bad" || fail "expected break_owner validation message, got: $(cat "$err6_break_bad")"
[ "$bad_break_init_rc" -ne 0 ] || fail "expected DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=init to fail"
grep -q "invalid DRIVE9_ARCHIVE_LOCK_BREAK_OWNER" "$err6_break_init" || fail "expected break_owner init validation message, got: $(cat "$err6_break_init")"
[ "$bad_break_free_rc" -ne 0 ] || fail "expected DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=free-abc to fail"
grep -q "invalid DRIVE9_ARCHIVE_LOCK_BREAK_OWNER" "$err6_break_free" || fail "expected break_owner free-* validation message, got: $(cat "$err6_break_free")"
pass "DRIVE9_ARCHIVE_RETAIN_COMMITS=0 and bounded validation (including token TTL and break_lock_owner) behave as expected"

echo "== 7. Stale branch dispatch ranks globally, does not overwrite main's latest.json, and preserves main under retain_commits=1"
R7="${WORK}/remote7"
L7="${WORK}/ops7.log"
: >"$L7"
git -C "$REPO" checkout -q -b stale-branch "$C1"
printf 'stale-old\n' >"${REPO}/stale.txt"
git -C "$REPO" add -A
GIT_AUTHOR_DATE="2025-01-01T00:00:00Z" GIT_COMMITTER_DATE="2025-01-01T00:00:00Z" \
  git -C "$REPO" commit -qm "stale ancient commit"
STALE_OLD="$(git -C "$REPO" rev-parse HEAD)"

printf 'stale-tip\n' >"${REPO}/stale.txt"
git -C "$REPO" add -A
GIT_AUTHOR_DATE="2025-01-02T00:00:00Z" GIT_COMMITTER_DATE="2025-01-02T00:00:00Z" \
  git -C "$REPO" commit -qm "stale tip commit"
git -C "$REPO" push -q origin HEAD:stale-branch
STALE_TIP="$(git -C "$REPO" rev-parse HEAD)"

git -C "$REPO" checkout -q main
printf 'main-new\n' >"${REPO}/state.txt"
git -C "$REPO" add -A
GIT_AUTHOR_DATE="2027-01-01T00:00:00Z" GIT_COMMITTER_DATE="2027-01-01T00:00:00Z" \
  git -C "$REPO" commit -qm "newer main commit"
git -C "$REPO" push -q origin HEAD:main
MAIN_NEW="$(git -C "$REPO" rev-parse HEAD)"

seed_complete_commit "$R7" "$STALE_OLD" "2025-01-01T00:00:00Z"
seed_complete_commit "$R7" "$MAIN_NEW" "2027-01-01T00:00:00Z"
seed_latest_index "$R7" "$MAIN_NEW"

run_publisher "$R7" "$L7" env GITHUB_EVENT_NAME=workflow_dispatch DRIVE9_ARCHIVE_BRANCH=stale-branch DRIVE9_ARCHIVE_RETAIN_COMMITS=1 >/dev/null
assert_missing "${R7}/drive9/commits/${STALE_OLD}"
assert_exists "${R7}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_exists "${R7}/drive9/commits/${STALE_TIP}/manifest.json"
[ "$(jq -r '.commit_sha' "${R7}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "stale-branch dispatch overwrote shared latest.json"
pass "stale-branch dispatch kept main's latest.json and preserved MAIN_NEW even with retain_commits=1"

echo "== 8. Remote SHA absent from local git is ranked by remote manifest published_at (not treated as oldest)"
# Clone ONLY stale-branch from a bare repo that does NOT have MAIN_NEW at all (simulates deleted/unfetched branch).
ISOLATED_ORIGIN="${WORK}/isolated-origin.git"
ISOLATED_REPO="${WORK}/isolated-repo"
git init -q --bare "$ISOLATED_ORIGIN"
git -C "$REPO" push -q "$ISOLATED_ORIGIN" stale-branch:stale-branch
git clone -q --single-branch --branch stale-branch "$ISOLATED_ORIGIN" "$ISOLATED_REPO"

R8="${WORK}/remote8"
L8="${WORK}/ops8.log"
: >"$L8"
# MAIN_NEW does not exist in ISOLATED_REPO or ISOLATED_ORIGIN, and is NOT protected by latest.json.
seed_complete_commit "$R8" "$STALE_OLD" "2025-01-01T00:00:00Z"
seed_complete_commit "$R8" "$MAIN_NEW" "2027-01-01T00:00:00Z"

run_publisher_in "$ISOLATED_REPO" "$R8" "$L8" env GITHUB_EVENT_NAME=workflow_dispatch DRIVE9_ARCHIVE_BRANCH=stale-branch DRIVE9_ARCHIVE_RETAIN_COMMITS=2 >/dev/null
assert_missing "${R8}/drive9/commits/${STALE_OLD}"
assert_exists "${R8}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_exists "${R8}/drive9/commits/${STALE_TIP}/manifest.json"
pass "unfetched remote commit MAIN_NEW ranked by remote manifest published_at and preserved over STALE_OLD"

echo "== 9. Recent in-progress incomplete directory is preserved by grace window while aged incomplete is pruned"
R9="${WORK}/remote9"
L9="${WORK}/ops9.log"
: >"$L9"
seed_complete_commit "$R9" "$C1"
seed_latest_index "$R9" "$C1"
# Recent in-progress upload (mtime = now, within 3600s grace window) — must NOT be pruned.
mkdir -p "${R9}/drive9/commits/${C2}/bin"
printf 'in-flight\n' >"${R9}/drive9/commits/${C2}/source.tar.gz"
# Aged abandoned upload (mtime = 2020, older than 3600s grace window) — MUST be pruned.
mkdir -p "${R9}/drive9/commits/${C3}/bin"
printf 'abandoned\n' >"${R9}/drive9/commits/${C3}/source.tar.gz"
touch -t 202001010000 "${R9}/drive9/commits/${C3}"

run_publisher "$R9" "$L9" env DRIVE9_ARCHIVE_RETAIN_COMMITS=1 DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS=3600 >/dev/null
assert_exists "${R9}/drive9/commits/${C2}/source.tar.gz"
assert_missing "${R9}/drive9/commits/${C3}"
assert_exists "${R9}/drive9/commits/${MAIN_NEW}/manifest.json"
pass "preserved fresh in-progress C2 under grace window (even with retain_commits=1) and pruned aged C3"

echo "== 10. Partial rm failure logs warning, continues deleting remaining expired commits, and still publishes"
R10="${WORK}/remote10"
L10="${WORK}/ops10.log"
: >"$L10"
seed_complete_commit "$R10" "$C1"
seed_complete_commit "$R10" "$C2"
seed_complete_commit "$R10" "$C3"
seed_latest_index "$R10" "$C3"

err10="${WORK}/err10.log"
run_publisher "$R10" "$L10" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 FAKE_FAIL_RM_PATTERN="$C1" >/dev/null 2>"$err10"
assert_exists "${R10}/drive9/commits/${C1}/manifest.json"
assert_missing "${R10}/drive9/commits/${C2}"
assert_exists "${R10}/drive9/commits/${C3}/manifest.json"
assert_exists "${R10}/drive9/commits/${MAIN_NEW}/manifest.json"
grep -q "partial failures: removed=1 failed=1" "$err10" || fail "expected partial failure summary in stderr"
grep -q "retaining archive lock" "$err10" || fail "expected ambiguous 503 rm failure to retain lock fail-closed: $(cat "$err10")"
pass "continued past rm failure on C1, pruned C2, logged summary, published MAIN_NEW, and retained lock fail-closed on ambiguous 503"

echo "== 11. Transient ls/stat errors (pre-publish and post-publish) warn without failing publish"
# 11a: Pre-publish ls 503 skips prune and still publishes.
R11A="${WORK}/remote11a"
L11A="${WORK}/ops11a.log"
: >"$L11A"
seed_complete_commit "$R11A" "$C1"
seed_complete_commit "$R11A" "$C2"
LS11A="${WORK}/ls11a.count"
err11a="${WORK}/err11a.log"
run_publisher "$R11A" "$L11A" env DRIVE9_ARCHIVE_RETAIN_COMMITS=1 FAKE_LS_COUNT_FILE="$LS11A" FAKE_FAIL_LS_ON_CALL=1 >/dev/null 2>"$err11a"
assert_exists "${R11A}/drive9/commits/${MAIN_NEW}/manifest.json"
grep -q "skipping retention prune" "$err11a" || fail "expected warning on pre-publish ls error"

# 11b: Pre-publish stat 503 preserves affected commit and still prunes others + publishes.
R11B="${WORK}/remote11b"
L11B="${WORK}/ops11b.log"
: >"$L11B"
seed_complete_commit "$R11B" "$C1"
seed_complete_commit "$R11B" "$C2"
seed_complete_commit "$R11B" "$C3"
seed_latest_index "$R11B" "$C3"
err11b="${WORK}/err11b.log"
run_publisher "$R11B" "$L11B" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 FAKE_FAIL_STAT_PATTERN="$C2" >/dev/null 2>"$err11b"
assert_missing "${R11B}/drive9/commits/${C1}"
assert_exists "${R11B}/drive9/commits/${C2}/manifest.json"
assert_exists "${R11B}/drive9/commits/${MAIN_NEW}/manifest.json"
grep -q "failed to inspect remote manifest for ${C2}" "$err11b" || fail "expected warning on transient stat error"

# 11c: Post-publish deferred_latest_prune ls 503 must NOT fail the already-published release.
R11C="${WORK}/remote11c"
L11C="${WORK}/ops11c.log"
: >"$L11C"
seed_complete_commit "$R11C" "$C1"
seed_complete_commit "$R11C" "$C3"
seed_latest_index "$R11C" "$C3"
LS11C="${WORK}/ls11c.count"
err11c="${WORK}/err11c.log"
run_publisher "$R11C" "$L11C" env DRIVE9_ARCHIVE_RETAIN_COMMITS=1 FAKE_LS_COUNT_FILE="$LS11C" FAKE_FAIL_LS_ON_CALL=2 >/dev/null 2>"$err11c"
assert_missing "${R11C}/drive9/commits/${C1}"
assert_exists "${R11C}/drive9/commits/${MAIN_NEW}/manifest.json"
[ "$(jq -r '.commit_sha' "${R11C}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "latest.json should be updated to MAIN_NEW"
grep -q "skipping retention prune" "$err11c" || fail "expected warning on post-publish ls error"
pass "pre- and post-publish transient ls/stat errors warned without failing publish"

echo "== 12. Missing/empty commits dir (404) and malformed latest.json are handled cleanly"
# 12a: Brand new remote (commits/ returns 404).
R12A="${WORK}/remote12a"
L12A="${WORK}/ops12a.log"
mkdir -p "$R12A"
: >"$L12A"
run_publisher "$R12A" "$L12A" env DRIVE9_ARCHIVE_RETAIN_COMMITS=5 >/dev/null
assert_exists "${R12A}/drive9/commits/${MAIN_NEW}/manifest.json"
[ "$(jq -r '.commit_sha' "${R12A}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "fresh remote publish failed"

# 12b: Empty commits/ directory + malformed latest.json.
R12B="${WORK}/remote12b"
L12B="${WORK}/ops12b.log"
mkdir -p "${R12B}/drive9/commits"
printf '{not-valid-json' >"${R12B}/drive9/latest.json"
: >"$L12B"
run_publisher "$R12B" "$L12B" env DRIVE9_ARCHIVE_RETAIN_COMMITS=5 >/dev/null
assert_exists "${R12B}/drive9/commits/${MAIN_NEW}/manifest.json"
[ "$(jq -r '.commit_sha' "${R12B}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "malformed latest.json was not repaired"

# 12c: Non-empty commits/ with malformed latest.json still prunes expired commits cleanly.
R12C="${WORK}/remote12c"
L12C="${WORK}/ops12c.log"
seed_complete_commit "$R12C" "$C1"
seed_complete_commit "$R12C" "$C2"
printf '{not-valid-json' >"${R12C}/drive9/latest.json"
: >"$L12C"
err12c="${WORK}/err12c.log"
run_publisher "$R12C" "$L12C" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 >/dev/null 2>"$err12c"
assert_missing "${R12C}/drive9/commits/${C1}"
assert_exists "${R12C}/drive9/commits/${C2}/manifest.json"
assert_exists "${R12C}/drive9/commits/${MAIN_NEW}/manifest.json"
grep -q "is malformed" "$err12c" || fail "expected malformed latest.json warning"
pass "missing/empty commits dir and malformed latest.json handled cleanly"

wait_for_file() {
  local path=$1
  local timeout_tenths=${2:-100}
  local i=0
  while [ ! -f "$path" ]; do
    i=$((i + 1))
    if [ "$i" -ge "$timeout_tenths" ]; then
      fail "timed out waiting for ${path}"
    fi
    sleep 0.1
  done
}

wait_for_log_match() {
  local log_file=$1
  local pattern=$2
  local timeout_tenths=${3:-150}
  local i=0
  while ! grep -q "$pattern" "$log_file" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge "$timeout_tenths" ]; then
      fail "timed out waiting for pattern '${pattern}' in ${log_file}"
    fi
    sleep 0.1
  done
}

wait_for_missing() {
  local path=$1
  local timeout_tenths=${2:-150}
  local i=0
  while [ -e "$path" ]; do
    i=$((i + 1))
    if [ "$i" -ge "$timeout_tenths" ]; then
      fail "timed out waiting for ${path} to disappear"
    fi
    sleep 0.1
  done
}

active_lease_owner() {
  local remote_root=$1
  local lock_dir="${remote_root}/drive9/.publish-lock"
  local entry base rest started rest2 owner
  for entry in "$lock_dir"/lease.*; do
    [ -e "$entry" ] || continue
    base="$(basename "$entry")"
    rest="${base#lease.}"
    started="${rest%%.*}"
    rest2="${rest#*.}"
    if [[ "$started" =~ ^[0-9]+$ ]] && [ "$started" -gt 0 ]; then
      owner="${rest2#*.}"
      printf '%s\n' "$owner"
      return 0
    fi
  done
  return 1
}

assert_no_active_lease() {
  local remote_root=$1
  local lock_dir="${remote_root}/drive9/.publish-lock"
  if [ -d "$lock_dir" ]; then
    local entry base rest started lease_count=0
    for entry in "$lock_dir"/lease.*; do
      [ -e "$entry" ] || continue
      lease_count=$((lease_count + 1))
      base="$(basename "$entry")"
      rest="${base#lease.}"
      started="${rest%%.*}"
      if ! [[ "$started" =~ ^[0-9]+$ ]] || [ "$started" -gt 0 ]; then
        fail "expected no active lease entries in ${lock_dir}, found: ${base}"
      fi
    done
    [ "$lease_count" -le 1 ] || fail "expected at most 1 circulating lease sentinel in ${lock_dir}, found ${lease_count}"
  fi
}

run_with_watchdog() {
  local timeout_sec=$1
  local err_log=$2
  shift 2
  local pid start now elapsed rc
  spawn_isolated_bg /dev/null "$err_log" "$@"
  pid="$LAST_BG_PID"
  start="$(date -u '+%s')"
  while kill -0 "$pid" 2>/dev/null; do
    now="$(date -u '+%s')"
    elapsed=$((now - start))
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      terminate_pgid "$pid"
      sleep 0.2
      kill_pgid "$pid"
      wait "$pid" 2>/dev/null || true
      cat "$err_log" >&2 || true
      fail "command timed out after ${timeout_sec}s wall clock"
    fi
    sleep 0.1
  done
  set +e
  wait "$pid"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    cat "$err_log" >&2 || true
    fail "command failed with exit code ${rc}"
  fi
}

run_with_watchdog_expect_fail() {
  local timeout_sec=$1
  local err_log=$2
  shift 2
  local pid start now elapsed rc
  spawn_isolated_bg /dev/null "$err_log" "$@"
  pid="$LAST_BG_PID"
  start="$(date -u '+%s')"
  while kill -0 "$pid" 2>/dev/null; do
    now="$(date -u '+%s')"
    elapsed=$((now - start))
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      terminate_pgid "$pid"
      sleep 0.2
      kill_pgid "$pid"
      wait "$pid" 2>/dev/null || true
      cat "$err_log" >&2 || true
      fail "command timed out after ${timeout_sec}s wall clock (expected fail-closed exit)"
    fi
    sleep 0.1
  done
  set +e
  wait "$pid"
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    fail "expected command to fail closed, but it exited 0"
  fi
}

run_publisher_in_with_arg() {
  local repo_dir=$1
  local remote_root=$2
  local ops_log=$3
  local commit_arg=$4
  shift 4
  (
    cd "$repo_dir"
    exec env \
      DRIVE9_SERVER="https://api.example.test" \
      DRIVE9_API_KEY="test-key" \
      DRIVE9_PUBLISHER="$FAKE_CLI" \
      DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
      DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
      FAKE_REMOTE_ROOT="$remote_root" \
      FAKE_OPS_LOG="$ops_log" \
      "$@" \
      "$SCRIPT" "$commit_arg"
  )
}

spawn_publisher_bg() {
  local repo_dir=$1
  local remote_root=$2
  local ops_log=$3
  local commit_arg=$4
  local out_file=$5
  local err_file=$6
  shift 6
  spawn_isolated_bg "$out_file" "$err_file" \
    bash -c 'cd "$1" && shift && exec "$@"' _ "$repo_dir" \
    env \
      DRIVE9_SERVER="https://api.example.test" \
      DRIVE9_API_KEY="test-key" \
      DRIVE9_PUBLISHER="$FAKE_CLI" \
      DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
      DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
      FAKE_REMOTE_ROOT="$remote_root" \
      FAKE_OPS_LOG="$ops_log" \
      "$@" \
      "$SCRIPT" "$commit_arg"
}

# Separate clones simulate two independent runners sharing the same remote archive.
RUNNER_A_REPO="${WORK}/runner-a-repo"
RUNNER_B_REPO="${WORK}/runner-b-repo"
git clone -q "$ORIGIN_GIT" "$RUNNER_A_REPO"
git clone -q "$ORIGIN_GIT" "$RUNNER_B_REPO"

echo "== 13. Concurrent same-SHA publishers: atomic staging hides partial uploads, active lock serializes waiters, and waiter reuses archive"
R13="${WORK}/remote13"
L13A="${WORK}/ops13a.log"
L13B="${WORK}/ops13b.log"
L13B2="${WORK}/ops13b2.log"
mkdir -p "$R13"
: >"$L13A"
: >"$L13B"
: >"$L13B2"
SIG13="${WORK}/pause13.signal"
REL13="${WORK}/pause13.release"
register_release_file "$REL13"
OUT13B="${WORK}/out13b.log"
: >"$OUT13B"

# Start Publisher A for MAIN_NEW and pause it right after source.tar.gz upload inside .staging/.
spawn_publisher_bg "$RUNNER_A_REPO" "$R13" "$L13A" "$MAIN_NEW" /dev/null "${WORK}/err13a.log" \
  RUNNER_TEMP="${WORK}/tmp13a" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
  FAKE_PAUSE_AFTER_UPLOAD_PATTERN=".staging/${MAIN_NEW}." \
  FAKE_PAUSE_SIGNAL_FILE="$SIG13" \
  FAKE_PAUSE_RELEASE_FILE="$REL13"
PID13A="$LAST_BG_PID"

wait_for_file "$SIG13" 150
# Atomic staging: commits/MAIN_NEW must NOT exist at all while Publisher A is uploading.
assert_missing "${R13}/drive9/commits/${MAIN_NEW}"
assert_exists "${R13}/drive9/.staging"
assert_exists "${R13}/drive9/.publish-lock"

# Zero-wait Publisher B2 targeting the SAME SHA must fail immediately on active lock with break_lock_owner guidance.
run_with_watchdog_expect_fail 15 "${WORK}/err13b2.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R13" \
    FAKE_OPS_LOG="$L13B2" \
    RUNNER_TEMP="${WORK}/tmp13b2" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=0 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    "$SCRIPT" "$MAIN_NEW"
grep -q "held by another active publisher" "${WORK}/err13b2.log" || fail "expected active-lock message from B2"
grep -q "DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=" "${WORK}/err13b2.log" || fail "expected break_lock_owner guidance from B2"
assert_missing "${R13}/drive9/commits/${MAIN_NEW}"

# Waiting Publisher B targeting the SAME SHA blocks until A finishes; observe B waiting before releasing A.
spawn_publisher_bg "$RUNNER_B_REPO" "$R13" "$L13B" "$MAIN_NEW" "$OUT13B" "${WORK}/err13b.log" \
  RUNNER_TEMP="${WORK}/tmp13b" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
  DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=30 \
  DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1
PID13B="$LAST_BG_PID"

wait_for_log_match "$OUT13B" "waiting for archive lock" 150
assert_missing "${R13}/drive9/commits/${MAIN_NEW}"

# Release Publisher A and verify both exit cleanly with atomically promoted archive and released lease.
touch "$REL13"
wait "$PID13A" || { cat "${WORK}/err13a.log" >&2; fail "Publisher A failed"; }
wait "$PID13B" || { cat "${WORK}/err13b.log" >&2; fail "Publisher B failed"; }
assert_exists "${R13}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_exists "${R13}/drive9/commits/${MAIN_NEW}/bin/drive9-linux-amd64"
grep -q "^mv drive9/\.staging/${MAIN_NEW}\..* drive9/commits/${MAIN_NEW}$" "$L13A" || fail "expected atomic staging promotion mv in A's ops log"
[ "$(jq -r '.commit_sha' "${R13}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "latest.json should point to MAIN_NEW"
assert_no_active_lease "$R13"
if grep -q "^rm drive9/commits/${MAIN_NEW}$" "$L13B"; then
  fail "Publisher B unexpectedly deleted MAIN_NEW"
fi
pass "same-SHA concurrent publishers serialized cleanly with atomic staging; B waited and reused promoted archive"

echo "== 14. Post-auth / pre-write server barrier: fail-closed lock refuses automatic takeover even hours after clock advance"
R14="${WORK}/remote14"
L14A="${WORK}/ops14a.log"
L14B="${WORK}/ops14b.log"
L14B2="${WORK}/ops14b2.log"
CLK14="${WORK}/clock14.epoch"
mkdir -p "$R14"
: >"$L14A"
: >"$L14B"
: >"$L14B2"
printf '1700000000\n' >"$CLK14"
SIG14="${WORK}/pause14.signal"
REL14="${WORK}/pause14.release"
register_release_file "$REL14"

# Pause Publisher A *inside FAKE_CLI AFTER verify_fs_auth succeeds* and *before* writing latest.json.
spawn_publisher_bg "$RUNNER_A_REPO" "$R14" "$L14A" "$MAIN_NEW" /dev/null "${WORK}/err14a.log" \
  RUNNER_TEMP="${WORK}/tmp14a" \
  DRIVE9_ARCHIVE_NOW_FILE="$CLK14" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
  DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS=30 \
  FAKE_PAUSE_AFTER_AUTH_CP_PATTERN="latest.json" \
  FAKE_PAUSE_AUTH_SIGNAL_FILE="$SIG14" \
  FAKE_PAUSE_AUTH_RELEASE_FILE="$REL14"
PID14A="$LAST_BG_PID"

wait_for_file "$SIG14" 150
owner14a="$(active_lease_owner "$R14")" || fail "expected active lease owner for Publisher A"
assert_exists "${R14}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_missing "${R14}/drive9/latest.json"

# Advance controlled clock by +2 hours (far beyond token TTL=30s).
printf '1700007200\n' >"$CLK14"

# Publisher B MUST refuse automatic takeover and fail closed without touching commits/ or latest.json.
run_with_watchdog_expect_fail 15 "${WORK}/err14b.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R14" \
    FAKE_OPS_LOG="$L14B" \
    RUNNER_TEMP="${WORK}/tmp14b" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK14" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=2 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    "$SCRIPT" "$MAIN_NEW"
grep -q "DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=${owner14a}" "${WORK}/err14b.log" || fail "expected fail-closed diagnostic naming owner ${owner14a}: $(cat "${WORK}/err14b.log")"
if grep -qE '^(upload|rm) ' "$L14B"; then
  fail "Publisher B must not mutate archive while A's admitted request is paused"
fi

# Resume Publisher A's already-authenticated latest.json write: completes cleanly in isolation and releases lock.
touch "$REL14"
wait "$PID14A" || { cat "${WORK}/err14a.log" >&2; fail "Publisher A failed after resuming post-auth write"; }
[ "$(jq -r '.commit_sha' "${R14}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "Publisher A did not finish writing latest.json"
assert_no_active_lease "$R14"

# Subsequent Publisher B now acquires the released lock and reuses MAIN_NEW cleanly.
run_with_watchdog 20 "${WORK}/err14b2.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R14" \
    FAKE_OPS_LOG="$L14B2" \
    RUNNER_TEMP="${WORK}/tmp14b2" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK14" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=10 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    "$SCRIPT" "$MAIN_NEW"
assert_no_active_lease "$R14"
pass "post-auth / pre-write barrier: fail-closed lock excluded takeover even +2h later; A finished cleanly in isolation and B succeeded afterwards"

echo "== 15. Concurrent different-SHA publishers serialize even with grace=0 (observing B wait before releasing A)"
R15="${WORK}/remote15"
L15A="${WORK}/ops15a.log"
L15B="${WORK}/ops15b.log"
mkdir -p "$R15"
: >"$L15A"
: >"$L15B"
SIG15="${WORK}/pause15.signal"
REL15="${WORK}/pause15.release"
register_release_file "$REL15"
OUT15B="${WORK}/out15b.log"
: >"$OUT15B"

spawn_publisher_bg "$RUNNER_A_REPO" "$R15" "$L15A" "$C3" /dev/null "${WORK}/err15a.log" \
  RUNNER_TEMP="${WORK}/tmp15a" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=2 \
  DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS=0 \
  FAKE_PAUSE_AFTER_UPLOAD_PATTERN=".staging/${C3}." \
  FAKE_PAUSE_SIGNAL_FILE="$SIG15" \
  FAKE_PAUSE_RELEASE_FILE="$REL15"
PID15A="$LAST_BG_PID"

wait_for_file "$SIG15" 150
assert_missing "${R15}/drive9/commits/${C3}"

spawn_publisher_bg "$RUNNER_B_REPO" "$R15" "$L15B" "$MAIN_NEW" "$OUT15B" "${WORK}/err15b.log" \
  RUNNER_TEMP="${WORK}/tmp15b" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=2 \
  DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS=0 \
  DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=30 \
  DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1
PID15B="$LAST_BG_PID"

wait_for_log_match "$OUT15B" "waiting for archive lock" 150
assert_missing "${R15}/drive9/commits/${C3}"

touch "$REL15"
wait "$PID15A" || { cat "${WORK}/err15a.log" >&2; fail "Publisher A (C3) failed"; }
wait "$PID15B" || { cat "${WORK}/err15b.log" >&2; fail "Publisher B (MAIN_NEW) failed"; }
assert_exists "${R15}/drive9/commits/${C3}/manifest.json"
assert_exists "${R15}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_no_active_lease "$R15"
pass "different-SHA concurrent publishers serialized cleanly even with grace=0; observed B wait and both completed"

echo "== 16. Post-auth / pre-rm server barrier: SIGTERM mid-mutation retains lock fail-closed, excludes takeover, and owner-pinned break-glass recovers after drain"
R16="${WORK}/remote16"
L16A="${WORK}/ops16a.log"
L16B1="${WORK}/ops16b1.log"
L16B2="${WORK}/ops16b2.log"
L16B3="${WORK}/ops16b3.log"
CLK16="${WORK}/clock16.epoch"
mkdir -p "${R16}/drive9/commits/${MAIN_NEW}/bin"
printf 'stale\n' >"${R16}/drive9/commits/${MAIN_NEW}/source.tar.gz"
: >"$L16A"
: >"$L16B1"
: >"$L16B2"
: >"$L16B3"
printf '1700000000\n' >"$CLK16"
SIG16="${WORK}/pause16.signal"
REL16="${WORK}/pause16.release"
register_release_file "$REL16"

# Pause Publisher A *inside FAKE_CLI AFTER verify_fs_auth succeeds* and *immediately before* `rm -rf commits/${MAIN_NEW}`.
spawn_publisher_bg "$RUNNER_A_REPO" "$R16" "$L16A" "$MAIN_NEW" /dev/null "${WORK}/err16a.log" \
  RUNNER_TEMP="${WORK}/tmp16a" \
  DRIVE9_ARCHIVE_NOW_FILE="$CLK16" \
  DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
  DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS=30 \
  FAKE_PAUSE_AFTER_AUTH_RM_PATTERN="commits/${MAIN_NEW}" \
  FAKE_PAUSE_AUTH_SIGNAL_FILE="$SIG16" \
  FAKE_PAUSE_AUTH_RELEASE_FILE="$REL16"
PID16A="$LAST_BG_PID"

wait_for_file "$SIG16" 150
owner16a="$(active_lease_owner "$R16")" || fail "expected active lease owner for Publisher A"
a16_tid="$(awk '$1 == "token-issue" { tid = $2 } END { print tid }' "$L16A")"
[ -n "$a16_tid" ] || fail "expected Publisher A to have issued a scoped token"

# Send SIGTERM ONLY to Publisher A's main process (NOT the paused FAKE_CLI child, simulating an
# already-authenticated server rm -r request still stalled on the server after client disconnect).
kill -TERM "$PID16A"
set +e
wait "$PID16A"
rc16a=$?
set -e
[ "$rc16a" -ne 0 ] || fail "expected SIGTERM'd Publisher A to exit non-zero"
grep -q "retaining archive lock" "${WORK}/err16a.log" || fail "expected fail-closed lock retention warning: $(cat "${WORK}/err16a.log")"
# Lock MUST still be held by owner16a (fail-closed) while the already-admitted rm -r is still paused.
[ "$(active_lease_owner "$R16")" = "$owner16a" ] || fail "expected lock to remain held by ${owner16a} after SIGTERM mid-mutation"

# Advance clock +2 hours; normal Publisher B MUST refuse automatic takeover.
printf '1700007200\n' >"$CLK16"
run_with_watchdog_expect_fail 15 "${WORK}/err16b1.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R16" \
    FAKE_OPS_LOG="$L16B1" \
    RUNNER_TEMP="${WORK}/tmp16b1" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK16" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=2 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    "$SCRIPT" "$MAIN_NEW"
grep -q "DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=${owner16a}" "${WORK}/err16b1.log" || fail "expected break_lock_owner guidance naming ${owner16a}"

# Wrong break_lock_owner MUST also refuse takeover.
run_with_watchdog_expect_fail 15 "${WORK}/err16b2.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R16" \
    FAKE_OPS_LOG="$L16B2" \
    RUNNER_TEMP="${WORK}/tmp16b2" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK16" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="wrong-owner-id" \
    "$SCRIPT" "$MAIN_NEW"
grep -q "did not match" "${WORK}/err16b2.log" || fail "expected break_lock_owner mismatch diagnostic"
[ "$(active_lease_owner "$R16")" = "$owner16a" ] || fail "wrong break_lock_owner unexpectedly modified lock"

# Release A's already-authenticated server rm -r so it drains and removes only the old legacy incomplete dir in isolation.
touch "$REL16"
wait_for_missing "${R16}/drive9/commits/${MAIN_NEW}" 150

# Once drained, operator-confirmed owner-pinned break-glass revokes A's token, takes over atomically, and publishes MAIN_NEW.
run_with_watchdog 20 "${WORK}/err16b3.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R16" \
    FAKE_OPS_LOG="$L16B3" \
    RUNNER_TEMP="${WORK}/tmp16b3" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK16" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=10 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="$owner16a" \
    "$SCRIPT" "$MAIN_NEW"
assert_exists "${R16}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_exists "${R16}/drive9/commits/${MAIN_NEW}/bin/drive9-linux-amd64"
[ "$(jq -r '.commit_sha' "${R16}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "B did not publish latest.json"
assert_no_active_lease "$R16"
pass "post-auth / pre-rm barrier: SIGTERM mid-mutation retained lock fail-closed, excluded takeover, drained old rm in isolation, and owner-pinned break-glass recovered cleanly"

echo "== 17. Invocation-private work_root with unset RUNNER_TEMP cleans up without colliding"
R17="${WORK}/remote17"
L17="${WORK}/ops17.log"
mkdir -p "$R17"
: >"$L17"
(
  cd "$RUNNER_A_REPO"
  env -u RUNNER_TEMP \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    FAKE_REMOTE_ROOT="$R17" \
    FAKE_OPS_LOG="$L17" \
    "$SCRIPT" "$MAIN_NEW" >/dev/null
)
assert_exists "${R17}/drive9/commits/${MAIN_NEW}/manifest.json"
leftover_tmp="$(find "${RUNNER_A_REPO}/.tmp" -mindepth 1 -maxdepth 1 -name 'drive9-agent-archive.*' 2>/dev/null || true)"
[ -z "$leftover_tmp" ] || fail "expected invocation-private work_root to be cleaned up, found: ${leftover_tmp}"
pass "invocation-private work_root created and removed cleanly when RUNNER_TEMP is unset"

echo "== 18. Break-glass refuses takeover during token revocation 503, then recovers and prunes aged orphaned .staging/"
R18="${WORK}/remote18"
L18A="${WORK}/ops18a.log"
L18B="${WORK}/ops18b.log"
CLK18="${WORK}/clock18.epoch"
mkdir -p "${R18}/drive9/.publish-lock/initialized"
mkdir -p "${R18}/drive9/.publish-lock/lease.1700000000.tok_stale_18.owner-stale"
mkdir -p "${R18}/drive9/.staging/${C2}.owner-dead/bin"
printf 'orphaned-stage\n' >"${R18}/drive9/.staging/${C2}.owner-dead/source.tar.gz"
touch -t 202001010000 "${R18}/drive9/.staging/${C2}.owner-dead"
mkdir -p "${R18}/.tokens"
printf '1700003600\tactive\n' >"${R18}/.tokens/tok_stale_18"
printf '1700007200\n' >"$CLK18"
: >"$L18A"
: >"$L18B"

run_with_watchdog_expect_fail 15 "${WORK}/err18a.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R18" \
    FAKE_OPS_LOG="$L18A" \
    RUNNER_TEMP="${WORK}/tmp18a" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK18" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=2 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="owner-stale" \
    FAKE_FAIL_REVOKE_PATTERN="tok_stale_18" \
    "$SCRIPT" "$MAIN_NEW"
grep -q "failed to revoke archive lock token tok_stale_18" "${WORK}/err18a.log" || fail "expected revocation failure warning: $(cat "${WORK}/err18a.log")"
assert_exists "${R18}/drive9/.publish-lock/lease.1700000000.tok_stale_18.owner-stale"
assert_missing "${R18}/drive9/commits/${MAIN_NEW}/manifest.json"
if grep -q '^upload ' "$L18A"; then
  fail "publisher must not upload archive data when break-glass revocation failed"
fi

# Once revocation succeeds, break-glass revokes tok_stale_18, prunes aged .staging/, promotes MAIN_NEW, and releases lock.
run_with_watchdog 20 "${WORK}/err18b.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R18" \
    FAKE_OPS_LOG="$L18B" \
    RUNNER_TEMP="${WORK}/tmp18b" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK18" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=10 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="owner-stale" \
    "$SCRIPT" "$MAIN_NEW"
grep -q "^token-revoke tok_stale_18$" "$L18B" || fail "expected break-glass publisher to revoke tok_stale_18"
assert_missing "${R18}/drive9/.staging/${C2}.owner-dead"
assert_exists "${R18}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_no_active_lease "$R18"
pass "break-glass refused takeover during revocation 503, then revoked token, pruned aged .staging/, and promoted MAIN_NEW"

echo "== 19. Harness isolated process-group cleanup reaps nested descendants on early failure and fails fast without PGID isolation"
GRANDCHILD_PID_FILE="${WORK}/grandchild19.pid"
err19_self="${WORK}/err19_self.log"
set +e
TEST_HARNESS_CLEANUP_SELFTEST_PID_FILE="$GRANDCHILD_PID_FILE" "$0" >/dev/null 2>"$err19_self"
sub_rc=$?
set -e
[ "$sub_rc" -ne 0 ] || fail "expected harness selftest early failure to exit non-zero"
grep -q "simulated early test harness failure" "$err19_self" || fail "expected selftest failure message, got: $(cat "$err19_self")"
grandchild_pid="$(cat "$GRANDCHILD_PID_FILE" 2>/dev/null || true)"
[ -n "$grandchild_pid" ] || fail "expected grandchild PID to be recorded"
if kill -0 "$grandchild_pid" 2>/dev/null; then
  kill -KILL "$grandchild_pid" 2>/dev/null || true
  fail "grandchild process ${grandchild_pid} survived harness isolated PGID cleanup"
fi

# Verify fail-fast when neither python3 nor setsid is available on PATH.
NO_PGID_BIN="${WORK}/no-pgid-bin"
mkdir -p "$NO_PGID_BIN"
for tool in mktemp rm dirname pwd; do
  tool_path="$(command -v "$tool")"
  ln -s "$tool_path" "${NO_PGID_BIN}/${tool}"
done
err19_nopgid="${WORK}/err19_nopgid.log"
set +e
PATH="$NO_PGID_BIN" "$(command -v bash)" "$0" >/dev/null 2>"$err19_nopgid"
nopgid_rc=$?
set -e
[ "$nopgid_rc" -ne 0 ] || fail "expected harness to fail fast when neither python3 nor setsid is available"
grep -q "process-group isolation requires python3 or setsid" "$err19_nopgid" || fail "expected PGID isolation requirement message, got: $(cat "$err19_nopgid")"
pass "harness PGID cleanup reaped nested descendants on early failure and failed fast without PGID isolation"

echo "== 20. Post-auth / pre-rm ambiguous CLI transport EOF (without SIGTERM): aborts without publishing, retains lock fail-closed, excludes overlap until drained"
R20="${WORK}/remote20"
L20A="${WORK}/ops20a.log"
L20B1="${WORK}/ops20b1.log"
L20B2="${WORK}/ops20b2.log"
CLK20="${WORK}/clock20.epoch"
mkdir -p "${R20}/drive9/commits/${MAIN_NEW}/bin"
printf 'stale\n' >"${R20}/drive9/commits/${MAIN_NEW}/source.tar.gz"
: >"$L20A"
: >"$L20B1"
: >"$L20B2"
printf '1700000000\n' >"$CLK20"
SIG20="${WORK}/pause20.signal"
REL20="${WORK}/pause20.release"
register_release_file "$REL20"

# Publisher A invokes `rm -r commits/${MAIN_NEW}`; FAKE_CLI passes verify_fs_auth, leaves the
# simulated server delete paused on SIG20/REL20, and returns an ambiguous transport EOF error
# with NO signal sent to Publisher A.
run_with_watchdog_expect_fail 15 "${WORK}/err20a.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_A_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R20" \
    FAKE_OPS_LOG="$L20A" \
    RUNNER_TEMP="${WORK}/tmp20a" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK20" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS=30 \
    FAKE_AMBIGUOUS_AFTER_AUTH_RM_PATTERN="commits/${MAIN_NEW}" \
    FAKE_PAUSE_AUTH_SIGNAL_FILE="$SIG20" \
    FAKE_PAUSE_AUTH_RELEASE_FILE="$REL20" \
    "$SCRIPT" "$MAIN_NEW"

wait_for_file "$SIG20" 150
grep -q "ambiguous failure removing incomplete target" "${WORK}/err20a.log" || fail "expected ambiguous target-rm abort message: $(cat "${WORK}/err20a.log")"
grep -q "retaining archive lock" "${WORK}/err20a.log" || fail "expected fail-closed lock retention on ambiguous rm EOF: $(cat "${WORK}/err20a.log")"
if grep -q '^upload ' "$L20A"; then
  fail "Publisher A must not continue uploading after ambiguous incomplete-target rm failure"
fi
owner20a="$(active_lease_owner "$R20")" || fail "expected lock to remain held fail-closed after ambiguous CLI EOF"
a20_tid="$(awk '$1 == "token-issue" { tid = $2 } END { print tid }' "$L20A")"
[ -n "$a20_tid" ] || fail "expected Publisher A to have issued a scoped token"

# Advance clock +2 hours; normal Publisher B MUST fail closed and not overlap the paused server delete.
printf '1700007200\n' >"$CLK20"
run_with_watchdog_expect_fail 15 "${WORK}/err20b1.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R20" \
    FAKE_OPS_LOG="$L20B1" \
    RUNNER_TEMP="${WORK}/tmp20b1" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK20" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=2 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    "$SCRIPT" "$MAIN_NEW"
grep -q "DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=${owner20a}" "${WORK}/err20b1.log" || fail "expected break_lock_owner guidance naming ${owner20a}"
if grep -qE '^(upload|rm) ' "$L20B1"; then
  fail "Publisher B must not mutate archive while A's ambiguous server delete is still paused"
fi

# Drain the paused server-side delete in isolation before operator-confirmed recovery.
touch "$REL20"
wait_for_missing "${R20}/drive9/commits/${MAIN_NEW}" 150
wait_for_log_match "$L20A" "^rm-delayed-server-commit drive9/commits/${MAIN_NEW}$" 150

# Explicit owner-pinned break-glass now recovers cleanly and publishes MAIN_NEW.
run_with_watchdog 20 "${WORK}/err20b2.log" \
  bash -c 'cd "$1" && shift && exec "$@"' _ "$RUNNER_B_REPO" \
  env \
    DRIVE9_SERVER="https://api.example.test" \
    DRIVE9_API_KEY="test-key" \
    DRIVE9_PUBLISHER="$FAKE_CLI" \
    DRIVE9_ARCHIVE_CLI_TARGETS="linux/amd64" \
    DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY="0" \
    FAKE_REMOTE_ROOT="$R20" \
    FAKE_OPS_LOG="$L20B2" \
    RUNNER_TEMP="${WORK}/tmp20b2" \
    DRIVE9_ARCHIVE_NOW_FILE="$CLK20" \
    DRIVE9_ARCHIVE_RETAIN_COMMITS=5 \
    DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS=10 \
    DRIVE9_ARCHIVE_LOCK_POLL_SECONDS=1 \
    DRIVE9_ARCHIVE_LOCK_BREAK_OWNER="$owner20a" \
    "$SCRIPT" "$MAIN_NEW"
assert_exists "${R20}/drive9/commits/${MAIN_NEW}/manifest.json"
assert_exists "${R20}/drive9/commits/${MAIN_NEW}/bin/drive9-linux-amd64"
[ "$(jq -r '.commit_sha' "${R20}/drive9/latest.json")" = "$MAIN_NEW" ] || fail "B did not publish latest.json"
assert_no_active_lease "$R20"
pass "ambiguous CLI transport EOF (without SIGTERM) aborted without overlap, retained lock fail-closed, drained old rm, and recovered via owner-pinned break-glass"

echo "ALL TESTS PASSED"
