#!/usr/bin/env bash
#
# Regression tests for scripts/publish-agent-archive.sh retention, incomplete
# commit cleanup, latest.json protection, and post-prune async-GC upload retry.
#
# Usage: scripts/test-publish-agent-archive.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/publish-agent-archive.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pass() {
  echo "   ok: $*"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

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

strip_ref() {
  local ref=$1
  ref="${ref#:}"
  ref="${ref#/}"
  printf '%s\n' "$ref"
}

cmd="${1:-}"
sub="${2:-}"
if [ "$cmd" != "fs" ]; then
  echo "unsupported command: $*" >&2
  exit 1
fi
shift 2

case "$sub" in
ls)
  rel="$(strip_ref "$1")"
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
  rel="$(strip_ref "$1")"
  target="${root}/${rel}"
  if [ ! -e "$target" ]; then
    echo "fs stat: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'stat %s\n' "$rel" >>"$ops_log"
  if [ -d "$target" ]; then
    printf 'isdir: true\n'
  else
    printf 'isdir: false\n'
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
  if [ -d "$target" ]; then
    echo "fs mkdir: path already exists" >&2
    exit 1
  fi
  printf 'mkdir %s\n' "$rel" >>"$ops_log"
  mkdir -p "$target"
  ;;
rm)
  recursive=0
  if [ "${1:-}" = "-r" ] || [ "${1:-}" = "--recursive" ]; then
    recursive=1
    shift
  fi
  rel="$(strip_ref "$1")"
  target="${root}/${rel}"
  if [ ! -e "$target" ]; then
    echo "fs rm: HTTP 404: not found" >&2
    exit 1
  fi
  printf 'rm %s\n' "$rel" >>"$ops_log"
  if [ "$recursive" -eq 1 ]; then
    rm -rf "$target"
  else
    rm -f "$target"
  fi
  ;;
cp)
  src=$1
  dst=$2
  if [[ "$dst" == :* ]]; then
    rel="$(strip_ref "$dst")"
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
DIST_DIR ?= dist
VERSION ?= dev
CLI_TARGETS ?= linux/amd64

build-cli-release:
	@set -euo pipefail; \
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
    --arg src_sha "$src_sha" \
    --arg bin_sha "$bin_sha" \
    '{
      schema_version: 1,
      repository: "mem9-ai/drive9",
      branch: "main",
      commit_sha: $sha,
      short_sha: $short,
      published_at: "2026-10-04T00:00:00Z",
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

run_publisher() {
  local remote_root=$1
  local ops_log=$2
  shift 2
  (
    cd "$REPO"
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

echo "== 1. Pre-publish prune removes incomplete + oldest expired commits and preserves latest/non-SHA entries"
R1="${WORK}/remote1"
L1="${WORK}/ops1.log"
: >"$L1"
seed_complete_commit "$R1" "$C1"
seed_complete_commit "$R1" "$C2"
seed_complete_commit "$R1" "$C3"
seed_latest_index "$R1" "$C3"
# Incomplete directories from prior failed runs (including target C5 itself).
mkdir -p "${R1}/drive9/commits/${C4}/bin"
printf 'partial\n' >"${R1}/drive9/commits/${C4}/source.tar.gz"
mkdir -p "${R1}/drive9/commits/${C5}/bin"
# Non-SHA entries must never be pruned.
mkdir -p "${R1}/drive9/commits/notes"
printf 'keep\n' >"${R1}/drive9/commits/README.txt"

run_publisher "$R1" "$L1" env DRIVE9_ARCHIVE_RETAIN_COMMITS=2 >/dev/null

assert_missing "${R1}/drive9/commits/${C1}"
assert_missing "${R1}/drive9/commits/${C2}"
assert_missing "${R1}/drive9/commits/${C4}"
assert_exists "${R1}/drive9/commits/${C3}/manifest.json"
assert_exists "${R1}/drive9/commits/${C5}/manifest.json"
assert_exists "${R1}/drive9/commits/notes"
assert_exists "${R1}/drive9/commits/README.txt"
[ "$(jq -r '.commit_sha' "${R1}/drive9/latest.json")" = "$C5" ] || fail "latest.json did not advance to C5"

# Verify deletion order: incomplete C4 first, then expired oldest-first (C1 before C2), all before any upload.
rm_lines="$(grep '^rm ' "$L1")"
want_rm="$(printf 'rm drive9/commits/%s\nrm drive9/commits/%s\nrm drive9/commits/%s' "$C4" "$C1" "$C2")"
[ "$rm_lines" = "$want_rm" ] || fail "unexpected rm order:\ngot:\n${rm_lines}\nwant:\n${want_rm}"
first_upload_line="$(grep -n '^upload ' "$L1" | head -n 1 | cut -d: -f1)"
last_rm_line="$(grep -n '^rm ' "$L1" | tail -n 1 | cut -d: -f1)"
[ "$last_rm_line" -lt "$first_upload_line" ] || fail "expected pruning to finish before first upload"
pass "pruned C4, C1, C2 oldest-first before upload; kept C3, C5, and non-SHA entries"

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

echo "== 4. Post-prune upload retries transient quota exceeded (async file_gc drain)"
# Seed an expired commit so pre-publish pruning deletes it and enables post-prune quota retry.
seed_complete_commit "$R1" "$C1"
C7="$(make_commit c7)"
L4="${WORK}/ops4.log"
: >"$L4"
Q4="${WORK}/quota-fail-count"
printf '2\n' >"$Q4"
run_publisher "$R1" "$L4" env DRIVE9_ARCHIVE_RETAIN_COMMITS=1 FAKE_FAIL_QUOTA_COUNT_FILE="$Q4" >/dev/null
assert_missing "${R1}/drive9/commits/${C1}"
assert_exists "${R1}/drive9/commits/${C7}/manifest.json"
assert_missing "${R1}/drive9/commits/${C6}"
[ "$(cat "$Q4")" = "0" ] || fail "expected both transient quota errors to be retried"
pass "retried quota exceeded after pruning and succeeded"

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

echo "== 6. DRIVE9_ARCHIVE_RETAIN_COMMITS=0 disables pruning and invalid values fail fast"
L6="${WORK}/ops6.log"
: >"$L6"
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_RETAIN_COMMITS=0 >/dev/null
if grep -q '^rm ' "$L6"; then
  fail "expected no pruning when DRIVE9_ARCHIVE_RETAIN_COMMITS=0"
fi
assert_exists "${R1}/drive9/commits/${C7}/manifest.json"
assert_exists "${R1}/drive9/commits/${C8}/manifest.json"

set +e
run_publisher "$R1" "$L6" env DRIVE9_ARCHIVE_RETAIN_COMMITS=invalid >/dev/null 2>&1
bad_rc=$?
set -e
[ "$bad_rc" -ne 0 ] || fail "expected invalid DRIVE9_ARCHIVE_RETAIN_COMMITS to fail"
pass "DRIVE9_ARCHIVE_RETAIN_COMMITS=0 and validation behave as expected"

echo "ALL TESTS PASSED"
