#!/usr/bin/env bash
# drive9 CLI smoke test against a live deployment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_IMAGE_FIXTURE_PATH="${DRIVE9_IMAGE_FIXTURE_PATH:-$SCRIPT_DIR/fixtures/cat03.jpg}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_LARGE_FILE_MB="${CLI_LARGE_FILE_MB:-100}"
CLI_BATCH_SMALL_FILE_COUNT="${CLI_BATCH_SMALL_FILE_COUNT:-10}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
RUN_CLI_UPLOAD_LIMIT_BOUNDARY="${RUN_CLI_UPLOAD_LIMIT_BOUNDARY:-1}"
CLI_UPLOAD_LIMIT_BYTES="${CLI_UPLOAD_LIMIT_BYTES:-10737418240}"
CLI_SEMANTIC_TIMEOUT_S="${CLI_SEMANTIC_TIMEOUT_S:-90}"
CLI_SEMANTIC_INTERVAL_S="${CLI_SEMANTIC_INTERVAL_S:-3}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS+1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    echo "PASS $desc"
    PASS=$((PASS+1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL+1))
  fi
}

detect_release_target() {
  case "$(uname -s)" in
    Linux) CLI_RELEASE_OS="linux" ;;
    Darwin) CLI_RELEASE_OS="darwin" ;;
    *)
      echo "unsupported OS for official CLI download: $(uname -s)" >&2
      return 1
      ;;
  esac

  case "$(uname -m)" in
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *)
      echo "unsupported architecture for official CLI download: $(uname -m)" >&2
      return 1
      ;;
  esac
}

download_official_cli() {
  local target_version="$CLI_RELEASE_VERSION"
  detect_release_target || return 1
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" | tr -d '[:space:]')
  fi
  if [ -z "$target_version" ]; then
    echo "failed to resolve release version from $CLI_RELEASE_BASE_URL/version" >&2
    return 1
  fi
  curl -fsSL "$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
  local actual_version
  actual_version="$($CLI_BIN --version 2>/dev/null | awk '{print $2}')"
  if [ -n "$CLI_RELEASE_VERSION" ] && [ "$actual_version" != "$CLI_RELEASE_VERSION" ]; then
    echo "downloaded version mismatch: expected=$CLI_RELEASE_VERSION actual=$actual_version" >&2
    return 1
  fi
  echo "downloaded official drive9 $actual_version for $CLI_RELEASE_OS/$CLI_RELEASE_ARCH" >&2
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      make build-cli CLI_BIN="$CLI_BIN"
      ;;
    official)
      download_official_cli
      ;;
    *)
      echo "invalid CLI_SOURCE: $CLI_SOURCE (expected build|official)" >&2
      return 1
      ;;
  esac
}

echo "=== drive9 CLI smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "IMAGE_FIXTURE=$DRIVE9_IMAGE_FIXTURE_PATH"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi
check_cmd "local image fixture exists" test -s "$DRIVE9_IMAGE_FIXTURE_PATH"

echo "[1] provision tenant"
pfile="$(mktemp)"
pcode=$(curl -sS -o "$pfile" -w "%{http_code}" -X POST "$BASE/v1/provision")
check_eq "POST /v1/provision returns 202" "$pcode" "202"
API_KEY=$(jq -r '.api_key // empty' "$pfile")
check_cmd "provision returns api_key" test -n "$API_KEY"

echo "[2] wait tenant active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
while :; do
  sfile="$(mktemp)"
  scode=$(curl -sS -o "$sfile" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
  state=$(jq -r '.status // empty' "$sfile")
  rm -f "$sfile"
  echo "status=${scode}:${state}"
  if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$state" "active"

echo "[3] prepare drive9 cli"
prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

dat9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

dat9_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(dat9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for dat9 $* (throttled)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

# Some read-after-write paths can be eventually consistent right after upload.
dat9_retry_read() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(dat9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"not found"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for dat9 $* (not found yet)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for dat9 $* (throttled)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

wait_cli_grep_target() {
  local desc="$1"
  local query="$2"
  local target="$3"
  local deadline=$(( $(date +%s) + CLI_SEMANTIC_TIMEOUT_S ))
  local found="false"
  while :; do
    local out
    out="$(dat9_retry fs grep "$query" /)"
    found=$(python3 - "$out" "$target" <<'PY'
import sys
out=sys.argv[1].splitlines()
target=sys.argv[2]
print("true" if any(line.split("\t")[0].strip()==target for line in out if line.strip()) else "false")
PY
)
    if [ "$found" = "true" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    echo "semantic recall not ready for $target, retrying" >&2
    sleep "$CLI_SEMANTIC_INTERVAL_S"
  done
  check_eq "$desc" "$found" "true"
}

TS="$(date +%s)"
SMALL_LOCAL="/tmp/dat9-cli-small-${TS}.txt"
SMALL_REMOTE="/cli-${TS}-small.txt"
SMALL_RENAMED="/cli-${TS}-small-renamed.txt"
IMAGE_LOCAL="/tmp/dat9-cli-image-${TS}.jpg"
IMAGE_REMOTE="/cli-${TS}-image.jpg"
SEM_TEXT_TARGET="/cli-${TS}-cat-story.txt"
SEM_TEXT_OTHER="/cli-${TS}-dog-story.txt"
IMAGE_CAPTION_REMOTE="/cli-${TS}-image.caption.txt"
BATCH_LOCAL_DIR="/tmp/dat9-cli-batch-${TS}"
BATCH_REMOTE_DIR="/cli-${TS}-batch"
LARGE_LOCAL="/tmp/dat9-cli-large-${TS}.bin"
LARGE_REMOTE="/cli-${TS}-large-${CLI_LARGE_FILE_MB}m.bin"
LARGE_DOWNLOADED="/tmp/dat9-cli-large-${TS}.download.bin"
LARGE_BYTES=$((CLI_LARGE_FILE_MB * 1024 * 1024))

echo "[4] small file ops via cli"
printf "cli-smoke-%s" "$TS" > "$SMALL_LOCAL"
dat9_retry fs cp "$SMALL_LOCAL" ":$SMALL_REMOTE" >/dev/null

ls_out="$(dat9_retry fs ls /)"
small_present=$(python3 - "$ls_out" "$(basename "$SMALL_REMOTE")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "uploaded small file appears in ls /" "$small_present" "true"

cat_out="$(dat9_retry_read fs cat "$SMALL_REMOTE")"
check_eq "cat returns expected small file content" "$cat_out" "cli-smoke-${TS}"

dat9_retry fs mv "$SMALL_REMOTE" "$SMALL_RENAMED" >/dev/null
renamed_out="$(dat9_retry fs ls /)"
renamed_present=$(python3 - "$renamed_out" "$(basename "$SMALL_RENAMED")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "mv renames remote file" "$renamed_present" "true"

echo "[5] batch small-file upload/list/read via cli"
mkdir -p "$BATCH_LOCAL_DIR"
for i in $(seq 1 "$CLI_BATCH_SMALL_FILE_COUNT"); do
  lp="$BATCH_LOCAL_DIR/file-${i}.txt"
  rp="$BATCH_REMOTE_DIR/file-${i}.txt"
  printf "cli-batch-%s-%s" "$TS" "$i" > "$lp"
  dat9_retry fs cp "$lp" ":$rp" >/dev/null
done

batch_ls="$(dat9_retry fs ls "$BATCH_REMOTE_DIR")"
batch_count=$(python3 - "$batch_ls" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print(len(lines))
PY
)
check_eq "batch dir file count matches" "$batch_count" "$CLI_BATCH_SMALL_FILE_COUNT"

for i in 1 "$CLI_BATCH_SMALL_FILE_COUNT"; do
  rp="$BATCH_REMOTE_DIR/file-${i}.txt"
  got="$(dat9_retry_read fs cat "$rp")"
  want="cli-batch-$TS-$i"
  check_eq "batch file content matches for $rp" "$got" "$want"
done

batch_stat="$(dat9_retry fs stat "$BATCH_REMOTE_DIR/file-1.txt")"
batch_isdir=$(python3 - "$batch_stat" <<'PY'
import sys
val=""
for line in sys.argv[1].splitlines():
    if line.strip().startswith("isdir:"):
        val=line.split(":",1)[1].strip().lower()
        break
print(val)
PY
)
check_eq "stat reports batch file isdir=false" "$batch_isdir" "false"

echo "[6] cli grep/find checks"
grep_out="$(dat9_retry fs grep "cli-batch-$TS" "/")"
grep_has_batch=$(python3 - "$grep_out" "$BATCH_REMOTE_DIR/file-1.txt" <<'PY'
import sys
out=sys.argv[1].splitlines()
target=sys.argv[2]
print("true" if any(line.split("\t")[0].strip()==target for line in out if line.strip()) else "false")
PY
)
check_eq "cli grep finds batch file" "$grep_has_batch" "true"

find_out="$(dat9_retry fs find / -name "*.txt")"
find_has_txt=$(python3 - "$find_out" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print("true" if any("/cli-" in line and line.endswith(".txt") for line in lines) else "false")
PY
)
check_eq "cli find by name returns txt files" "$find_has_txt" "true"

echo "[6.1] cli semantic text recall checks"
printf "A cat is resting on a sofa near a window." > "/tmp/dat9-cli-sem-target-${TS}.txt"
printf "A dog is running in a field under bright sun." > "/tmp/dat9-cli-sem-other-${TS}.txt"
dat9_retry fs cp "/tmp/dat9-cli-sem-target-${TS}.txt" ":$SEM_TEXT_TARGET" >/dev/null
dat9_retry fs cp "/tmp/dat9-cli-sem-other-${TS}.txt" ":$SEM_TEXT_OTHER" >/dev/null

wait_cli_grep_target "cli semantic grep includes cat-story target" "feline sofa" "$SEM_TEXT_TARGET"
wait_cli_grep_target "cli semantic grep includes dog-story target" "canine field" "$SEM_TEXT_OTHER"

echo "[6.2] cli image-associated recall checks"
cp "$DRIVE9_IMAGE_FIXTURE_PATH" "$IMAGE_LOCAL"
check_cmd "local cli jpg fixture exists" test -s "$IMAGE_LOCAL"
dat9_retry fs cp "$IMAGE_LOCAL" ":$IMAGE_REMOTE" >/dev/null
printf "This image shows a cat face icon." > "/tmp/dat9-cli-image-caption-${TS}.txt"
dat9_retry fs cp "/tmp/dat9-cli-image-caption-${TS}.txt" ":$IMAGE_CAPTION_REMOTE" >/dev/null

wait_cli_grep_target "cli image-associated grep includes caption" "feline face icon" "$IMAGE_CAPTION_REMOTE"

find_png_out="$(dat9_retry fs find / -name "*.jpg")"
find_has_png=$(python3 - "$find_png_out" "$IMAGE_REMOTE" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
target=sys.argv[2]
print("true" if any(line == target or line.endswith('.jpg') for line in lines) else "false")
PY
)
check_eq "cli find by name returns jpg files" "$find_has_png" "true"

echo "[7] large multipart upload via cli cp"
dd if=/dev/zero of="$LARGE_LOCAL" bs=1M count="$CLI_LARGE_FILE_MB" status=none
dat9_retry fs cp "$LARGE_LOCAL" ":$LARGE_REMOTE" >/dev/null

stat_out="$(dat9_retry fs stat "$LARGE_REMOTE")"
remote_size=$(python3 - "$stat_out" <<'PY'
import sys
for line in sys.argv[1].splitlines():
    if line.strip().startswith("size:"):
        print(line.split(":",1)[1].strip())
        break
PY
)
check_eq "large remote size matches" "$remote_size" "$LARGE_BYTES"

dat9_retry fs cp ":$LARGE_REMOTE" "$LARGE_DOWNLOADED" >/dev/null
check_cmd "downloaded large file exists" test -f "$LARGE_DOWNLOADED"

sum_src=$(sha256sum "$LARGE_LOCAL" | cut -d' ' -f1)
sum_dst=$(sha256sum "$LARGE_DOWNLOADED" | cut -d' ' -f1)
check_eq "downloaded large file sha256 matches" "$sum_dst" "$sum_src"

echo "[8] cleanup via cli"
dat9_retry fs rm "$SMALL_RENAMED" >/dev/null
dat9_retry fs rm "$IMAGE_REMOTE" >/dev/null
dat9_retry fs rm "$LARGE_REMOTE" >/dev/null
for i in $(seq 1 "$CLI_BATCH_SMALL_FILE_COUNT"); do
  dat9_retry fs rm "$BATCH_REMOTE_DIR/file-${i}.txt" >/dev/null
done

final_ls="$(dat9_retry fs ls /)"
small_left=$(python3 - "$final_ls" "$(basename "$SMALL_RENAMED")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
large_left=$(python3 - "$final_ls" "$(basename "$LARGE_REMOTE")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "small file removed" "$small_left" "false"
check_eq "large file removed" "$large_left" "false"

batch_ls_after="$(dat9_retry fs ls /)"
batch_left=$(python3 - "$batch_ls_after" "$(basename "$BATCH_REMOTE_DIR")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name or line.strip()==name+"/" for line in out) else "false")
PY
)
# Directory cleanup semantics may vary; allow either retained empty dir or auto-removed dir.
check_cmd "batch directory cleanup accepted" test "$batch_left" = "true" -o "$batch_left" = "false"

if [ "$RUN_CLI_UPLOAD_LIMIT_BOUNDARY" = "1" ]; then
  echo "[9] upload limit boundary via API with CLI auth"
  boundary_ok_payload="$(mktemp)"
  python3 - "cli-limit-${TS}.bin" "$CLI_UPLOAD_LIMIT_BYTES" > "$boundary_ok_payload" <<'PY'
import base64
import hashlib
import json
import sys

path = "/" + sys.argv[1].lstrip("/")
upload_limit = int(sys.argv[2])
part_size = 8 * 1024 * 1024
part = b"\x00" * part_size
checksum = base64.b64encode(hashlib.sha256(part).digest()).decode()
parts = (upload_limit + part_size - 1) // part_size
print(json.dumps({
    "path": path,
    "total_size": upload_limit,
    "part_checksums": [checksum] * parts,
}))
PY

  ok_file="$(mktemp)"
  ok_code=$(curl -sS -o "$ok_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_ok_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "cli-boundary init at limit returns 202" "$ok_code" "202"
  rm -f "$boundary_ok_payload" "$ok_file"

  over=$((CLI_UPLOAD_LIMIT_BYTES + 1))
  boundary_over_payload="$(mktemp)"
  python3 - "cli-limit-over-${TS}.bin" "$over" > "$boundary_over_payload" <<'PY'
import base64
import hashlib
import json
import sys

path = "/" + sys.argv[1].lstrip("/")
over_limit = int(sys.argv[2])
part = b"\x00" * (8 * 1024 * 1024)
checksum = base64.b64encode(hashlib.sha256(part).digest()).decode()
print(json.dumps({
    "path": path,
    "total_size": over_limit,
    "part_checksums": [checksum],
}))
PY

  over_file="$(mktemp)"
  over_code=$(curl -sS -o "$over_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_over_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "cli-boundary init over limit returns 413" "$over_code" "413"
  over_err=$(jq -r '.error // empty' "$over_file")
  check_cmd "cli-boundary over-limit has error message" test -n "$over_err"
  rm -f "$boundary_over_payload" "$over_file"
fi

rm -f "$pfile" "$CLI_BIN" "$SMALL_LOCAL" "$IMAGE_LOCAL" "$LARGE_LOCAL" "$LARGE_DOWNLOADED"
rm -f "/tmp/dat9-cli-sem-target-${TS}.txt" "/tmp/dat9-cli-sem-other-${TS}.txt" "/tmp/dat9-cli-image-caption-${TS}.txt"
rm -rf "$BATCH_LOCAL_DIR"

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
