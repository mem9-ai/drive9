#!/usr/bin/env bash
# Portable profile pack/unpack e2e against a live deployment.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
PORTABLE_PACK_E2E_KEEP_WORK="${PORTABLE_PACK_E2E_KEEP_WORK:-0}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL + 1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
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
    x86_64 | amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64 | arm64) CLI_RELEASE_ARCH="arm64" ;;
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

drive9() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_ENV_HOME" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(drive9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $* (throttled)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

write_tree_manifest() {
  local root="$1"
  local out="$2"
  python3 - "$root" >"$out" <<'PY'
import hashlib
import json
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])
records = []

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
    names = sorted(dirnames + filenames)
    dirnames[:] = sorted(name for name in dirnames if not os.path.islink(os.path.join(dirpath, name)))
    for name in names:
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        st = os.lstat(path)
        mode = stat.S_IMODE(st.st_mode)
        if stat.S_ISDIR(st.st_mode):
            records.append({"path": rel + "/", "type": "dir", "mode": mode})
        elif stat.S_ISLNK(st.st_mode):
            records.append({"path": rel, "type": "symlink", "mode": mode, "target": os.readlink(path)})
        elif stat.S_ISREG(st.st_mode):
            records.append({"path": rel, "type": "file", "mode": mode, "size": st.st_size, "sha256": digest(path)})
        else:
            records.append({"path": rel, "type": "other", "mode": mode})

for record in sorted(records, key=lambda item: item["path"]):
    print(json.dumps(record, sort_keys=True, separators=(",", ":")))
PY
}

default_pack_archive_path() {
  local remote_root="$1"
  local profile="$2"
  python3 - "$remote_root" "$profile" <<'PY'
import hashlib
import posixpath
import sys

remote_root = sys.argv[1].strip() or "/"
profile = sys.argv[2].strip() or "coding-agent"
if not remote_root.startswith("/"):
    remote_root = "/" + remote_root
remote_root = posixpath.normpath(remote_root)
if remote_root == ".":
    remote_root = "/"
label = posixpath.basename(remote_root.rstrip("/")) or "root"
safe = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in label).strip(".-") or "root"
digest = hashlib.sha256((profile + "\0" + remote_root).encode()).digest()[:8].hex()
print(f"/.drive9/packs/{safe}-{digest}.tar.gz")
PY
}

cleanup() {
  local rc=$?
  if [ -n "${PACK_REMOTE_ARCHIVE:-}" ] && [ -n "${CLI_BIN:-}" ] && [ -n "${API_KEY:-}" ]; then
    drive9_retry fs rm "$PACK_REMOTE_ARCHIVE" >/dev/null 2>&1 || true
  fi
  if [ "${PORTABLE_PACK_E2E_KEEP_WORK:-0}" != "1" ] && [ -n "${WORK_ROOT:-}" ]; then
    rm -rf "$WORK_ROOT"
  elif [ -n "${WORK_ROOT:-}" ]; then
    echo "kept work root: $WORK_ROOT"
  fi
  exit "$rc"
}

echo "=== drive9 portable pack/unpack e2e ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
check_cmd "git is available" bash -c 'command -v git >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
check_cmd "node is available" bash -c 'command -v node >/dev/null'
check_cmd "npm is available" bash -c 'command -v npm >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available for official CLI" bash -c 'command -v curl >/dev/null'
fi

echo "[1] provision tenant"
if [ -z "$DRIVE9_API_KEY" ]; then
  pfile="$(mktemp)"
  pcode=$(curl -sS -o "$pfile" -w "%{http_code}" -X POST "$BASE/v1/provision")
  check_eq "POST /v1/provision returns 202" "$pcode" "202"
  API_KEY=$(jq -r '.api_key // empty' "$pfile")
  rm -f "$pfile"
  check_cmd "provision returns api_key" test -n "$API_KEY"
else
  API_KEY="$DRIVE9_API_KEY"
  check_cmd "DRIVE9_API_KEY is set" test -n "$API_KEY"
fi

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

TS="$(date +%s)-$$"
WORK_ROOT="$(mktemp -d "/tmp/drive9-portable-pack-e2e-${TS}.XXXXXX")"
CLI_ENV_HOME="$WORK_ROOT/home"
SRC_LOCAL_ROOT="$WORK_ROOT/source-local"
RESTORE_LOCAL_ROOT="$WORK_ROOT/restore-local"
SRC_REPO="$SRC_LOCAL_ROOT/overlay/workspace/app"
RESTORE_REPO="$RESTORE_LOCAL_ROOT/overlay/workspace/app"
REMOTE_ROOT="/portable-pack-e2e-${TS}"
PACK_PROFILE="portable"
PACK_REMOTE_ARCHIVE="$(default_pack_archive_path "$REMOTE_ROOT" "$PACK_PROFILE")"
trap cleanup EXIT

echo "work_root=$WORK_ROOT"
echo "remote_root=$REMOTE_ROOT"
echo "pack_archive=$PACK_REMOTE_ARCHIVE"

echo "[4] build stable local fixture repo"
mkdir -p "$SRC_REPO/src" "$SRC_REPO/vendor/drive9-e2e-local-dep" "$SRC_LOCAL_ROOT/cache"
printf "outside-overlay-%s\n" "$TS" >"$SRC_LOCAL_ROOT/cache/not-packed.txt"

cat >"$SRC_REPO/.gitignore" <<'EOF'
node_modules/
EOF
cat >"$SRC_REPO/package.json" <<'EOF'
{
  "name": "drive9-portable-e2e-app",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "verify": "node src/index.js"
  },
  "dependencies": {
    "drive9-e2e-local-dep": "file:vendor/drive9-e2e-local-dep"
  }
}
EOF
cat >"$SRC_REPO/vendor/drive9-e2e-local-dep/package.json" <<'EOF'
{
  "name": "drive9-e2e-local-dep",
  "version": "1.0.0",
  "main": "index.js"
}
EOF
cat >"$SRC_REPO/vendor/drive9-e2e-local-dep/index.js" <<EOF
exports.answer = function answer() {
  return "dep-${TS}";
};
EOF
cat >"$SRC_REPO/src/index.js" <<'EOF'
const dep = require("drive9-e2e-local-dep");
console.log(`answer=${dep.answer()}`);
EOF
printf "obsolete before pack e2e\n" >"$SRC_REPO/src/obsolete.js"
printf "# portable pack e2e\n\ninitial fixture\n" >"$SRC_REPO/README.md"

npm --prefix "$SRC_REPO" install --ignore-scripts --no-audit --no-fund >/dev/null
check_cmd "npm install creates node_modules" test -e "$SRC_REPO/node_modules/drive9-e2e-local-dep"
check_cmd "installed local package works" bash -c 'node "$1/src/index.js" >/dev/null' -- "$SRC_REPO"

git -C "$SRC_REPO" init >/dev/null
git -C "$SRC_REPO" checkout -b main >/dev/null 2>&1 || true
git -C "$SRC_REPO" config user.name "drive9 e2e"
git -C "$SRC_REPO" config user.email "drive9-e2e@example.invalid"
git -C "$SRC_REPO" add .gitignore package.json package-lock.json README.md src vendor
git -C "$SRC_REPO" commit -m "initial fixture" >/dev/null
git -C "$SRC_REPO" checkout -b feature/portable-e2e >/dev/null

cat >"$SRC_REPO/src/index.js" <<EOF
const dep = require("drive9-e2e-local-dep");
console.log("modified-${TS}:" + dep.answer());
EOF
git -C "$SRC_REPO" add src/index.js
printf "export const generated = %q;\n" "$TS" >"$SRC_REPO/src/generated.js"
git -C "$SRC_REPO" add src/generated.js
git -C "$SRC_REPO" rm src/obsolete.js >/dev/null
printf "# portable pack e2e\n\nmodified fixture %s\n" "$TS" >"$SRC_REPO/README.md"
mkdir -p "$SRC_REPO/notes"
printf "untracked note %s\n" "$TS" >"$SRC_REPO/notes/todo.txt"

git -C "$SRC_REPO" status --porcelain=v1 --branch >"$WORK_ROOT/source-status.txt"
git -C "$SRC_REPO" rev-parse HEAD >"$WORK_ROOT/source-head.txt"
git -C "$SRC_REPO" branch --show-current >"$WORK_ROOT/source-branch.txt"
write_tree_manifest "$SRC_LOCAL_ROOT/overlay" "$WORK_ROOT/source-manifest.jsonl"

check_cmd "fixture git status includes staged modification" grep -q '^M  src/index.js$' "$WORK_ROOT/source-status.txt"
check_cmd "fixture git status includes staged addition" grep -q '^A  src/generated.js$' "$WORK_ROOT/source-status.txt"
check_cmd "fixture git status includes staged delete" grep -q '^D  src/obsolete.js$' "$WORK_ROOT/source-status.txt"
check_cmd "fixture git status includes unstaged README change" grep -q '^ M README.md$' "$WORK_ROOT/source-status.txt"
check_cmd "fixture git status includes untracked notes" grep -q '^?? notes/$' "$WORK_ROOT/source-status.txt"
check_eq "fixture branch is feature branch" "$(cat "$WORK_ROOT/source-branch.txt")" "feature/portable-e2e"

echo "[5] pack portable profile"
drive9_retry profile show "$PACK_PROFILE" >/dev/null
drive9_retry pack --local-root "$SRC_LOCAL_ROOT" --remote-root "$REMOTE_ROOT" --profile "$PACK_PROFILE" >"$WORK_ROOT/pack.log"
pack_archive_stat="$(drive9_retry fs stat "$PACK_REMOTE_ARCHIVE")"
pack_archive_size=$(python3 - "$pack_archive_stat" <<'PY'
import sys
for line in sys.argv[1].splitlines():
    if line.strip().startswith("size:"):
        print(line.split(":", 1)[1].strip())
        break
PY
)
check_cmd "portable pack archive has non-zero remote size" bash -c 'test "${1:-0}" -gt 0' -- "$pack_archive_size"

echo "[6] unpack into fresh local root"
drive9_retry unpack --local-root "$RESTORE_LOCAL_ROOT" --remote-root "$REMOTE_ROOT" --profile "$PACK_PROFILE" >"$WORK_ROOT/unpack.log"
write_tree_manifest "$RESTORE_LOCAL_ROOT/overlay" "$WORK_ROOT/restore-manifest.jsonl"

check_cmd "unpack restores exact overlay manifest" diff -u "$WORK_ROOT/source-manifest.jsonl" "$WORK_ROOT/restore-manifest.jsonl"
check_cmd "unpack does not restore non-overlay local cache" test ! -e "$RESTORE_LOCAL_ROOT/cache/not-packed.txt"

git -C "$RESTORE_REPO" status --porcelain=v1 --branch >"$WORK_ROOT/restore-status.txt"
git -C "$RESTORE_REPO" rev-parse HEAD >"$WORK_ROOT/restore-head.txt"
git -C "$RESTORE_REPO" branch --show-current >"$WORK_ROOT/restore-branch.txt"

check_cmd "restored git status matches source" diff -u "$WORK_ROOT/source-status.txt" "$WORK_ROOT/restore-status.txt"
check_cmd "restored git HEAD matches source" diff -u "$WORK_ROOT/source-head.txt" "$WORK_ROOT/restore-head.txt"
check_cmd "restored git branch matches source" diff -u "$WORK_ROOT/source-branch.txt" "$WORK_ROOT/restore-branch.txt"
check_cmd "restored node_modules package exists" test -e "$RESTORE_REPO/node_modules/drive9-e2e-local-dep"
check_cmd "restored local package works" bash -c 'node "$1/src/index.js" >/dev/null' -- "$RESTORE_REPO"
check_eq "restored README content matches" "$(cat "$RESTORE_REPO/README.md")" "$(cat "$SRC_REPO/README.md")"
check_eq "restored untracked note matches" "$(cat "$RESTORE_REPO/notes/todo.txt")" "$(cat "$SRC_REPO/notes/todo.txt")"

echo
echo "=== portable pack/unpack e2e result ==="
echo "PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
