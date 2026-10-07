#!/usr/bin/env bash
# Cross-repository Runtime proof against a real drive9-server, FUSE mount, and
# Docker process. The exec request is intentionally issued exactly once.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"
RUN_ID="$(date +%s)-$$"
ROOT_REL="runtime-visibility-${RUN_ID}"
ROOT="/${ROOT_REL}"
INBOUND_PATH="${ROOT_REL}/upper/etc/api-inbound.txt"
OUTBOUND_PATH="${ROOT_REL}/upper/workspace/outbound.txt"
INBOUND_PAYLOAD="api-to-exec-${RUN_ID}"
OUTBOUND_PAYLOAD="exec-to-api-${RUN_ID}"
WORK_DIR="$(mktemp -d)"
RAW_MOUNT="$WORK_DIR/raw-mount"
RAW_STATE="$WORK_DIR/raw-state"
RAW_MOUNTED=0

cleanup() {
  if [ "$RAW_MOUNTED" = 1 ]; then
    HOME="$RAW_STATE" XDG_RUNTIME_DIR="$RAW_STATE" \
      "$CLI_BIN" umount --no-auto-pack --timeout 30s "$RAW_MOUNT" \
      >/dev/null 2>&1 || true
  fi
  if [ -n "$API_KEY" ]; then
    curl -sS --max-time 20 -X DELETE \
      -H "Authorization: Bearer ${API_KEY}" \
      "${BASE}/v1/fs/${ROOT_REL}?recursive" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

fail() {
  printf 'FAIL runtime exec visibility: %s\n' "$1" >&2
  if [ -s "$WORK_DIR/exec.stderr" ]; then
    sed -n '1,80p' "$WORK_DIR/exec.stderr" >&2
  fi
  if [ -s "$WORK_DIR/raw-mount.log" ]; then
    sed -n '1,80p' "$WORK_DIR/raw-mount.log" >&2
  fi
  exit 1
}

mount_raw_workspace() {
  mkdir -p "$RAW_MOUNT" "$RAW_STATE"
  chmod 0700 "$RAW_STATE"
  if ! HOME="$RAW_STATE" XDG_RUNTIME_DIR="$RAW_STATE" \
    DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" \
    "$CLI_BIN" mount --no-supervise --no-persist-credentials \
      --mode=fuse --profile=extent --require-extent-xattr-v1 \
      --durability=write-sync --flush-debounce=0 \
      ":$ROOT" "$RAW_MOUNT" >"$WORK_DIR/raw-mount.log" 2>&1; then
    fail "fresh raw-upper diagnostic mount failed"
  fi
  RAW_MOUNTED=1
}

unmount_raw_workspace() {
  HOME="$RAW_STATE" XDG_RUNTIME_DIR="$RAW_STATE" \
    "$CLI_BIN" mount drain --timeout 30s "$RAW_MOUNT" \
    >>"$WORK_DIR/raw-mount.log" 2>&1 \
    || fail "fresh raw-upper diagnostic drain failed"
  HOME="$RAW_STATE" XDG_RUNTIME_DIR="$RAW_STATE" \
    "$CLI_BIN" umount --no-auto-pack --timeout 30s "$RAW_MOUNT" \
    >>"$WORK_DIR/raw-mount.log" 2>&1 \
    || fail "fresh raw-upper diagnostic unmount failed"
  RAW_MOUNTED=0
}

if [ ! -x "$CLI_BIN" ]; then
  fail "DRIVE9_CLI_BIN is not executable"
fi

if [ -z "$API_KEY" ]; then
  provision_json="$(curl -fsS --max-time 30 -X POST "${BASE}/v1/provision")" \
    || fail "tenant provision failed"
  API_KEY="$(printf '%s' "$provision_json" | jq -r '.api_key // empty')"
  [ -n "$API_KEY" ] || fail "tenant provision returned no API key"
fi

deadline=$((SECONDS + POLL_TIMEOUT_S))
while :; do
  status="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/status" | jq -r '.status // empty')" || status=""
  if [ "$status" = "active" ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "tenant did not become active"
  fi
  sleep "$POLL_INTERVAL_S"
done

capabilities="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/runtime/capabilities")" || fail "Runtime capabilities unavailable"
printf '%s' "$capabilities" | jq -e \
  '.streaming == true and .separate_stdout_stderr == true and .cancel == true and .detached == false and .replay == false
   and any(.providers[].candidates[]; .execution_class == "linux-full"
     and .rootfs.capability_version == "drive9_rootfs.user_union.extent.v4"
     and .capabilities["drive9.extent_xattr.v1"] == true
     and .production_eligible == true and .bounded_selection_eligible == true)' \
  >/dev/null || fail "Runtime capabilities do not match the one-shot contract"

curl -fsS --max-time 20 -X POST -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${ROOT_REL}?mkdir" >/dev/null \
  || fail "workspace root creation failed"

# First sandbox: execute a lower-image binary and persist changes across the
# literal Linux root. This invocation also initializes the immutable rootfs
# binding and Drive9 upper/work backing.
if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s -- \
  /bin/sh -c 'set -eu
    test -x /bin/sh
    test "$(stat -c %u:%g:%a /)" = 0:0:755
    test "$(stat -c %u:%g /bin/sh)" = 0:0
    test "$(id -u)" = 65532
    test "$(awk '\''$1 == "CapEff:" { print $2 }'\'' /proc/self/status)" = 0000000000000002
    printf lower-image-ok
    printf root-state > /root/persist.txt
    printf home-state > /home/agent/persist.txt
    printf etc-state > /etc/drive9.conf
    printf workspace-state > /workspace/persist.txt
    printf rename-state > /workspace/rename-from.txt
    mv /workspace/rename-from.txt /workspace/rename-to.txt
    printf delete-state > /workspace/deleted.txt
    rm /workspace/deleted.txt
    rm /etc/drive9-lower-delete
    if chmod 0600 /etc/drive9-lower-rename 2>/dev/null; then exit 1; fi
    mv /etc/drive9-lower-rename /etc/drive9-lower-renamed
    test "$(stat -c %u:%g /etc/drive9-lower-renamed)" = 0:0
    test "$(stat -c %u:%g /etc/drive9.conf)" = 65532:65532
    chmod 0640 /etc/drive9.conf
    ln -s ../etc/drive9.conf /root/config-link
    ln /workspace/persist.txt /workspace/persist-hardlink.txt
    printf transient > /tmp/not-persistent
    printf transient > /run/not-persistent' \
  >"$WORK_DIR/exec.stdout" 2>"$WORK_DIR/exec.stderr"; then
  fail "first rootfs Runtime exec failed"
fi

[ "$(cat "$WORK_DIR/exec.stdout")" = lower-image-ok ] \
  || fail "provider lower-image /bin/sh did not execute"

# Before another union mount can mask where the metadata was lost, open the
# durable workspace with a fresh Drive9 FUSE process and inspect the raw upper
# inode. The HEAD projection and the private fuse-overlayfs xattr must agree on
# the same extent-backed fact after the first Runtime drain/unmount.
RAW_ETC_REL="${ROOT_REL}/upper/etc/drive9.conf"
RAW_ETC_PATH="$RAW_MOUNT/upper/etc/drive9.conf"
raw_head="$(curl -fsSI --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${RAW_ETC_REL}" | tr -d '\r')" \
  || fail "raw-upper HEAD failed"
raw_layout="$(printf '%s\n' "$raw_head" | awk -F': ' 'tolower($1)=="x-dat9-content-layout"{print $2}')"
raw_extent_ino="$(printf '%s\n' "$raw_head" | awk -F': ' 'tolower($1)=="x-dat9-extent-ino"{print $2}')"
[ "$raw_layout" = extent ] \
  || fail "raw-upper layout expected=extent actual=${raw_layout:-<missing>}"
[ -n "$raw_extent_ino" ] \
  || fail "raw-upper extent inode is missing"

mount_raw_workspace
raw_metadata="$(python3 - "$RAW_ETC_PATH" "$raw_layout" "$raw_extent_ino" <<'PY'
import json
import os
import stat
import sys

path, layout, extent_ino = sys.argv[1:]
try:
    current = os.lstat(path)
except OSError as exc:
    print(json.dumps({"error": f"lstat:{exc.errno}", "path": path}, sort_keys=True))
    raise SystemExit(2)
try:
    names = sorted(os.listxattr(path))
except OSError as exc:
    print(json.dumps({"error": f"listxattr:{exc.errno}", "path": path}, sort_keys=True))
    raise SystemExit(3)
try:
    override = os.getxattr(path, b"user.fuseoverlayfs.override_stat").decode("ascii")
except OSError as exc:
    override = f"<getxattr-error:{exc.errno}>"
override_owner = None
override_mode = None
parts = override.split(":")
if len(parts) == 3:
    try:
        override_owner = f"{int(parts[0])}:{int(parts[1])}"
        override_mode = format(int(parts[2], 8) & 0o7777, "o")
    except ValueError:
        pass
print(json.dumps({
    "path": path,
    "layout": layout,
    "extent_ino": extent_ino,
    "stat_inode": current.st_ino,
    "stat_owner": f"{current.st_uid}:{current.st_gid}",
    "stat_mode": format(stat.S_IMODE(current.st_mode), "o"),
    "xattrs": names,
    "override_stat": override,
    "override_owner": override_owner,
    "override_mode": override_mode,
}, sort_keys=True))
PY
)" || fail "raw-upper metadata inspection failed: ${raw_metadata:-<no-output>}"
printf 'runtime raw-upper metadata: %s\n' "$raw_metadata" >&2
raw_override="$(printf '%s' "$raw_metadata" | jq -r '.override_stat // empty')"
raw_override_owner="$(printf '%s' "$raw_metadata" | jq -r '.override_owner // empty')"
raw_override_mode="$(printf '%s' "$raw_metadata" | jq -r '.override_mode // empty')"
[ "$raw_override_owner" = 65532:65532 ] \
  || fail "raw-upper override_stat owner expected=65532:65532 actual=${raw_override:-<missing>}"
[ "$raw_override_mode" = 640 ] \
  || fail "raw-upper override_stat mode expected=640 actual=${raw_override:-<missing>}"
unmount_raw_workspace

# Direction 1: after rootfs initialization, an acknowledged FS API write into
# the durable upper must be observed at the corresponding merged-root path by
# a new sandbox.
printf '%s' "$INBOUND_PAYLOAD" | curl -fsS --max-time 20 -X PUT \
  -H "Authorization: Bearer ${API_KEY}" --data-binary @- \
  "${BASE}/v1/fs/${INBOUND_PATH}" >/dev/null \
  || fail "FS API inbound upper write failed"

if ! DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" exec \
  --workspace "$ROOT" --timeout 30s --env "INBOUND_PAYLOAD=${INBOUND_PAYLOAD}" --env "OUTBOUND_PAYLOAD=${OUTBOUND_PAYLOAD}" -- \
  /bin/sh -c 'set -eu
    fail_check() {
      printf "rootfs assertion failed: %s expected=%s actual=%s\n" "$1" "$2" "$3" >&2
      exit 1
    }
    expect_file() {
      actual="$(cat "$2")" || fail_check "$1:$2" "$3" "<read-error>"
      [ "$actual" = "$3" ] || fail_check "$1:$2" "$3" "$actual"
    }
    expect_stat() {
      actual="$(stat -c "$2" "$3")" || fail_check "$1:$3" "$4" "<stat-error>"
      [ "$actual" = "$4" ] || fail_check "$1:$3" "$4" "$actual"
    }
    expect_absent() {
      [ ! -e "$2" ] || fail_check "$1:$2" "absent" "present"
    }
    expect_stat merged-root %u:%g:%a / 0:0:755
    expect_stat lower-owner %u:%g /bin/sh 0:0
    expect_file api-inbound /etc/api-inbound.txt "$INBOUND_PAYLOAD"
    expect_file root-persist /root/persist.txt root-state
    expect_file home-persist /home/agent/persist.txt home-state
    expect_file etc-persist /etc/drive9.conf etc-state
    expect_file workspace-persist /workspace/persist.txt workspace-state
    expect_file rename-persist /workspace/rename-to.txt rename-state
    expect_absent rename-source /workspace/rename-from.txt
    expect_absent deleted-upper /workspace/deleted.txt
    expect_absent deleted-lower /etc/drive9-lower-delete
    expect_file renamed-lower /etc/drive9-lower-renamed rename-lower-state
    expect_stat renamed-lower-owner %u:%g /etc/drive9-lower-renamed 0:0
    expect_stat etc-owner %u:%g /etc/drive9.conf 65532:65532
    expect_stat etc-mode %a /etc/drive9.conf 640
    actual="$(readlink /root/config-link)" || fail_check "config-link:/root/config-link" "../etc/drive9.conf" "<readlink-error>"
    [ "$actual" = ../etc/drive9.conf ] || fail_check "config-link:/root/config-link" "../etc/drive9.conf" "$actual"
    first_inode="$(stat -c %i /workspace/persist.txt)" || fail_check "hardlink-source:/workspace/persist.txt" "readable-inode" "<stat-error>"
    second_inode="$(stat -c %i /workspace/persist-hardlink.txt)" || fail_check "hardlink-target:/workspace/persist-hardlink.txt" "$first_inode" "<stat-error>"
    [ "$first_inode" = "$second_inode" ] || fail_check "hardlink-inode" "$first_inode" "$second_inode"
    expect_absent tmp-ephemeral /tmp/not-persistent
    expect_absent run-ephemeral /run/not-persistent
    for environment in /proc/[0-9]*/environ; do
      test -r "$environment" || continue
      if tr "\000" "\n" < "$environment" | grep -Eq "^DRIVE9_(API_KEY|SERVER|VAULT_TOKEN)="; then
        fail_check "credential-leak:$environment" "no-drive9-credential" "present"
      fi
    done
    printf "%s" "$OUTBOUND_PAYLOAD" > /workspace/outbound.txt \
      || fail_check "outbound-write:/workspace/outbound.txt" "$OUTBOUND_PAYLOAD" "<write-error>"' \
  >"$WORK_DIR/exec-2.stdout" 2>"$WORK_DIR/exec-2.stderr"; then
  cp "$WORK_DIR/exec-2.stderr" "$WORK_DIR/exec.stderr"
  fail "second rootfs Runtime exec failed"
fi

# Direction 2: a process write followed by the service's mandatory drain and
# unmount must be immediately visible to a fresh Drive9 mount. Extent-backed
# bytes are intentionally not served inline by GET /v1/fs, so HEAD proves the
# durable metadata projection and the independent mount proves exact bytes.
# Do not poll: a successful terminal frame is itself the visibility boundary.
outbound_head="$(curl -fsSI --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${OUTBOUND_PATH}" | tr -d '\r')" \
  || fail "Runtime write metadata was not readable through the FS API"
outbound_layout="$(printf '%s\n' "$outbound_head" | awk -F': ' 'tolower($1)=="x-dat9-content-layout"{print $2}')"
[ "$outbound_layout" = extent ] \
  || fail "Runtime write layout expected=extent actual=${outbound_layout:-<missing>}"

mount_raw_workspace
actual_outbound="$(cat "$RAW_MOUNT/upper/workspace/outbound.txt")" \
  || fail "Runtime write was not readable through a fresh Drive9 mount"
unmount_raw_workspace
[ "$actual_outbound" = "$OUTBOUND_PAYLOAD" ] \
  || fail "Runtime write did not match through the fresh Drive9 mount"

printf 'PASS runtime exec bidirectional workspace visibility\n'
