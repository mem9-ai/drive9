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

run_privileged() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
    return
  fi
  sudo -n "$@"
}

mount_raw_workspace() {
  mkdir -p "$RAW_MOUNT" "$RAW_STATE"
  chmod 0700 "$RAW_STATE"
  # Permit the root-only diagnostic below to cross the FUSE mount boundary;
  # default_permissions still enforces the restored native uid/gid/mode.
  if ! HOME="$RAW_STATE" XDG_RUNTIME_DIR="$RAW_STATE" \
    DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" \
    "$CLI_BIN" mount --no-supervise --no-persist-credentials \
      --mode=fuse --profile=extent --allow-other \
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

raw_metadata_for() {
  run_privileged python3 - "$1" "$2" "$3" <<'PY'
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
print(json.dumps({
    "path": path,
    "layout": layout,
    "extent_ino": extent_ino,
    "stat_inode": current.st_ino,
    "stat_owner": f"{current.st_uid}:{current.st_gid}",
    "stat_mode": format(stat.S_IMODE(current.st_mode), "o"),
}, sort_keys=True))
PY
}

report_initial_failure_state() {
  mount_raw_workspace
  for relative_path in upper upper/root upper/root/persist.txt; do
    path="$RAW_MOUNT/$relative_path"
    if [ ! -e "$path" ]; then
      printf 'runtime initial-failure raw metadata: {"path":"%s","state":"absent"}\n' \
        "$relative_path" >&2
      continue
    fi
    metadata="$(raw_metadata_for "$path" diagnostic "")" \
      || fail "initial-failure raw metadata inspection failed: ${metadata:-<no-output>}"
    printf 'runtime initial-failure raw metadata: %s\n' "$metadata" >&2
  done
  unmount_raw_workspace
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
     and .persistence == "full_root"
     and .rootfs.capability_version == "drive9_rootfs.user_union.extent.v6"
     and .capabilities["workspace_persistence"] == true
     and .capabilities["full_root_persistence"] == true
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
    fail_check() {
      printf "rootfs initialization failed: %s expected=%s actual=%s\n" "$1" "$2" "$3" >&2
      exit 1
    }
    expect_stat() {
      actual="$(stat -c "$2" "$3")" || fail_check "$1:$3" "$4" "<stat-error>"
      [ "$actual" = "$4" ] || fail_check "$1:$3" "$4" "$actual"
    }
    [ -x /bin/sh ] || fail_check lower-shell executable not-executable
    expect_stat merged-root %u:%g:%a / 0:0:755
    expect_stat root-directory %u:%g:%a /root 0:0:700
    expect_stat lower-owner %u:%g /bin/sh 0:0
    uid="$(id -u)"
    [ "$uid" = 65532 ] || fail_check process-uid 65532 "$uid"
    capability="$(awk '\''$1 == "CapEff:" { print $2 }'\'' /proc/self/status)"
    [ "$capability" = 0000000000000002 ] \
      || fail_check process-capability 0000000000000002 "$capability"
    root_mount="$(awk '\''$5 == "/" { print }'\'' /proc/self/mountinfo | tail -1)"
    printf "runtime initial-root mount: %s uid=%s cap_eff=%s root_stat=%s\n" \
      "$root_mount" "$uid" "$capability" "$(stat -c %u:%g:%a /root)" >&2
    printf "%s" "$root_mount" | grep -q " - fuse.fuse-overlayfs " \
      || fail_check rootfs-mount-type fuse.fuse-overlayfs "$root_mount"
    printf lower-image-ok
    printf root-state > /root/persist.txt \
      || fail_check root-write writable failed
    printf home-state > /home/agent/persist.txt \
      || fail_check home-write writable failed
    printf etc-state > /etc/drive9.conf \
      || fail_check etc-write writable failed
    printf workspace-state > /workspace/persist.txt \
      || fail_check workspace-write writable failed
    printf rename-state > /workspace/rename-from.txt \
      || fail_check rename-source-write writable failed
    mv /workspace/rename-from.txt /workspace/rename-to.txt \
      || fail_check upper-rename success failed
    printf delete-state > /workspace/deleted.txt \
      || fail_check delete-source-write writable failed
    rm /workspace/deleted.txt \
      || fail_check upper-delete success failed
    rm /etc/drive9-lower-delete \
      || fail_check lower-delete success failed
    if chmod 0600 /etc/drive9-lower-rename 2>/dev/null; then
      fail_check lower-chmod-rejection rejected succeeded
    fi
    mv /etc/drive9-lower-rename /etc/drive9-lower-renamed \
      || fail_check lower-rename success failed
    expect_stat renamed-lower-owner %u:%g /etc/drive9-lower-renamed 0:0
    expect_stat etc-owner %u:%g /etc/drive9.conf 65532:65532
    chmod 0640 /etc/drive9.conf \
      || fail_check etc-chmod success failed
    ln -s ../etc/drive9.conf /root/config-link \
      || fail_check root-symlink success failed
    ln /workspace/persist.txt /workspace/persist-hardlink.txt \
      || fail_check workspace-hardlink success failed
    printf transient > /tmp/not-persistent \
      || fail_check tmp-write writable failed
    printf transient > /run/not-persistent \
      || fail_check run-write writable failed' \
  >"$WORK_DIR/exec.stdout" 2>"$WORK_DIR/exec.stderr"; then
  report_initial_failure_state
  fail "first rootfs Runtime exec failed"
fi

[ "$(cat "$WORK_DIR/exec.stdout")" = lower-image-ok ] \
  || fail "provider lower-image /bin/sh did not execute"

# Before another union mount can mask where the metadata was lost, open the
# durable workspace with a fresh Drive9 FUSE process and inspect the raw upper
# inode. The HEAD projection and native uid/gid/mode must agree on the same
# extent-backed fact after the first Runtime drain/unmount.
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
raw_metadata="$(raw_metadata_for "$RAW_ETC_PATH" "$raw_layout" "$raw_extent_ino")" \
  || fail "raw-upper metadata inspection failed: ${raw_metadata:-<no-output>}"
printf 'runtime raw-upper metadata: %s\n' "$raw_metadata" >&2
[ "$(printf '%s' "$raw_metadata" | jq -r '.stat_owner')" = 65532:65532 ] \
  || fail "raw-upper native owner is not 65532:65532"
[ "$(printf '%s' "$raw_metadata" | jq -r '.stat_mode')" = 640 ] \
  || fail "raw-upper native mode is not 640"

RAW_ROOT_FILE_REL="${ROOT_REL}/upper/root/persist.txt"
RAW_ROOT_FILE_PATH="$RAW_MOUNT/upper/root/persist.txt"
raw_root_head="$(curl -fsSI --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
  "${BASE}/v1/fs/${RAW_ROOT_FILE_REL}" | tr -d '\r')" \
  || fail "raw-upper root file HEAD failed"
raw_root_layout="$(printf '%s\n' "$raw_root_head" | awk -F': ' 'tolower($1)=="x-dat9-content-layout"{print $2}')"
raw_root_extent_ino="$(printf '%s\n' "$raw_root_head" | awk -F': ' 'tolower($1)=="x-dat9-extent-ino"{print $2}')"
[ "$raw_root_layout" = extent ] \
  || fail "raw-upper root file layout expected=extent actual=${raw_root_layout:-<missing>}"
[ -n "$raw_root_extent_ino" ] \
  || fail "raw-upper root file extent inode is missing"
raw_root_directory_metadata="$(raw_metadata_for "$RAW_MOUNT/upper/root" directory "")" \
  || fail "raw-upper root directory metadata inspection failed: ${raw_root_directory_metadata:-<no-output>}"
raw_root_file_metadata="$(raw_metadata_for "$RAW_ROOT_FILE_PATH" "$raw_root_layout" "$raw_root_extent_ino")" \
  || fail "raw-upper root file metadata inspection failed: ${raw_root_file_metadata:-<no-output>}"
printf 'runtime raw-upper root directory metadata: %s\n' "$raw_root_directory_metadata" >&2
printf 'runtime raw-upper root file metadata: %s\n' "$raw_root_file_metadata" >&2
[ "$(printf '%s' "$raw_root_directory_metadata" | jq -r '.stat_owner')" = 0:0 ] \
  || fail "raw-upper root directory native owner is not 0:0"
[ "$(printf '%s' "$raw_root_directory_metadata" | jq -r '.stat_mode')" = 700 ] \
  || fail "raw-upper root directory native mode is not 700"
[ "$(printf '%s' "$raw_root_file_metadata" | jq -r '.stat_owner')" = 65532:65532 ] \
  || fail "raw-upper root file native owner is not 65532:65532"
[ "$(printf '%s' "$raw_root_file_metadata" | jq -r '.stat_mode')" = 644 ] \
  || fail "raw-upper root file native mode is not 644"
[ "$(run_privileged cat "$RAW_ROOT_FILE_PATH")" = root-state ] \
  || fail "raw-upper root file bytes are not durable"
[ -f "$RAW_MOUNT/upper/etc/.wh.drive9-lower-delete" ] \
  || fail "lower-file deletion is not a regular .wh file"
[ -f "$RAW_MOUNT/upper/etc/.wh.drive9-lower-rename" ] \
  || fail "lower-file rename source is not a regular .wh file"
# The persisted /root mode is intentionally 0700/root:root. Inspect bytes and
# xattrs as root so this diagnostic proves durability without weakening the
# permission contract it just verified.
run_privileged python3 - "$RAW_MOUNT/upper" "$RAW_ETC_PATH" "$RAW_ROOT_FILE_PATH" <<'PY' \
  || fail "raw upper unexpectedly depends on overlay private xattrs"
import os
import sys
for path in sys.argv[1:]:
    forbidden = [name for name in os.listxattr(path)
                 if name.startswith("user.fuseoverlayfs.")
                 or name.startswith("security.fuseoverlayfs.")
                 or name.startswith("trusted.overlay.")
                 or name.startswith("user.overlay.")
                 or name == "user.containers.override_stat"]
    assert not forbidden, (path, forbidden)
PY
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
    expect_stat root-directory %u:%g:%a /root 0:0:700
    expect_stat lower-owner %u:%g /bin/sh 0:0
    capability="$(awk '\''$1 == "CapEff:" { print $2 }'\'' /proc/self/status)"
    [ "$capability" = 0000000000000002 ] \
      || fail_check process-capability 0000000000000002 "$capability"
    root_mount="$(awk '\''$5 == "/" { print }'\'' /proc/self/mountinfo | tail -1)"
    printf "runtime fresh-root mount: %s uid=%s cap_eff=%s root_stat=%s\n" \
      "$root_mount" "$(id -u)" "$capability" "$(stat -c %u:%g:%a /root)" >&2
    printf "%s" "$root_mount" | grep -q " - fuse.fuse-overlayfs " \
      || fail_check rootfs-mount-type fuse.fuse-overlayfs "$root_mount"
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
