#!/usr/bin/env bash
# Prove whether a real Drive9 FUSE mount can serve as the durable upper/work
# filesystem for a Linux overlay root. This is deliberately a remount test:
# owner/mode plus ordinary whiteout/opaque sentinel files must reconstruct the
# same merged root from Drive9's native inode and namespace state.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
API_KEY="${DRIVE9_API_KEY:-}"
FUSE_READY_TIMEOUT_S="${FUSE_READY_TIMEOUT_S:-30}"
ROOTFS_IMAGE="${ROOTFS_PROBE_IMAGE:-debian:bookworm-slim}"
ROOTFS_DRIVER="${ROOTFS_OVERLAY_DRIVER:-fuse-overlayfs}"
EXPECT_KERNEL_REJECT="${ROOTFS_EXPECT_KERNEL_REJECT:-0}"
RUN_ID="$(date +%s)-$$"
REMOTE_ROOT="/runtime-rootfs-probe-${RUN_ID}"
WORK_DIR="$(mktemp -d)"
LOWER="$WORK_DIR/lower"
FUSE_ROOT="$WORK_DIR/drive9"
MERGED="$WORK_DIR/merged"
MOUNT_LOG="$WORK_DIR/fuse.log"
OVERLAY_LOG="$WORK_DIR/overlay.log"
FUSE_PID=""
FIRST_FUSE_PID=""
OVERLAY_PID=""
FIRST_OVERLAY_PID=""
OVERLAY_MOUNTED=0
TMP_MOUNTED=0
RUN_MOUNTED=0

assert_native_upper_metadata() {
  local stage="${1:-unknown-stage}"
  local file_mode file_owner dir_mode dir_owner root_mode root_owner
  # Force the directory-entry path before the explicit stats. ReaddirPlus may
  # satisfy a later stat from its EntryOut without issuing Lookup/GetAttr, so
  # it must carry the same native metadata as those paths.
  sudo python3 - "$FUSE_ROOT/upper" <<'PY'
import os
import sys
for root, dirs, files in os.walk(sys.argv[1]):
    with os.scandir(root) as entries:
        for entry in entries:
            entry.stat(follow_symlinks=False)
PY
  file_mode="$(sudo stat -c '%a' "$FUSE_ROOT/upper/etc/drive9.conf")"
  file_owner="$(sudo stat -c '%u:%g' "$FUSE_ROOT/upper/workspace/persist.txt")"
  dir_mode="$(sudo stat -c '%a' "$FUSE_ROOT/upper/home/agent")"
  dir_owner="$(sudo stat -c '%u:%g' "$FUSE_ROOT/upper/home/agent")"
  root_mode="$(sudo stat -c '%a' "$FUSE_ROOT/upper/root")"
  root_owner="$(sudo stat -c '%u:%g' "$FUSE_ROOT/upper/root")"
  [ "$file_mode" = 640 ] \
    || fail "${stage} raw upper existing-file mode is ${file_mode}, want 640"
  [ "$file_owner" = 1234:2345 ] \
    || fail "${stage} raw upper file ownership is ${file_owner}, want 1234:2345"
  [ "$dir_mode" = 750 ] \
    || fail "${stage} raw upper directory mode is ${dir_mode}, want 750"
  [ "$dir_owner" = 3456:4567 ] \
    || fail "${stage} raw upper directory ownership is ${dir_owner}, want 3456:4567"
  [ "$root_mode" = 700 ] \
    || fail "${stage} raw upper /root directory mode is ${root_mode}, want 700"
  [ "$root_owner" = 0:0 ] \
    || fail "${stage} raw upper /root directory ownership is ${root_owner}, want 0:0"
}

report_native_directory_metadata() {
  local stage="${1:-unknown-stage}"
  local head_headers head_extent_ino list_json extent_ino attr_json
  head_headers="$(curl -fsSI --max-time 20 \
    -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/fs${REMOTE_ROOT}/upper/home/agent")" \
    || fail "${stage} could not stat native directory projection"
  head_extent_ino="$(printf '%s\n' "$head_headers" | awk -F': ' \
    'tolower($1) == "x-dat9-extent-ino" {gsub("\r", "", $2); print $2; exit}')"
  [ -n "$head_extent_ino" ] && [ "$head_extent_ino" != 0 ] \
    || fail "${stage} native directory stat has no extent inode"
  list_json="$(curl -fsS --max-time 20 \
    -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/fs${REMOTE_ROOT}/upper/home?list=1")" \
    || fail "${stage} could not list native directory projection"
  extent_ino="$(printf '%s' "$list_json" | jq -r \
    '.entries[] | select(.name == "agent" and .isDir == true) | .extent_ino // empty')"
  [ -n "$extent_ino" ] && [ "$extent_ino" != 0 ] \
    || fail "${stage} native directory projection has no extent inode"
  [ "$extent_ino" = "$head_extent_ino" ] \
    || fail "${stage} native directory stat/list inode mismatch: ${head_extent_ino} != ${extent_ino}"
  attr_json="$(curl -fsS --max-time 20 -X POST \
    -H "Authorization: Bearer ${API_KEY}" \
    -H 'Content-Type: application/json' \
    -H 'X-Drive9-Extent-Op: getattr' \
    --data "{\"inode\":${extent_ino}}" \
    "${BASE}/v1/extent/meta")" \
    || fail "${stage} could not read native directory inode"
  printf 'rootfs probe native directory %s: inode=%s owner=%s:%s mode=%s type=%s\n' \
    "$stage" \
    "$extent_ino" \
    "$(printf '%s' "$attr_json" | jq -r '.attr.Uid // "missing"')" \
    "$(printf '%s' "$attr_json" | jq -r '.attr.Gid // "missing"')" \
    "$(printf '%s' "$attr_json" | jq -r '.attr.Mode // "missing"')" \
    "$(printf '%s' "$attr_json" | jq -r '.attr.Typ // "missing"')"
}

assert_merged_metadata() {
  local file_mode file_owner dir_mode dir_owner
  file_mode="$(sudo stat -c '%a' "$MERGED/etc/drive9.conf")"
  file_owner="$(sudo stat -c '%u:%g' "$MERGED/workspace/persist.txt")"
  dir_mode="$(sudo stat -c '%a' "$MERGED/home/agent")"
  dir_owner="$(sudo stat -c '%u:%g' "$MERGED/home/agent")"
  [ "$file_mode" = 640 ] \
    || fail "merged file mode is ${file_mode}, want 640"
  [ "$file_owner" = 1234:2345 ] \
    || fail "merged file ownership is ${file_owner}, want 1234:2345"
  [ "$dir_mode" = 750 ] \
    || fail "merged directory mode is ${dir_mode}, want 750"
  [ "$dir_owner" = 3456:4567 ] \
    || fail "merged directory ownership is ${dir_owner}, want 3456:4567"
}

assert_regular_overlay_markers() {
  [ -f "$FUSE_ROOT/upper/etc/.wh.lower-delete" ] \
    || fail "lower-file whiteout is not a regular .wh.<name> file"
  [ -f "$FUSE_ROOT/upper/etc/.wh.lower-rename" ] \
    || fail "renamed lower-file whiteout is not a regular .wh.<name> file"
  [ -f "$FUSE_ROOT/upper/etc/opaque/.wh..wh..opq" ] \
    || fail "opaque-directory marker is not a regular .wh..wh..opq file"
}

assert_no_private_overlay_xattrs() {
  sudo python3 - \
    "$FUSE_ROOT/upper" \
    "$FUSE_ROOT/upper/etc/drive9.conf" \
    "$FUSE_ROOT/upper/etc/.wh.lower-delete" \
    "$FUSE_ROOT/upper/etc/opaque/.wh..wh..opq" <<'PY'
import errno
import os
import sys

for path in sys.argv[1:]:
    try:
        names = os.listxattr(path)
    except OSError as exc:
        if exc.errno in (errno.ENOTSUP, getattr(errno, "EOPNOTSUPP", errno.ENOTSUP)):
            continue
        raise
    forbidden = [
        name for name in names
        if name.startswith("user.fuseoverlayfs.")
        or name.startswith("security.fuseoverlayfs.")
        or name.startswith("trusted.overlay.")
        or name.startswith("user.overlay.")
        or name == "user.containers.override_stat"
    ]
    if forbidden:
        raise AssertionError(f"private overlay xattr on {path}: {forbidden}")
PY
}

fail() {
  printf 'FAIL runtime rootfs overlay probe: %s\n' "$1" >&2
  if [ -s "$MOUNT_LOG" ]; then
    sed -n '1,120p' "$MOUNT_LOG" >&2
  fi
  if [ -s "$OVERLAY_LOG" ]; then
    sed -n '1,120p' "$OVERLAY_LOG" >&2
  fi
  exit 1
}

run_cli() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

unmount_overlay() {
  local best_effort="${1:-0}"
  if [ "$RUN_MOUNTED" = 1 ]; then
    if ! sudo umount "$MERGED/run" >/dev/null 2>&1 && [ "$best_effort" != 1 ]; then
      return 1
    fi
    RUN_MOUNTED=0
  fi
  if [ "$TMP_MOUNTED" = 1 ]; then
    if ! sudo umount "$MERGED/tmp" >/dev/null 2>&1 && [ "$best_effort" != 1 ]; then
      return 1
    fi
    TMP_MOUNTED=0
  fi
  if [ "$OVERLAY_MOUNTED" = 1 ]; then
    if ! sudo umount "$MERGED" >/dev/null 2>&1 && [ "$best_effort" != 1 ]; then
      return 1
    fi
    OVERLAY_MOUNTED=0
  fi
  if [ -n "$OVERLAY_PID" ]; then
    if [ "$best_effort" = 1 ] && kill -0 "$OVERLAY_PID" 2>/dev/null; then
      kill "$OVERLAY_PID" >/dev/null 2>&1 || true
    fi
    if ! wait "$OVERLAY_PID" && [ "$best_effort" != 1 ]; then
      return 1
    fi
    OVERLAY_PID=""
  fi
  if mountpoint -q "$MERGED"; then
    [ "$best_effort" = 1 ] || return 1
  fi
}

unmount_drive9() {
  if mountpoint -q "$FUSE_ROOT"; then
    run_cli umount --timeout 30s "$FUSE_ROOT" >/dev/null
  fi
  if [ -n "$FUSE_PID" ]; then
    wait "$FUSE_PID"
    FUSE_PID=""
  fi
  ! mountpoint -q "$FUSE_ROOT"
}

cleanup() {
  unmount_overlay 1
  unmount_drive9 >/dev/null 2>&1 || true
  if [ -n "$API_KEY" ]; then
    curl -sS --max-time 20 -X DELETE \
      -H "Authorization: Bearer ${API_KEY}" \
      "${BASE}/v1/fs/${REMOTE_ROOT#/}?recursive=true" >/dev/null 2>&1 || true
  fi
  sudo rm -rf -- "$WORK_DIR"
}
trap cleanup EXIT

wait_for_mount() {
  local deadline=$((SECONDS + FUSE_READY_TIMEOUT_S))
  while ! mountpoint -q "$FUSE_ROOT"; do
    if ! kill -0 "$FUSE_PID" 2>/dev/null; then
      wait "$FUSE_PID" || true
      fail "Drive9 FUSE mount exited before becoming ready"
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      fail "Drive9 FUSE mount did not become ready"
    fi
    sleep 0.2
  done
}

mount_drive9() {
  : >"$MOUNT_LOG"
  run_cli mount --mode=fuse --foreground --no-supervise \
    --allow-other \
    --profile=extent \
    --durability=write-sync --flush-debounce=0 \
    ":$REMOTE_ROOT" "$FUSE_ROOT" >>"$MOUNT_LOG" 2>&1 &
  FUSE_PID=$!
  wait_for_mount
}

mount_overlay() {
  mkdir -p "$FUSE_ROOT/upper" "$FUSE_ROOT/work" "$MERGED"
  case "$ROOTFS_DRIVER" in
    kernel)
      if ! sudo mount -t overlay overlay \
        -o "lowerdir=$LOWER,upperdir=$FUSE_ROOT/upper,workdir=$FUSE_ROOT/work,userxattr" \
        "$MERGED"; then
        if [ "$EXPECT_KERNEL_REJECT" = 1 ]; then
          printf 'PASS Linux kernel rejected Drive9 FUSE as an overlay upper/work filesystem\n'
          exit 0
        fi
        fail "kernel rejected Drive9 FUSE as an overlay upper/work filesystem"
      fi
      if [ "$EXPECT_KERNEL_REJECT" = 1 ]; then
        fail "kernel unexpectedly accepted Drive9 FUSE as an overlay upper/work filesystem"
      fi
      ;;
    fuse-overlayfs)
      : >"$OVERLAY_LOG"
      local overlay_options="allow_other,lowerdir=$LOWER,upperdir=$FUSE_ROOT/upper,workdir=$FUSE_ROOT/work"
      case ",$overlay_options," in
        *,xattr_permissions,*|*,xattr_permissions=*)
          fail "fuse-overlayfs options must use native inode permissions"
          ;;
      esac
      sudo env FUSE_OVERLAYFS_DISABLE_OVL_WHITEOUT=1 fuse-overlayfs -f \
        -o "$overlay_options" \
        "$MERGED" >>"$OVERLAY_LOG" 2>&1 &
      OVERLAY_PID=$!
      local deadline=$((SECONDS + FUSE_READY_TIMEOUT_S))
      while ! mountpoint -q "$MERGED"; do
        if ! kill -0 "$OVERLAY_PID" 2>/dev/null; then
          wait "$OVERLAY_PID" || true
          OVERLAY_PID=""
          fail "fuse-overlayfs exited before becoming ready"
        fi
        if [ "$SECONDS" -ge "$deadline" ]; then
          fail "fuse-overlayfs did not become ready"
        fi
        sleep 0.2
      done
      ;;
    *)
      fail "unsupported rootfs overlay driver: $ROOTFS_DRIVER"
      ;;
  esac
  OVERLAY_MOUNTED=1
  if ! sudo mount -t tmpfs -o mode=1777,nosuid,nodev tmpfs "$MERGED/tmp"; then
    fail "failed to mount ephemeral /tmp inside the merged root"
  fi
  TMP_MOUNTED=1
  if ! sudo mount -t tmpfs -o mode=755,nosuid,nodev tmpfs "$MERGED/run"; then
    fail "failed to mount ephemeral /run inside the merged root"
  fi
  RUN_MOUNTED=1
}

if [ "$(uname -s)" != Linux ]; then
  fail "probe requires Linux"
fi
case "$EXPECT_KERNEL_REJECT" in
  0) ;;
  1)
    [ "$ROOTFS_DRIVER" = kernel ] \
      || fail "ROOTFS_EXPECT_KERNEL_REJECT=1 requires ROOTFS_OVERLAY_DRIVER=kernel"
    ;;
  *) fail "ROOTFS_EXPECT_KERNEL_REJECT must be 0 or 1" ;;
esac
for command in curl docker file jq mount mountpoint python3 sudo tar umount; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is missing: $command"
done
if [ "$ROOTFS_DRIVER" = fuse-overlayfs ]; then
  command -v fuse-overlayfs >/dev/null 2>&1 || fail "required command is missing: fuse-overlayfs"
fi
[ -x "$CLI_BIN" ] || fail "DRIVE9_CLI_BIN is not executable"

if [ -z "$API_KEY" ]; then
  provision_json="$(curl -fsS --max-time 30 -X POST "${BASE}/v1/provision")" \
    || fail "tenant provision failed"
  API_KEY="$(printf '%s' "$provision_json" | jq -r '.api_key // empty')"
  [ -n "$API_KEY" ] || fail "tenant provision returned no API key"
fi

deadline=$((SECONDS + 120))
while :; do
  status="$(curl -fsS --max-time 20 -H "Authorization: Bearer ${API_KEY}" \
    "${BASE}/v1/status" | jq -r '.status // empty')" || status=""
  [ "$status" = active ] && break
  [ "$SECONDS" -lt "$deadline" ] || fail "tenant did not become active"
  sleep 2
done

run_cli fs mkdir ":$REMOTE_ROOT" >/dev/null \
  || fail "failed to create the disposable Drive9 root"

mkdir -p "$LOWER" "$FUSE_ROOT" "$MERGED"
docker pull "$ROOTFS_IMAGE" >/dev/null
container_id="$(docker create "$ROOTFS_IMAGE")"
docker export "$container_id" | tar -C "$LOWER" -xf -
docker rm "$container_id" >/dev/null
file -Lb "$LOWER/bin/sh" | grep -q 'dynamically linked' \
  || fail "probe lower /bin/sh is not dynamically linked"

# Deterministic lower-only entries exercise copy-up, whiteout, opaque-dir, and
# rename behavior independently of the chosen base image contents.
mkdir -p "$LOWER/etc/opaque" "$LOWER/workspace" "$LOWER/home/agent" "$LOWER/root" "$LOWER/tmp" "$LOWER/run"
sudo chown 0:0 "$LOWER/root"
sudo chmod 0700 "$LOWER/root"
printf 'delete-me\n' >"$LOWER/etc/lower-delete"
printf 'rename-me\n' >"$LOWER/etc/lower-rename"
printf 'hidden-child\n' >"$LOWER/etc/opaque/lower-child"

mount_drive9
FIRST_FUSE_PID="$FUSE_PID"

mount_overlay
FIRST_OVERLAY_PID="$OVERLAY_PID"

sudo chroot "$MERGED" /bin/sh -c 'test -x /bin/sh && printf lower-image-ok' \
  | grep -qx 'lower-image-ok' || fail "lower image binary did not execute from merged root"
sudo sh -c "printf root-state >'$MERGED/root/persist.txt'"
sudo sh -c "printf home-state >'$MERGED/home/agent/persist.txt'"
sudo sh -c "printf etc-state >'$MERGED/etc/drive9.conf'"
sudo sh -c "printf workspace-state >'$MERGED/workspace/persist.txt'"
sudo rm "$MERGED/etc/lower-delete"
sudo mv "$MERGED/etc/lower-rename" "$MERGED/etc/lower-renamed"
sudo rm -rf "$MERGED/etc/opaque"
sudo mkdir "$MERGED/etc/opaque"
sudo sh -c "printf upper-child >'$MERGED/etc/opaque/upper-child'"
sudo chmod 0640 "$MERGED/etc/drive9.conf"
sudo chown 1234:2345 "$MERGED/workspace/persist.txt"
sudo chmod 0750 "$MERGED/home/agent"
sudo chown 3456:4567 "$MERGED/home/agent"
sudo ln -s ../etc/drive9.conf "$MERGED/root/config-link"
sudo ln "$MERGED/workspace/persist.txt" "$MERGED/workspace/persist-hardlink.txt"
sudo touch -t 202001020304.05 "$MERGED/etc/drive9.conf"
sudo dd if=/dev/zero of="$MERGED/workspace/large.bin" bs=1M count=4 status=none
sudo mv "$MERGED/workspace/persist-hardlink.txt" "$MERGED/workspace/renamed-hardlink.txt"
sudo sh -c "printf transient-tmp >'$MERGED/tmp/not-persistent'"
sudo sh -c "printf transient-run >'$MERGED/run/not-persistent'"
sync
assert_merged_metadata
report_native_directory_metadata after-chown

unmount_overlay || fail "$ROOTFS_DRIVER did not stop cleanly"
unmount_overlay || fail "$ROOTFS_DRIVER teardown was not idempotent"
if [ -n "$FIRST_OVERLAY_PID" ] && kill -0 "$FIRST_OVERLAY_PID" 2>/dev/null; then
  fail "first fuse-overlayfs process is still alive after unmount"
fi
mountpoint -q "$FUSE_ROOT" || fail "Drive9 mount disappeared before drain"
assert_native_upper_metadata same-mount
report_native_directory_metadata after-overlay-unmount
assert_regular_overlay_markers
assert_no_private_overlay_xattrs \
  || fail "raw upper depends on private overlay xattrs"
run_cli mount drain --timeout 30s "$FUSE_ROOT" >/dev/null \
  || fail "Drive9 drain failed after rootfs mutation"
unmount_drive9 || fail "first Drive9 FUSE mount did not stop cleanly"
unmount_drive9 || fail "Drive9 FUSE teardown was not idempotent"
if kill -0 "$FIRST_FUSE_PID" 2>/dev/null; then
  fail "first Drive9 FUSE process is still alive after unmount"
fi

# Recreate both the FUSE mount and overlay mount. No session-local metadata may
# be needed to reconstruct the merged root.
mount_drive9
[ "$FUSE_PID" != "$FIRST_FUSE_PID" ] || fail "Drive9 remount reused the old FUSE process"
report_native_directory_metadata fresh-remount
assert_native_upper_metadata fresh-remount
assert_regular_overlay_markers
assert_no_private_overlay_xattrs \
  || fail "remounted raw upper depends on private overlay xattrs"
mount_overlay
[ -z "$FIRST_OVERLAY_PID" ] || [ "$OVERLAY_PID" != "$FIRST_OVERLAY_PID" ] \
  || fail "rootfs remount reused the old fuse-overlayfs process"

sudo chroot "$MERGED" /bin/sh -c 'test -x /bin/sh && printf lower-image-ok' \
  | grep -qx 'lower-image-ok' || fail "lower image binary failed after Drive9 remount"
[ "$(sudo cat "$MERGED/root/persist.txt")" = root-state ] || fail "/root write did not persist"
[ "$(sudo cat "$MERGED/home/agent/persist.txt")" = home-state ] || fail "/home write did not persist"
[ "$(sudo cat "$MERGED/etc/drive9.conf")" = etc-state ] || fail "/etc write did not persist"
[ "$(sudo cat "$MERGED/workspace/persist.txt")" = workspace-state ] || fail "/workspace write did not persist"
[ ! -e "$MERGED/etc/lower-delete" ] || fail "lower-file whiteout did not persist"
[ "$(sudo cat "$MERGED/etc/lower-renamed")" = rename-me ] || fail "lower-file rename did not persist"
[ ! -e "$MERGED/etc/opaque/lower-child" ] || fail "opaque-directory marker did not persist"
[ "$(sudo cat "$MERGED/etc/opaque/upper-child")" = upper-child ] || fail "opaque-directory upper child did not persist"
[ "$(stat -c '%a' "$MERGED/etc/drive9.conf")" = 640 ] || fail "chmod did not persist"
[ "$(stat -c '%u:%g' "$MERGED/workspace/persist.txt")" = 1234:2345 ] || fail "chown uid/gid did not persist"
[ "$(stat -c '%a' "$MERGED/home/agent")" = 750 ] || fail "directory chmod did not persist"
[ "$(stat -c '%u:%g' "$MERGED/home/agent")" = 3456:4567 ] || fail "directory chown did not persist"
[ "$(readlink "$MERGED/root/config-link")" = ../etc/drive9.conf ] || fail "symlink did not persist"
[ "$(stat -c '%i' "$MERGED/workspace/persist.txt")" = "$(stat -c '%i' "$MERGED/workspace/renamed-hardlink.txt")" ] \
  || fail "hardlink identity did not persist"
[ "$(stat -c '%y' "$MERGED/etc/drive9.conf" | cut -d. -f1)" = '2020-01-02 03:04:05' ] \
  || fail "mtime did not persist"
[ "$(stat -c '%s' "$MERGED/workspace/large.bin")" = 4194304 ] || fail "large file did not persist"
[ ! -e "$MERGED/tmp/not-persistent" ] || fail "/tmp unexpectedly persisted"
[ ! -e "$MERGED/run/not-persistent" ] || fail "/run unexpectedly persisted"

printf 'PASS Drive9 FUSE is a durable Linux %s upper/work filesystem\n' "$ROOTFS_DRIVER"
