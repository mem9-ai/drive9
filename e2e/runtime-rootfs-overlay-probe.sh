#!/usr/bin/env bash
# Prove whether a real Drive9 FUSE mount can serve as the durable upper/work
# filesystem for a Linux overlay root. This is deliberately a remount test:
# an overlay that works only while FUSE keeps whiteout/opaque xattrs in memory
# is not a durable sandbox rootfs.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
CLI_BIN="${DRIVE9_CLI_BIN:-./bin/drive9}"
API_KEY="${DRIVE9_API_KEY:-}"
FUSE_READY_TIMEOUT_S="${FUSE_READY_TIMEOUT_S:-30}"
ROOTFS_IMAGE="${ROOTFS_PROBE_IMAGE:-busybox:1.36.1}"
RUN_ID="$(date +%s)-$$"
REMOTE_ROOT="/runtime-rootfs-probe-${RUN_ID}"
WORK_DIR="$(mktemp -d)"
LOWER="$WORK_DIR/lower"
FUSE_ROOT="$WORK_DIR/drive9"
MERGED="$WORK_DIR/merged"
MOUNT_LOG="$WORK_DIR/fuse.log"
FUSE_PID=""
FIRST_FUSE_PID=""
OVERLAY_MOUNTED=0
TMP_MOUNTED=0
RUN_MOUNTED=0

fail() {
  printf 'FAIL runtime rootfs overlay probe: %s\n' "$1" >&2
  if [ -s "$MOUNT_LOG" ]; then
    sed -n '1,120p' "$MOUNT_LOG" >&2
  fi
  exit 1
}

run_cli() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

unmount_overlay() {
  if [ "$RUN_MOUNTED" = 1 ]; then
    sudo umount "$MERGED/run" >/dev/null 2>&1 || true
    RUN_MOUNTED=0
  fi
  if [ "$TMP_MOUNTED" = 1 ]; then
    sudo umount "$MERGED/tmp" >/dev/null 2>&1 || true
    TMP_MOUNTED=0
  fi
  if [ "$OVERLAY_MOUNTED" = 1 ]; then
    sudo umount "$MERGED" >/dev/null 2>&1 || true
    OVERLAY_MOUNTED=0
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
  unmount_overlay
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
    --profile=none --durability=write-sync --flush-debounce=0 \
    ":$REMOTE_ROOT" "$FUSE_ROOT" >>"$MOUNT_LOG" 2>&1 &
  FUSE_PID=$!
  wait_for_mount
}

mount_overlay() {
  mkdir -p "$FUSE_ROOT/upper" "$FUSE_ROOT/work" "$MERGED"
  if ! sudo mount -t overlay overlay \
    -o "lowerdir=$LOWER,upperdir=$FUSE_ROOT/upper,workdir=$FUSE_ROOT/work,userxattr" \
    "$MERGED"; then
    fail "kernel rejected Drive9 FUSE as an overlay upper/work filesystem"
  fi
  OVERLAY_MOUNTED=1
  sudo mount -t tmpfs -o mode=1777,nosuid,nodev tmpfs "$MERGED/tmp"
  TMP_MOUNTED=1
  sudo mount -t tmpfs -o mode=755,nosuid,nodev tmpfs "$MERGED/run"
  RUN_MOUNTED=1
}

if [ "$(uname -s)" != Linux ]; then
  fail "probe requires Linux"
fi
for command in curl docker jq mount mountpoint python3 sudo tar umount; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is missing: $command"
done
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

# Deterministic lower-only entries exercise copy-up, whiteout, opaque-dir, and
# rename behavior independently of the chosen base image contents.
mkdir -p "$LOWER/etc/opaque" "$LOWER/workspace" "$LOWER/home/agent" "$LOWER/root" "$LOWER/tmp" "$LOWER/run"
printf 'delete-me\n' >"$LOWER/etc/lower-delete"
printf 'rename-me\n' >"$LOWER/etc/lower-rename"
printf 'hidden-child\n' >"$LOWER/etc/opaque/lower-child"

mount_drive9
FIRST_FUSE_PID="$FUSE_PID"
mount_overlay

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
sudo ln -s ../etc/drive9.conf "$MERGED/root/config-link"
sudo ln "$MERGED/workspace/persist.txt" "$MERGED/workspace/persist-hardlink.txt"
sudo touch -t 202001020304.05 "$MERGED/etc/drive9.conf"
sudo dd if=/dev/zero of="$MERGED/workspace/large.bin" bs=1M count=4 status=none
sudo python3 - "$MERGED/etc/drive9.conf" <<'PY'
import os
import sys
os.setxattr(sys.argv[1], b"user.drive9.probe", b"durable")
PY
sudo sh -c "printf transient-tmp >'$MERGED/tmp/not-persistent'"
sudo sh -c "printf transient-run >'$MERGED/run/not-persistent'"
sync

unmount_overlay
run_cli fs drain-file-system --mount-path "$FUSE_ROOT" --timeout 30s >/dev/null \
  || fail "Drive9 drain failed after rootfs mutation"
unmount_drive9 || fail "first Drive9 FUSE mount did not stop cleanly"
if kill -0 "$FIRST_FUSE_PID" 2>/dev/null; then
  fail "first Drive9 FUSE process is still alive after unmount"
fi

# Recreate both the FUSE mount and overlay mount. No session-local metadata may
# be needed to reconstruct the merged root.
mount_drive9
[ "$FUSE_PID" != "$FIRST_FUSE_PID" ] || fail "Drive9 remount reused the old FUSE process"
sudo python3 - "$FUSE_ROOT/upper/etc/drive9.conf" <<'PY' \
  || fail "xattr was absent on the fresh Drive9 FUSE mount before overlay reconstruction"
import os
import sys
assert os.getxattr(sys.argv[1], b"user.drive9.probe") == b"durable"
PY
mount_overlay

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
[ "$(readlink "$MERGED/root/config-link")" = ../etc/drive9.conf ] || fail "symlink did not persist"
[ "$(stat -c '%i' "$MERGED/workspace/persist.txt")" = "$(stat -c '%i' "$MERGED/workspace/persist-hardlink.txt")" ] \
  || fail "hardlink identity did not persist"
[ "$(stat -c '%y' "$MERGED/etc/drive9.conf" | cut -d. -f1)" = '2020-01-02 03:04:05' ] \
  || fail "mtime did not persist"
[ "$(stat -c '%s' "$MERGED/workspace/large.bin")" = 4194304 ] || fail "large file did not persist"
sudo python3 - "$MERGED/etc/drive9.conf" <<'PY' \
  || fail "xattr did not persist"
import os
import sys
assert os.getxattr(sys.argv[1], b"user.drive9.probe") == b"durable"
PY
[ ! -e "$MERGED/tmp/not-persistent" ] || fail "/tmp unexpectedly persisted"
[ ! -e "$MERGED/run/not-persistent" ] || fail "/run unexpectedly persisted"

printf 'PASS Drive9 FUSE is a durable Linux overlay upper/work filesystem\n'
