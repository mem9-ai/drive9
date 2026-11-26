#!/bin/sh
# drive9/fuse/cross-fs-webdav-isolation: a WebDAV mount of tenant B never exposes
# tenant A's paths.
#
# Requires a WebDAV mount helper (mount.davfs on Linux, mount_webdav on macOS).
# Hosts without one report this case as unsupported; the FUSE counterpart lives
# in cross-fs-isolation.
set -eu

TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/cross-fs-webdav-isolation"
MP_B="$RUN_DIR/cross-fs-webdav-b"
MP_WEBDAV="$RUN_DIR/cross-fs-webdav"
b_state="$WORK_ROOT/tenant-b.json"
LIB="$(cd "$(dirname "$0")" && pwd)/cross-fs-lib.sh"
mkdir -p "$WORK_ROOT"
export TS WORK_ROOT MP_B MP_WEBDAV b_state LIB

cleanup() {
    . "$LIB"
    cross_fs_teardown
}
trap cleanup EXIT INT TERM

require --req 'drive9-cross-fs-webdav-helper' --claim 'webdav-mount-helper-available' -- sh -c '
    command -v mount.davfs >/dev/null 2>&1 || command -v mount_webdav >/dev/null 2>&1
'

check shell --req 'drive9-cross-fs-webdav-second-tenant' --claim 'tenant-b-provisioned' -- sh -c '
    set -eu
    . "$LIB"
    cross_fs_provision_b
'

check shell --req 'drive9-cross-fs-webdav-isolation' --claim 'webdav-mount-keeps-tenants-apart' -- sh -c '
    set -eu
    . "$LIB"
    b_key=$(cross_fs_b_key)
    mkdir -p "$MP_WEBDAV"
    "$CLI" mount --server "$SERVER" --api-key "$b_key" --mode=webdav --profile none \
        --cache-dir "$WORK_ROOT/cache-webdav" ":/" "$MP_WEBDAV" > "$WORK_ROOT/mount-webdav.log" 2>&1
    printf "webdav-b-%s\n" "$TS" > "$MP_WEBDAV/webdav-${TS}.txt"
    test "$(cat "$MP_WEBDAV/webdav-${TS}.txt")" = "webdav-b-$TS"
    test ! -e "$MOUNT/webdav-${TS}.txt"

    printf "fuse-a-%s\n" "$TS" > "$MOUNT/fuse-${TS}.txt"
    test ! -e "$MP_WEBDAV/fuse-${TS}.txt"
'
