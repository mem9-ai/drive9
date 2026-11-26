#!/bin/sh
# drive9/fuse/cross-fs-isolation: two tenants on one server stay isolated at the
# mount path.
#
# The existing multi-tenant isolation smoke is HTTP-only, but the mount is the
# primary access surface. This case provisions a second tenant, mounts it next
# to the first, and proves that:
#   1. the same path written through both mounts keeps each tenant's own
#      content (no path-level bleed in either direction);
#   2. neither credential can address the other tenant's exclusive path over
#      the data plane (404, never 200).
#
# The scoped-token and WebDAV variants live in cross-fs-scoped-token and
# cross-fs-webdav-isolation so each capability reports its own unsupported
# result instead of hiding the checks that did run.
set -eu

TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/cross-fs-isolation"
MP_B="$RUN_DIR/cross-fs-b"
b_state="$WORK_ROOT/tenant-b.json"
LIB="$(cd "$(dirname "$0")" && pwd)/cross-fs-lib.sh"
mkdir -p "$WORK_ROOT"
export TS WORK_ROOT MP_B b_state LIB

cleanup() {
    . "$LIB"
    cross_fs_teardown
}
trap cleanup EXIT INT TERM

check shell --req 'drive9-cross-fs-second-tenant' --claim 'tenant-b-provisioned-and-mounted' -- sh -c '
    set -eu
    . "$LIB"
    cross_fs_provision_b
'

check shell --req 'drive9-cross-fs-path-isolation' --claim 'same-path-content-stays-per-tenant' -- sh -c '
    set -eu
    . "$LIB"
    printf "from-a-%s\n" "$TS" > "$MOUNT/iso-${TS}.txt"
    printf "from-b-%s\n" "$TS" > "$MP_B/iso-${TS}.txt"
    test "$(cat "$MOUNT/iso-${TS}.txt")" = "from-a-$TS"
    test "$(cat "$MP_B/iso-${TS}.txt")" = "from-b-$TS"

    printf "only-in-b-%s\n" "$TS" > "$MP_B/only-b-${TS}.txt"
    test ! -e "$MOUNT/only-b-${TS}.txt"
    code=$(d9-http --status-only "/v1/fs/only-b-${TS}.txt")
    test "$code" = "404"

    printf "only-in-a-%s\n" "$TS" > "$MOUNT/only-a-${TS}.txt"
    b_key=$(cross_fs_b_key)
    code=$(d9-http --status-only -H "Authorization: Bearer $b_key" "/v1/fs/only-a-${TS}.txt")
    test "$code" = "404"
'
