#!/bin/sh
# drive9/cli/pack-100k: 100000-file pack/unpack round-trip with file count and
# per-file SHA-256 verification (--no-mount, daily benchmark tier).
#
# This is the scale at which customers actually move a workspace between
# sandboxes. The generated tree is packed to a remote archive, unpacked into a
# fresh local root, and the two trees must agree byte for byte. The integrity
# negatives live in drive9/cli/pack-integrity, which shares the fixture and
# round-trip helpers in pack-integrity-lib.sh.
#
# Tunables:
#   PACK_FILES=100000 files per generated tree (override down for a smoke run)
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

PACK_FILES="${PACK_FILES:-100000}"
TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/pack-100k"
SRC_LOCAL_ROOT="$WORK_ROOT/source-local"
RESTORE_LOCAL_ROOT="$WORK_ROOT/restore-local"
REMOTE_ARCHIVE="/pack-100k-${TS}.tar.gz"
MANIFEST_PY="$RUN_DIR/pack_100k_manifest.py"
ARCHIVE_PY="$RUN_DIR/pack_100k_archive.py"
LIB="$(cd "$(dirname "$0")" && pwd)/pack-integrity-lib.sh"
export PACK_FILES WORK_ROOT SRC_LOCAL_ROOT RESTORE_LOCAL_ROOT REMOTE_ARCHIVE
export MANIFEST_PY ARCHIVE_PY TS LIB

cleanup() {
    "$CLI" fs rm -r ":$REMOTE_ARCHIVE" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

check shell --req 'drive9-pack-100k-fixture' --claim 'pack-100k-fixture-built' -- sh -c '
    . "$LIB"
    write_pack_integrity_helpers
    mkdir -p "$WORK_ROOT"
    pack_integrity_build "$SRC_LOCAL_ROOT/overlay" "$PACK_FILES"
    test "$(find "$SRC_LOCAL_ROOT/overlay" -type f | wc -l | tr -d " ")" = "$PACK_FILES"
'

check shell --req 'drive9-pack-100k-roundtrip' --claim 'pack-100k-count-and-sha256-match' -- sh -c '
    . "$LIB"
    pack_integrity_roundtrip "$SRC_LOCAL_ROOT" "$RESTORE_LOCAL_ROOT" ":$REMOTE_ARCHIVE" "$PACK_FILES"
'
