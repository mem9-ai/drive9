#!/bin/sh
# drive9/cli/pack-integrity: pack/unpack verifies the archive before installing
# it (--no-mount).
#
# The archive carries a trailing checksum entry (format version 2) with the size
# and SHA-256 of every file it holds. This case proves the checks are real:
#   1. a clean archive round-trips with matching file count and per-file sha256;
#   2. a single flipped byte makes unpack fail with a content mismatch and
#      leaves the destination untouched;
#   3. an archive claiming a newer format version is refused with an actionable
#      message instead of being half-installed;
#   4. an archive that drops a file's checksum is refused, so a truncated
#      writer cannot silently skip verification.
#
# Tunables:
#   PACK_FILES=2000 files per generated tree
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"

PACK_FILES="${PACK_FILES:-2000}"
TS="$(date +%s)-$$"
WORK_ROOT="$RUN_DIR/pack-integrity"
SRC_LOCAL_ROOT="$WORK_ROOT/source-local"
RESTORE_LOCAL_ROOT="$WORK_ROOT/restore-local"
CORRUPT_LOCAL_ROOT="$WORK_ROOT/corrupt-local"
FUTURE_LOCAL_ROOT="$WORK_ROOT/future-local"
NOCSUM_LOCAL_ROOT="$WORK_ROOT/nocsum-local"
REMOTE_ARCHIVE="/pack-integrity-${TS}.tar.gz"
MANIFEST_PY="$RUN_DIR/pack_integrity_manifest.py"
ARCHIVE_PY="$RUN_DIR/pack_integrity_archive.py"
LIB="$(cd "$(dirname "$0")" && pwd)/pack-integrity-lib.sh"
export PACK_FILES WORK_ROOT SRC_LOCAL_ROOT RESTORE_LOCAL_ROOT CORRUPT_LOCAL_ROOT
export FUTURE_LOCAL_ROOT NOCSUM_LOCAL_ROOT REMOTE_ARCHIVE MANIFEST_PY ARCHIVE_PY
export TS LIB

cleanup() {
    "$CLI" fs rm -r ":$REMOTE_ARCHIVE" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

check shell --req 'drive9-pack-integrity-fixture' --claim "pack-integrity-fixture-${PACK_FILES}-files" -- sh -c '
    . "$LIB"
    write_pack_integrity_helpers
    mkdir -p "$WORK_ROOT"
    pack_integrity_build "$SRC_LOCAL_ROOT/overlay" "$PACK_FILES"
    test "$(find "$SRC_LOCAL_ROOT/overlay" -type f | wc -l | tr -d " ")" = "$PACK_FILES"
'

check shell --req 'drive9-pack-integrity-roundtrip' --claim 'pack-unpack-count-and-sha256-match' -- sh -c '
    . "$LIB"
    pack_integrity_roundtrip "$SRC_LOCAL_ROOT" "$RESTORE_LOCAL_ROOT" ":$REMOTE_ARCHIVE" "$PACK_FILES"
'

check shell --req 'drive9-pack-integrity-corrupt' --claim 'corrupt-archive-refused-without-install' -- sh -c '
    . "$LIB"
    python3 "$ARCHIVE_PY" "$WORK_ROOT/archive.tar.gz" "$WORK_ROOT/corrupt.tar.gz" corrupt-entry
    "$CLI" fs cp "$WORK_ROOT/corrupt.tar.gz" ":$REMOTE_ARCHIVE" >/dev/null
    if "$CLI" unpack --local-root "$CORRUPT_LOCAL_ROOT" ":$REMOTE_ARCHIVE" >"$WORK_ROOT/corrupt.out" 2>&1; then
        echo "unpack accepted a corrupted archive" >&2
        exit 1
    fi
    grep -q "pack content mismatch" "$WORK_ROOT/corrupt.out"
    test ! -e "$CORRUPT_LOCAL_ROOT/overlay"
'

check shell --req 'drive9-pack-integrity-future-version' --claim 'future-version-refused' -- sh -c '
    . "$LIB"
    python3 "$ARCHIVE_PY" "$WORK_ROOT/archive.tar.gz" "$WORK_ROOT/future.tar.gz" future-version
    "$CLI" fs cp "$WORK_ROOT/future.tar.gz" ":$REMOTE_ARCHIVE" >/dev/null
    if "$CLI" unpack --local-root "$FUTURE_LOCAL_ROOT" ":$REMOTE_ARCHIVE" >"$WORK_ROOT/future.out" 2>&1; then
        echo "unpack accepted a future format version" >&2
        exit 1
    fi
    grep -q "unsupported pack archive version 99" "$WORK_ROOT/future.out"
'

check shell --req 'drive9-pack-integrity-missing-checksum' --claim 'archive-without-checksums-refused' -- sh -c '
    . "$LIB"
    python3 "$ARCHIVE_PY" "$WORK_ROOT/archive.tar.gz" "$WORK_ROOT/nocsum.tar.gz" drop-checksums
    "$CLI" fs cp "$WORK_ROOT/nocsum.tar.gz" ":$REMOTE_ARCHIVE" >/dev/null
    if "$CLI" unpack --local-root "$NOCSUM_LOCAL_ROOT" ":$REMOTE_ARCHIVE" >"$WORK_ROOT/nocsum.out" 2>&1; then
        echo "unpack accepted a version 2 archive without its checksum entry" >&2
        exit 1
    fi
    grep -q ".drive9-pack-checksums.json" "$WORK_ROOT/nocsum.out"
'
