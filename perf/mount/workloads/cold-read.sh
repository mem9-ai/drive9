#!/usr/bin/env bash
set -euo pipefail

MNT="${MNT:?MNT required}"
RUN_DIR="${RUN_DIR:?RUN_DIR required}"
DRIVE9_BIN="${DRIVE9_BIN:?DRIVE9_BIN required}"
DRIVE9_REMOTE_ROOT="${DRIVE9_REMOTE_ROOT:?DRIVE9_REMOTE_ROOT required}"

SIZE_MB="${COLD_READ_SEED_MB:-256}"
FILE_NAME="${COLD_READ_FILE_NAME:-cold-read.bin}"
LOCAL="$RUN_DIR/$FILE_NAME.seed"
REMOTE_PATH="${DRIVE9_REMOTE_ROOT%/}/$FILE_NAME"

dd if=/dev/zero of="$LOCAL" bs=1048576 count="$SIZE_MB"
"$DRIVE9_BIN" fs cp "$LOCAL" ":$REMOTE_PATH"
cat "$MNT/$FILE_NAME" > /dev/null
