#!/usr/bin/env bash
set -euo pipefail

MNT="${MNT:?MNT required}"
SIZE_MB="${LARGE_READ_SEED_MB:-256}"
FILE="$MNT/large-read.bin"

if [[ ! -f "$FILE" ]]; then
  dd if=/dev/zero of="$FILE" bs=1048576 count="$SIZE_MB"
  sync
fi

cat "$FILE" > /dev/null
