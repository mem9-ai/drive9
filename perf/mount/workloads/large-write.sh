#!/usr/bin/env bash
set -euo pipefail

MNT="${MNT:?MNT required}"
SIZE_MB="${LARGE_WRITE_MB:-256}"
OUT="$MNT/large-write.bin"

rm -f "$OUT"
dd if=/dev/zero of="$OUT" bs=1048576 count="$SIZE_MB"
sync
ls -lh "$OUT"
