#!/usr/bin/env bash
set -euo pipefail

MNT="${MNT:?MNT required}"
DIRS="${METADATA_DIRS:-100}"
FILES_PER_DIR="${METADATA_FILES_PER_DIR:-10}"
ROOT="$MNT/metadata-walk"

rm -rf "$ROOT"
mkdir -p "$ROOT"

for d in $(seq 1 "$DIRS"); do
  dir="$ROOT/dir-$d"
  mkdir -p "$dir"
  for f in $(seq 1 "$FILES_PER_DIR"); do
    printf 'metadata %d/%d\n' "$d" "$f" > "$dir/file-$f.txt"
  done
done

sync
find "$ROOT" -type f -print > "$RUN_DIR/metadata-find.out"
find "$ROOT" -type f -exec stat {} + > "$RUN_DIR/metadata-stat.out"
