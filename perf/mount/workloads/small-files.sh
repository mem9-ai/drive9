#!/usr/bin/env bash
set -euo pipefail

MNT="${MNT:?MNT required}"
COUNT="${SMALL_FILES_COUNT:-1000}"
BYTES="${SMALL_FILES_BYTES:-128}"
DIR="$MNT/small-files"

rm -rf "$DIR"
mkdir -p "$DIR"

payload="$(printf 'x%.0s' $(seq 1 "$BYTES"))"
for i in $(seq 1 "$COUNT"); do
  printf '%s-%06d\n' "$payload" "$i" > "$DIR/file-$i.txt"
done

sync
find "$DIR" -type f | wc -l
