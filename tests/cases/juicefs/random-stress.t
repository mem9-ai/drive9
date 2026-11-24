#!/bin/sh
# juicefs/random-stress: concurrent create/read-back/atomic-rename stress across
# 4 workers x 64 files via `fstest fsrand --mode concurrent`, verified across a
# fresh-cache remount.
set -eu

WORKERS=${WORKERS:-4}
FILES=${FILES:-64}
WANT=$((WORKERS * FILES))

check fstest --suite fsrand --args "--mode concurrent --workers $WORKERS --files $FILES" --req 'juicefs-random-stress'

drain --mode require-ok --timeout 180s
sample pre_remount

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'juicefs-random-stress-count' -- \
    sh -c "test \"\$(find . -name 'stress-*' -type f | wc -l | tr -d ' ')\" = $WANT"
check manifest-equals --this now --against @pre_remount \
    --ignore '.sim/**' --req 'juicefs-random-stress-remount-parity'
