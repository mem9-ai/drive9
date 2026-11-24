#!/bin/sh
# juicefs/fsrand: seeded random filesystem op stream (mkdir/write/pwrite/append/
# rename/symlink/unlink/rmdir/truncate) verified against a host-local directory
# oracle every 25 ops and across a fresh-cache remount.
set -eu

OPS=${OPS:-1000}
SEED=${SEED:-9}

check fstest --suite fsrand --args "--mode tree -n $OPS -s $SEED --check-every 25" --req 'juicefs-fsrand-model'

drain --mode require-ok --timeout 180s
sample pre_remount

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check manifest-equals --this now --against @pre_remount \
    --ignore '.sim/**' --req 'juicefs-fsrand-remount-parity'
