#!/bin/sh
# community/fsx: xfstests fsx behavioral port — deterministic random
# operation stream with close/open flushes and shadow-buffer verification,
# followed by drain + fresh-cache remount manifest & byte-for-byte parity.
set -eu

check fstest --suite fsx --args '-N 1000 -c 10 --keep-good' --req 'community-fsx'

sample pre_remount
drain --mode require-ok --timeout 120s
check sync-ok --req 'community-fsx-sync'

remount --cache fresh
cd "$MOUNT"

check manifest-equals --this @pre_remount --against now \
    --ignore '.sim/**' --req 'community-fsx-remount-parity'
check fstest --suite fsx --args '--verify-good --cleanup' --req 'community-fsx-verify-good'

drain --mode require-ok --timeout 120s
check sync-ok --req 'community-fsx-cleanup-sync'
