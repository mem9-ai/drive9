#!/bin/sh
# juicefs/random-rw: 1024 random 1-4096B pwrite+fsync ops against a 4MiB file,
# verified byte-for-byte against an in-memory shadow buffer and across a
# fresh-cache remount.
set -eu

check fstest --suite fsrand --args '--mode rw -n 1024 -s 20260930 --file rw.bin --file-size 4194304' --req 'juicefs-random-rw-model'

drain --mode require-ok --timeout 120s
sample pre_remount

cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'juicefs-random-rw-size' -- sh -c 'test "$(d9-stat size rw.bin)" = 4194304'
check manifest-equals --this now --against @pre_remount \
    --ignore '.sim/**' --req 'juicefs-random-rw-remount-parity'
